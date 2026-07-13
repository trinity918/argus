// Command ingestor is the standalone ingestion service: it connects to Binance,
// normalizes the feed, and publishes market-data envelopes to NATS. In the
// distributed topology it scales independently of detection.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/argus-mss/argus/internal/app"
	"github.com/argus-mss/argus/internal/exchange/binance"
	"github.com/argus-mss/argus/internal/metrics"
	"github.com/argus-mss/argus/internal/transport"
	"github.com/prometheus/client_golang/prometheus"
)

func main() {
	var (
		symbols     = flag.String("symbols", "BTCUSDT", "comma-separated symbols")
		natsURL     = flag.String("nats", envOr("NATS_URL", ""), "NATS URL (default nats://127.0.0.1:4222)")
		metricsAddr = flag.String("metrics-addr", ":2112", "metrics listen address")
	)
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	bus, err := transport.ConnectNATS(*natsURL)
	if err != nil {
		log.Error("nats connect", "err", err)
		os.Exit(1)
	}
	defer bus.Close()

	syms := splitSymbols(*symbols)
	client := binance.New(binance.Config{Symbols: syms, Logger: log}, app.EnvelopePublisher{Bus: bus})

	m := metrics.New()
	registerIngestStats(m.Registry(), client)
	go serveMetrics(*metricsAddr, m, log)

	log.Info("ingestor starting", "symbols", syms, "nats", *natsURL)
	if err := client.Run(ctx); err != nil && ctx.Err() == nil {
		log.Error("ingestion stopped", "err", err)
		os.Exit(1)
	}
}

func registerIngestStats(reg *prometheus.Registry, c *binance.Client) {
	counters := []struct {
		name, help string
		get        func(binance.Stats) int64
	}{
		{"argus_ingest_messages_received_total", "WebSocket frames received.", func(s binance.Stats) int64 { return s.MessagesReceived }},
		{"argus_ingest_messages_dropped_total", "Frames shed under backpressure.", func(s binance.Stats) int64 { return s.MessagesDropped }},
		{"argus_ingest_reconnects_total", "WebSocket reconnects.", func(s binance.Stats) int64 { return s.Reconnects }},
		{"argus_ingest_resyncs_total", "Order-book resyncs after sequence gaps.", func(s binance.Stats) int64 { return s.Resyncs }},
		{"argus_ingest_trades_total", "Trade events published.", func(s binance.Stats) int64 { return s.TradesEmitted }},
		{"argus_ingest_depth_total", "Depth diffs published.", func(s binance.Stats) int64 { return s.DepthEmitted }},
		{"argus_ingest_snapshot_fetches_total", "REST snapshot fetches.", func(s binance.Stats) int64 { return s.SnapshotFetches }},
		{"argus_ingest_snapshot_errors_total", "Failed snapshot fetches.", func(s binance.Stats) int64 { return s.SnapshotErrors }},
	}
	for _, c2 := range counters {
		get := c2.get
		reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Name: c2.name, Help: c2.help},
			func() float64 { return float64(get(c.Stats())) }))
	}
}

func serveMetrics(addr string, m *metrics.Metrics, log *slog.Logger) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	if err := http.ListenAndServe(addr, mux); err != nil && err != http.ErrServerClosed {
		log.Error("metrics server", "err", err)
	}
}

func splitSymbols(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, strings.ToUpper(p))
		}
	}
	if len(out) == 0 {
		out = []string{"BTCUSDT"}
	}
	return out
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
