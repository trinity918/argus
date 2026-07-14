// Command ingestor is the standalone ingestion service: it connects to one
// venue (Binance or OKX), normalizes the feed, and publishes market-data
// envelopes to NATS. Multi-venue surveillance runs one ingestor per venue —
// each scales, restarts, and backpressures independently, which is the whole
// point of the distributed topology.
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
	"github.com/argus-mss/argus/internal/exchange/okx"
	"github.com/argus-mss/argus/internal/metrics"
	"github.com/argus-mss/argus/internal/transport"
	"github.com/prometheus/client_golang/prometheus"
)

func main() {
	var (
		exchange    = flag.String("exchange", envOr("EXCHANGE", "binance"), "venue to ingest: binance | okx")
		symbols     = flag.String("symbols", "", "comma-separated symbols (default BTCUSDT for binance, BTC-USDT for okx)")
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

	pub := app.EnvelopePublisher{Bus: bus}
	m := metrics.New()

	var run func(context.Context) error
	var syms []string
	switch *exchange {
	case "binance":
		syms = splitSymbols(*symbols, "BTCUSDT")
		client := binance.New(binance.Config{Symbols: syms, Logger: log}, pub)
		registerStats(m.Registry(), func() ingestStats {
			s := client.Stats()
			return ingestStats{s.MessagesReceived, s.MessagesDropped, s.Reconnects, s.Resyncs, s.TradesEmitted, s.DepthEmitted}
		})
		run = client.Run
	case "okx":
		syms = splitSymbols(*symbols, "BTC-USDT")
		client := okx.New(okx.Config{Symbols: syms, Logger: log}, pub)
		registerStats(m.Registry(), func() ingestStats {
			s := client.Stats()
			return ingestStats{s.MessagesReceived, s.MessagesDropped, s.Reconnects, s.Resyncs, s.TradesEmitted, s.DepthEmitted}
		})
		run = client.Run
	default:
		log.Error("unknown exchange", "exchange", *exchange)
		os.Exit(2)
	}

	go serveMetrics(*metricsAddr, m, log)

	log.Info("ingestor starting", "exchange", *exchange, "symbols", syms, "nats", *natsURL)
	if err := run(ctx); err != nil && ctx.Err() == nil {
		log.Error("ingestion stopped", "err", err)
		os.Exit(1)
	}
}

// ingestStats is the venue-agnostic slice of counters exported as metrics.
type ingestStats struct {
	received, dropped, reconnects, resyncs, trades, depth int64
}

func registerStats(reg *prometheus.Registry, get func() ingestStats) {
	counters := []struct {
		name, help string
		pick       func(ingestStats) int64
	}{
		{"argus_ingest_messages_received_total", "WebSocket frames received.", func(s ingestStats) int64 { return s.received }},
		{"argus_ingest_messages_dropped_total", "Frames shed under backpressure.", func(s ingestStats) int64 { return s.dropped }},
		{"argus_ingest_reconnects_total", "WebSocket reconnects.", func(s ingestStats) int64 { return s.reconnects }},
		{"argus_ingest_resyncs_total", "Order-book resyncs after sequence gaps.", func(s ingestStats) int64 { return s.resyncs }},
		{"argus_ingest_trades_total", "Trade events published.", func(s ingestStats) int64 { return s.trades }},
		{"argus_ingest_depth_total", "Depth diffs published.", func(s ingestStats) int64 { return s.depth }},
	}
	for _, c := range counters {
		pick := c.pick
		reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Name: c.name, Help: c.help},
			func() float64 { return float64(pick(get())) }))
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

// splitSymbols parses a comma list, upper-casing entries. OKX instIds keep
// their dash (BTC-USDT); upper-casing is a no-op for well-formed input.
func splitSymbols(s, def string) []string {
	if strings.TrimSpace(s) == "" {
		return []string{def}
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, strings.ToUpper(p))
		}
	}
	if len(out) == 0 {
		out = []string{def}
	}
	return out
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
