// Command detector is the standalone detection service: it consumes market-data
// from NATS, runs the rule-based detectors, appends every alert to the tamper-
// evident audit trail, and republishes alerts + feature vectors.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/argus-mss/argus/internal/app"
	"github.com/argus-mss/argus/internal/audit"
	"github.com/argus-mss/argus/internal/detect"
	"github.com/argus-mss/argus/internal/metrics"
	"github.com/argus-mss/argus/internal/transport"
)

func main() {
	var (
		natsURL     = flag.String("nats", envOr("NATS_URL", ""), "NATS URL")
		auditDir    = flag.String("audit-dir", envOr("AUDIT_DIR", "./data/audit"), "audit trail directory")
		cpEvery     = flag.Int("checkpoint-interval", 32, "entries per signed Merkle checkpoint")
		metricsAddr = flag.String("metrics-addr", ":2113", "metrics listen address")
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

	signer, err := audit.LoadOrCreateSigner(filepath.Join(*auditDir, "key.hex"))
	if err != nil {
		log.Error("audit signer", "err", err)
		os.Exit(1)
	}
	writeFile(filepath.Join(*auditDir, "pubkey.hex"), signer.PublicKeyHex(), log)
	chain, err := audit.Open(*auditDir, signer, *cpEvery)
	if err != nil {
		log.Error("audit chain", "err", err)
		os.Exit(1)
	}
	defer chain.Close()

	m := metrics.New()
	go serveMetrics(*metricsAddr, m, log)

	log.Info("detector starting", "nats", *natsURL, "audit_dir", *auditDir, "signer", signer.PublicKeyHex())
	if err := app.RunDetection(ctx, bus, detect.DefaultConfig(), chain, m); err != nil && ctx.Err() == nil {
		log.Error("detection stopped", "err", err)
		os.Exit(1)
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

func writeFile(path, content string, log *slog.Logger) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		log.Warn("mkdir", "err", err)
		return
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		log.Warn("write file", "path", path, "err", err)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
