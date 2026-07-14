// Command argusd runs the entire Argus stack in one process over the in-process
// bus: ingestion (live Binance or the synthetic scenario), detection, the
// tamper-evident audit log, and the dashboard/API. It needs no NATS and no
// Docker — `go run ./cmd/argusd` gives a working surveillance system.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/argus-mss/argus/internal/api"
	"github.com/argus-mss/argus/internal/app"
	"github.com/argus-mss/argus/internal/audit"
	"github.com/argus-mss/argus/internal/detect"
	"github.com/argus-mss/argus/internal/exchange/binance"
	"github.com/argus-mss/argus/internal/exchange/okx"
	"github.com/argus-mss/argus/internal/metrics"
	"github.com/argus-mss/argus/internal/scenario"
	"github.com/argus-mss/argus/internal/transport"
)

func main() {
	var (
		addr     = flag.String("addr", ":8080", "dashboard/API listen address")
		symbols  = flag.String("symbols", "BTCUSDT", "comma-separated symbols")
		auditDir = flag.String("audit-dir", "./data/audit", "audit trail directory")
		cpEvery  = flag.Int("checkpoint-interval", 32, "entries per signed Merkle checkpoint")
		live     = flag.Bool("live", false, "ingest live exchange data instead of the synthetic scenario")
		exchange = flag.String("exchange", "binance", "live venue: binance | okx (okx symbols look like BTC-USDT)")
		speed    = flag.Float64("speed", 1.0, "scenario playback speed multiplier")
	)
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	syms := splitSymbols(*symbols)
	m := metrics.New()
	bus := transport.NewInProc()
	defer bus.Close()

	// Audit chain + signer.
	signer, err := audit.LoadOrCreateSigner(filepath.Join(*auditDir, "key.hex"))
	if err != nil {
		log.Error("audit signer", "err", err)
		os.Exit(1)
	}
	writePubKey(*auditDir, signer.PublicKeyHex(), log)
	chain, err := audit.Open(*auditDir, signer, *cpEvery)
	if err != nil {
		log.Error("audit chain", "err", err)
		os.Exit(1)
	}
	defer chain.Close()

	// API/dashboard.
	srv := api.New(api.Config{
		AuditLogPath:        filepath.Join(*auditDir, "audit.log"),
		AuditCheckpointPath: filepath.Join(*auditDir, "checkpoints.log"),
		AuditPubKeyHex:      signer.PublicKeyHex(),
		MetricsHandler:      m.Handler(),
	})
	if err := srv.AttachBus(bus); err != nil {
		log.Error("attach bus", "err", err)
		os.Exit(1)
	}

	// Detection pipeline.
	go func() {
		if err := app.RunDetection(ctx, bus, detect.DefaultConfig(), chain, m); err != nil && ctx.Err() == nil {
			log.Error("detection", "err", err)
		}
	}()

	// Ingestion: live or scenario.
	go func() {
		if *live {
			log.Info("ingesting live data", "exchange", *exchange, "symbols", syms)
			pub := app.EnvelopePublisher{Bus: bus}
			var run func(context.Context) error
			switch *exchange {
			case "okx":
				run = okx.New(okx.Config{Symbols: syms, Logger: log}, pub).Run
			default:
				run = binance.New(binance.Config{Symbols: syms, Logger: log}, pub).Run
			}
			if err := run(ctx); err != nil && ctx.Err() == nil {
				log.Error("ingestion", "err", err)
			}
			return
		}
		log.Info("replaying synthetic manipulation scenario", "symbol", syms[0], "speed", *speed)
		pub := app.EnvelopePublisher{Bus: bus}
		for ctx.Err() == nil {
			if err := scenario.Play(ctx, pub, scenario.Manipulations(syms[0]), *speed); err != nil {
				return
			}
			select {
			case <-time.After(3 * time.Second):
			case <-ctx.Done():
				return
			}
		}
	}()

	httpSrv := &http.Server{Addr: *addr, Handler: srv.Handler()}
	go func() {
		log.Info("dashboard listening", "addr", *addr, "url", "http://localhost"+*addr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("http server", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutCtx)
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

func writePubKey(dir, hexKey string, log *slog.Logger) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Warn("mkdir audit dir", "err", err)
		return
	}
	if err := os.WriteFile(filepath.Join(dir, "pubkey.hex"), []byte(hexKey), 0o644); err != nil {
		log.Warn("write pubkey", "err", err)
	}
}
