// Command api is the standalone dashboard/API service: it subscribes to alerts
// and checkpoints on NATS and serves the dashboard, live WebSocket feed, stats,
// and on-demand audit verification (reading the shared audit volume).
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
	"github.com/argus-mss/argus/internal/metrics"
	"github.com/argus-mss/argus/internal/transport"
)

func main() {
	var (
		addr     = flag.String("addr", ":8080", "dashboard/API listen address")
		natsURL  = flag.String("nats", envOr("NATS_URL", ""), "NATS URL")
		auditDir = flag.String("audit-dir", envOr("AUDIT_DIR", "./data/audit"), "audit trail directory (shared with detector)")
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

	pubHex := ""
	if b, err := os.ReadFile(filepath.Join(*auditDir, "pubkey.hex")); err == nil {
		pubHex = strings.TrimSpace(string(b))
	}

	m := metrics.New()
	srv := api.New(api.Config{
		AuditLogPath:        filepath.Join(*auditDir, "audit.log"),
		AuditCheckpointPath: filepath.Join(*auditDir, "checkpoints.log"),
		AuditPubKeyHex:      pubHex,
		MetricsHandler:      m.Handler(),
	})
	if err := srv.AttachBus(bus); err != nil {
		log.Error("attach bus", "err", err)
		os.Exit(1)
	}

	httpSrv := &http.Server{Addr: *addr, Handler: srv.Handler()}
	go func() {
		log.Info("dashboard listening", "addr", *addr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("http server", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutCtx)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
