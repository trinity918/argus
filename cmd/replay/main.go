// Command replay publishes the synthetic manipulation scenario to NATS, so the
// distributed stack can be demonstrated end-to-end without a live market feed.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/argus-mss/argus/internal/app"
	"github.com/argus-mss/argus/internal/scenario"
	"github.com/argus-mss/argus/internal/transport"
)

func main() {
	var (
		natsURL = flag.String("nats", envOr("NATS_URL", ""), "NATS URL")
		symbol  = flag.String("symbol", "BTCUSDT", "symbol to replay")
		speed   = flag.Float64("speed", 1.0, "playback speed multiplier")
		loop    = flag.Bool("loop", true, "loop the scenario continuously")
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
	steps := scenario.Manipulations(*symbol)
	log.Info("replaying scenario", "symbol", *symbol, "steps", len(steps), "speed", *speed, "loop", *loop)

	for ctx.Err() == nil {
		if err := scenario.Play(ctx, pub, steps, *speed); err != nil {
			break
		}
		if !*loop {
			break
		}
		select {
		case <-time.After(3 * time.Second):
		case <-ctx.Done():
		}
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
