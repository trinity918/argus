// Package app holds the wiring that turns the library packages into running
// services. Keeping it here means the single-binary demo (cmd/argusd, in-proc
// bus) and the distributed services (cmd/ingestor + cmd/detector over NATS)
// share one detection pipeline with zero duplication — only the Bus differs.
package app

import (
	"context"
	"encoding/json"
	"time"

	"github.com/argus-mss/argus/internal/audit"
	"github.com/argus-mss/argus/internal/detect"
	"github.com/argus-mss/argus/internal/events"
	"github.com/argus-mss/argus/internal/features"
	"github.com/argus-mss/argus/internal/metrics"
	"github.com/argus-mss/argus/internal/transport"
)

// EnvelopePublisher adapts a transport.Bus to the binance.Publisher interface,
// routing each normalized envelope to its per-symbol market-data subject.
type EnvelopePublisher struct{ Bus transport.Bus }

// Publish marshals and publishes an envelope to md.<symbol>.
func (p EnvelopePublisher) Publish(env events.Envelope) error {
	sym := envSymbol(env)
	if sym == "" {
		return nil
	}
	b, err := json.Marshal(env)
	if err != nil {
		return err
	}
	return p.Bus.Publish(transport.MarketData(sym), b)
}

func envSymbol(env events.Envelope) string {
	switch {
	case env.Trade != nil:
		return env.Trade.Symbol
	case env.Depth != nil:
		return env.Depth.Symbol
	}
	return ""
}

// SubscribeMarketData decodes market-data envelopes off the bus into fn.
func SubscribeMarketData(bus transport.Bus, fn func(events.Envelope)) (transport.Subscription, error) {
	return SubscribeMarketDataFiltered(bus, transport.MarketDataAll, fn)
}

// SubscribeMarketDataFiltered subscribes to a subject pattern instead of all
// symbols — the sharding seam: running N detectors each with a disjoint
// pattern (md.BTCUSDT vs md.ETH*) partitions the symbol space horizontally
// with no coordination, because detector state is strictly per-symbol.
func SubscribeMarketDataFiltered(bus transport.Bus, pattern string, fn func(events.Envelope)) (transport.Subscription, error) {
	return bus.Subscribe(pattern, func(_ string, data []byte) {
		var env events.Envelope
		if err := json.Unmarshal(data, &env); err != nil {
			return
		}
		fn(env)
	})
}

// DetectionOption customizes RunDetection.
type DetectionOption func(*detectionOptions)

type detectionOptions struct {
	pattern string
}

// WithSubjectFilter restricts the detector to a market-data subject pattern
// (NATS wildcards allowed), e.g. "md.BTCUSDT" or "md.*". Default: all symbols.
func WithSubjectFilter(pattern string) DetectionOption {
	return func(o *detectionOptions) { o.pattern = pattern }
}

// RunDetection wires the detection engine to a bus: it subscribes to market
// data, appends every alert to the audit chain, and republishes alerts and
// feature vectors. It blocks until ctx is cancelled. chain and m may be nil.
func RunDetection(ctx context.Context, bus transport.Bus, cfg detect.Config, chain *audit.Chain, m *metrics.Metrics, opts ...DetectionOption) error {
	o := detectionOptions{pattern: transport.MarketDataAll}
	for _, opt := range opts {
		opt(&o)
	}
	emitAlert := func(a detect.Alert) {
		b, err := json.Marshal(a)
		if err != nil {
			return
		}
		// Append to the tamper-evident log *before* publishing, so nothing is
		// surfaced that isn't already committed to the audit trail.
		if chain != nil {
			if _, err := chain.Append(b); err == nil && m != nil {
				m.AuditEntries.Inc()
			}
		}
		_ = bus.Publish(transport.AlertRule, b)
		if m != nil {
			m.Alerts.WithLabelValues(a.Detector, a.SeverityLabel).Inc()
			m.DetectionLatencyUs.Observe(float64(a.DetectLatencyUs))
		}
	}
	emitFeatures := func(v features.Vector) {
		b, err := json.Marshal(v)
		if err != nil {
			return
		}
		_ = bus.Publish(transport.Features(v.Symbol), b)
		if m != nil {
			m.Features.Inc()
		}
	}

	if chain != nil {
		chain.OnCheckpoint = func(cp audit.Checkpoint) {
			if m != nil {
				m.AuditCheckpoints.Inc()
			}
			if b, err := json.Marshal(cp); err == nil {
				_ = bus.Publish(transport.AuditCheckpoint, b)
			}
		}
	}

	eng := detect.NewEngine(cfg, emitAlert, detect.WithFeatures(emitFeatures))
	sub, err := SubscribeMarketDataFiltered(bus, o.pattern, func(env events.Envelope) {
		start := time.Now()
		eng.HandleEnvelope(env)
		if m != nil {
			m.EventsProcessed.Inc()
			m.ProcessingLatencyUs.Observe(float64(time.Since(start).Microseconds()))
		}
	})
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	<-ctx.Done()
	return ctx.Err()
}
