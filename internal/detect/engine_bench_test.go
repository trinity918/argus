package detect_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/argus-mss/argus/internal/detect"
	"github.com/argus-mss/argus/internal/events"
	fp "github.com/argus-mss/argus/internal/fixedpoint"
	"github.com/argus-mss/argus/internal/scenario"
)

// BenchmarkEngineDepthUpdates measures raw single-core engine throughput on a
// stream of realistic small depth diffs against a warm book — the dominant
// event type on a live feed. ns/op is per event; events/sec/core ≈ 1e9/ns_op.
func BenchmarkEngineDepthUpdates(b *testing.B) {
	eng := detect.NewEngine(detect.DefaultConfig(), func(detect.Alert) {})

	// Warm book: 50 levels a side around 30,000.
	bids := make([]events.Level, 50)
	asks := make([]events.Level, 50)
	for i := 0; i < 50; i++ {
		bids[i] = events.Level{Price: fp.MustParse(fmt.Sprintf("%d.%02d", 29999, 99-i)), Qty: fp.MustParse("1")}
		asks[i] = events.Level{Price: fp.MustParse(fmt.Sprintf("%d.%02d", 30000, i+1)), Qty: fp.MustParse("1")}
	}
	snap := events.Depth{Symbol: "BTCUSDT", Bids: bids, Asks: asks, IsSnapshot: true, IngestTsNs: time.Now().UnixNano()}
	eng.HandleEnvelope(events.Envelope{Kind: events.KindDepth, Depth: &snap})

	// Pre-build a rotating set of two-level diffs to avoid measuring setup.
	diffs := make([]events.Envelope, 64)
	for i := range diffs {
		d := events.Depth{
			Symbol: "BTCUSDT",
			Bids: []events.Level{{
				Price: bids[i%50].Price,
				Qty:   fp.MustParse(fmt.Sprintf("%d.5", 1+i%3)),
			}},
			Asks: []events.Level{{
				Price: asks[(i+7)%50].Price,
				Qty:   fp.MustParse(fmt.Sprintf("%d.25", 1+i%2)),
			}},
			IngestTsNs: time.Now().UnixNano(),
		}
		diffs[i] = events.Envelope{Kind: events.KindDepth, Depth: &d}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eng.HandleEnvelope(diffs[i%len(diffs)])
	}
}

// BenchmarkEngineScenario measures throughput over the full mixed manipulation
// tape (snapshots, diffs, trades, alerts firing, features extracted) — the
// worst-realistic-case per-event cost.
func BenchmarkEngineScenario(b *testing.B) {
	steps := scenario.Manipulations("BTCUSDT")
	envs := make([]events.Envelope, len(steps))
	for i, s := range steps {
		envs[i] = s.Env
	}
	eng := detect.NewEngine(detect.DefaultConfig(), func(detect.Alert) {})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eng.HandleEnvelope(envs[i%len(envs)])
	}
}
