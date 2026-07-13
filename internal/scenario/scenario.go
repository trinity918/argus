// Package scenario generates a scripted market-data tape that deliberately
// contains each manipulation pattern, so the system can be demonstrated and
// tested deterministically without a live feed. The same tape drives cmd/replay
// (real-time, over the bus) and the end-to-end integration test (virtual clock,
// straight into the engine).
package scenario

import (
	"context"
	"fmt"
	"time"

	"github.com/argus-mss/argus/internal/events"
	fp "github.com/argus-mss/argus/internal/fixedpoint"
)

// Step is one tape event and the delay to wait before emitting it.
type Step struct {
	DelayNs int64
	Env     events.Envelope
}

// Publisher is the sink Play emits to (satisfied by app.EnvelopePublisher).
type Publisher interface {
	Publish(env events.Envelope) error
}

type builder struct {
	symbol string
	nowNs  int64
	last   int64
	steps  []Step
}

func (b *builder) at(delay time.Duration) {
	b.nowNs += int64(delay)
}

func (b *builder) push(env events.Envelope) {
	b.steps = append(b.steps, Step{DelayNs: b.nowNs - b.last, Env: env})
	b.last = b.nowNs
}

func (b *builder) depth(snapshot bool, bids, asks []events.Level) {
	d := &events.Depth{
		Symbol:     b.symbol,
		Bids:       bids,
		Asks:       asks,
		IsSnapshot: snapshot,
		IngestTsNs: b.nowNs,
		Exchange:   "scenario",
	}
	b.push(events.Envelope{Kind: events.KindDepth, Depth: d})
}

func (b *builder) trade(price, qty string, side events.Side) {
	t := &events.Trade{
		Symbol:     b.symbol,
		Price:      fp.MustParse(price),
		Qty:        fp.MustParse(qty),
		Aggressor:  side,
		IngestTsNs: b.nowNs,
		Exchange:   "scenario",
	}
	b.push(events.Envelope{Kind: events.KindTrade, Trade: t})
}

func lv(p, q string) events.Level {
	return events.Level{Price: fp.MustParse(p), Qty: fp.MustParse(q)}
}

// Manipulations returns a tape for a BTC-priced symbol that triggers, in order:
// spoofing, layering, momentum ignition, quote stuffing, and a wash-trade
// footprint. Prices sit around 30,000 so basis-point moves look realistic.
func Manipulations(symbol string) []Step {
	b := &builder{symbol: symbol, nowNs: int64(1_000_000_000_000)}
	b.last = b.nowNs

	// 1. Snapshot to sync the book (mid = 30000.01).
	b.depth(true,
		[]events.Level{lv("30000.00", "1"), lv("29999.99", "1"), lv("29999.98", "1"), lv("29999.97", "1"), lv("29999.96", "1")},
		[]events.Level{lv("30000.02", "1"), lv("30000.03", "1"), lv("30000.04", "1"), lv("30000.05", "1"), lv("30000.06", "1")},
	)

	// 2. Warm the liquidity baseline with normal-sized adds far from touch.
	cents := 2999950
	for i := 0; i < 20; i++ {
		b.at(5 * time.Millisecond)
		b.depth(false, []events.Level{lv(fmt.Sprintf("%d.%02d", cents/100, cents%100), "1")}, nil)
		cents--
	}

	// 3. Spoofing: a large bid appears at touch, then is pulled unexecuted.
	b.at(800 * time.Millisecond)
	b.depth(false, []events.Level{lv("30000.00", "30")}, nil)
	b.at(1000 * time.Millisecond)
	b.depth(false, []events.Level{lv("30000.00", "1")}, nil)

	// 4. Layering: three large bids stacked, then pulled in concert.
	b.at(600 * time.Millisecond)
	for _, p := range []string{"30000.00", "29999.99", "29999.98"} {
		b.depth(false, []events.Level{lv(p, "30")}, nil)
	}
	b.at(700 * time.Millisecond)
	for i, p := range []string{"30000.00", "29999.99", "29999.98"} {
		if i > 0 {
			b.at(15 * time.Millisecond)
		}
		b.depth(false, []events.Level{lv(p, "1")}, nil)
	}

	// 5. Momentum ignition: aggressive buys drive +15bps (+45.0), then reverse.
	b.at(700 * time.Millisecond)
	for i, p := range []string{"30000.00", "30015.00", "30030.00", "30046.00"} {
		if i > 0 {
			b.at(120 * time.Millisecond)
		}
		b.trade(p, "2", events.Buy)
	}
	b.at(300 * time.Millisecond)
	b.trade("30022.00", "3", events.Sell) // retrace past 50%

	// 6. Quote stuffing: 250 book updates within a second, no executions.
	// A long quiet gap ensures the preceding trades have aged out of the
	// stuffing/wash windows even at elevated playback speeds.
	b.at(3500 * time.Millisecond)
	stuff := 2900000
	for d := 0; d < 5; d++ {
		if d > 0 {
			b.at(50 * time.Millisecond)
		}
		levels := make([]events.Level, 0, 50)
		for i := 0; i < 50; i++ {
			levels = append(levels, lv(fmt.Sprintf("%d.%02d", stuff/100, stuff%100), "1"))
			stuff++
		}
		b.depth(false, levels, nil)
	}

	// 7. Wash-trade footprint: offsetting prints in a razor-thin band. The gap
	// clears the wide-band momentum prints from the wash detector's window.
	b.at(6000 * time.Millisecond)
	sides := []events.Side{events.Buy, events.Sell}
	for i := 0; i < 8; i++ {
		if i > 0 {
			b.at(100 * time.Millisecond)
		}
		price := "30000.00"
		if i%2 == 1 {
			price = "30000.01"
		}
		b.trade(price, "0.5", sides[i%2])
	}

	return b.steps
}

// Play emits steps to pub in real time (scaled by speed; speed>1 is faster).
// It stamps each event's ingest time at emission so latency metrics reflect the
// live pipeline rather than the synthetic tape clock.
func Play(ctx context.Context, pub Publisher, steps []Step, speed float64) error {
	if speed <= 0 {
		speed = 1
	}
	for _, s := range steps {
		if d := time.Duration(float64(s.DelayNs) / speed); d > 0 {
			select {
			case <-time.After(d):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		env := s.Env
		stampNow(env, time.Now().UnixNano())
		if err := pub.Publish(env); err != nil {
			return err
		}
	}
	return nil
}

func stampNow(env events.Envelope, ns int64) {
	if env.Trade != nil {
		env.Trade.IngestTsNs = ns
	}
	if env.Depth != nil {
		env.Depth.IngestTsNs = ns
	}
}
