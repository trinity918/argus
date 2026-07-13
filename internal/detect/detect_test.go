package detect

import (
	"fmt"
	"testing"
	"time"

	"github.com/argus-mss/argus/internal/events"
	"github.com/argus-mss/argus/internal/features"
	fp "github.com/argus-mss/argus/internal/fixedpoint"
	"github.com/argus-mss/argus/internal/orderbook"
)

// --- helpers ---

func lv(p, q string) events.Level {
	return events.Level{Price: fp.MustParse(p), Qty: fp.MustParse(q)}
}

func mkDepth(symbol string, snapshot bool, bids, asks []events.Level, ingestNs int64) events.Depth {
	return events.Depth{
		Symbol:     symbol,
		Bids:       bids,
		Asks:       asks,
		IsSnapshot: snapshot,
		IngestTsNs: ingestNs,
	}
}

func mkTrade(symbol, price, qty string, side events.Side, ingestNs int64) events.Trade {
	return events.Trade{
		Symbol:     symbol,
		Price:      fp.MustParse(price),
		Qty:        fp.MustParse(qty),
		Aggressor:  side,
		IngestTsNs: ingestNs,
	}
}

// feedDepth mirrors what the engine does: apply to the book, then run OnDepth.
func feedDepth(det Detector, st *SymbolState, d events.Depth, now int64) []Alert {
	deltas := st.Book.Apply(d)
	return det.OnDepth(now, st, d, deltas)
}

func hasDetector(alerts []Alert, name string) bool {
	for _, a := range alerts {
		if a.Detector == name {
			return true
		}
	}
	return false
}

// --- window ---

func TestWindowEviction(t *testing.T) {
	w := NewWindow[int](time.Second)
	w.Add(0, 1)
	w.Add(int64(500*time.Millisecond), 2)
	w.Add(int64(1200*time.Millisecond), 3) // evicts ts=0 (older than 200ms cutoff)
	if w.Len() != 2 {
		t.Fatalf("len = %d, want 2", w.Len())
	}
	w.Add(int64(3*time.Second), 4) // evicts everything but itself
	if w.Len() != 1 {
		t.Fatalf("len = %d, want 1", w.Len())
	}
}

// --- spoofing / layering ---

// warmSpoof builds a synced book (mid ~100.01) and warms the size EMA so the
// detector's baseline "typical size" is ~1.
func warmSpoof(t *testing.T) (Detector, *SymbolState, int64) {
	t.Helper()
	cfg := DefaultConfig()
	st := &SymbolState{Symbol: "BTCUSDT", Book: orderbook.New("BTCUSDT", "")}
	det := newSpoofingDetector("BTCUSDT", cfg)
	base := int64(1_000_000_000_000)
	st.Book.Apply(mkDepth("BTCUSDT", true,
		[]events.Level{lv("100.00", "1"), lv("99.99", "1"), lv("99.98", "1")},
		[]events.Level{lv("100.02", "1"), lv("100.03", "1"), lv("100.04", "1")}, 0))
	price := 9950
	for i := 0; i < 20; i++ {
		p := fmt.Sprintf("%.2f", float64(price)/100.0)
		feedDepth(det, st, mkDepth("BTCUSDT", false, []events.Level{lv(p, "1")}, nil, base), base)
		price--
	}
	return det, st, base
}

func TestSpoofingFires(t *testing.T) {
	det, st, base := warmSpoof(t)
	t1 := base + int64(time.Second)
	if a := feedDepth(det, st, mkDepth("BTCUSDT", false, []events.Level{lv("100.00", "30")}, nil, t1), t1); len(a) != 0 {
		t.Fatalf("large add itself should not alert, got %v", a)
	}
	t2 := base + int64(2*time.Second)
	alerts := feedDepth(det, st, mkDepth("BTCUSDT", false, []events.Level{lv("100.00", "1")}, nil, t2), t2)
	if !hasDetector(alerts, DetectorSpoofing) {
		t.Fatalf("expected spoofing alert, got %+v", alerts)
	}
	for _, a := range alerts {
		if a.Detector == DetectorSpoofing && a.Severity != SeverityHigh {
			t.Errorf("spoofing severity = %v, want high", a.Severity)
		}
	}
}

func TestSpoofingNoFireWhenExecuted(t *testing.T) {
	det, st, base := warmSpoof(t)
	t1 := base + int64(time.Second)
	feedDepth(det, st, mkDepth("BTCUSDT", false, []events.Level{lv("100.00", "30")}, nil, t1), t1)
	// The order largely executes: attribute 10 traded at 100.00 (> 20% of 30).
	det.OnTrade(t1+1, st, mkTrade("BTCUSDT", "100.00", "10", events.Sell, t1))
	t2 := base + int64(2*time.Second)
	alerts := feedDepth(det, st, mkDepth("BTCUSDT", false, []events.Level{lv("100.00", "1")}, nil, t2), t2)
	if hasDetector(alerts, DetectorSpoofing) {
		t.Fatalf("executed order must not be flagged as spoof, got %+v", alerts)
	}
}

func TestSpoofingNoFireWhenPulledLate(t *testing.T) {
	det, st, base := warmSpoof(t)
	t1 := base + int64(time.Second)
	feedDepth(det, st, mkDepth("BTCUSDT", false, []events.Level{lv("100.00", "30")}, nil, t1), t1)
	tLate := t1 + int64(5*time.Second) // beyond 3s pull window
	alerts := feedDepth(det, st, mkDepth("BTCUSDT", false, []events.Level{lv("100.00", "1")}, nil, tLate), tLate)
	if hasDetector(alerts, DetectorSpoofing) {
		t.Fatalf("late pull must not be flagged, got %+v", alerts)
	}
}

func TestLayeringFires(t *testing.T) {
	det, st, base := warmSpoof(t)
	t1 := base + int64(time.Second)
	// Stack large orders on the bid at three levels.
	for _, p := range []string{"100.00", "99.99", "99.98"} {
		feedDepth(det, st, mkDepth("BTCUSDT", false, []events.Level{lv(p, "30")}, nil, t1), t1)
	}
	// Pull them all in concert.
	var all []Alert
	t2 := base + int64(2*time.Second)
	for i, p := range []string{"100.00", "99.99", "99.98"} {
		now := t2 + int64(i)*int64(10*time.Millisecond)
		all = append(all, feedDepth(det, st, mkDepth("BTCUSDT", false, []events.Level{lv(p, "1")}, nil, now), now)...)
	}
	if !hasDetector(all, DetectorLayering) {
		t.Fatalf("expected a layering alert, got %+v", all)
	}
}

// --- momentum ignition ---

func TestMomentumIgnitionFires(t *testing.T) {
	cfg := DefaultConfig()
	st := &SymbolState{Symbol: "BTCUSDT", Book: orderbook.New("BTCUSDT", "")}
	det := newMomentumDetector("BTCUSDT", cfg)
	base := int64(1_000_000_000_000)

	// Ignition leg: aggressive buys drive price +16bps within the window.
	leg := []struct {
		price string
		dt    time.Duration
	}{
		{"100.00", 0},
		{"100.05", 100 * time.Millisecond},
		{"100.10", 200 * time.Millisecond},
		{"100.16", 300 * time.Millisecond},
	}
	var armed bool
	for _, s := range leg {
		now := base + int64(s.dt)
		a := det.OnTrade(now, st, mkTrade("BTCUSDT", s.price, "2", events.Buy, now))
		if len(a) != 0 {
			t.Fatalf("no alert expected during leg, got %v", a)
		}
		armed = true
	}
	_ = armed
	// Reversal: a sell prints back below the 50% retrace (100.08).
	now := base + int64(600*time.Millisecond)
	alerts := det.OnTrade(now, st, mkTrade("BTCUSDT", "100.05", "2", events.Sell, now))
	if !hasDetector(alerts, DetectorMomentum) {
		t.Fatalf("expected momentum ignition alert, got %+v", alerts)
	}
}

func TestMomentumNoFireOnSteadyTrend(t *testing.T) {
	cfg := DefaultConfig()
	st := &SymbolState{Symbol: "BTCUSDT", Book: orderbook.New("BTCUSDT", "")}
	det := newMomentumDetector("BTCUSDT", cfg)
	base := int64(1_000_000_000_000)
	// A steady climb that never reverses should not fire.
	prices := []string{"100.00", "100.05", "100.10", "100.16", "100.20", "100.25"}
	for i, p := range prices {
		now := base + int64(i)*int64(150*time.Millisecond)
		if a := det.OnTrade(now, st, mkTrade("BTCUSDT", p, "2", events.Buy, now)); hasDetector(a, DetectorMomentum) {
			t.Fatalf("steady trend should not ignite-alert, got %+v", a)
		}
	}
}

// --- quote stuffing ---

func TestQuoteStuffingFires(t *testing.T) {
	cfg := DefaultConfig()
	st := &SymbolState{Symbol: "BTCUSDT", Book: orderbook.New("BTCUSDT", "")}
	det := newQuoteStuffingDetector("BTCUSDT", cfg)
	base := int64(1_000_000_000_000)
	st.Book.Apply(mkDepth("BTCUSDT", true,
		[]events.Level{lv("100.00", "1")}, []events.Level{lv("100.02", "1")}, 0))

	// 5 diffs x 50 new levels = 250 book updates within the 1s window, no trades.
	var fired bool
	priceCents := 9000
	for d := 0; d < 5; d++ {
		levels := make([]events.Level, 0, 50)
		for i := 0; i < 50; i++ {
			levels = append(levels, lv(fmt.Sprintf("%.2f", float64(priceCents)/100.0), "1"))
			priceCents++
		}
		now := base + int64(d)*int64(50*time.Millisecond)
		if hasDetector(feedDepth(det, st, mkDepth("BTCUSDT", false, levels, nil, now), now), DetectorQuoteStuffing) {
			fired = true
		}
	}
	if !fired {
		t.Fatal("expected quote stuffing alert from high churn / no trades")
	}
}

// --- wash trade footprint ---

func TestWashTradeFootprintFires(t *testing.T) {
	cfg := DefaultConfig()
	st := &SymbolState{Symbol: "BTCUSDT", Book: orderbook.New("BTCUSDT", "")}
	det := newWashTradeDetector("BTCUSDT", cfg)
	base := int64(1_000_000_000_000)
	sides := []events.Side{events.Buy, events.Sell}
	var fired bool
	for i := 0; i < 8; i++ {
		price := "100.00"
		if i%2 == 1 {
			price = "100.001"
		}
		now := base + int64(i)*int64(100*time.Millisecond)
		a := det.OnTrade(now, st, mkTrade("BTCUSDT", price, "0.5", sides[i%2], now))
		if hasDetector(a, DetectorWashTrade) {
			fired = true
			for _, al := range a {
				if al.Severity != SeverityLow {
					t.Errorf("wash trade should be low severity, got %v", al.Severity)
				}
			}
		}
	}
	if !fired {
		t.Fatal("expected wash-trade footprint alert")
	}
}

// --- engine integration: alert plumbing + features ---

func TestEngineMomentumPlumbing(t *testing.T) {
	cfg := DefaultConfig()
	var clk int64 = 1_000_000_000_000
	var got []Alert
	eng := NewEngine(cfg, func(a Alert) { got = append(got, a) }, WithClock(func() int64 { return clk }))

	feed := func(price string, side events.Side) {
		tr := mkTrade("BTCUSDT", price, "2", side, clk-2000) // 2us before detection
		eng.HandleEnvelope(events.Envelope{Kind: events.KindTrade, Trade: &tr})
	}
	for _, step := range []struct {
		p  string
		s  events.Side
		dt time.Duration
	}{
		{"100.00", events.Buy, 0},
		{"100.05", events.Buy, 100 * time.Millisecond},
		{"100.10", events.Buy, 200 * time.Millisecond},
		{"100.16", events.Buy, 300 * time.Millisecond},
		{"100.05", events.Sell, 600 * time.Millisecond},
	} {
		clk = 1_000_000_000_000 + int64(step.dt)
		feed(step.p, step.s)
	}
	if !hasDetector(got, DetectorMomentum) {
		t.Fatalf("engine should emit momentum alert, got %+v", got)
	}
	for _, a := range got {
		if a.ID == "" || a.SeverityLabel == "" {
			t.Errorf("engine must populate ID and SeverityLabel: %+v", a)
		}
		if a.DetectLatencyUs < 0 {
			t.Errorf("latency must be non-negative: %d", a.DetectLatencyUs)
		}
	}
}

func TestEngineEmitsFeatures(t *testing.T) {
	cfg := DefaultConfig()
	cfg.FeatureEmitEvery = 1 // emit on every event for the test
	var clk int64 = 1_000_000_000_000
	var vecs []features.Vector
	eng := NewEngine(cfg, func(Alert) {},
		WithClock(func() int64 { return clk }),
		WithFeatures(func(v features.Vector) { vecs = append(vecs, v) }))

	snap := mkDepth("BTCUSDT", true,
		[]events.Level{lv("100.00", "2"), lv("99.99", "1")},
		[]events.Level{lv("100.02", "1"), lv("100.03", "1")}, clk)
	eng.HandleEnvelope(events.Envelope{Kind: events.KindDepth, Depth: &snap})
	// A trade to exercise trade-window features.
	tr := mkTrade("BTCUSDT", "100.01", "0.5", events.Buy, clk)
	eng.HandleEnvelope(events.Envelope{Kind: events.KindTrade, Trade: &tr})

	if len(vecs) == 0 {
		t.Fatal("expected at least one feature vector")
	}
	last := vecs[len(vecs)-1]
	for _, key := range []string{features.SpreadBps, features.OFI, features.MicropriceDevBps, features.CancelRatio} {
		if _, ok := last.Features[key]; !ok {
			t.Errorf("feature %q missing from %+v", key, last.Features)
		}
	}
}
