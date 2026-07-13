package orderbook

import (
	"testing"

	"github.com/argus-mss/argus/internal/events"
	fp "github.com/argus-mss/argus/internal/fixedpoint"
)

func lvl(price, qty string) events.Level {
	return events.Level{Price: fp.MustParse(price), Qty: fp.MustParse(qty)}
}

func snapshot(bids, asks []events.Level, uid int64) events.Depth {
	return events.Depth{Symbol: "BTCUSDT", Bids: bids, Asks: asks, FinalUpdateID: uid, IsSnapshot: true}
}

func TestSnapshotAndBestOfBook(t *testing.T) {
	b := New("BTCUSDT", "binance")
	b.Apply(snapshot(
		[]events.Level{lvl("100.0", "1"), lvl("99.5", "2"), lvl("99.0", "3")},
		[]events.Level{lvl("100.5", "1"), lvl("101.0", "2"), lvl("101.5", "3")},
		10,
	))
	if !b.Synced() {
		t.Fatal("book should be synced after snapshot")
	}
	bb, _ := b.BestBid()
	ba, _ := b.BestAsk()
	if bb.Price != fp.MustParse("100.0") {
		t.Errorf("best bid = %s, want 100.0", bb.Price)
	}
	if ba.Price != fp.MustParse("100.5") {
		t.Errorf("best ask = %s, want 100.5", ba.Price)
	}
	if b.LastUpdateID() != 10 {
		t.Errorf("lastUpdateID = %d, want 10", b.LastUpdateID())
	}
}

func TestLadderStaysSorted(t *testing.T) {
	b := New("BTCUSDT", "binance")
	b.Apply(snapshot(nil, nil, 1))
	// Insert asks out of order; ladder must end up ascending.
	b.Apply(events.Depth{Symbol: "BTCUSDT", FinalUpdateID: 2,
		Asks: []events.Level{lvl("103", "1"), lvl("101", "1"), lvl("102", "1")}})
	asks := b.TopAsks(3)
	if asks[0].Price != fp.MustParse("101") || asks[1].Price != fp.MustParse("102") || asks[2].Price != fp.MustParse("103") {
		t.Fatalf("asks not ascending: %v", asks)
	}
	// Insert bids out of order; ladder must end up descending.
	b.Apply(events.Depth{Symbol: "BTCUSDT", FinalUpdateID: 3,
		Bids: []events.Level{lvl("98", "1"), lvl("100", "1"), lvl("99", "1")}})
	bids := b.TopBids(3)
	if bids[0].Price != fp.MustParse("100") || bids[1].Price != fp.MustParse("99") || bids[2].Price != fp.MustParse("98") {
		t.Fatalf("bids not descending: %v", bids)
	}
}

func TestApplyReturnsDeltas(t *testing.T) {
	b := New("BTCUSDT", "binance")
	b.Apply(snapshot([]events.Level{lvl("100", "5")}, []events.Level{lvl("101", "5")}, 1))

	// Grow the bid at 100 from 5 -> 20 (a large size appearing).
	deltas := b.Apply(events.Depth{Symbol: "BTCUSDT", FinalUpdateID: 2,
		Bids: []events.Level{lvl("100", "20")}})
	if len(deltas) != 1 {
		t.Fatalf("expected 1 delta, got %d", len(deltas))
	}
	d := deltas[0]
	if !d.Added() || d.OldQty != fp.MustParse("5") || d.NewQty != fp.MustParse("20") {
		t.Fatalf("unexpected delta: %+v", d)
	}

	// Pull it back to 0 (a cancel).
	deltas = b.Apply(events.Depth{Symbol: "BTCUSDT", FinalUpdateID: 3,
		Bids: []events.Level{lvl("100", "0")}})
	if len(deltas) != 1 || !deltas[0].Removed() || deltas[0].NewQty != 0 {
		t.Fatalf("expected a removal delta, got %+v", deltas)
	}
	if _, ok := b.BestBid(); ok {
		t.Fatal("bid side should be empty after pulling the only level")
	}
}

func TestSnapshotReturnsNoDeltas(t *testing.T) {
	b := New("BTCUSDT", "binance")
	deltas := b.Apply(snapshot([]events.Level{lvl("100", "5")}, []events.Level{lvl("101", "5")}, 1))
	if deltas != nil {
		t.Fatalf("snapshot must not produce deltas, got %v", deltas)
	}
}

func TestIncrementalBeforeSyncIgnored(t *testing.T) {
	b := New("BTCUSDT", "binance")
	deltas := b.Apply(events.Depth{Symbol: "BTCUSDT", FinalUpdateID: 2,
		Bids: []events.Level{lvl("100", "1")}})
	if deltas != nil || b.Synced() {
		t.Fatal("incremental update before snapshot must be ignored")
	}
}

func TestMicropriceLeansToHeavierSide(t *testing.T) {
	b := New("BTCUSDT", "binance")
	// Much more size on the bid: fair value should sit above the arithmetic mid.
	b.Apply(snapshot([]events.Level{lvl("100", "100")}, []events.Level{lvl("102", "1")}, 1))
	mid, _ := b.Mid()
	micro, _ := b.Microprice()
	if micro <= mid {
		t.Fatalf("microprice %.4f should exceed mid %.4f when bid is heavier", micro, mid)
	}
}

func TestSumQtyAndDepth(t *testing.T) {
	b := New("BTCUSDT", "binance")
	b.Apply(snapshot(
		[]events.Level{lvl("100", "1"), lvl("99", "2"), lvl("98", "3")},
		[]events.Level{lvl("101", "1")}, 1))
	if got := b.SumQty(events.Buy, 2); got != fp.MustParse("3") {
		t.Errorf("SumQty(top2 bids) = %s, want 3", got)
	}
	if b.Depth(events.Buy) != 3 {
		t.Errorf("bid depth = %d, want 3", b.Depth(events.Buy))
	}
}
