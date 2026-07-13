// Package orderbook maintains a per-symbol L2 limit order book from normalized
// depth events. It keeps each side as a price-sorted ladder (bids descending,
// asks ascending) so top-of-book and top-N depth queries — the primitives every
// detector needs — are O(1)/O(n) with no per-query scan of the full ladder.
//
// Apply returns the exact set of (old -> new) level changes it performed. The
// spoofing and layering detectors run off these deltas: a large size that
// appears at a level and then vanishes without trades is the observable
// footprint of a pulled order.
package orderbook

import (
	"sort"

	"github.com/argus-mss/argus/internal/events"
	"github.com/argus-mss/argus/internal/fixedpoint"
)

// LevelDelta records a single price level transitioning from OldQty to NewQty.
type LevelDelta struct {
	Side   events.Side
	Price  fixedpoint.Value
	OldQty fixedpoint.Value
	NewQty fixedpoint.Value
}

// Added reports whether resting size grew at this level.
func (d LevelDelta) Added() bool { return d.NewQty > d.OldQty }

// Removed reports whether resting size shrank at this level.
func (d LevelDelta) Removed() bool { return d.NewQty < d.OldQty }

// Book is a single-symbol L2 book. Not safe for concurrent use; each detector
// engine owns its books on one goroutine.
type Book struct {
	Symbol   string
	Exchange string

	bids []events.Level // descending by price
	asks []events.Level // ascending by price

	lastUpdateID int64
	synced       bool
}

// New returns an empty, unsynced book for a symbol.
func New(symbol, exchange string) *Book {
	return &Book{Symbol: symbol, Exchange: exchange}
}

// Synced reports whether a snapshot has been applied and the book is live.
func (b *Book) Synced() bool { return b.synced }

// LastUpdateID returns the venue update id of the most recently applied event.
func (b *Book) LastUpdateID() int64 { return b.lastUpdateID }

// Reset clears the book back to unsynced.
func (b *Book) Reset() {
	b.bids = b.bids[:0]
	b.asks = b.asks[:0]
	b.lastUpdateID = 0
	b.synced = false
}

// Apply mutates the book with a depth event and returns the level deltas it
// performed. A snapshot resets the book and returns no deltas (snapshot loads
// are not order activity and must not be interpreted as add/cancel behavior).
// Incremental events received before the book is synced are ignored.
func (b *Book) Apply(d events.Depth) []LevelDelta {
	if d.IsSnapshot {
		b.Reset()
		for _, lv := range d.Bids {
			if lv.Qty > 0 {
				b.setLevel(events.Buy, lv.Price, lv.Qty)
			}
		}
		for _, lv := range d.Asks {
			if lv.Qty > 0 {
				b.setLevel(events.Sell, lv.Price, lv.Qty)
			}
		}
		b.lastUpdateID = d.FinalUpdateID
		b.synced = true
		return nil
	}
	if !b.synced {
		return nil
	}
	deltas := make([]LevelDelta, 0, len(d.Bids)+len(d.Asks))
	for _, lv := range d.Bids {
		old := b.qtyAt(events.Buy, lv.Price)
		if old != lv.Qty {
			deltas = append(deltas, LevelDelta{Side: events.Buy, Price: lv.Price, OldQty: old, NewQty: lv.Qty})
		}
		b.setLevel(events.Buy, lv.Price, lv.Qty)
	}
	for _, lv := range d.Asks {
		old := b.qtyAt(events.Sell, lv.Price)
		if old != lv.Qty {
			deltas = append(deltas, LevelDelta{Side: events.Sell, Price: lv.Price, OldQty: old, NewQty: lv.Qty})
		}
		b.setLevel(events.Sell, lv.Price, lv.Qty)
	}
	b.lastUpdateID = d.FinalUpdateID
	return deltas
}

// qtyAt returns the resting quantity at a price level, or 0 if absent.
func (b *Book) qtyAt(side events.Side, price fixedpoint.Value) fixedpoint.Value {
	levels := b.bids
	if side == events.Sell {
		levels = b.asks
	}
	i, found := b.search(levels, side, price)
	if found {
		return levels[i].Qty
	}
	return 0
}

// setLevel inserts, updates, or (when qty==0) removes a price level, keeping
// the ladder sorted.
func (b *Book) setLevel(side events.Side, price, qty fixedpoint.Value) {
	if side == events.Buy {
		b.bids = mutate(b.bids, side, price, qty, b.search)
	} else {
		b.asks = mutate(b.asks, side, price, qty, b.search)
	}
}

// search returns the index of price in a sorted ladder and whether it exists.
// Bids are descending, asks ascending.
func (b *Book) search(levels []events.Level, side events.Side, price fixedpoint.Value) (int, bool) {
	var i int
	if side == events.Buy {
		i = sort.Search(len(levels), func(i int) bool { return levels[i].Price <= price })
	} else {
		i = sort.Search(len(levels), func(i int) bool { return levels[i].Price >= price })
	}
	if i < len(levels) && levels[i].Price == price {
		return i, true
	}
	return i, false
}

// mutate applies a set/remove at price to a ladder and returns the new slice.
func mutate(levels []events.Level, side events.Side, price, qty fixedpoint.Value,
	search func([]events.Level, events.Side, fixedpoint.Value) (int, bool)) []events.Level {
	i, found := search(levels, side, price)
	if found {
		if qty == 0 {
			return append(levels[:i], levels[i+1:]...)
		}
		levels[i].Qty = qty
		return levels
	}
	if qty == 0 {
		return levels // removing an absent level is a no-op
	}
	levels = append(levels, events.Level{})
	copy(levels[i+1:], levels[i:])
	levels[i] = events.Level{Price: price, Qty: qty}
	return levels
}

// BestBid returns the highest bid and true, or a zero level and false if empty.
func (b *Book) BestBid() (events.Level, bool) {
	if len(b.bids) == 0 {
		return events.Level{}, false
	}
	return b.bids[0], true
}

// BestAsk returns the lowest ask and true, or a zero level and false if empty.
func (b *Book) BestAsk() (events.Level, bool) {
	if len(b.asks) == 0 {
		return events.Level{}, false
	}
	return b.asks[0], true
}

// TopBids returns up to n best bids (descending).
func (b *Book) TopBids(n int) []events.Level { return top(b.bids, n) }

// TopAsks returns up to n best asks (ascending).
func (b *Book) TopAsks(n int) []events.Level { return top(b.asks, n) }

func top(levels []events.Level, n int) []events.Level {
	if n > len(levels) {
		n = len(levels)
	}
	out := make([]events.Level, n)
	copy(out, levels[:n])
	return out
}

// Mid returns the arithmetic mid price as a float64, and false if either side
// is empty.
func (b *Book) Mid() (float64, bool) {
	bb, ok1 := b.BestBid()
	ba, ok2 := b.BestAsk()
	if !ok1 || !ok2 {
		return 0, false
	}
	return (bb.Price.Float() + ba.Price.Float()) / 2, true
}

// Microprice returns the size-weighted mid, a better fair-value estimator than
// the arithmetic mid because it leans toward the side with more resting size.
func (b *Book) Microprice() (float64, bool) {
	bb, ok1 := b.BestBid()
	ba, ok2 := b.BestAsk()
	if !ok1 || !ok2 {
		return 0, false
	}
	qb, qa := bb.Qty.Float(), ba.Qty.Float()
	if qb+qa == 0 {
		return (bb.Price.Float() + ba.Price.Float()) / 2, true
	}
	// Weight each side's price by the *opposite* side's size.
	return (bb.Price.Float()*qa + ba.Price.Float()*qb) / (qb + qa), true
}

// Spread returns bestAsk - bestBid, and false if either side is empty.
func (b *Book) Spread() (fixedpoint.Value, bool) {
	bb, ok1 := b.BestBid()
	ba, ok2 := b.BestAsk()
	if !ok1 || !ok2 {
		return 0, false
	}
	return ba.Price - bb.Price, true
}

// SumQty returns the total resting size across the top n levels of a side.
func (b *Book) SumQty(side events.Side, n int) fixedpoint.Value {
	levels := b.bids
	if side == events.Sell {
		levels = b.asks
	}
	if n > len(levels) {
		n = len(levels)
	}
	var sum fixedpoint.Value
	for i := 0; i < n; i++ {
		sum += levels[i].Qty
	}
	return sum
}

// Depth returns the number of populated price levels on a side.
func (b *Book) Depth(side events.Side) int {
	if side == events.Sell {
		return len(b.asks)
	}
	return len(b.bids)
}
