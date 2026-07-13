package binance

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/argus-mss/argus/internal/events"
	"github.com/argus-mss/argus/internal/orderbook"
)

// collector is a Publisher that records envelopes and rebuilds a book, so the
// live test can assert the end-to-end ingestion path produced a coherent book.
type collector struct {
	mu     sync.Mutex
	trades int
	diffs  int
	snaps  int
	book   *orderbook.Book
}

func (c *collector) Publish(env events.Envelope) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch env.Kind {
	case events.KindTrade:
		c.trades++
	case events.KindDepth:
		if env.Depth.IsSnapshot {
			c.snaps++
		} else {
			c.diffs++
		}
		c.book.Apply(*env.Depth)
	}
	return nil
}

// TestLiveBinanceSmoke exercises the real Binance connection. It is skipped
// unless ARGUS_LIVE=1 so offline CI stays green.
func TestLiveBinanceSmoke(t *testing.T) {
	if os.Getenv("ARGUS_LIVE") != "1" {
		t.Skip("set ARGUS_LIVE=1 to run the live Binance smoke test")
	}
	c := &collector{book: orderbook.New("BTCUSDT", "binance")}
	client := New(Config{Symbols: []string{"BTCUSDT"}}, c)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	_ = client.Run(ctx) // returns ctx deadline error

	c.mu.Lock()
	defer c.mu.Unlock()
	t.Logf("live stats: %+v", client.Stats())
	t.Logf("collector: snaps=%d diffs=%d trades=%d", c.snaps, c.diffs, c.trades)

	if c.snaps < 1 {
		t.Fatal("expected at least one snapshot")
	}
	if c.diffs < 5 {
		t.Fatalf("expected several depth diffs, got %d", c.diffs)
	}
	if !c.book.Synced() {
		t.Fatal("book should be synced")
	}
	bb, ok1 := c.book.BestBid()
	ba, ok2 := c.book.BestAsk()
	if !ok1 || !ok2 {
		t.Fatal("book should have both sides populated")
	}
	if ba.Price <= bb.Price {
		t.Fatalf("crossed/locked book: bid=%s ask=%s", bb.Price, ba.Price)
	}
	t.Logf("BTCUSDT top-of-book: bid %s / ask %s (spread %s)",
		bb.Price, ba.Price, ba.Price-bb.Price)
}
