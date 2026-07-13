package binance

import (
	"testing"

	"github.com/argus-mss/argus/internal/events"
)

func diff(U, u int64) events.Depth {
	return events.Depth{Symbol: "BTCUSDT", FirstUpdateID: U, FinalUpdateID: u}
}
func snap(lastUpdateID int64) events.Depth {
	return events.Depth{Symbol: "BTCUSDT", FinalUpdateID: lastUpdateID, IsSnapshot: true}
}

func TestBookSyncHappyPath(t *testing.T) {
	s := newBookSync("BTCUSDT", 100)
	// Buffer diffs that arrive before the snapshot returns.
	s.onDiff(diff(5, 10))
	s.onDiff(diff(11, 15))
	s.onDiff(diff(16, 20))

	out, need := s.onSnapshot(snap(12))
	if need {
		t.Fatal("snapshot should be usable")
	}
	// Expect: snapshot marker, then diff(11,15) (brackets 13) and diff(16,20).
	if len(out) != 3 || !out[0].IsSnapshot || out[1].FinalUpdateID != 15 || out[2].FinalUpdateID != 20 {
		t.Fatalf("unexpected emit set: %+v", out)
	}
	if !s.live || s.lastU != 20 {
		t.Fatalf("expected live at lastU=20, got live=%v lastU=%d", s.live, s.lastU)
	}
	// Contiguous live diff applies.
	got, gap := s.onDiff(diff(21, 25))
	if gap || len(got) != 1 || got[0].FinalUpdateID != 25 {
		t.Fatalf("contiguous diff should apply, got=%v gap=%v", got, gap)
	}
	// A gap forces a resync.
	got, gap = s.onDiff(diff(27, 30))
	if !gap || got != nil {
		t.Fatalf("expected gap resync, got=%v gap=%v", got, gap)
	}
	if s.live {
		t.Fatal("should drop to buffering after gap")
	}
}

func TestBookSyncSnapshotNewerThanBuffer(t *testing.T) {
	s := newBookSync("BTCUSDT", 100)
	s.onDiff(diff(5, 10))
	s.onDiff(diff(11, 15))
	// Snapshot is ahead of everything buffered; all buffered diffs are stale.
	out, need := s.onSnapshot(snap(20))
	if need || len(out) != 1 || !out[0].IsSnapshot {
		t.Fatalf("expected only snapshot marker, got need=%v out=%+v", need, out)
	}
	// First live diff after snapshot uses the bracket rule (U <= lastU+1 <= u).
	got, gap := s.onDiff(diff(21, 25))
	if gap || len(got) != 1 {
		t.Fatalf("first live diff should apply via bracket, got=%v gap=%v", got, gap)
	}
}

func TestBookSyncStaleSnapshotNeedsRefetch(t *testing.T) {
	s := newBookSync("BTCUSDT", 100)
	// The buffered stream is far ahead of the snapshot we got back.
	s.onDiff(diff(50, 55))
	s.onDiff(diff(56, 60))
	out, need := s.onSnapshot(snap(12))
	if !need || out != nil {
		t.Fatalf("stale snapshot must request another, got need=%v out=%+v", need, out)
	}
	if s.live {
		t.Fatal("must remain not-live until a usable snapshot arrives")
	}
}

func TestBookSyncStaleDuplicateSkipped(t *testing.T) {
	s := newBookSync("BTCUSDT", 100)
	s.onSnapshot(snap(20))
	s.onDiff(diff(21, 25))             // advance to lastU=25
	got, gap := s.onDiff(diff(10, 18)) // entirely older
	if gap || got != nil {
		t.Fatalf("stale duplicate should be skipped silently, got=%v gap=%v", got, gap)
	}
	if s.lastU != 25 {
		t.Fatalf("lastU should stay 25, got %d", s.lastU)
	}
}

func TestBookSyncBufferOverflowDrops(t *testing.T) {
	s := newBookSync("BTCUSDT", 2)
	s.onDiff(diff(1, 2))
	s.onDiff(diff(3, 4))
	s.onDiff(diff(5, 6)) // overflow: oldest dropped
	if s.BufferDrops != 1 {
		t.Fatalf("expected 1 buffer drop, got %d", s.BufferDrops)
	}
	if len(s.buffer) != 2 || s.buffer[0].FinalUpdateID != 4 {
		t.Fatalf("buffer should hold the 2 newest, got %+v", s.buffer)
	}
}
