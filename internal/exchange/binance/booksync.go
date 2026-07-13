package binance

import "github.com/argus-mss/argus/internal/events"

// bookSync implements Binance's documented "manage a local order book
// correctly" algorithm as a pure state machine (no goroutines, no network), so
// the fiddly sequencing rules can be exhaustively unit-tested:
//
//  1. While waiting for a REST snapshot, buffer incremental diffs.
//  2. On snapshot with lastUpdateId L, drop buffered diffs whose u <= L.
//  3. The first applied diff must satisfy U <= L+1 <= u; otherwise the snapshot
//     is stale relative to the stream and a fresh one is fetched.
//  4. Thereafter every diff must satisfy U == lastU+1; any gap forces a resync.
//
// It additionally tolerates dropped diffs (our bounded ingest queue may shed
// load): a detected gap simply triggers a resync, which restores a correct book
// rather than silently corrupting it.
type bookSync struct {
	symbol string

	live           bool
	lastU          int64
	firstSinceSnap bool
	buffer         []events.Depth
	maxBuffer      int
	Resyncs        int
	BufferDrops    int
}

func newBookSync(symbol string, maxBuffer int) *bookSync {
	if maxBuffer <= 0 {
		maxBuffer = 4096
	}
	return &bookSync{symbol: symbol, maxBuffer: maxBuffer}
}

// reset returns the sync to the buffering (not-live) state, e.g. on reconnect.
func (s *bookSync) reset() {
	s.live = false
	s.firstSinceSnap = false
	s.lastU = 0
	s.buffer = s.buffer[:0]
}

// onDiff processes an incremental depth diff.
//
// While not live it buffers the diff and returns nothing. While live it
// validates sequencing and returns the diff to publish, or signals gap==true —
// in which case the caller must fetch a fresh snapshot and feed onSnapshot; the
// sync begins buffering again starting from this diff.
func (s *bookSync) onDiff(d events.Depth) (toEmit []events.Depth, gap bool) {
	if !s.live {
		s.bufferDiff(d)
		return nil, false
	}
	apply, gapped := s.tryApply(d)
	if gapped {
		s.live = false
		s.firstSinceSnap = false
		s.buffer = s.buffer[:0]
		s.bufferDiff(d)
		s.Resyncs++
		return nil, true
	}
	if apply {
		return []events.Depth{d}, false
	}
	return nil, false // stale duplicate, silently skipped
}

// onSnapshot initializes the book from a REST snapshot and replays applicable
// buffered diffs. It returns the ordered events to publish (the snapshot marker
// followed by applicable diffs). If needAnother is true the snapshot was
// unusable (stale relative to the buffered stream) and the caller must fetch a
// newer one; nothing should be published in that case.
func (s *bookSync) onSnapshot(snap events.Depth) (toEmit []events.Depth, needAnother bool) {
	s.lastU = snap.FinalUpdateID
	s.firstSinceSnap = true
	out := []events.Depth{snap}
	for _, d := range s.buffer {
		apply, gap := s.tryApply(d)
		if gap {
			// The snapshot cannot be reconciled with the buffered stream.
			s.buffer = s.buffer[:0]
			s.firstSinceSnap = false
			s.Resyncs++
			return nil, true
		}
		if apply {
			out = append(out, d)
		}
	}
	s.buffer = s.buffer[:0]
	s.live = true
	return out, false
}

// tryApply validates a single diff against the current sequence position and,
// if valid, advances lastU. It reports whether the diff should be applied and
// whether it revealed a gap.
func (s *bookSync) tryApply(d events.Depth) (apply bool, gap bool) {
	if d.FinalUpdateID <= s.lastU {
		return false, false // entirely covered already: stale duplicate
	}
	if s.firstSinceSnap {
		// First diff after a snapshot must bracket lastU+1.
		if d.FirstUpdateID > s.lastU+1 {
			return false, true
		}
		s.firstSinceSnap = false
		s.lastU = d.FinalUpdateID
		return true, false
	}
	if d.FirstUpdateID != s.lastU+1 {
		return false, true // sequence gap
	}
	s.lastU = d.FinalUpdateID
	return true, false
}

func (s *bookSync) bufferDiff(d events.Depth) {
	if len(s.buffer) >= s.maxBuffer {
		// Shed the oldest buffered diff. This creates a gap that onSnapshot will
		// detect and recover from via resync — bounded memory, still correct.
		copy(s.buffer, s.buffer[1:])
		s.buffer = s.buffer[:len(s.buffer)-1]
		s.BufferDrops++
	}
	s.buffer = append(s.buffer, d)
}
