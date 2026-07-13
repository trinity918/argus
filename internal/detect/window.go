package detect

import "time"

// stamped pairs a value with its event timestamp (ns).
type stamped[T any] struct {
	ts int64
	v  T
}

// Window is a time-bounded ring of timestamped values: the sliding-window
// primitive every detector is built on. Add is amortized O(1); eviction copies
// survivors to the front rather than resliced-forever, so memory stays bounded
// under a sustained feed.
type Window[T any] struct {
	dur int64
	buf []stamped[T]
}

// NewWindow returns a window retaining values for duration d.
func NewWindow[T any](d time.Duration) *Window[T] {
	return &Window[T]{dur: int64(d)}
}

// Add appends v at time ts and evicts anything older than ts-dur. Timestamps
// are assumed non-decreasing, which holds for a single ordered market-data
// subject.
func (w *Window[T]) Add(ts int64, v T) {
	w.buf = append(w.buf, stamped[T]{ts, v})
	w.evict(ts)
}

// Evict drops values older than now-dur without adding anything (useful when a
// detector needs the current window at an arbitrary time).
func (w *Window[T]) Evict(now int64) { w.evict(now) }

func (w *Window[T]) evict(now int64) {
	cutoff := now - w.dur
	i := 0
	for i < len(w.buf) && w.buf[i].ts < cutoff {
		i++
	}
	if i > 0 {
		w.buf = append(w.buf[:0], w.buf[i:]...)
	}
}

// Len returns the number of retained values.
func (w *Window[T]) Len() int { return len(w.buf) }

// ForEach iterates retained values oldest-first.
func (w *Window[T]) ForEach(fn func(ts int64, v T)) {
	for _, s := range w.buf {
		fn(s.ts, s.v)
	}
}

// Span returns the timestamp range [oldest,newest] and whether the window is
// non-empty.
func (w *Window[T]) Span() (oldest, newest int64, ok bool) {
	if len(w.buf) == 0 {
		return 0, 0, false
	}
	return w.buf[0].ts, w.buf[len(w.buf)-1].ts, true
}
