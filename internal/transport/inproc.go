package transport

import (
	"errors"
	"sync"
)

// InProc is an in-process Bus. Delivery is synchronous and in publish order,
// which makes deterministic tests and the single-binary demo trivial: when
// Publish returns, every matching handler has run to completion.
//
// Backpressure in the single-binary topology is handled upstream, in the
// ingestor's bounded hand-off channel, rather than here — mirroring how a real
// deployment relies on the broker + bounded consumer queues.
type InProc struct {
	mu     sync.RWMutex
	subs   map[int]*inprocSub
	nextID int
	closed bool
}

type inprocSub struct {
	id      int
	pattern string
	handler Handler
	bus     *InProc
}

// NewInProc returns an empty in-process bus.
func NewInProc() *InProc {
	return &InProc{subs: make(map[int]*inprocSub)}
}

// ErrBusClosed is returned once the bus has been closed.
var ErrBusClosed = errors.New("transport: bus closed")

// Publish delivers data to every subscription whose pattern matches subject.
// Handlers run synchronously in the caller's goroutine, in subscription order.
func (b *InProc) Publish(subject string, data []byte) error {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return ErrBusClosed
	}
	// Snapshot matching handlers so a handler may (un)subscribe without
	// deadlocking or mutating the map mid-iteration.
	var targets []Handler
	for _, s := range b.subs {
		if subjectMatches(s.pattern, subject) {
			targets = append(targets, s.handler)
		}
	}
	b.mu.RUnlock()

	for _, h := range targets {
		h(subject, data)
	}
	return nil
}

// Subscribe registers a handler for a subject pattern.
func (b *InProc) Subscribe(pattern string, h Handler) (Subscription, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ErrBusClosed
	}
	b.nextID++
	s := &inprocSub{id: b.nextID, pattern: pattern, handler: h, bus: b}
	b.subs[s.id] = s
	return s, nil
}

// Close drops all subscriptions.
func (b *InProc) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	b.subs = make(map[int]*inprocSub)
	return nil
}

func (s *inprocSub) Unsubscribe() error {
	s.bus.mu.Lock()
	defer s.bus.mu.Unlock()
	delete(s.bus.subs, s.id)
	return nil
}
