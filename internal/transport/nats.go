package transport

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

// NATS is a Bus backed by a real NATS connection — the production topology in
// which ingestors and detectors are independent, independently-scaled
// processes. Core NATS (at-most-once) is the right default for a market-data
// firehose: if a detector falls behind, dropping stale book updates and
// resyncing beats unbounded queue growth. JetStream (at-least-once, durable)
// would be layered in only for the audit/alert path where loss is unacceptable
// — see README for that tradeoff.
type NATS struct {
	conn *nats.Conn
}

// ConnectNATS dials a NATS server with sane reconnect defaults for a long-lived
// streaming service.
func ConnectNATS(url string) (*NATS, error) {
	if url == "" {
		url = nats.DefaultURL
	}
	conn, err := nats.Connect(url,
		nats.Name("argus"),
		nats.MaxReconnects(-1), // reconnect forever
		nats.ReconnectWait(500*time.Millisecond),
		nats.ReconnectBufSize(8*1024*1024), // buffer publishes across a blip
		nats.PingInterval(20*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("connect nats %q: %w", url, err)
	}
	return &NATS{conn: conn}, nil
}

// Publish sends data on subject (fire-and-forget core NATS).
func (n *NATS) Publish(subject string, data []byte) error {
	return n.conn.Publish(subject, data)
}

// Subscribe registers an async handler. NATS delivers on its own dispatcher
// goroutine per subscription, giving natural parallelism across detectors.
func (n *NATS) Subscribe(pattern string, h Handler) (Subscription, error) {
	sub, err := n.conn.Subscribe(pattern, func(msg *nats.Msg) {
		h(msg.Subject, msg.Data)
	})
	if err != nil {
		return nil, err
	}
	// Raise the slow-consumer limits well above defaults for a high-rate feed;
	// exceeding them drops messages and increments Dropped(), which we surface
	// as a metric rather than letting memory grow without bound.
	_ = sub.SetPendingLimits(1_000_000, 256*1024*1024)
	return &natsSub{sub: sub}, nil
}

// Close flushes and drains the connection.
func (n *NATS) Close() error {
	if err := n.conn.Drain(); err != nil {
		n.conn.Close()
		return err
	}
	return nil
}

// Conn exposes the underlying connection for metrics (e.g. slow-consumer drops).
func (n *NATS) Conn() *nats.Conn { return n.conn }

type natsSub struct{ sub *nats.Subscription }

func (s *natsSub) Unsubscribe() error { return s.sub.Unsubscribe() }
