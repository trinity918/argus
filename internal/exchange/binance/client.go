// Package binance is the ingestion adapter for Binance spot. It connects to the
// combined depth+trade WebSocket, normalizes every frame into the internal
// events schema, and guarantees a gap-checked, snapshot-anchored depth stream
// downstream via the bookSync state machine.
//
// Concurrency model: exactly one reader goroutine pulls frames off the socket
// into a bounded channel; one event-loop goroutine owns all book-sync state and
// snapshot coordination. State is therefore lock-free. The bounded channel is
// the backpressure valve — if detection falls behind, frames are shed and the
// resulting sequence gap triggers a resync that restores a correct book.
package binance

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/argus-mss/argus/internal/events"
	"github.com/gorilla/websocket"
)

// Publisher receives normalized envelopes for fan-out onto the transport bus.
type Publisher interface {
	Publish(env events.Envelope) error
}

// Config parameterizes the ingestion client.
type Config struct {
	Symbols       []string      // e.g. []string{"BTCUSDT","ETHUSDT"}
	WSBase        string        // default wss://stream.binance.com:9443
	RESTBase      string        // default https://api.binance.com
	DepthLimit    int           // REST snapshot depth (5..5000; default 1000)
	MsgBuffer     int           // reader->loop channel capacity (default 8192)
	MaxBookBuffer int           // per-symbol buffered-diff cap (default 4096)
	ReadTimeout   time.Duration // socket read deadline (default 60s)
	Logger        *slog.Logger
}

// Stats is a point-in-time snapshot of ingestion counters.
type Stats struct {
	MessagesReceived int64
	MessagesDropped  int64
	Reconnects       int64
	Resyncs          int64
	TradesEmitted    int64
	DepthEmitted     int64
	SnapshotFetches  int64
	SnapshotErrors   int64
}

type counters struct {
	received    atomic.Int64
	dropped     atomic.Int64
	reconnects  atomic.Int64
	resyncs     atomic.Int64
	trades      atomic.Int64
	depth       atomic.Int64
	snapFetches atomic.Int64
	snapErrors  atomic.Int64
}

// Client is a running Binance ingestion adapter.
type Client struct {
	cfg   Config
	pub   Publisher
	http  *http.Client
	log   *slog.Logger
	syncs map[string]*bookSync // keyed by upper-case symbol

	counters counters

	initialBackoff time.Duration
	maxBackoff     time.Duration
	readTimeout    time.Duration
}

// New builds a client. Symbols are normalized to upper case internally.
func New(cfg Config, pub Publisher) *Client {
	if cfg.WSBase == "" {
		cfg.WSBase = "wss://stream.binance.com:9443"
	}
	if cfg.RESTBase == "" {
		cfg.RESTBase = "https://api.binance.com"
	}
	if cfg.DepthLimit == 0 {
		cfg.DepthLimit = 1000
	}
	if cfg.MsgBuffer == 0 {
		cfg.MsgBuffer = 8192
	}
	if cfg.MaxBookBuffer == 0 {
		cfg.MaxBookBuffer = 4096
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = 60 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	syncs := make(map[string]*bookSync, len(cfg.Symbols))
	upper := make([]string, 0, len(cfg.Symbols))
	for _, s := range cfg.Symbols {
		u := strings.ToUpper(s)
		upper = append(upper, u)
		syncs[u] = newBookSync(u, cfg.MaxBookBuffer)
	}
	cfg.Symbols = upper
	return &Client{
		cfg:            cfg,
		pub:            pub,
		http:           &http.Client{Timeout: 10 * time.Second},
		log:            cfg.Logger,
		syncs:          syncs,
		initialBackoff: 1 * time.Second,
		maxBackoff:     30 * time.Second,
		readTimeout:    cfg.ReadTimeout,
	}
}

// Stats returns a snapshot of ingestion counters (safe to call concurrently).
func (c *Client) Stats() Stats {
	return Stats{
		MessagesReceived: c.counters.received.Load(),
		MessagesDropped:  c.counters.dropped.Load(),
		Reconnects:       c.counters.reconnects.Load(),
		Resyncs:          c.counters.resyncs.Load(),
		TradesEmitted:    c.counters.trades.Load(),
		DepthEmitted:     c.counters.depth.Load(),
		SnapshotFetches:  c.counters.snapFetches.Load(),
		SnapshotErrors:   c.counters.snapErrors.Load(),
	}
}

// Run connects and streams until ctx is cancelled, reconnecting with capped
// exponential backoff across transient failures.
func (c *Client) Run(ctx context.Context) error {
	backoff := c.initialBackoff
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		progressed, err := c.runOnce(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if progressed {
			backoff = c.initialBackoff
		}
		c.counters.reconnects.Add(1)
		c.log.Warn("binance stream ended; reconnecting", "err", err, "in", backoff.String())
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
		backoff = minDur(backoff*2, c.maxBackoff)
	}
}

// snapResult carries an async REST snapshot back to the event loop.
type snapResult struct {
	symbol string
	snap   events.Depth
	err    error
}

// runOnce owns a single WebSocket connection. It returns whether it made
// progress (received at least one frame) so the caller can reset backoff.
func (c *Client) runOnce(parent context.Context) (bool, error) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, c.streamURL(), nil)
	if err != nil {
		return false, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()
	conn.SetReadLimit(16 << 20)
	conn.SetPingHandler(func(appData string) error {
		_ = conn.SetReadDeadline(time.Now().Add(c.readTimeout))
		return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(5*time.Second))
	})

	for _, s := range c.syncs {
		s.reset()
	}

	msgCh := make(chan []byte, c.cfg.MsgBuffer)
	readErr := make(chan error, 1)
	go func() {
		for {
			_ = conn.SetReadDeadline(time.Now().Add(c.readTimeout))
			_, data, err := conn.ReadMessage()
			if err != nil {
				readErr <- err
				return
			}
			c.counters.received.Add(1)
			select {
			case msgCh <- data:
			default:
				// Backpressure: shed the frame. A dropped depth diff creates a
				// sequence gap that bookSync detects and repairs via resync.
				c.counters.dropped.Add(1)
			}
		}
	}()

	snapCh := make(chan snapResult, len(c.syncs)*2+8)
	pending := make(map[string]bool)
	requestSnap := func(symbol string, delay time.Duration) {
		if pending[symbol] {
			return
		}
		pending[symbol] = true
		c.counters.snapFetches.Add(1)
		go func() {
			if delay > 0 {
				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return
				}
			}
			snap, err := c.fetchSnapshot(ctx, symbol)
			select {
			case snapCh <- snapResult{symbol: symbol, snap: snap, err: err}:
			case <-ctx.Done():
			}
		}()
	}
	for _, sym := range c.cfg.Symbols {
		requestSnap(sym, 0)
	}

	progressed := false
	for {
		select {
		case <-ctx.Done():
			return progressed, ctx.Err()
		case err := <-readErr:
			return progressed, err
		case data := <-msgCh:
			progressed = true
			c.handleMessage(data, requestSnap)
		case res := <-snapCh:
			pending[res.symbol] = false
			c.handleSnapshot(res, requestSnap)
		}
	}
}

func (c *Client) handleMessage(data []byte, requestSnap func(string, time.Duration)) {
	env, err := parseMessage(data, nowNs())
	if err != nil {
		c.log.Debug("parse error", "err", err)
		return
	}
	if env == nil {
		return
	}
	switch env.Kind {
	case events.KindTrade:
		c.publish(*env)
		c.counters.trades.Add(1)
	case events.KindDepth:
		s := c.syncs[env.Depth.Symbol]
		if s == nil {
			return
		}
		toEmit, gap := s.onDiff(*env.Depth)
		c.emitDepth(toEmit)
		if gap {
			c.counters.resyncs.Add(1)
			c.log.Info("sequence gap; resyncing", "symbol", env.Depth.Symbol)
			requestSnap(env.Depth.Symbol, 0)
		}
	}
}

func (c *Client) handleSnapshot(res snapResult, requestSnap func(string, time.Duration)) {
	if res.err != nil {
		c.counters.snapErrors.Add(1)
		c.log.Warn("snapshot fetch failed; retrying", "symbol", res.symbol, "err", res.err)
		requestSnap(res.symbol, 500*time.Millisecond)
		return
	}
	s := c.syncs[res.symbol]
	if s == nil {
		return
	}
	toEmit, needAnother := s.onSnapshot(res.snap)
	if needAnother {
		c.counters.resyncs.Add(1)
		c.log.Info("snapshot stale vs buffer; refetching", "symbol", res.symbol)
		requestSnap(res.symbol, 200*time.Millisecond)
		return
	}
	c.emitDepth(toEmit)
	c.log.Info("book synced", "symbol", res.symbol, "last_update_id", res.snap.FinalUpdateID)
}

func (c *Client) emitDepth(depths []events.Depth) {
	for i := range depths {
		d := depths[i]
		c.publish(events.Envelope{Kind: events.KindDepth, Depth: &d})
		if !d.IsSnapshot {
			c.counters.depth.Add(1)
		}
	}
}

func (c *Client) publish(env events.Envelope) {
	if err := c.pub.Publish(env); err != nil {
		c.log.Warn("publish failed", "err", err)
	}
}

func (c *Client) streamURL() string {
	streams := make([]string, 0, len(c.cfg.Symbols)*2)
	for _, sym := range c.cfg.Symbols {
		s := strings.ToLower(sym)
		streams = append(streams, s+"@depth@100ms", s+"@trade")
	}
	return c.cfg.WSBase + "/stream?streams=" + strings.Join(streams, "/")
}

func minDur(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
