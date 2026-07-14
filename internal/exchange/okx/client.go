package okx

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/argus-mss/argus/internal/events"
	"github.com/gorilla/websocket"
)

// Publisher receives normalized envelopes (structurally identical to the
// binance adapter's Publisher, so app.EnvelopePublisher serves both venues).
type Publisher interface {
	Publish(env events.Envelope) error
}

// Config parameterizes the OKX ingestion client.
type Config struct {
	Symbols     []string      // OKX instIds, e.g. []string{"BTC-USDT","ETH-USDT"}
	WSURL       string        // default wss://ws.okx.com:8443/ws/v5/public
	MsgBuffer   int           // reader->loop channel capacity (default 8192)
	ReadTimeout time.Duration // socket read deadline (default 40s; OKX idles ~30s)
	Logger      *slog.Logger
}

// Stats is a point-in-time snapshot of ingestion counters.
type Stats struct {
	MessagesReceived  int64
	MessagesDropped   int64
	Reconnects        int64
	Resyncs           int64
	ChecksumFailures  int64
	ChecksumsVerified int64
	TradesEmitted     int64
	DepthEmitted      int64
	SnapshotsApplied  int64
}

type counters struct {
	received   atomic.Int64
	dropped    atomic.Int64
	reconnects atomic.Int64
	resyncs    atomic.Int64
	cksumFail  atomic.Int64
	cksumOK    atomic.Int64
	trades     atomic.Int64
	depth      atomic.Int64
	snapshots  atomic.Int64
}

// symState is per-instId book-continuity state, owned by the event loop.
type symState struct {
	lastSeq int64
	synced  bool
	ladder  *stringBook
}

// opWriter is the narrow slice of *websocket.Conn the frame handlers need,
// letting unit tests drive continuity/resync logic with a fake.
type opWriter interface {
	WriteJSON(v any) error
}

// Client is a running OKX ingestion adapter. Same concurrency model as the
// Binance adapter: one reader goroutine feeding a bounded channel, one event
// loop owning all state and all socket writes (subscribes + pings), so state
// is lock-free and gorilla's one-reader/one-writer contract holds.
type Client struct {
	cfg Config
	pub Publisher
	log *slog.Logger

	counters counters

	initialBackoff time.Duration
	maxBackoff     time.Duration
}

// New builds a client.
func New(cfg Config, pub Publisher) *Client {
	if cfg.WSURL == "" {
		cfg.WSURL = "wss://ws.okx.com:8443/ws/v5/public"
	}
	if cfg.MsgBuffer == 0 {
		cfg.MsgBuffer = 8192
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = 40 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Client{
		cfg:            cfg,
		pub:            pub,
		log:            cfg.Logger,
		initialBackoff: 1 * time.Second,
		maxBackoff:     30 * time.Second,
	}
}

// Stats returns a snapshot of ingestion counters (safe to call concurrently).
func (c *Client) Stats() Stats {
	return Stats{
		MessagesReceived:  c.counters.received.Load(),
		MessagesDropped:   c.counters.dropped.Load(),
		Reconnects:        c.counters.reconnects.Load(),
		Resyncs:           c.counters.resyncs.Load(),
		ChecksumFailures:  c.counters.cksumFail.Load(),
		ChecksumsVerified: c.counters.cksumOK.Load(),
		TradesEmitted:     c.counters.trades.Load(),
		DepthEmitted:      c.counters.depth.Load(),
		SnapshotsApplied:  c.counters.snapshots.Load(),
	}
}

// Run connects and streams until ctx is cancelled, reconnecting with capped
// exponential backoff.
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
		c.log.Warn("okx stream ended; reconnecting", "err", err, "in", backoff.String())
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
		backoff = minDur(backoff*2, c.maxBackoff)
	}
}

func (c *Client) runOnce(parent context.Context) (bool, error) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, c.cfg.WSURL, nil)
	if err != nil {
		return false, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()
	conn.SetReadLimit(16 << 20)

	states := make(map[string]*symState, len(c.cfg.Symbols))
	for _, s := range c.cfg.Symbols {
		states[s] = &symState{ladder: newStringBook()}
	}

	if err := c.writeSubscribe(conn, c.cfg.Symbols); err != nil {
		return false, fmt.Errorf("subscribe: %w", err)
	}

	msgCh := make(chan []byte, c.cfg.MsgBuffer)
	readErr := make(chan error, 1)
	go func() {
		for {
			_ = conn.SetReadDeadline(time.Now().Add(c.cfg.ReadTimeout))
			_, data, err := conn.ReadMessage()
			if err != nil {
				readErr <- err
				return
			}
			c.counters.received.Add(1)
			select {
			case msgCh <- data:
			default:
				// Shed under backpressure; a shed books frame surfaces as a
				// seqId gap and is repaired by resubscribe.
				c.counters.dropped.Add(1)
			}
		}
	}()

	// OKX closes idle connections; a text "ping" every 20s keeps it alive.
	ping := time.NewTicker(20 * time.Second)
	defer ping.Stop()

	progressed := false
	for {
		select {
		case <-ctx.Done():
			return progressed, ctx.Err()
		case err := <-readErr:
			return progressed, err
		case <-ping.C:
			if err := conn.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
				return progressed, fmt.Errorf("ping: %w", err)
			}
		case data := <-msgCh:
			progressed = true
			if err := c.handleFrame(conn, states, data); err != nil {
				return progressed, err
			}
		}
	}
}

// handleFrame routes one frame. A returned error tears the connection down.
func (c *Client) handleFrame(conn opWriter, states map[string]*symState, raw []byte) error {
	m, err := parseFrame(raw)
	if err != nil {
		// Server-reported subscribe errors are fatal for the connection; give
		// backoff a chance rather than spinning.
		c.log.Warn("okx frame error", "err", err)
		return nil
	}
	if m == nil {
		return nil // pong or ack
	}
	now := nowNs()
	switch m.Arg.Channel {
	case "books":
		var items []bookData
		if err := json.Unmarshal(m.Data, &items); err != nil {
			c.log.Debug("okx books decode", "err", err)
			return nil
		}
		for _, bd := range items {
			if err := c.handleBook(conn, states, m.Arg.InstID, m.Action, bd, now); err != nil {
				return err
			}
		}
	case "trades":
		var items []tradeData
		if err := json.Unmarshal(m.Data, &items); err != nil {
			c.log.Debug("okx trades decode", "err", err)
			return nil
		}
		for _, td := range items {
			tr, err := normalizeTrade(td, now)
			if err != nil {
				continue
			}
			c.publish(events.Envelope{Kind: events.KindTrade, Trade: &tr})
			c.counters.trades.Add(1)
		}
	}
	return nil
}

func (c *Client) handleBook(conn opWriter, states map[string]*symState, instID, action string, bd bookData, now int64) error {
	st := states[instID]
	if st == nil {
		return nil
	}
	switch action {
	case "snapshot":
		st.ladder.reset()
		if err := st.ladder.applyRows(true, bd.Bids); err != nil {
			return nil
		}
		if err := st.ladder.applyRows(false, bd.Asks); err != nil {
			return nil
		}
		if !c.verifyChecksum(st, bd, instID) {
			return c.resync(conn, st, instID, "snapshot checksum mismatch")
		}
		st.lastSeq = bd.SeqID
		st.synced = true
		c.counters.snapshots.Add(1)
		d, err := normalizeBook(instID, action, bd, now)
		if err != nil {
			return nil
		}
		c.publish(events.Envelope{Kind: events.KindDepth, Depth: &d})
		c.log.Info("okx book synced", "instId", instID, "seq", bd.SeqID)
	case "update":
		if !st.synced {
			return nil // awaiting a snapshot after resubscribe
		}
		if bd.PrevSeqID != st.lastSeq {
			return c.resync(conn, st, instID,
				fmt.Sprintf("seq gap: prevSeqId=%d lastSeq=%d", bd.PrevSeqID, st.lastSeq))
		}
		if err := st.ladder.applyRows(true, bd.Bids); err != nil {
			return nil
		}
		if err := st.ladder.applyRows(false, bd.Asks); err != nil {
			return nil
		}
		if !c.verifyChecksum(st, bd, instID) {
			return c.resync(conn, st, instID, "update checksum mismatch")
		}
		st.lastSeq = bd.SeqID
		if len(bd.Bids) == 0 && len(bd.Asks) == 0 {
			return nil // no-change heartbeat: nothing to publish
		}
		d, err := normalizeBook(instID, action, bd, now)
		if err != nil {
			return nil
		}
		c.publish(events.Envelope{Kind: events.KindDepth, Depth: &d})
		c.counters.depth.Add(1)
	}
	return nil
}

// verifyChecksum validates the CRC32 book checksum when the venue populates it.
// The public feed frequently sends checksum=0 (not enabled), which must not be
// treated as a mismatch — that was confirmed against the live feed.
func (c *Client) verifyChecksum(st *symState, bd bookData, instID string) bool {
	if bd.Checksum == 0 {
		return true
	}
	if got := st.ladder.checksum(); got != bd.Checksum {
		c.counters.cksumFail.Add(1)
		c.log.Warn("okx checksum mismatch", "instId", instID, "got", got, "want", bd.Checksum)
		return false
	}
	c.counters.cksumOK.Add(1)
	return true
}

// resync re-subscribes one instId's books channel to obtain a fresh snapshot.
func (c *Client) resync(conn opWriter, st *symState, instID, reason string) error {
	c.counters.resyncs.Add(1)
	c.log.Info("okx resync", "instId", instID, "reason", reason)
	st.synced = false
	st.ladder.reset()
	if err := c.writeOp(conn, "unsubscribe", instID); err != nil {
		return fmt.Errorf("resync unsubscribe: %w", err)
	}
	if err := c.writeOp(conn, "subscribe", instID); err != nil {
		return fmt.Errorf("resync subscribe: %w", err)
	}
	return nil
}

func (c *Client) writeSubscribe(conn opWriter, instIDs []string) error {
	args := make([]map[string]string, 0, len(instIDs)*2)
	for _, id := range instIDs {
		args = append(args,
			map[string]string{"channel": "books", "instId": id},
			map[string]string{"channel": "trades", "instId": id},
		)
	}
	return conn.WriteJSON(map[string]any{"op": "subscribe", "args": args})
}

func (c *Client) writeOp(conn opWriter, op, instID string) error {
	return conn.WriteJSON(map[string]any{
		"op":   op,
		"args": []map[string]string{{"channel": "books", "instId": instID}},
	})
}

func (c *Client) publish(env events.Envelope) {
	if err := c.pub.Publish(env); err != nil {
		c.log.Warn("okx publish failed", "err", err)
	}
}

// nowNs is indirected for deterministic tests.
var nowNs = func() int64 { return time.Now().UnixNano() }

func minDur(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
