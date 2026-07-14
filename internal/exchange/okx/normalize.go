// Package okx is the ingestion adapter for OKX spot public market data. It
// demonstrates that the normalized event schema earns its keep: a second venue
// with a completely different wire protocol (snapshot-on-subscribe instead of
// REST anchoring, seqId/prevSeqId continuity instead of U/u brackets, CRC32
// book checksums) maps into the same internal events, so every detector works
// on OKX data without a single change.
package okx

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/argus-mss/argus/internal/events"
	"github.com/argus-mss/argus/internal/fixedpoint"
)

const exchangeName = "okx"

// wsMsg is the outer OKX frame: either an event ack
// ({"event":"subscribe",...}) or a data push ({"arg":...,"data":[...]}).
type wsMsg struct {
	Event  string `json:"event"`
	Code   string `json:"code"`
	Msg    string `json:"msg"`
	Action string `json:"action"` // books only: "snapshot" | "update"
	Arg    struct {
		Channel string `json:"channel"`
		InstID  string `json:"instId"`
	} `json:"arg"`
	Data json.RawMessage `json:"data"`
}

// bookData is one item of a books push. Levels are [px, sz, deprecated,
// numOrders] as decimal strings; sz "0" removes the level.
type bookData struct {
	Asks      [][]string `json:"asks"`
	Bids      [][]string `json:"bids"`
	Ts        string     `json:"ts"`
	Checksum  int32      `json:"checksum"`
	SeqID     int64      `json:"seqId"`
	PrevSeqID int64      `json:"prevSeqId"`
}

// tradeData is one item of a trades push. Side is the taker side.
type tradeData struct {
	InstID  string `json:"instId"`
	TradeID string `json:"tradeId"`
	Px      string `json:"px"`
	Sz      string `json:"sz"`
	Side    string `json:"side"`
	Ts      string `json:"ts"`
}

// parseFrame decodes an OKX frame. Plain "pong" replies and event acks yield
// (nil, "", nil); errors on event frames with an error code are surfaced.
func parseFrame(raw []byte) (*wsMsg, error) {
	if len(raw) == 4 && string(raw) == "pong" {
		return nil, nil
	}
	var m wsMsg
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("okx: decode frame: %w", err)
	}
	if m.Event == "error" {
		return nil, fmt.Errorf("okx: server error code=%s msg=%q", m.Code, m.Msg)
	}
	if m.Event != "" { // subscribe/unsubscribe acks
		return nil, nil
	}
	return &m, nil
}

// normalizeBook converts a bookData item into the internal Depth event.
func normalizeBook(instID string, action string, bd bookData, ingestNs int64) (events.Depth, error) {
	bids, err := parseLevels(bd.Bids)
	if err != nil {
		return events.Depth{}, fmt.Errorf("okx bids: %w", err)
	}
	asks, err := parseLevels(bd.Asks)
	if err != nil {
		return events.Depth{}, fmt.Errorf("okx asks: %w", err)
	}
	tsMs, _ := strconv.ParseInt(bd.Ts, 10, 64)
	return events.Depth{
		Symbol:        instID,
		FirstUpdateID: bd.PrevSeqID,
		FinalUpdateID: bd.SeqID,
		Bids:          bids,
		Asks:          asks,
		ExchangeTsMs:  tsMs,
		IngestTsNs:    ingestNs,
		Exchange:      exchangeName,
		IsSnapshot:    action == "snapshot",
	}, nil
}

// normalizeTrade converts a tradeData item into the internal Trade event.
func normalizeTrade(td tradeData, ingestNs int64) (events.Trade, error) {
	px, err := fixedpoint.Parse(td.Px)
	if err != nil {
		return events.Trade{}, fmt.Errorf("okx trade px %q: %w", td.Px, err)
	}
	sz, err := fixedpoint.Parse(td.Sz)
	if err != nil {
		return events.Trade{}, fmt.Errorf("okx trade sz %q: %w", td.Sz, err)
	}
	id, _ := strconv.ParseInt(td.TradeID, 10, 64)
	tsMs, _ := strconv.ParseInt(td.Ts, 10, 64)
	aggressor := events.Buy
	if td.Side == "sell" {
		aggressor = events.Sell
	}
	return events.Trade{
		Symbol:       td.InstID,
		Price:        px,
		Qty:          sz,
		Aggressor:    aggressor,
		TradeID:      id,
		ExchangeTsMs: tsMs,
		IngestTsNs:   ingestNs,
		Exchange:     exchangeName,
	}, nil
}

// parseLevels converts OKX [px, sz, _, count] rows to fixed-point levels.
func parseLevels(raw [][]string) ([]events.Level, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	out := make([]events.Level, 0, len(raw))
	for _, row := range raw {
		if len(row) < 2 {
			return nil, fmt.Errorf("malformed level %v", row)
		}
		px, err := fixedpoint.Parse(row[0])
		if err != nil {
			return nil, fmt.Errorf("level px %q: %w", row[0], err)
		}
		sz, err := fixedpoint.Parse(row[1])
		if err != nil {
			return nil, fmt.Errorf("level sz %q: %w", row[1], err)
		}
		out = append(out, events.Level{Price: px, Qty: sz})
	}
	return out, nil
}
