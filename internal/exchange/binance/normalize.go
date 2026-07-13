package binance

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/argus-mss/argus/internal/events"
	"github.com/argus-mss/argus/internal/fixedpoint"
)

const exchangeName = "binance"

// combinedMsg is the outer wrapper Binance sends on a combined stream:
// {"stream":"btcusdt@depth@100ms","data":{...}}.
type combinedMsg struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

// eventType peeks at the lowercase "e" discriminator without fully decoding.
//
// Go's encoding/json matches object keys case-insensitively, so a struct field
// tagged "e" would otherwise greedily absorb the uppercase "E" event-time
// *number* and fail to unmarshal into a string. The EventTime field gives "E"
// an exact-match home so the discriminator routes correctly. Every raw payload
// struct below applies the same technique.
type eventType struct {
	E         string          `json:"e"`
	EventTime json.RawMessage `json:"E"`
}

// rawDepth mirrors the depthUpdate payload. Price/qty are decimal strings,
// which we parse exactly into fixed point.
type rawDepth struct {
	Ev string     `json:"e"` // event type (exact-match home for lowercase "e")
	E  int64      `json:"E"` // event time (ms)
	S  string     `json:"s"` // symbol
	U  int64      `json:"U"` // first update id in event
	Uu int64      `json:"u"` // final update id in event
	B  [][]string `json:"b"` // bids [price, qty]
	A  [][]string `json:"a"` // asks
}

// rawTrade mirrors the trade payload.
type rawTrade struct {
	Ev string `json:"e"` // event type (exact-match home for lowercase "e")
	E  int64  `json:"E"` // event time (ms)
	S  string `json:"s"`
	T  int64  `json:"t"` // trade id
	P  string `json:"p"` // price
	Q  string `json:"q"` // quantity
	Tt int64  `json:"T"` // trade time (ms)
	M  bool   `json:"m"` // is the buyer the market maker?
}

// parseMessage decodes one combined-stream frame into a normalized Envelope.
// ingestNs is the local receipt time stamped for latency measurement. A nil
// Envelope with nil error means the frame was a control/unknown type to skip.
func parseMessage(raw []byte, ingestNs int64) (*events.Envelope, error) {
	var outer combinedMsg
	if err := json.Unmarshal(raw, &outer); err != nil {
		return nil, fmt.Errorf("decode combined frame: %w", err)
	}
	data := outer.Data
	if len(data) == 0 {
		// Not a combined frame (e.g. a subscription ack); ignore.
		return nil, nil
	}
	var et eventType
	if err := json.Unmarshal(data, &et); err != nil {
		return nil, fmt.Errorf("decode event type: %w", err)
	}
	switch et.E {
	case "depthUpdate":
		d, err := parseDepth(data, ingestNs)
		if err != nil {
			return nil, err
		}
		return &events.Envelope{Kind: events.KindDepth, Depth: d}, nil
	case "trade":
		t, err := parseTrade(data, ingestNs)
		if err != nil {
			return nil, err
		}
		return &events.Envelope{Kind: events.KindTrade, Trade: t}, nil
	default:
		return nil, nil // unknown/other stream type: skip
	}
}

func parseDepth(data []byte, ingestNs int64) (*events.Depth, error) {
	var r rawDepth
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("decode depthUpdate: %w", err)
	}
	bids, err := parseLevels(r.B)
	if err != nil {
		return nil, fmt.Errorf("bids: %w", err)
	}
	asks, err := parseLevels(r.A)
	if err != nil {
		return nil, fmt.Errorf("asks: %w", err)
	}
	return &events.Depth{
		Symbol:        r.S,
		FirstUpdateID: r.U,
		FinalUpdateID: r.Uu,
		Bids:          bids,
		Asks:          asks,
		ExchangeTsMs:  r.E,
		IngestTsNs:    ingestNs,
		Exchange:      exchangeName,
		IsSnapshot:    false,
	}, nil
}

func parseTrade(data []byte, ingestNs int64) (*events.Trade, error) {
	var r rawTrade
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, fmt.Errorf("decode trade: %w", err)
	}
	price, err := fixedpoint.Parse(r.P)
	if err != nil {
		return nil, fmt.Errorf("trade price %q: %w", r.P, err)
	}
	qty, err := fixedpoint.Parse(r.Q)
	if err != nil {
		return nil, fmt.Errorf("trade qty %q: %w", r.Q, err)
	}
	// m == true means the buyer was the maker, so the aggressor (taker) sold.
	aggressor := events.Buy
	if r.M {
		aggressor = events.Sell
	}
	return &events.Trade{
		Symbol:       r.S,
		Price:        price,
		Qty:          qty,
		Aggressor:    aggressor,
		TradeID:      r.T,
		ExchangeTsMs: r.Tt,
		IngestTsNs:   ingestNs,
		Exchange:     exchangeName,
	}, nil
}

// parseLevels converts [][price,qty] string pairs to fixed-point levels.
func parseLevels(raw [][]string) ([]events.Level, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	out := make([]events.Level, 0, len(raw))
	for _, pair := range raw {
		if len(pair) < 2 {
			return nil, fmt.Errorf("malformed level %v", pair)
		}
		price, err := fixedpoint.Parse(pair[0])
		if err != nil {
			return nil, fmt.Errorf("level price %q: %w", pair[0], err)
		}
		qty, err := fixedpoint.Parse(pair[1])
		if err != nil {
			return nil, fmt.Errorf("level qty %q: %w", pair[1], err)
		}
		out = append(out, events.Level{Price: price, Qty: qty})
	}
	return out, nil
}

// nowNs is indirected for deterministic tests.
var nowNs = func() int64 { return time.Now().UnixNano() }
