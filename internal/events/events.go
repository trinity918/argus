// Package events defines the single normalized schema that every exchange
// adapter maps into. Detectors and the audit layer only ever see these types,
// so adding a new venue (Coinbase, Kraken, …) never touches detection logic.
package events

import "github.com/argus-mss/argus/internal/fixedpoint"

// Side is the taker (aggressor) side of a trade, or the side of a book level.
type Side uint8

const (
	// Buy means the aggressor lifted the offer (upward pressure).
	Buy Side = iota
	// Sell means the aggressor hit the bid (downward pressure).
	Sell
)

func (s Side) String() string {
	if s == Buy {
		return "buy"
	}
	return "sell"
}

// Kind discriminates the event union carried on the transport bus.
type Kind uint8

const (
	KindTrade Kind = iota
	KindDepth
)

// Level is a single price level in a depth update: an absolute new quantity at
// a price. Qty == 0 means the level was removed.
type Level struct {
	Price fixedpoint.Value `json:"p"`
	Qty   fixedpoint.Value `json:"q"`
}

// Trade is a normalized execution print.
type Trade struct {
	Symbol string           `json:"symbol"`
	Price  fixedpoint.Value `json:"price"`
	Qty    fixedpoint.Value `json:"qty"`
	// Aggressor is the side of the taker. On Binance this is derived from the
	// "is buyer the maker" flag: maker-buyer => aggressor sold.
	Aggressor Side  `json:"aggressor"`
	TradeID   int64 `json:"trade_id"`
	// ExchangeTsMs is the venue event timestamp (ms since epoch).
	ExchangeTsMs int64 `json:"exchange_ts_ms"`
	// IngestTsNs is the local monotonic-ish receipt time (ns), used to measure
	// end-to-end detection latency.
	IngestTsNs int64  `json:"ingest_ts_ns"`
	Exchange   string `json:"exchange"`
}

// Depth is a normalized incremental L2 update (or a snapshot when IsSnapshot).
type Depth struct {
	Symbol string `json:"symbol"`
	// FirstUpdateID / FinalUpdateID mirror Binance's U/u fields and drive the
	// local-book sequencing invariant (each event's U == previous u + 1).
	FirstUpdateID int64   `json:"first_update_id"`
	FinalUpdateID int64   `json:"final_update_id"`
	Bids          []Level `json:"bids"`
	Asks          []Level `json:"asks"`
	ExchangeTsMs  int64   `json:"exchange_ts_ms"`
	IngestTsNs    int64   `json:"ingest_ts_ns"`
	Exchange      string  `json:"exchange"`
	IsSnapshot    bool    `json:"is_snapshot"`
}

// Envelope is the wire form used on the transport bus. Exactly one of the
// pointers is non-nil, discriminated by Kind, so a single subscriber can
// receive both trades and depth in order.
type Envelope struct {
	Kind  Kind   `json:"kind"`
	Trade *Trade `json:"trade,omitempty"`
	Depth *Depth `json:"depth,omitempty"`
}
