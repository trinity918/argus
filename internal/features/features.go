// Package features defines the order-flow feature vector shared between the Go
// detection engine (producer) and the Python ML scorer (consumer). Keeping the
// schema here — and stable — is what lets the two languages agree over plain
// JSON on the transport bus without a codegen step.
package features

// These are the canonical feature keys. The ML scorer reads them by name, so
// additions are backward-compatible but renames are breaking.
const (
	SpreadBps        = "spread_bps"         // (ask-bid)/mid in basis points
	OFI              = "ofi"                // top-N order-flow imbalance in [-1,1]
	MicropriceDevBps = "microprice_dev_bps" // (microprice-mid)/mid in basis points
	TradeIntensity   = "trade_intensity"    // trades per second over the window
	SignedVol        = "signed_vol"         // net aggressor volume over the window
	CancelRatio      = "cancel_ratio"       // cancels / (adds+cancels) over the window
	BidDepth         = "bid_depth"          // populated bid levels
	AskDepth         = "ask_depth"          // populated ask levels
)

// Vector is a timestamped order-flow feature sample for one symbol.
type Vector struct {
	Symbol   string             `json:"symbol"`
	TsNs     int64              `json:"ts_ns"`
	Features map[string]float64 `json:"features"`
}
