package detect

import (
	"time"

	fp "github.com/argus-mss/argus/internal/fixedpoint"
)

// Config holds every detector threshold in one place. Defaults are tuned for
// liquid crypto majors (BTCUSDT/ETHUSDT); a real deployment would calibrate
// per-symbol from historical distributions. Every value is an interview talking
// point: each is a deliberate sensitivity/false-positive tradeoff.
type Config struct {
	// --- Spoofing / layering ---
	// A resting add counts as suspicious when its size exceeds this multiple of
	// the venue's recent typical resting size (a self-calibrating EMA).
	SpoofSizeMultiple float64
	// Minimum EMA samples before the size baseline is trusted.
	SpoofMinSamples int
	// A flagged add must be pulled (this fraction of its size removed) within
	// SpoofPullWindow, having executed less than SpoofMaxTradedFrac of its size.
	SpoofPullWindow    time.Duration
	SpoofPullFrac      float64
	SpoofMaxTradedFrac float64
	// Only levels within this distance of mid can move the market, so only these
	// are considered spoofs.
	SpoofMaxDistBps float64
	// Layering: this many distinct same-side levels pulled within LayeringWindow.
	LayeringLevels int
	LayeringWindow time.Duration

	// --- Momentum ignition ---
	// An ignition leg is a price move of at least IgniteBps with same-direction
	// net aggressor volume of at least IgniteVol within IgniteWindow, followed by
	// a reversal of ReverseFrac of the leg within ReverseWindow.
	IgniteBps     float64
	IgniteVol     fp.Value
	IgniteWindow  time.Duration
	ReverseFrac   float64
	ReverseWindow time.Duration

	// --- Quote stuffing ---
	// This many book updates within StuffWindow while at most StuffMaxTrades
	// execute — churn without price discovery.
	StuffChurn     int
	StuffMaxTrades int
	StuffWindow    time.Duration
	StuffCooldown  time.Duration

	// --- Wash-trade footprint (heuristic; see README limitations) ---
	// A cluster of at least WashMinTrades trades in a price band <= WashBandBps,
	// with both aggressor sides present (minority side >= WashBalance of trades)
	// and total volume >= WashMinVol.
	WashMinTrades int
	WashBandBps   float64
	WashBalance   float64
	WashMinVol    fp.Value
	WashWindow    time.Duration
	WashCooldown  time.Duration

	// --- Feature extraction (for the ML scorer) ---
	FeatureLevels    int
	FeatureWindow    time.Duration
	FeatureEmitEvery int
}

// DefaultConfig returns production-ish defaults.
func DefaultConfig() Config {
	return Config{
		SpoofSizeMultiple:  6,
		SpoofMinSamples:    15,
		SpoofPullWindow:    3 * time.Second,
		SpoofPullFrac:      0.7,
		SpoofMaxTradedFrac: 0.2,
		SpoofMaxDistBps:    25,
		LayeringLevels:     3,
		LayeringWindow:     4 * time.Second,

		IgniteBps:     15,
		IgniteVol:     fp.MustParse("5"),
		IgniteWindow:  1500 * time.Millisecond,
		ReverseFrac:   0.5,
		ReverseWindow: 2500 * time.Millisecond,

		StuffChurn:     250,
		StuffMaxTrades: 3,
		StuffWindow:    1 * time.Second,
		StuffCooldown:  5 * time.Second,

		WashMinTrades: 6,
		WashBandBps:   3,
		WashBalance:   0.3,
		WashMinVol:    fp.MustParse("1"),
		WashWindow:    2 * time.Second,
		WashCooldown:  5 * time.Second,

		FeatureLevels:    5,
		FeatureWindow:    2 * time.Second,
		FeatureEmitEvery: 20,
	}
}
