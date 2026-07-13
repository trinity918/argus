package detect

import (
	"github.com/argus-mss/argus/internal/events"
	"github.com/argus-mss/argus/internal/orderbook"
)

// quoteStuffingDetector flags order-book churn without price discovery: a flood
// of add/cancel updates within a short window while almost nothing executes.
// This is the footprint of quote stuffing — spamming the book to slow down
// competitors or obscure genuine liquidity.
type quoteStuffingDetector struct {
	symbol string
	cfg    Config

	churn    *Window[struct{}] // one entry per book-level update
	trades   *Window[struct{}]
	lastFire int64
}

func newQuoteStuffingDetector(symbol string, cfg Config) Detector {
	return &quoteStuffingDetector{
		symbol: symbol,
		cfg:    cfg,
		churn:  NewWindow[struct{}](cfg.StuffWindow),
		trades: NewWindow[struct{}](cfg.StuffWindow),
	}
}

func (d *quoteStuffingDetector) Name() string { return DetectorQuoteStuffing }

func (d *quoteStuffingDetector) OnTrade(now int64, st *SymbolState, tr events.Trade) []Alert {
	d.trades.Add(now, struct{}{})
	return nil
}

func (d *quoteStuffingDetector) OnDepth(now int64, st *SymbolState, dep events.Depth, deltas []orderbook.LevelDelta) []Alert {
	if dep.IsSnapshot {
		return nil
	}
	for range deltas {
		d.churn.Add(now, struct{}{})
	}
	d.trades.Evict(now)

	if d.churn.Len() < d.cfg.StuffChurn || d.trades.Len() > d.cfg.StuffMaxTrades {
		return nil
	}
	if now-d.lastFire < int64(d.cfg.StuffCooldown) {
		return nil
	}
	d.lastFire = now
	churn := d.churn.Len()
	return []Alert{{
		TsNs:        now,
		Detector:    DetectorQuoteStuffing,
		Symbol:      d.symbol,
		Severity:    SeverityMedium,
		Score:       clamp01(float64(churn) / float64(2*d.cfg.StuffChurn)),
		Description: "high book update rate with negligible executions (quote stuffing)",
		Evidence: map[string]any{
			"updates_in_window": churn,
			"trades_in_window":  d.trades.Len(),
			"window_ms":         round4(float64(d.cfg.StuffWindow) / 1e6),
		},
	}}
}
