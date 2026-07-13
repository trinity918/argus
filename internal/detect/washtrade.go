package detect

import (
	"github.com/argus-mss/argus/internal/events"
	"github.com/argus-mss/argus/internal/orderbook"
)

// washTradeDetector flags a wash-trade *footprint*: a burst of trades confined
// to a razor-thin price band with aggressor side flip-flopping — volume printed
// without price discovery, the tape-level look of a party trading with itself.
//
// LIMITATION (stated honestly and surfaced in every alert): public feeds are
// anonymous, so this cannot confirm the two sides share a beneficial owner. It
// is a low-confidence triage signal, not proof — the README expands on this.
type washTradeDetector struct {
	symbol string
	cfg    Config

	trades   *Window[washTrade]
	lastFire int64
}

type washTrade struct {
	price float64
	qty   float64
	side  events.Side
}

func newWashTradeDetector(symbol string, cfg Config) Detector {
	return &washTradeDetector{
		symbol: symbol,
		cfg:    cfg,
		trades: NewWindow[washTrade](cfg.WashWindow),
	}
}

func (d *washTradeDetector) Name() string { return DetectorWashTrade }

func (d *washTradeDetector) OnDepth(int64, *SymbolState, events.Depth, []orderbook.LevelDelta) []Alert {
	return nil
}

func (d *washTradeDetector) OnTrade(now int64, st *SymbolState, tr events.Trade) []Alert {
	d.trades.Add(now, washTrade{price: tr.Price.Float(), qty: tr.Qty.Float(), side: tr.Aggressor})

	if d.trades.Len() < d.cfg.WashMinTrades {
		return nil
	}
	if now-d.lastFire < int64(d.cfg.WashCooldown) {
		return nil
	}

	var hi, lo, vol, buys, sells float64
	n := 0
	first := true
	d.trades.ForEach(func(_ int64, w washTrade) {
		if first {
			hi, lo = w.price, w.price
			first = false
		}
		if w.price > hi {
			hi = w.price
		}
		if w.price < lo {
			lo = w.price
		}
		vol += w.qty
		if w.side == events.Buy {
			buys += w.qty
		} else {
			sells += w.qty
		}
		n++
	})
	mid := (hi + lo) / 2
	if mid == 0 {
		return nil
	}
	bandBps := (hi - lo) / mid * 1e4
	minority := buys
	if sells < minority {
		minority = sells
	}
	balance := 0.0
	if vol > 0 {
		balance = minority / vol
	}

	// Tight band, both sides active, enough volume => footprint.
	if bandBps > d.cfg.WashBandBps || balance < d.cfg.WashBalance || vol < d.cfg.WashMinVol.Float() {
		return nil
	}
	d.lastFire = now
	return []Alert{{
		TsNs:        now,
		Detector:    DetectorWashTrade,
		Symbol:      d.symbol,
		Severity:    SeverityLow, // anonymous data => low confidence by construction
		Score:       clamp01(balance),
		Description: "clustered offsetting trades in a tight band with no price discovery (possible wash trading)",
		Evidence: map[string]any{
			"trades":       n,
			"band_bps":     round4(bandBps),
			"volume":       round4(vol),
			"side_balance": round4(balance),
			"note":         "anonymous L2 feed: footprint only, cannot confirm common ownership",
		},
	}}
}
