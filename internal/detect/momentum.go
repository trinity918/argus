package detect

import (
	"github.com/argus-mss/argus/internal/events"
	"github.com/argus-mss/argus/internal/orderbook"
)

// momentumDetector flags momentum ignition: an aggressive burst of same-side
// marketable orders that jerks the price, followed by a rapid reversal — the
// signature of a participant igniting a move to trigger others' momentum logic,
// then fading it. It is a two-phase state machine over the trade tape.
type momentumDetector struct {
	symbol string
	cfg    Config

	trades *Window[signedTrade]

	watching bool
	dir      events.Side
	legStart float64
	legPeak  float64
	peakTs   int64
}

type signedTrade struct {
	price float64
	qty   float64
	side  events.Side // aggressor
}

func newMomentumDetector(symbol string, cfg Config) Detector {
	return &momentumDetector{
		symbol: symbol,
		cfg:    cfg,
		trades: NewWindow[signedTrade](cfg.IgniteWindow),
	}
}

func (d *momentumDetector) Name() string { return DetectorMomentum }

func (d *momentumDetector) OnDepth(int64, *SymbolState, events.Depth, []orderbook.LevelDelta) []Alert {
	return nil
}

func (d *momentumDetector) OnTrade(now int64, st *SymbolState, tr events.Trade) []Alert {
	price := tr.Price.Float()
	d.trades.Add(now, signedTrade{price: price, qty: tr.Qty.Float(), side: tr.Aggressor})

	if d.watching {
		return d.trackReversal(now, price)
	}
	return d.detectLeg(now, price)
}

// detectLeg looks for a strong, one-sided directional move over the ignition
// window and, on finding one, arms the reversal watch.
func (d *momentumDetector) detectLeg(now int64, price float64) []Alert {
	startPrice, buyVol, sellVol, hi, lo := d.scanWindow()
	if startPrice == 0 {
		return nil
	}
	upMove := bps(hi, startPrice)
	downMove := -bps(lo, startPrice)
	netUp := buyVol - sellVol
	igniteVol := d.cfg.IgniteVol.Float()

	switch {
	case upMove >= d.cfg.IgniteBps && netUp >= igniteVol:
		d.arm(events.Buy, startPrice, hi, now)
	case downMove >= d.cfg.IgniteBps && -netUp >= igniteVol:
		d.arm(events.Sell, startPrice, lo, now)
	}
	return nil
}

func (d *momentumDetector) arm(dir events.Side, start, peak float64, now int64) {
	d.watching = true
	d.dir = dir
	d.legStart = start
	d.legPeak = peak
	d.peakTs = now
}

// trackReversal extends the leg's extreme and fires when price retraces
// ReverseFrac of the leg within ReverseWindow; it disarms on timeout.
func (d *momentumDetector) trackReversal(now int64, price float64) []Alert {
	if now-d.peakTs > int64(d.cfg.ReverseWindow) {
		d.watching = false
		return nil
	}
	legMag := d.legPeak - d.legStart // signed: >0 up, <0 down
	if d.dir == events.Buy {
		if price > d.legPeak {
			d.legPeak = price
			d.peakTs = now
			legMag = d.legPeak - d.legStart
		}
		if price <= d.legPeak-d.cfg.ReverseFrac*legMag {
			return d.fire(now, price)
		}
	} else {
		if price < d.legPeak {
			d.legPeak = price
			d.peakTs = now
			legMag = d.legPeak - d.legStart
		}
		if price >= d.legPeak-d.cfg.ReverseFrac*legMag { // legMag<0, so this is legPeak+frac*|mag|
			return d.fire(now, price)
		}
	}
	return nil
}

func (d *momentumDetector) fire(now int64, price float64) []Alert {
	legBps := bps(d.legPeak, d.legStart)
	retraceBps := bps(price, d.legPeak)
	d.watching = false
	return []Alert{{
		TsNs:        now,
		Detector:    DetectorMomentum,
		Symbol:      d.symbol,
		Severity:    SeverityHigh,
		Score:       clamp01(absf(legBps) / (2 * d.cfg.IgniteBps)),
		Description: "aggressive directional burst followed by rapid reversal (momentum ignition)",
		Evidence: map[string]any{
			"direction":     d.dir.String(),
			"leg_bps":       round4(legBps),
			"retrace_bps":   round4(retraceBps),
			"leg_start":     round4(d.legStart),
			"leg_peak":      round4(d.legPeak),
			"reverse_price": round4(price),
		},
	}}
}

// scanWindow summarizes the current ignition window: the oldest price, buy/sell
// aggressor volume, and the high/low reached.
func (d *momentumDetector) scanWindow() (start, buyVol, sellVol, hi, lo float64) {
	first := true
	d.trades.ForEach(func(_ int64, s signedTrade) {
		if first {
			start, hi, lo = s.price, s.price, s.price
			first = false
		}
		if s.price > hi {
			hi = s.price
		}
		if s.price < lo {
			lo = s.price
		}
		if s.side == events.Buy {
			buyVol += s.qty
		} else {
			sellVol += s.qty
		}
	})
	return start, buyVol, sellVol, hi, lo
}

func absf(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
