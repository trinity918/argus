package detect

import (
	"github.com/argus-mss/argus/internal/events"
	fp "github.com/argus-mss/argus/internal/fixedpoint"
	"github.com/argus-mss/argus/internal/orderbook"
)

// spoofingDetector flags the footprint of spoofing and layering on anonymous
// L2 data: an unusually large order appearing near top-of-book that is then
// cancelled — without having executed — before the market reaches it. Layering
// is the same footprint stacked across several price levels on one side.
//
// "Unusually large" is defined against a self-calibrating EMA of recent resting
// sizes, so the detector adapts to each venue's liquidity without hand-set
// notional thresholds.
type spoofingDetector struct {
	symbol string
	cfg    Config

	avgSize float64 // EMA of typical resting add size
	samples int

	pending map[levelKey]*pendingAdd
	pulls   *Window[pullEvent] // recent pulls, for layering

	lastSpoofFire int64 // alert-emission cooldowns (pull tracking is unthrottled)
	lastLayerFire int64
}

type levelKey struct {
	side  events.Side
	price fp.Value
}

type pendingAdd struct {
	qty    fp.Value // resting size when flagged
	ts     int64
	traded fp.Value // qty executed at this level since it was flagged
}

type pullEvent struct {
	side  events.Side
	price fp.Value
}

func newSpoofingDetector(symbol string, cfg Config) Detector {
	return &spoofingDetector{
		symbol:  symbol,
		cfg:     cfg,
		pending: make(map[levelKey]*pendingAdd),
		pulls:   NewWindow[pullEvent](cfg.LayeringWindow),
	}
}

func (d *spoofingDetector) Name() string { return DetectorSpoofing }

const emaAlpha = 0.05

func (d *spoofingDetector) OnTrade(now int64, st *SymbolState, tr events.Trade) []Alert {
	// Executions at a flagged level mean the order was (partly) genuine, not a
	// pull. Attribute traded volume to any pending level at that price.
	for _, side := range []events.Side{events.Buy, events.Sell} {
		if p, ok := d.pending[levelKey{side, tr.Price}]; ok {
			p.traded += tr.Qty
		}
	}
	return nil
}

func (d *spoofingDetector) OnDepth(now int64, st *SymbolState, dep events.Depth, deltas []orderbook.LevelDelta) []Alert {
	if dep.IsSnapshot {
		return nil
	}
	d.sweepExpired(now)
	mid, haveMid := st.Book.Mid()
	var alerts []Alert
	for _, dl := range deltas {
		key := levelKey{dl.Side, dl.Price}
		switch {
		case dl.Added():
			added := dl.NewQty.Float()
			flagged := false
			if haveMid && d.samples >= d.cfg.SpoofMinSamples &&
				added >= d.cfg.SpoofSizeMultiple*d.avgSize &&
				withinDist(dl.Price, mid, d.cfg.SpoofMaxDistBps) {
				d.pending[key] = &pendingAdd{qty: dl.NewQty, ts: now}
				flagged = true
			}
			if !flagged {
				// Only genuine (non-spoof) sizes update the liquidity baseline.
				d.updateEMA(added)
			}
		case dl.Removed():
			a, emitted, pulled := d.evalPull(now, key, dl)
			if emitted {
				alerts = append(alerts, a)
			}
			// Layering is evaluated on every qualifying pull, even when the
			// individual spoof alert was throttled — a layering pattern must
			// not hide behind the spoof cooldown.
			if pulled {
				if la, ok := d.evalLayering(now, dl.Side); ok {
					alerts = append(alerts, la)
				}
			}
		}
	}
	return alerts
}

// evalPull decides whether a size reduction at a flagged level is a spoof
// pull. It returns the alert, whether it should be emitted (cooldown passed),
// and whether the pull qualified at all (and was therefore recorded for
// layering detection regardless of emission).
func (d *spoofingDetector) evalPull(now int64, key levelKey, dl orderbook.LevelDelta) (Alert, bool, bool) {
	p, ok := d.pending[key]
	if !ok {
		return Alert{}, false, false
	}
	// Removed enough of the flagged size?
	remaining := dl.NewQty.Float()
	if remaining > (1-d.cfg.SpoofPullFrac)*p.qty.Float() {
		return Alert{}, false, false
	}
	// Cancelled within the window?
	if now-p.ts > int64(d.cfg.SpoofPullWindow) {
		delete(d.pending, key)
		return Alert{}, false, false
	}
	// Largely unexecuted?
	if p.traded.Float() > d.cfg.SpoofMaxTradedFrac*p.qty.Float() {
		delete(d.pending, key)
		return Alert{}, false, false
	}
	delete(d.pending, key)
	d.pulls.Add(now, pullEvent{side: key.side, price: key.price})

	// Alert-emission throttle: on a volatile live tape, qualifying pulls can
	// occur many times per second; the audit trail records the pattern, not
	// every instance of it.
	if now-d.lastSpoofFire < int64(d.cfg.SpoofCooldown) {
		return Alert{}, false, true
	}
	d.lastSpoofFire = now

	lifeMs := float64(now-p.ts) / 1e6
	score := clamp01(p.qty.Float() / (d.cfg.SpoofSizeMultiple * d.avgSize) / 3)
	return Alert{
		TsNs:        now,
		Detector:    DetectorSpoofing,
		Symbol:      d.symbol,
		Severity:    SeverityHigh,
		Score:       score,
		Description: "large resting order pulled before execution near top-of-book",
		Evidence: map[string]any{
			"side":          key.side.String(),
			"price":         key.price.String(),
			"flagged_qty":   p.qty.String(),
			"typical_qty":   round4(d.avgSize),
			"size_multiple": round4(p.qty.Float() / d.avgSize),
			"lifetime_ms":   round4(lifeMs),
			"traded_qty":    p.traded.String(),
		},
	}, true, true
}

// evalLayering fires when enough distinct same-side levels were pulled recently.
func (d *spoofingDetector) evalLayering(now int64, side events.Side) (Alert, bool) {
	d.pulls.Evict(now)
	distinct := make(map[fp.Value]struct{})
	d.pulls.ForEach(func(_ int64, pe pullEvent) {
		if pe.side == side {
			distinct[pe.price] = struct{}{}
		}
	})
	if len(distinct) < d.cfg.LayeringLevels {
		return Alert{}, false
	}
	if now-d.lastLayerFire < int64(d.cfg.LayeringCooldown) {
		return Alert{}, false
	}
	d.lastLayerFire = now
	return Alert{
		TsNs:        now,
		Detector:    DetectorLayering,
		Symbol:      d.symbol,
		Severity:    SeverityCritical,
		Score:       clamp01(float64(len(distinct)) / float64(d.cfg.LayeringLevels) / 2),
		Description: "multiple large orders layered on one side and pulled in concert",
		Evidence: map[string]any{
			"side":          side.String(),
			"levels_pulled": len(distinct),
			"window_ms":     round4(float64(d.cfg.LayeringWindow) / 1e6),
		},
	}, true
}

func (d *spoofingDetector) sweepExpired(now int64) {
	for k, p := range d.pending {
		if now-p.ts > int64(d.cfg.SpoofPullWindow) {
			delete(d.pending, k)
		}
	}
}

func (d *spoofingDetector) updateEMA(v float64) {
	if d.samples == 0 {
		d.avgSize = v
	} else {
		d.avgSize += emaAlpha * (v - d.avgSize)
	}
	d.samples++
}

func withinDist(price fp.Value, mid, maxBps float64) bool {
	dev := (price.Float() - mid) / mid * 1e4
	if dev < 0 {
		dev = -dev
	}
	return dev <= maxBps
}

func clamp01(x float64) float64 {
	if x < 0 {
		return 0
	}
	if x > 1 {
		return 1
	}
	return x
}
