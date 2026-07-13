// Package detect is the surveillance core: it consumes normalized market-data
// envelopes, maintains per-symbol order books, and runs a set of sliding-window
// detectors that emit alerts for manipulation footprints. It also extracts
// order-flow feature vectors for the optional ML scorer.
//
// Important framing: public exchange feeds are anonymous (no participant IDs),
// so these detectors identify market *footprints consistent with* manipulation
// — the tape-level signal a surveillance desk triages before pulling attributed
// order data. They do not, and cannot from L2 alone, prove intent. See README.
package detect

import (
	"fmt"

	"github.com/argus-mss/argus/internal/events"
	"github.com/argus-mss/argus/internal/features"
	"github.com/argus-mss/argus/internal/orderbook"
)

// SymbolState is the shared per-symbol context detectors read from.
type SymbolState struct {
	Symbol string
	Book   *orderbook.Book
}

// Detector is one manipulation pattern. Instances are created per symbol, so a
// detector may hold private per-symbol window state without any locking.
type Detector interface {
	Name() string
	OnTrade(now int64, st *SymbolState, tr events.Trade) []Alert
	OnDepth(now int64, st *SymbolState, d events.Depth, deltas []orderbook.LevelDelta) []Alert
}

// Factory constructs a per-symbol detector.
type Factory func(symbol string, cfg Config) Detector

// DefaultFactories returns the full rule-based detector suite.
func DefaultFactories() []Factory {
	return []Factory{
		newSpoofingDetector,
		newMomentumDetector,
		newQuoteStuffingDetector,
		newWashTradeDetector,
	}
}

// Engine routes envelopes to detectors and publishes alerts + features. It is
// single-goroutine by contract (the transport delivers one subject's messages
// in order on one goroutine), so its state needs no locks.
type Engine struct {
	cfg          Config
	factories    []Factory
	rts          map[string]*runtime
	emitAlert    func(Alert)
	emitFeatures func(features.Vector)
	alertSeq     uint64
	now          func() int64
}

type runtime struct {
	state      *SymbolState
	detectors  []Detector
	tradeW     *Window[float64] // signed aggressor qty for feature window
	churnW     *Window[bool]    // true=cancel, false=add
	eventCount int
}

// Option customizes an Engine.
type Option func(*Engine)

// WithFactories overrides the detector suite (used in tests).
func WithFactories(f ...Factory) Option { return func(e *Engine) { e.factories = f } }

// WithClock overrides the time source (used in tests for determinism).
func WithClock(now func() int64) Option { return func(e *Engine) { e.now = now } }

// WithFeatures sets the feature-vector sink.
func WithFeatures(emit func(features.Vector)) Option {
	return func(e *Engine) { e.emitFeatures = emit }
}

// NewEngine builds an engine that publishes alerts via emitAlert.
func NewEngine(cfg Config, emitAlert func(Alert), opts ...Option) *Engine {
	e := &Engine{
		cfg:       cfg,
		factories: DefaultFactories(),
		rts:       make(map[string]*runtime),
		emitAlert: emitAlert,
		now:       nowNs,
	}
	for _, o := range opts {
		o(e)
	}
	return e
}

func (e *Engine) runtimeFor(symbol string) *runtime {
	rt, ok := e.rts[symbol]
	if ok {
		return rt
	}
	st := &SymbolState{Symbol: symbol, Book: orderbook.New(symbol, "")}
	dets := make([]Detector, 0, len(e.factories))
	for _, f := range e.factories {
		dets = append(dets, f(symbol, e.cfg))
	}
	rt = &runtime{
		state:     st,
		detectors: dets,
		tradeW:    NewWindow[float64](e.cfg.FeatureWindow),
		churnW:    NewWindow[bool](e.cfg.FeatureWindow),
	}
	e.rts[symbol] = rt
	return rt
}

// HandleEnvelope is the single entry point: dispatch one market-data event to
// the relevant symbol's book, detectors, and feature extractor.
func (e *Engine) HandleEnvelope(env events.Envelope) {
	now := e.now()
	switch env.Kind {
	case events.KindDepth:
		if env.Depth == nil {
			return
		}
		d := *env.Depth
		rt := e.runtimeFor(d.Symbol)
		deltas := rt.state.Book.Apply(d)
		for _, det := range rt.detectors {
			for _, a := range det.OnDepth(now, rt.state, d, deltas) {
				e.publish(a, now, d.IngestTsNs)
			}
		}
		for _, dl := range deltas {
			rt.churnW.Add(now, dl.Removed())
		}
		e.maybeEmitFeatures(rt, now)
	case events.KindTrade:
		if env.Trade == nil {
			return
		}
		tr := *env.Trade
		rt := e.runtimeFor(tr.Symbol)
		for _, det := range rt.detectors {
			for _, a := range det.OnTrade(now, rt.state, tr) {
				e.publish(a, now, tr.IngestTsNs)
			}
		}
		signed := tr.Qty.Float()
		if tr.Aggressor == events.Sell {
			signed = -signed
		}
		rt.tradeW.Add(now, signed)
		e.maybeEmitFeatures(rt, now)
	}
}

func (e *Engine) publish(a Alert, now, ingestTsNs int64) {
	e.alertSeq++
	if a.ID == "" {
		a.ID = fmt.Sprintf("%s-%s-%d", a.Symbol, a.Detector, e.alertSeq)
	}
	if a.TsNs == 0 {
		a.TsNs = now
	}
	a.SeverityLabel = a.Severity.String()
	if lat := (now - ingestTsNs) / 1000; lat >= 0 {
		a.DetectLatencyUs = lat
	}
	e.emitAlert(a)
}

func (e *Engine) maybeEmitFeatures(rt *runtime, now int64) {
	rt.eventCount++
	if e.emitFeatures == nil || rt.eventCount%e.cfg.FeatureEmitEvery != 0 {
		return
	}
	if v, ok := e.buildFeatures(rt, now); ok {
		e.emitFeatures(v)
	}
}

func (e *Engine) buildFeatures(rt *runtime, now int64) (features.Vector, bool) {
	b := rt.state.Book
	if !b.Synced() {
		return features.Vector{}, false
	}
	mid, ok := b.Mid()
	if !ok || mid == 0 {
		return features.Vector{}, false
	}
	n := e.cfg.FeatureLevels
	bidSz := b.SumQty(events.Buy, n).Float()
	askSz := b.SumQty(events.Sell, n).Float()
	ofi := 0.0
	if bidSz+askSz > 0 {
		ofi = (bidSz - askSz) / (bidSz + askSz)
	}
	spreadBps := 0.0
	if sp, ok := b.Spread(); ok {
		spreadBps = sp.Float() / mid * 1e4
	}
	microDevBps := 0.0
	if micro, ok := b.Microprice(); ok {
		microDevBps = (micro - mid) / mid * 1e4
	}

	rt.tradeW.Evict(now)
	rt.churnW.Evict(now)
	var signedVol float64
	rt.tradeW.ForEach(func(_ int64, v float64) { signedVol += v })
	winSecs := float64(e.cfg.FeatureWindow) / 1e9
	intensity := 0.0
	if winSecs > 0 {
		intensity = float64(rt.tradeW.Len()) / winSecs
	}
	cancels := 0
	rt.churnW.ForEach(func(_ int64, isCancel bool) {
		if isCancel {
			cancels++
		}
	})
	cancelRatio := 0.0
	if rt.churnW.Len() > 0 {
		cancelRatio = float64(cancels) / float64(rt.churnW.Len())
	}

	return features.Vector{
		Symbol: rt.state.Symbol,
		TsNs:   now,
		Features: map[string]float64{
			features.SpreadBps:        spreadBps,
			features.OFI:              ofi,
			features.MicropriceDevBps: microDevBps,
			features.TradeIntensity:   intensity,
			features.SignedVol:        signedVol,
			features.CancelRatio:      cancelRatio,
			features.BidDepth:         float64(b.Depth(events.Buy)),
			features.AskDepth:         float64(b.Depth(events.Sell)),
		},
	}, true
}

// bps returns (a-b)/b in basis points.
func bps(a, b float64) float64 {
	if b == 0 {
		return 0
	}
	return (a - b) / b * 1e4
}
