package detect

// Severity ranks alert urgency. It is emitted both as an integer (for
// thresholding) and a label (for humans and the dashboard).
type Severity int

const (
	SeverityLow Severity = iota + 1
	SeverityMedium
	SeverityHigh
	SeverityCritical
)

func (s Severity) String() string {
	switch s {
	case SeverityLow:
		return "low"
	case SeverityMedium:
		return "medium"
	case SeverityHigh:
		return "high"
	case SeverityCritical:
		return "critical"
	default:
		return "unknown"
	}
}

// Detector name constants keep producer and consumers in sync.
const (
	DetectorSpoofing      = "spoofing"
	DetectorLayering      = "layering"
	DetectorMomentum      = "momentum_ignition"
	DetectorQuoteStuffing = "quote_stuffing"
	DetectorWashTrade     = "wash_trade"
)

// Alert is the surveillance output. It is the unit appended to the tamper-
// evident audit log, so its JSON encoding must be stable — Evidence uses a
// map, whose keys Go marshals in sorted order, keeping encodings deterministic.
type Alert struct {
	ID              string         `json:"id"`
	TsNs            int64          `json:"ts_ns"`
	Detector        string         `json:"detector"`
	Symbol          string         `json:"symbol"`
	Severity        Severity       `json:"severity"`
	SeverityLabel   string         `json:"severity_label"`
	Score           float64        `json:"score"`
	Description     string         `json:"description"`
	DetectLatencyUs int64          `json:"detect_latency_us"`
	Evidence        map[string]any `json:"evidence,omitempty"`
}
