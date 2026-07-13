// Package transport is the fan-out layer between ingestion and detection. It
// exposes one small Bus interface with two implementations:
//
//   - inproc: synchronous, in-order, zero-dependency. Powers the single-binary
//     demo and every unit/integration test, so CI needs no broker.
//   - natsbus: real NATS, the production topology where ingestors and detectors
//     are separate processes/containers scaled independently.
//
// Keeping detection code written against Bus (not *nats.Conn) is what lets the
// same detectors run in a laptop demo and in a multi-container deployment
// without change.
package transport

import "strings"

// Handler receives the concrete subject a message arrived on plus its payload.
type Handler func(subject string, data []byte)

// Subscription is a live subscription that can be torn down.
type Subscription interface {
	Unsubscribe() error
}

// Bus is a subject-addressed publish/subscribe transport. Subjects use the
// NATS token grammar (dot-separated, '*' = one token, '>' = one-or-more tail
// tokens) so the inproc and NATS implementations are behaviorally identical.
type Bus interface {
	Publish(subject string, data []byte) error
	Subscribe(subject string, h Handler) (Subscription, error)
	Close() error
}

// Subject helpers centralize the naming scheme so producers and consumers can
// never drift apart.

// MarketData is the per-symbol subject carrying interleaved trade+depth
// envelopes. Ordering is preserved per subject, which detectors depend on.
func MarketData(symbol string) string { return "md." + strings.ToUpper(symbol) }

// MarketDataAll matches every symbol's market-data stream.
const MarketDataAll = "md.*"

// Features is the per-symbol subject carrying order-flow feature vectors for
// the ML scorer.
func Features(symbol string) string { return "features." + strings.ToUpper(symbol) }

// FeaturesAll matches every symbol's feature stream.
const FeaturesAll = "features.*"

// Alert subjects. Rule-based detectors publish to AlertRule, the ML scorer to
// AlertML; the API subscribes to AlertsAll to surface both.
const (
	AlertRule = "alerts.rule"
	AlertML   = "alerts.ml"
	AlertsAll = "alerts.>"
)

// AuditCheckpoint carries signed Merkle checkpoints for optional live
// verification dashboards.
const AuditCheckpoint = "audit.checkpoint"

// subjectMatches reports whether a concrete subject matches a subscription
// pattern using NATS wildcard semantics: '*' matches exactly one token and '>'
// matches one or more trailing tokens (and is only meaningful as the final
// token).
func subjectMatches(pattern, subject string) bool {
	if pattern == subject {
		return true
	}
	p := strings.Split(pattern, ".")
	s := strings.Split(subject, ".")
	for i, tok := range p {
		if tok == ">" {
			return len(s) > i // '>' requires at least one remaining token
		}
		if i >= len(s) {
			return false
		}
		if tok == "*" {
			continue
		}
		if tok != s[i] {
			return false
		}
	}
	return len(p) == len(s)
}
