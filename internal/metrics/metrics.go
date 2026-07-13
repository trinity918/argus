// Package metrics centralizes Prometheus instrumentation. Every service exposes
// the same registry over /metrics; the histograms are denominated in
// microseconds because the surveillance value proposition is low-latency
// detection, and "p99 detection latency in µs" is the number an interviewer for
// a low-latency role wants to see.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds the application counters/histograms and their registry.
type Metrics struct {
	reg *prometheus.Registry

	EventsProcessed     prometheus.Counter
	Alerts              *prometheus.CounterVec
	DetectionLatencyUs  prometheus.Histogram
	ProcessingLatencyUs prometheus.Histogram
	AuditEntries        prometheus.Counter
	AuditCheckpoints    prometheus.Counter
	Features            prometheus.Counter
}

// latencyBuckets span sub-10µs to 50ms, the useful range for an in-memory
// streaming detector.
var latencyBuckets = []float64{5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 25000, 50000}

// New builds a Metrics with a fresh registry (plus Go/process collectors).
func New() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	m := &Metrics{
		reg: reg,
		EventsProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "argus_events_processed_total",
			Help: "Market-data events processed by the detection engine.",
		}),
		Alerts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "argus_alerts_total",
			Help: "Alerts emitted, by detector and severity.",
		}, []string{"detector", "severity"}),
		DetectionLatencyUs: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "argus_detection_latency_microseconds",
			Help:    "End-to-end latency from event ingest to alert emission (µs).",
			Buckets: latencyBuckets,
		}),
		ProcessingLatencyUs: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "argus_processing_latency_microseconds",
			Help:    "Per-event detection engine processing time (µs).",
			Buckets: latencyBuckets,
		}),
		AuditEntries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "argus_audit_entries_total",
			Help: "Alerts appended to the tamper-evident audit chain.",
		}),
		AuditCheckpoints: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "argus_audit_checkpoints_total",
			Help: "Signed Merkle checkpoints written.",
		}),
		Features: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "argus_features_emitted_total",
			Help: "Order-flow feature vectors emitted for the ML scorer.",
		}),
	}
	reg.MustRegister(
		m.EventsProcessed, m.Alerts, m.DetectionLatencyUs,
		m.ProcessingLatencyUs, m.AuditEntries, m.AuditCheckpoints, m.Features,
	)
	return m
}

// Registry exposes the registry so services can register extra collectors
// (e.g. live ingestion stats).
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// Handler returns the /metrics HTTP handler.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}
