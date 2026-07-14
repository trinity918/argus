// Package api serves the surveillance dashboard, a live alert WebSocket, and an
// on-demand audit-chain verification endpoint. It subscribes to the alert and
// checkpoint subjects on the bus, so it works identically whether the alerts
// come from the in-proc demo or from detector services over NATS.
package api

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/argus-mss/argus/internal/audit"
	"github.com/argus-mss/argus/internal/detect"
	"github.com/argus-mss/argus/internal/events"
	"github.com/argus-mss/argus/internal/features"
	"github.com/argus-mss/argus/internal/transport"
	"github.com/gorilla/websocket"
)

// MarketTile is the live per-symbol microstructure summary shown on the
// dashboard. Feature values come straight off the features.* subject the
// detection engine already publishes for the ML scorer — zero extra
// computation, one more subscriber.
type MarketTile struct {
	Symbol         string  `json:"symbol"`
	Exchange       string  `json:"exchange,omitempty"`
	LastPrice      float64 `json:"last_price,omitempty"`
	LastTradeTsNs  int64   `json:"last_trade_ts_ns,omitempty"`
	SpreadBps      float64 `json:"spread_bps"`
	OFI            float64 `json:"ofi"`
	TradeIntensity float64 `json:"trade_intensity"`
	CancelRatio    float64 `json:"cancel_ratio"`
	FeatureTsNs    int64   `json:"feature_ts_ns,omitempty"`
}

// Config parameterizes the API server.
type Config struct {
	MaxRecent           int    // recent-alert ring size (default 500)
	AuditLogPath        string // path to audit.log for verification
	AuditCheckpointPath string // path to checkpoints.log
	AuditPubKeyHex      string // pinned signer public key (hex), optional
	MetricsHandler      http.Handler
}

// Server holds dashboard state.
type Server struct {
	cfg Config

	mu         sync.RWMutex
	recent     []detect.Alert
	byDetector map[string]int
	bySeverity map[string]int
	total      int
	lastCheck  *audit.Checkpoint
	markets    map[string]*MarketTile
	start      time.Time
	auditPub   ed25519.PublicKey

	hub      *hub
	upgrader websocket.Upgrader
}

// New builds a Server.
func New(cfg Config) *Server {
	if cfg.MaxRecent <= 0 {
		cfg.MaxRecent = 500
	}
	s := &Server{
		cfg:        cfg,
		byDetector: make(map[string]int),
		bySeverity: make(map[string]int),
		markets:    make(map[string]*MarketTile),
		start:      time.Now(),
		hub:        newHub(),
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 4096,
			// Same-origin dashboard; allow all origins for the demo.
			CheckOrigin: func(*http.Request) bool { return true },
		},
	}
	if cfg.AuditPubKeyHex != "" {
		if b, err := hex.DecodeString(cfg.AuditPubKeyHex); err == nil && len(b) == ed25519.PublicKeySize {
			s.auditPub = ed25519.PublicKey(b)
		}
	}
	return s
}

// AttachBus subscribes to alert and checkpoint subjects.
func (s *Server) AttachBus(bus transport.Bus) error {
	if _, err := bus.Subscribe(transport.AlertsAll, func(_ string, data []byte) {
		var a detect.Alert
		if err := json.Unmarshal(data, &a); err != nil {
			return
		}
		s.ingestAlert(a, data)
	}); err != nil {
		return err
	}
	if _, err := bus.Subscribe(transport.AuditCheckpoint, func(_ string, data []byte) {
		var cp audit.Checkpoint
		if err := json.Unmarshal(data, &cp); err == nil {
			s.mu.Lock()
			s.lastCheck = &cp
			s.mu.Unlock()
		}
	}); err != nil {
		return err
	}
	// Live microstructure tiles: features come off the same subject the ML
	// scorer consumes; last prices come from the trade stream.
	if _, err := bus.Subscribe(transport.FeaturesAll, func(_ string, data []byte) {
		var v features.Vector
		if err := json.Unmarshal(data, &v); err != nil || v.Symbol == "" {
			return
		}
		s.mu.Lock()
		t := s.tileFor(v.Symbol)
		t.SpreadBps = v.Features[features.SpreadBps]
		t.OFI = v.Features[features.OFI]
		t.TradeIntensity = v.Features[features.TradeIntensity]
		t.CancelRatio = v.Features[features.CancelRatio]
		t.FeatureTsNs = v.TsNs
		s.mu.Unlock()
	}); err != nil {
		return err
	}
	if _, err := bus.Subscribe(transport.MarketDataAll, func(_ string, data []byte) {
		var env events.Envelope
		if err := json.Unmarshal(data, &env); err != nil || env.Trade == nil {
			return // depth frames and malformed data are ignored here
		}
		s.mu.Lock()
		t := s.tileFor(env.Trade.Symbol)
		t.LastPrice = env.Trade.Price.Float()
		t.LastTradeTsNs = env.Trade.IngestTsNs
		t.Exchange = env.Trade.Exchange
		s.mu.Unlock()
	}); err != nil {
		return err
	}
	return nil
}

// tileFor returns (creating if needed) the tile for a symbol. Caller holds mu.
func (s *Server) tileFor(symbol string) *MarketTile {
	t, ok := s.markets[symbol]
	if !ok {
		t = &MarketTile{Symbol: symbol}
		s.markets[symbol] = t
	}
	return t
}

func (s *Server) ingestAlert(a detect.Alert, raw []byte) {
	s.mu.Lock()
	s.recent = append(s.recent, a)
	if len(s.recent) > s.cfg.MaxRecent {
		s.recent = s.recent[len(s.recent)-s.cfg.MaxRecent:]
	}
	s.byDetector[a.Detector]++
	s.bySeverity[a.SeverityLabel]++
	s.total++
	s.mu.Unlock()
	s.hub.broadcast(raw)
}

// Handler returns the HTTP routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/api/alerts", s.handleAlerts)
	mux.HandleFunc("/api/stats", s.handleStats)
	mux.HandleFunc("/api/audit/verify", s.handleVerify)
	mux.HandleFunc("/ws", s.handleWS)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	if s.cfg.MetricsHandler != nil {
		mux.Handle("/metrics", s.cfg.MetricsHandler)
	}
	return mux
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(dashboardHTML)
}

func (s *Server) handleAlerts(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := parsePositive(v); err == nil {
			limit = n
		}
	}
	s.mu.RLock()
	n := len(s.recent)
	if limit > n {
		limit = n
	}
	out := make([]detect.Alert, limit)
	copy(out, s.recent[n-limit:]) // most recent `limit`
	s.mu.RUnlock()
	// Reverse so newest is first.
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	writeJSON(w, out)
}

// StatsResponse is the dashboard summary payload.
type StatsResponse struct {
	UptimeSeconds  float64               `json:"uptime_seconds"`
	TotalAlerts    int                   `json:"total_alerts"`
	ByDetector     map[string]int        `json:"by_detector"`
	BySeverity     map[string]int        `json:"by_severity"`
	AlertsPerMin   float64               `json:"alerts_per_min"`
	WSClients      int                   `json:"ws_clients"`
	Markets        map[string]MarketTile `json:"markets,omitempty"`
	LastCheckpoint *audit.Checkpoint     `json:"last_checkpoint,omitempty"`
}

func (s *Server) handleStats(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	uptime := time.Since(s.start).Seconds()
	markets := make(map[string]MarketTile, len(s.markets))
	for k, t := range s.markets {
		markets[k] = *t
	}
	resp := StatsResponse{
		UptimeSeconds:  round2(uptime),
		TotalAlerts:    s.total,
		ByDetector:     copyMap(s.byDetector),
		BySeverity:     copyMap(s.bySeverity),
		WSClients:      s.hub.count(),
		Markets:        markets,
		LastCheckpoint: s.lastCheck,
	}
	if uptime > 0 {
		resp.AlertsPerMin = round2(float64(s.total) / uptime * 60)
	}
	s.mu.RUnlock()
	writeJSON(w, resp)
}

func (s *Server) handleVerify(w http.ResponseWriter, _ *http.Request) {
	if s.cfg.AuditLogPath == "" {
		writeJSON(w, map[string]any{"ok": false, "reason": "audit verification not configured"})
		return
	}
	res, err := audit.Verify(s.cfg.AuditLogPath, s.cfg.AuditCheckpointPath, s.auditPub)
	if err != nil {
		writeJSON(w, map[string]any{"ok": false, "reason": err.Error()})
		return
	}
	writeJSON(w, res)
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	c := &wsClient{conn: conn, send: make(chan []byte, 256)}
	s.hub.add(c)
	go c.writePump()
	// readPump: discard client messages, detect close, then unregister.
	go func() {
		defer s.hub.remove(c)
		conn.SetReadLimit(1024)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()
}

// --- helpers ---

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func copyMap(m map[string]int) map[string]int {
	out := make(map[string]int, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func parsePositive(s string) (int, error) {
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, errBadInt
		}
		n = n*10 + int(r-'0')
	}
	if n == 0 {
		return 0, errBadInt
	}
	return n, nil
}

func round2(x float64) float64 { return float64(int64(x*100+0.5)) / 100 }

var errBadInt = &parseErr{}

type parseErr struct{}

func (*parseErr) Error() string { return "invalid integer" }
