package app_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/argus-mss/argus/internal/audit"
	"github.com/argus-mss/argus/internal/detect"
	"github.com/argus-mss/argus/internal/scenario"
)

// TestEndToEndScenario drives the scripted manipulation tape through the real
// detection engine and audit chain with a deterministic virtual clock, then
// proves (a) every detector fired, (b) the audit trail verifies, and (c) any
// tampering is caught. This is the whole system exercised in one test.
func TestEndToEndScenario(t *testing.T) {
	dir := t.TempDir()
	signer, err := audit.GenerateSigner()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := audit.Open(dir, signer, 16)
	if err != nil {
		t.Fatal(err)
	}

	var clk int64 = 1_000_000_000_000
	fired := map[string]int{}
	var appended int
	emit := func(a detect.Alert) {
		fired[a.Detector]++
		b, _ := json.Marshal(a)
		if _, err := chain.Append(b); err != nil {
			t.Fatalf("audit append: %v", err)
		}
		appended++
	}
	eng := detect.NewEngine(detect.DefaultConfig(), emit, detect.WithClock(func() int64 { return clk }))

	for _, s := range scenario.Manipulations("BTCUSDT") {
		clk += s.DelayNs
		eng.HandleEnvelope(s.Env)
	}
	if err := chain.Close(); err != nil {
		t.Fatal(err)
	}

	for _, d := range []string{
		detect.DetectorSpoofing, detect.DetectorLayering, detect.DetectorMomentum,
		detect.DetectorQuoteStuffing, detect.DetectorWashTrade,
	} {
		if fired[d] == 0 {
			t.Errorf("detector %q did not fire in the scenario", d)
		}
	}
	t.Logf("alerts by detector: %v (total %d)", fired, appended)

	logPath := filepath.Join(dir, "audit.log")
	cpPath := filepath.Join(dir, "checkpoints.log")
	res, err := audit.Verify(logPath, cpPath, signer.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	if !res.OK {
		t.Fatalf("audit verify failed: %s", res.Reason)
	}
	if res.Entries != appended {
		t.Errorf("audit entries = %d, want %d", res.Entries, appended)
	}

	// Tamper with the first entry's payload; verification must now fail.
	tamperFirstEntry(t, logPath)
	res2, _ := audit.Verify(logPath, cpPath, signer.PublicKey())
	if res2.OK {
		t.Fatal("tampered audit trail must not verify")
	}
}

func tamperFirstEntry(t *testing.T, logPath string) {
	t.Helper()
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	nl := indexByte(data, '\n')
	if nl < 0 {
		t.Fatal("no entries to tamper")
	}
	var e audit.Entry
	if err := json.Unmarshal(data[:nl], &e); err != nil {
		t.Fatal(err)
	}
	e.Payload = json.RawMessage(`{"tampered":true}`)
	line, _ := json.Marshal(e)
	out := append(line, '\n')
	out = append(out, data[nl+1:]...)
	if err := os.WriteFile(logPath, out, 0o644); err != nil {
		t.Fatal(err)
	}
}

func indexByte(b []byte, c byte) int {
	for i, x := range b {
		if x == c {
			return i
		}
	}
	return -1
}
