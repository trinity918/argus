package okx

import (
	"encoding/json"
	"hash/crc32"
	"log/slog"
	"testing"

	"github.com/argus-mss/argus/internal/events"
	fp "github.com/argus-mss/argus/internal/fixedpoint"
)

// --- fixtures: real frames captured from the live OKX public feed ---

const ackFrame = `{"event":"subscribe","arg":{"channel":"books","instId":"BTC-USDT"},"connId":"4a0c3369"}`

const snapshotFrame = `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[{"asks":[["62571.2","0.72032554","0","6"],["62572.1","0.00022","0","1"]],"bids":[["62571.1","0.5","0","2"],["62570.0","1.25","0","1"]],"ts":"1784016942000","checksum":0,"prevSeqId":-1,"seqId":100}]}`

const updateFrame = `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[{"asks":[["62571.2","0.60583592","0","6"],["62572.1","0","0","0"]],"bids":[],"ts":"1784016942108","checksum":0,"seqId":110,"prevSeqId":100}]}`

const gapFrame = `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[{"asks":[["62599.6","0.0793711","0","1"]],"bids":[],"ts":"1784016942208","checksum":0,"seqId":130,"prevSeqId":120}]}`

const tradeFrame = `{"arg":{"channel":"trades","instId":"BTC-USDT"},"data":[{"instId":"BTC-USDT","tradeId":"1032787952","px":"62571.2","sz":"0.07990896","side":"buy","ts":"1784016942060","count":"1","source":"0","seqId":78755034232}]}`

// --- test doubles ---

type capturePub struct{ envs []events.Envelope }

func (p *capturePub) Publish(env events.Envelope) error {
	p.envs = append(p.envs, env)
	return nil
}

type fakeWriter struct{ ops []map[string]any }

func (w *fakeWriter) WriteJSON(v any) error {
	b, _ := json.Marshal(v)
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	w.ops = append(w.ops, m)
	return nil
}

func newTestClient(pub Publisher) (*Client, map[string]*symState) {
	c := New(Config{Symbols: []string{"BTC-USDT"}, Logger: slog.Default()}, pub)
	states := map[string]*symState{"BTC-USDT": {ladder: newStringBook()}}
	return c, states
}

// --- frame parsing ---

func TestParseFrameKinds(t *testing.T) {
	if m, err := parseFrame([]byte("pong")); m != nil || err != nil {
		t.Fatalf("pong should be skipped, got m=%v err=%v", m, err)
	}
	if m, err := parseFrame([]byte(ackFrame)); m != nil || err != nil {
		t.Fatalf("ack should be skipped, got m=%v err=%v", m, err)
	}
	if _, err := parseFrame([]byte(`{"event":"error","code":"60012","msg":"bad request"}`)); err == nil {
		t.Fatal("server error frame should surface an error")
	}
	m, err := parseFrame([]byte(snapshotFrame))
	if err != nil || m == nil || m.Arg.Channel != "books" || m.Action != "snapshot" {
		t.Fatalf("snapshot parse failed: m=%+v err=%v", m, err)
	}
}

// --- normalization ---

func TestSnapshotAndUpdateFlow(t *testing.T) {
	pub := &capturePub{}
	c, states := newTestClient(pub)
	w := &fakeWriter{}

	if err := c.handleFrame(w, states, []byte(snapshotFrame)); err != nil {
		t.Fatal(err)
	}
	st := states["BTC-USDT"]
	if !st.synced || st.lastSeq != 100 {
		t.Fatalf("expected synced at seq 100, got synced=%v lastSeq=%d", st.synced, st.lastSeq)
	}
	if len(pub.envs) != 1 || pub.envs[0].Depth == nil || !pub.envs[0].Depth.IsSnapshot {
		t.Fatalf("expected one snapshot depth envelope, got %+v", pub.envs)
	}
	d := pub.envs[0].Depth
	if d.Exchange != "okx" || d.Symbol != "BTC-USDT" || d.FinalUpdateID != 100 {
		t.Fatalf("bad snapshot fields: %+v", d)
	}

	// Contiguous update applies (prevSeqId 100 == lastSeq).
	if err := c.handleFrame(w, states, []byte(updateFrame)); err != nil {
		t.Fatal(err)
	}
	if st.lastSeq != 110 {
		t.Fatalf("lastSeq = %d, want 110", st.lastSeq)
	}
	if len(pub.envs) != 2 || pub.envs[1].Depth.IsSnapshot {
		t.Fatalf("expected an incremental depth envelope, got %+v", pub.envs)
	}
	// The ladder tracked the update: 62572.1 removed, 62571.2 resized.
	if got := st.ladder.asks[0].szStr; got != "0.60583592" {
		t.Fatalf("ladder top ask size = %q, want 0.60583592", got)
	}
	if len(st.ladder.asks) != 1 {
		t.Fatalf("ask ladder len = %d, want 1 (62572.1 removed)", len(st.ladder.asks))
	}
	if len(w.ops) != 0 {
		t.Fatalf("no resubscribe ops expected, got %v", w.ops)
	}
}

func TestSeqGapTriggersResubscribe(t *testing.T) {
	pub := &capturePub{}
	c, states := newTestClient(pub)
	w := &fakeWriter{}
	if err := c.handleFrame(w, states, []byte(snapshotFrame)); err != nil {
		t.Fatal(err)
	}
	// gapFrame has prevSeqId=120 but lastSeq=100 -> gap.
	if err := c.handleFrame(w, states, []byte(gapFrame)); err != nil {
		t.Fatal(err)
	}
	st := states["BTC-USDT"]
	if st.synced {
		t.Fatal("gap must drop the symbol to unsynced")
	}
	if len(w.ops) != 2 || w.ops[0]["op"] != "unsubscribe" || w.ops[1]["op"] != "subscribe" {
		t.Fatalf("expected unsubscribe+subscribe, got %v", w.ops)
	}
	if c.Stats().Resyncs != 1 {
		t.Fatalf("resyncs = %d, want 1", c.Stats().Resyncs)
	}
	// Updates while unsynced are ignored until the fresh snapshot arrives.
	before := len(pub.envs)
	if err := c.handleFrame(w, states, []byte(updateFrame)); err != nil {
		t.Fatal(err)
	}
	if len(pub.envs) != before {
		t.Fatal("updates before resync snapshot must not be published")
	}
}

func TestTradeNormalization(t *testing.T) {
	pub := &capturePub{}
	c, states := newTestClient(pub)
	if err := c.handleFrame(&fakeWriter{}, states, []byte(tradeFrame)); err != nil {
		t.Fatal(err)
	}
	if len(pub.envs) != 1 || pub.envs[0].Trade == nil {
		t.Fatalf("expected one trade envelope, got %+v", pub.envs)
	}
	tr := pub.envs[0].Trade
	if tr.Price != fp.MustParse("62571.2") || tr.Qty != fp.MustParse("0.07990896") {
		t.Fatalf("bad trade values: %+v", tr)
	}
	if tr.Aggressor != events.Buy || tr.Exchange != "okx" || tr.TradeID != 1032787952 {
		t.Fatalf("bad trade fields: %+v", tr)
	}
}

// --- string ladder + checksum ---

func TestStringBookOrderingAndRemoval(t *testing.T) {
	b := newStringBook()
	_ = b.applyRows(false, [][]string{{"3372", "8"}, {"3366.8", "9"}, {"3368", "8"}})
	_ = b.applyRows(true, [][]string{{"3366.1", "7"}, {"3360", "2"}})
	if b.asks[0].pxStr != "3366.8" || b.asks[2].pxStr != "3372" {
		t.Fatalf("asks not ascending: %+v", b.asks)
	}
	if b.bids[0].pxStr != "3366.1" {
		t.Fatalf("bids not descending: %+v", b.bids)
	}
	_ = b.applyRows(true, [][]string{{"3360", "0"}}) // remove
	if len(b.bids) != 1 {
		t.Fatalf("expected removal, bids=%+v", b.bids)
	}
}

// TestChecksumPayloadMatchesOKXDocExample uses the interleaving example from
// the OKX API docs: bids [3366.1,7], asks [3366.8,9],[3368,8],[3372,8] yields
// "3366.1:7:3366.8:9:3368:8:3372:8" (remaining asks appended once bids run out).
func TestChecksumPayloadMatchesOKXDocExample(t *testing.T) {
	b := newStringBook()
	_ = b.applyRows(true, [][]string{{"3366.1", "7"}})
	_ = b.applyRows(false, [][]string{{"3366.8", "9"}, {"3368", "8"}, {"3372", "8"}})
	want := "3366.1:7:3366.8:9:3368:8:3372:8"
	if got := b.checksumPayload(); got != want {
		t.Fatalf("payload = %q, want %q", got, want)
	}
	if b.checksum() != int32(crc32.ChecksumIEEE([]byte(want))) {
		t.Fatal("checksum must be signed CRC32 of the payload")
	}
}

func TestChecksumZeroIsNotValidated(t *testing.T) {
	pub := &capturePub{}
	c, states := newTestClient(pub)
	// snapshotFrame carries checksum:0 (as the live feed does); it must sync.
	if err := c.handleFrame(&fakeWriter{}, states, []byte(snapshotFrame)); err != nil {
		t.Fatal(err)
	}
	if !states["BTC-USDT"].synced {
		t.Fatal("checksum=0 must be treated as 'not provided', not a mismatch")
	}
	if c.Stats().ChecksumFailures != 0 {
		t.Fatal("no checksum failures expected")
	}
}

func TestChecksumMismatchTriggersResync(t *testing.T) {
	pub := &capturePub{}
	c, states := newTestClient(pub)
	w := &fakeWriter{}
	if err := c.handleFrame(w, states, []byte(snapshotFrame)); err != nil {
		t.Fatal(err)
	}
	// Craft an update with a deliberately wrong non-zero checksum.
	bad := `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[{"asks":[["62571.2","0.5","0","1"]],"bids":[],"ts":"1","checksum":12345,"seqId":110,"prevSeqId":100}]}`
	if err := c.handleFrame(w, states, []byte(bad)); err != nil {
		t.Fatal(err)
	}
	if states["BTC-USDT"].synced {
		t.Fatal("checksum mismatch must force resync")
	}
	if c.Stats().ChecksumFailures != 1 {
		t.Fatalf("checksum failures = %d, want 1", c.Stats().ChecksumFailures)
	}
}
