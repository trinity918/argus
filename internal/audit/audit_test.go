package audit

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func mkSigner(t *testing.T) *Signer {
	t.Helper()
	s, err := GenerateSigner()
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func appendAlerts(t *testing.T, c *Chain, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		payload := []byte(fmt.Sprintf(`{"id":"a%d","detector":"spoofing","score":%d}`, i, i))
		if _, err := c.Append(payload); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
}

func paths(dir string) (string, string) {
	return filepath.Join(dir, "audit.log"), filepath.Join(dir, "checkpoints.log")
}

func TestMerkleRootDeterministicAndSensitive(t *testing.T) {
	h := func(b byte) [32]byte { var x [32]byte; x[0] = b; return x }
	a := MerkleRoot([][32]byte{h(1), h(2), h(3)})
	b := MerkleRoot([][32]byte{h(1), h(2), h(3)})
	if a != b {
		t.Fatal("merkle root should be deterministic")
	}
	c := MerkleRoot([][32]byte{h(1), h(2), h(4)}) // one leaf changed
	if a == c {
		t.Fatal("merkle root should change when a leaf changes")
	}
	// Odd promotion must not equal the even-padded interpretation trivially.
	if MerkleRoot([][32]byte{h(1)}) == MerkleRoot([][32]byte{h(1), h(1)}) {
		t.Fatal("single leaf must not collide with duplicated leaf")
	}
}

func TestSignerRoundTripAndPersistence(t *testing.T) {
	dir := t.TempDir()
	keyPath := filepath.Join(dir, "key.hex")
	s1, err := LoadOrCreateSigner(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := LoadOrCreateSigner(keyPath) // must load the same key
	if err != nil {
		t.Fatal(err)
	}
	if s1.PublicKeyHex() != s2.PublicKeyHex() {
		t.Fatal("persisted signer should reload identical key")
	}
	msg := []byte("hello")
	if !VerifySig(s2.PublicKey(), msg, s1.Sign(msg)) {
		t.Fatal("signature from reloaded key should verify")
	}
}

func TestAppendAndVerifyHappyPath(t *testing.T) {
	dir := t.TempDir()
	signer := mkSigner(t)
	c, err := Open(dir, signer, 4)
	if err != nil {
		t.Fatal(err)
	}
	appendAlerts(t, c, 10)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	logPath, cpPath := paths(dir)
	res, err := Verify(logPath, cpPath, signer.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	if !res.OK {
		t.Fatalf("verify failed: %s (seq %d)", res.Reason, res.FailedSeq)
	}
	if res.Entries != 10 {
		t.Errorf("entries = %d, want 10", res.Entries)
	}
	if res.CheckpointsVerified != res.Checkpoints || res.Checkpoints < 2 {
		t.Errorf("checkpoints verified=%d total=%d", res.CheckpointsVerified, res.Checkpoints)
	}
}

// rewriteLines rewrites a file with the given transform applied to its lines.
func rewriteLines(t *testing.T, path string, transform func([]string) []string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	lines = transform(lines)
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestTamperedPayloadDetected(t *testing.T) {
	dir := t.TempDir()
	signer := mkSigner(t)
	c, _ := Open(dir, signer, 4)
	appendAlerts(t, c, 10)
	c.Close()
	logPath, cpPath := paths(dir)

	// Alter the payload of entry seq=6 while leaving its stored hash intact.
	rewriteLines(t, logPath, func(lines []string) []string {
		var e Entry
		if err := json.Unmarshal([]byte(lines[5]), &e); err != nil {
			t.Fatal(err)
		}
		e.Payload = json.RawMessage(`{"id":"a5","detector":"spoofing","score":999}`)
		b, _ := json.Marshal(e)
		lines[5] = string(b)
		return lines
	})

	res, _ := Verify(logPath, cpPath, signer.PublicKey())
	if res.OK {
		t.Fatal("tampering must be detected")
	}
	if res.FailedSeq != 6 || !strings.Contains(res.Reason, "hash mismatch") {
		t.Fatalf("expected hash mismatch at seq 6, got %q at seq %d", res.Reason, res.FailedSeq)
	}
}

func TestReorderDetected(t *testing.T) {
	dir := t.TempDir()
	signer := mkSigner(t)
	c, _ := Open(dir, signer, 100) // no auto checkpoints; test the chain itself
	appendAlerts(t, c, 6)
	c.Close()
	logPath, cpPath := paths(dir)

	rewriteLines(t, logPath, func(lines []string) []string {
		lines[2], lines[3] = lines[3], lines[2] // swap seq 3 and 4
		return lines
	})
	res, _ := Verify(logPath, cpPath, signer.PublicKey())
	if res.OK {
		t.Fatal("reordering must be detected")
	}
}

func TestTruncationBelowCheckpointDetected(t *testing.T) {
	dir := t.TempDir()
	signer := mkSigner(t)
	c, _ := Open(dir, signer, 4)
	appendAlerts(t, c, 8) // checkpoints cover 1-4 and 5-8
	c.Close()
	logPath, cpPath := paths(dir)

	// Drop the last two entries (7,8); checkpoint 5-8 now references missing rows.
	rewriteLines(t, logPath, func(lines []string) []string { return lines[:6] })

	res, _ := Verify(logPath, cpPath, signer.PublicKey())
	if res.OK {
		t.Fatal("truncation below a checkpoint must be detected")
	}
	if !strings.Contains(res.Reason, "missing entry") {
		t.Fatalf("expected missing-entry reason, got %q", res.Reason)
	}
}

func TestWrongPinnedKeyRejected(t *testing.T) {
	dir := t.TempDir()
	signer := mkSigner(t)
	c, _ := Open(dir, signer, 4)
	appendAlerts(t, c, 8)
	c.Close()
	logPath, cpPath := paths(dir)

	otherPub, _, _ := ed25519.GenerateKey(rand.Reader)
	res, _ := Verify(logPath, cpPath, otherPub)
	if res.OK {
		t.Fatal("checkpoints signed by a different key must be rejected when pinned")
	}
	if !strings.Contains(res.Reason, "unexpected key") {
		t.Fatalf("expected unexpected-key reason, got %q", res.Reason)
	}
}

func TestResumeContinuesChain(t *testing.T) {
	dir := t.TempDir()
	signer := mkSigner(t)

	c1, _ := Open(dir, signer, 100)
	appendAlerts(t, c1, 3)
	c1.Close()

	c2, err := Open(dir, signer, 100) // resume
	if err != nil {
		t.Fatal(err)
	}
	if c2.Seq() != 3 {
		t.Fatalf("resumed seq = %d, want 3", c2.Seq())
	}
	appendAlerts(t, c2, 2)
	c2.Close()

	logPath, cpPath := paths(dir)
	res, _ := Verify(logPath, cpPath, signer.PublicKey())
	if !res.OK {
		t.Fatalf("resumed chain must verify: %s", res.Reason)
	}
	if res.Entries != 5 {
		t.Fatalf("entries = %d, want 5", res.Entries)
	}
}
