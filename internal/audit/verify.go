package audit

import (
	"bufio"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
)

const maxLine = 16 << 20 // 16 MiB per JSONL record

// ReadEntries reads all chain entries from a JSONL file. A missing file yields
// an empty slice (a fresh chain), not an error.
func ReadEntries(path string) ([]Entry, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	var out []Entry
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), maxLine)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var e Entry
		if err := json.Unmarshal(line, &e); err != nil {
			return nil, fmt.Errorf("audit: parse entry: %w", err)
		}
		out = append(out, e)
	}
	return out, sc.Err()
}

// ReadCheckpoints reads all checkpoints from a JSONL file.
func ReadCheckpoints(path string) ([]Checkpoint, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	var out []Checkpoint
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), maxLine)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var cp Checkpoint
		if err := json.Unmarshal(line, &cp); err != nil {
			return nil, fmt.Errorf("audit: parse checkpoint: %w", err)
		}
		out = append(out, cp)
	}
	return out, sc.Err()
}

// VerifyResult reports the outcome of verifying an audit trail.
type VerifyResult struct {
	OK                  bool   `json:"ok"`
	Entries             int    `json:"entries"`
	Checkpoints         int    `json:"checkpoints"`
	CheckpointsVerified int    `json:"checkpoints_verified"`
	Reason              string `json:"reason,omitempty"` // first failure, if any
	FailedSeq           uint64 `json:"failed_seq,omitempty"`
}

// verifyChain walks the hash chain, recomputing each entry hash and checking
// linkage and sequencing. It returns the authoritative recomputed hash per seq
// for Merkle verification.
func verifyChain(entries []Entry) (map[uint64][32]byte, VerifyResult) {
	hashes := make(map[uint64][32]byte, len(entries))
	var prev [32]byte // genesis: all zero
	expect := uint64(1)
	for _, e := range entries {
		if e.Seq != expect {
			return hashes, VerifyResult{Reason: fmt.Sprintf("sequence break: expected %d, got %d (insertion, reordering, or truncation)", expect, e.Seq), FailedSeq: e.Seq}
		}
		storedPrev, err := hex.DecodeString(e.PrevHash)
		if err != nil || len(storedPrev) != 32 {
			return hashes, VerifyResult{Reason: "malformed prev_hash", FailedSeq: e.Seq}
		}
		if [32]byte(storedPrev) != prev {
			return hashes, VerifyResult{Reason: "broken chain link: prev_hash does not match previous entry", FailedSeq: e.Seq}
		}
		got := computeHash(e.Seq, e.TsNs, e.Payload, prev)
		storedHash, err := hex.DecodeString(e.Hash)
		if err != nil || len(storedHash) != 32 {
			return hashes, VerifyResult{Reason: "malformed hash", FailedSeq: e.Seq}
		}
		if got != [32]byte(storedHash) {
			return hashes, VerifyResult{Reason: "hash mismatch: entry content was altered", FailedSeq: e.Seq}
		}
		hashes[e.Seq] = got
		prev = got
		expect++
	}
	return hashes, VerifyResult{OK: true, Entries: len(entries)}
}

// Verify performs a full verification of an audit trail: the hash chain, then
// every signed Merkle checkpoint. If expectedPub is non-nil, checkpoints must be
// signed by that pinned key (the strong guarantee); if nil, each checkpoint is
// verified against its own embedded key (self-consistency only).
func Verify(logPath, checkpointPath string, expectedPub ed25519.PublicKey) (VerifyResult, error) {
	entries, err := ReadEntries(logPath)
	if err != nil {
		return VerifyResult{}, err
	}
	hashes, res := verifyChain(entries)
	if !res.OK {
		return res, nil
	}

	cps, err := ReadCheckpoints(checkpointPath)
	if err != nil {
		return VerifyResult{}, err
	}
	res.Checkpoints = len(cps)

	for _, cp := range cps {
		pubBytes, err := hex.DecodeString(cp.PublicKey)
		if err != nil || len(pubBytes) != ed25519.PublicKeySize {
			return VerifyResult{Entries: len(entries), Checkpoints: len(cps), Reason: "malformed checkpoint public key", FailedSeq: cp.ToSeq}, nil
		}
		if expectedPub != nil && !ed25519.PublicKey(pubBytes).Equal(expectedPub) {
			return VerifyResult{Entries: len(entries), Checkpoints: len(cps), Reason: "checkpoint signed by an unexpected key", FailedSeq: cp.ToSeq}, nil
		}
		// Gather recomputed hashes for the checkpoint's range.
		leaves := make([][32]byte, 0, cp.ToSeq-cp.FromSeq+1)
		for s := cp.FromSeq; s <= cp.ToSeq; s++ {
			h, ok := hashes[s]
			if !ok {
				return VerifyResult{Entries: len(entries), Checkpoints: len(cps), Reason: fmt.Sprintf("checkpoint references missing entry %d (log truncated below a checkpoint)", s), FailedSeq: s}, nil
			}
			leaves = append(leaves, h)
		}
		root := MerkleRoot(leaves)
		storedRoot, err := hex.DecodeString(cp.MerkleRoot)
		if err != nil || root != [32]byte(storedRoot) {
			return VerifyResult{Entries: len(entries), Checkpoints: len(cps), Reason: "merkle root mismatch: checkpointed entries were altered", FailedSeq: cp.ToSeq}, nil
		}
		sig, err := hex.DecodeString(cp.Signature)
		if err != nil {
			return VerifyResult{Entries: len(entries), Checkpoints: len(cps), Reason: "malformed checkpoint signature", FailedSeq: cp.ToSeq}, nil
		}
		if !VerifySig(ed25519.PublicKey(pubBytes), checkpointMessage(cp.FromSeq, cp.ToSeq, root), sig) {
			return VerifyResult{Entries: len(entries), Checkpoints: len(cps), Reason: "invalid checkpoint signature", FailedSeq: cp.ToSeq}, nil
		}
		res.CheckpointsVerified++
	}

	res.OK = true
	return res, nil
}
