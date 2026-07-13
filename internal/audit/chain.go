// Package audit implements a tamper-evident audit trail for surveillance
// alerts. Every alert is appended to a hash chain — each entry commits to the
// previous entry's hash — and the chain is periodically checkpointed with a
// Merkle root that is Ed25519-signed. The combination yields:
//
//   - integrity: any mutation, reordering, or insertion breaks the chain and is
//     located precisely by the verifier;
//   - non-repudiation: signed Merkle checkpoints prove the log was produced by
//     the key holder and fix the contents up to each checkpoint;
//   - efficiency: one signature vouches for a whole range of entries.
//
// This cryptographic "who-said-what-when, provably unaltered" story is exactly
// what a compliance/surveillance system needs and what generic detectors lack.
package audit

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	entryDomain      = "argus.audit.entry.v1"
	checkpointDomain = "argus.audit.checkpoint.v1"
)

// Entry is one link in the hash chain. Payload is the opaque alert JSON (stored
// verbatim so its hash is reproducible byte-for-byte).
type Entry struct {
	Seq      uint64          `json:"seq"`
	TsNs     int64           `json:"ts_ns"`
	Payload  json.RawMessage `json:"payload"`
	PrevHash string          `json:"prev_hash"` // hex
	Hash     string          `json:"hash"`      // hex
}

// Checkpoint is a signed commitment to entries [FromSeq, ToSeq].
type Checkpoint struct {
	FromSeq    uint64 `json:"from_seq"`
	ToSeq      uint64 `json:"to_seq"`
	Count      int    `json:"count"`
	MerkleRoot string `json:"merkle_root"` // hex
	Signature  string `json:"signature"`   // hex
	PublicKey  string `json:"public_key"`  // hex
	TsNs       int64  `json:"ts_ns"`
}

// computeHash derives an entry hash with domain separation and length-prefixed
// fields so no two distinct entries can collide via field-boundary ambiguity.
func computeHash(seq uint64, tsNs int64, payload []byte, prev [32]byte) [32]byte {
	h := sha256.New()
	h.Write([]byte(entryDomain))
	var u8 [8]byte
	binary.BigEndian.PutUint64(u8[:], seq)
	h.Write(u8[:])
	binary.BigEndian.PutUint64(u8[:], uint64(tsNs))
	h.Write(u8[:])
	var u4 [4]byte
	binary.BigEndian.PutUint32(u4[:], uint32(len(payload)))
	h.Write(u4[:])
	h.Write(payload)
	h.Write(prev[:])
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// checkpointMessage is the byte string signed for a checkpoint.
func checkpointMessage(fromSeq, toSeq uint64, root [32]byte) []byte {
	buf := make([]byte, 0, len(checkpointDomain)+16+32)
	buf = append(buf, checkpointDomain...)
	var u8 [8]byte
	binary.BigEndian.PutUint64(u8[:], fromSeq)
	buf = append(buf, u8[:]...)
	binary.BigEndian.PutUint64(u8[:], toSeq)
	buf = append(buf, u8[:]...)
	buf = append(buf, root[:]...)
	return buf
}

// Chain is a live append-only audit log. Safe for concurrent Append calls.
type Chain struct {
	mu sync.Mutex

	logFile *os.File
	logBuf  *bufio.Writer
	cpFile  *os.File

	signer   *Signer
	interval uint64

	seq      uint64
	prevHash [32]byte

	cpStart uint64     // last checkpointed seq (entries after this are pending)
	leaves  [][32]byte // pending entry hashes since last checkpoint
	clock   func() int64

	OnCheckpoint func(Checkpoint) // optional hook (e.g. publish to bus)
}

// Open opens (or resumes) a chain in dir, checkpointing every `interval`
// entries. Passing interval<=0 disables automatic checkpoints (use Checkpoint()
// manually, e.g. on shutdown).
func Open(dir string, signer *Signer, interval int) (*Chain, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	logPath := filepath.Join(dir, "audit.log")
	cpPath := filepath.Join(dir, "checkpoints.log")

	c := &Chain{
		signer:   signer,
		interval: uint64(max(interval, 0)),
		clock:    func() int64 { return time.Now().UnixNano() },
	}
	if err := c.resume(logPath, cpPath); err != nil {
		return nil, err
	}

	lf, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	cf, err := os.OpenFile(cpPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		lf.Close()
		return nil, err
	}
	c.logFile = lf
	c.logBuf = bufio.NewWriter(lf)
	c.cpFile = cf
	return c, nil
}

// resume reconstructs seq, prevHash, cpStart, and pending leaves from any
// existing files so a restarted process continues the same chain.
func (c *Chain) resume(logPath, cpPath string) error {
	entries, err := ReadEntries(logPath)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	last := entries[len(entries)-1]
	c.seq = last.Seq
	hb, err := hex.DecodeString(last.Hash)
	if err != nil || len(hb) != 32 {
		return fmt.Errorf("audit: corrupt last entry hash on resume")
	}
	copy(c.prevHash[:], hb)

	cps, err := ReadCheckpoints(cpPath)
	if err != nil {
		return err
	}
	if len(cps) > 0 {
		c.cpStart = cps[len(cps)-1].ToSeq
	}
	for _, e := range entries {
		if e.Seq > c.cpStart {
			var eh [32]byte
			b, _ := hex.DecodeString(e.Hash)
			copy(eh[:], b)
			c.leaves = append(c.leaves, eh)
		}
	}
	return nil
}

// Append writes an alert payload as the next chain entry and returns it. If the
// checkpoint interval is reached it also writes (and signs) a checkpoint.
func (c *Chain) Append(payload []byte) (Entry, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Compact the payload so the bytes we hash are exactly the bytes we store.
	// encoding/json re-compacts json.RawMessage on marshal, so hashing the raw
	// (possibly whitespaced) input would not survive a write/read round-trip.
	var compact bytes.Buffer
	if err := json.Compact(&compact, payload); err != nil {
		return Entry{}, fmt.Errorf("audit: payload is not valid JSON: %w", err)
	}
	canonical := compact.Bytes()

	c.seq++
	ts := c.clock()
	hash := computeHash(c.seq, ts, canonical, c.prevHash)

	e := Entry{
		Seq:      c.seq,
		TsNs:     ts,
		Payload:  append(json.RawMessage(nil), canonical...),
		PrevHash: hex.EncodeToString(c.prevHash[:]),
		Hash:     hex.EncodeToString(hash[:]),
	}
	line, err := json.Marshal(e)
	if err != nil {
		c.seq--
		return Entry{}, err
	}
	if _, err := c.logBuf.Write(append(line, '\n')); err != nil {
		c.seq--
		return Entry{}, err
	}
	if err := c.logBuf.Flush(); err != nil {
		return Entry{}, err
	}
	c.prevHash = hash
	c.leaves = append(c.leaves, hash)

	if c.interval > 0 && c.seq-c.cpStart >= c.interval {
		if _, err := c.checkpointLocked(); err != nil {
			return e, err
		}
	}
	return e, nil
}

// Checkpoint forces a checkpoint over any pending entries (e.g. on shutdown).
// Returns ok=false if there is nothing pending.
func (c *Chain) Checkpoint() (Checkpoint, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.leaves) == 0 {
		return Checkpoint{}, false, nil
	}
	cp, err := c.checkpointLocked()
	return cp, err == nil, err
}

func (c *Chain) checkpointLocked() (Checkpoint, error) {
	root := MerkleRoot(c.leaves)
	from := c.cpStart + 1
	to := c.seq
	sig := c.signer.Sign(checkpointMessage(from, to, root))
	cp := Checkpoint{
		FromSeq:    from,
		ToSeq:      to,
		Count:      len(c.leaves),
		MerkleRoot: hex.EncodeToString(root[:]),
		Signature:  hex.EncodeToString(sig),
		PublicKey:  c.signer.PublicKeyHex(),
		TsNs:       c.clock(),
	}
	line, err := json.Marshal(cp)
	if err != nil {
		return Checkpoint{}, err
	}
	if _, err := c.cpFile.Write(append(line, '\n')); err != nil {
		return Checkpoint{}, err
	}
	// Durability: flush the log buffer and fsync both files so a checkpoint is a
	// genuine on-disk commitment.
	if err := c.logBuf.Flush(); err != nil {
		return Checkpoint{}, err
	}
	_ = c.logFile.Sync()
	_ = c.cpFile.Sync()

	c.cpStart = to
	c.leaves = c.leaves[:0]
	if c.OnCheckpoint != nil {
		c.OnCheckpoint(cp)
	}
	return cp, nil
}

// Seq returns the last written sequence number.
func (c *Chain) Seq() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.seq
}

// Close flushes, writes a final checkpoint over pending entries, and closes.
func (c *Chain) Close() error {
	if _, _, err := c.Checkpoint(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.logBuf.Flush(); err != nil {
		return err
	}
	_ = c.logFile.Sync()
	_ = c.cpFile.Sync()
	err1 := c.logFile.Close()
	err2 := c.cpFile.Close()
	if err1 != nil {
		return err1
	}
	return err2
}
