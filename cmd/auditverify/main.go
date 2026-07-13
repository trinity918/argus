// Command auditverify independently verifies an Argus audit trail: it walks the
// hash chain, recomputes every entry hash, and checks each signed Merkle
// checkpoint. It exits non-zero if the trail has been altered in any way — the
// tool a regulator or auditor would run to prove the log's integrity.
package main

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/argus-mss/argus/internal/audit"
)

func main() {
	var (
		dir     = flag.String("dir", "./data/audit", "audit directory (expects audit.log, checkpoints.log, pubkey.hex)")
		logPath = flag.String("log", "", "override path to audit.log")
		cpPath  = flag.String("checkpoints", "", "override path to checkpoints.log")
		pubHex  = flag.String("pubkey", "", "pinned signer public key (hex); default reads <dir>/pubkey.hex")
		asJSON  = flag.Bool("json", false, "emit the result as JSON")
	)
	flag.Parse()

	lp := *logPath
	if lp == "" {
		lp = filepath.Join(*dir, "audit.log")
	}
	cp := *cpPath
	if cp == "" {
		cp = filepath.Join(*dir, "checkpoints.log")
	}

	var pub ed25519.PublicKey
	ph := *pubHex
	if ph == "" {
		if b, err := os.ReadFile(filepath.Join(*dir, "pubkey.hex")); err == nil {
			ph = strings.TrimSpace(string(b))
		}
	}
	if ph != "" {
		b, err := hex.DecodeString(ph)
		if err != nil || len(b) != ed25519.PublicKeySize {
			fmt.Fprintln(os.Stderr, "invalid --pubkey")
			os.Exit(2)
		}
		pub = ed25519.PublicKey(b)
	}

	res, err := audit.Verify(lp, cp, pub)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}

	if *asJSON {
		b, _ := json.MarshalIndent(res, "", "  ")
		fmt.Println(string(b))
	} else {
		printHuman(res, pub != nil)
	}
	if !res.OK {
		os.Exit(1)
	}
}

func printHuman(res audit.VerifyResult, pinned bool) {
	fmt.Println("Argus audit trail verification")
	fmt.Println("──────────────────────────────")
	fmt.Printf("  entries:              %d\n", res.Entries)
	fmt.Printf("  checkpoints:          %d\n", res.Checkpoints)
	fmt.Printf("  checkpoints verified: %d\n", res.CheckpointsVerified)
	fmt.Printf("  signer pinned:        %v\n", pinned)
	fmt.Println("──────────────────────────────")
	if res.OK {
		fmt.Println("  RESULT: ✓ VERIFIED — chain intact, checkpoints valid")
		return
	}
	fmt.Printf("  RESULT: ✗ FAILED — %s\n", res.Reason)
	if res.FailedSeq != 0 {
		fmt.Printf("          at sequence %d\n", res.FailedSeq)
	}
}
