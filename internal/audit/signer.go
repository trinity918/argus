package audit

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
)

// Signer holds an Ed25519 keypair used to sign Merkle checkpoints. Signing the
// checkpoints — not every entry — gives non-repudiation cheaply: one signature
// vouches for an entire range of the hash chain, and anyone with the public key
// can verify the log was produced by the holder of the private key and has not
// been altered.
type Signer struct {
	priv ed25519.PrivateKey
	pub  ed25519.PublicKey
}

// GenerateSigner creates a fresh random keypair.
func GenerateSigner() (*Signer, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	return &Signer{priv: priv, pub: pub}, nil
}

// LoadOrCreateSigner loads a hex-encoded Ed25519 seed from path, creating and
// persisting a new one (mode 0600) if the file does not exist.
func LoadOrCreateSigner(path string) (*Signer, error) {
	data, err := os.ReadFile(path)
	if err == nil {
		seed, derr := hex.DecodeString(strings.TrimSpace(string(data)))
		if derr != nil || len(seed) != ed25519.SeedSize {
			return nil, fmt.Errorf("audit: malformed key file %s", path)
		}
		priv := ed25519.NewKeyFromSeed(seed)
		return &Signer{priv: priv, pub: priv.Public().(ed25519.PublicKey)}, nil
	}
	if !os.IsNotExist(err) {
		return nil, err
	}
	s, err := GenerateSigner()
	if err != nil {
		return nil, err
	}
	seed := s.priv.Seed()
	if err := os.WriteFile(path, []byte(hex.EncodeToString(seed)), 0o600); err != nil {
		return nil, fmt.Errorf("audit: persist key: %w", err)
	}
	return s, nil
}

// Sign returns the Ed25519 signature over msg.
func (s *Signer) Sign(msg []byte) []byte { return ed25519.Sign(s.priv, msg) }

// PublicKey returns the verifying key.
func (s *Signer) PublicKey() ed25519.PublicKey { return s.pub }

// PublicKeyHex returns the hex-encoded public key.
func (s *Signer) PublicKeyHex() string { return hex.EncodeToString(s.pub) }

// VerifySig checks a signature against a public key.
func VerifySig(pub ed25519.PublicKey, msg, sig []byte) bool {
	return ed25519.Verify(pub, msg, sig)
}
