package audit

import "crypto/sha256"

// Merkle tree over entry hashes with RFC-6962-style domain separation: leaves
// and internal nodes are hashed with distinct prefixes (0x00 / 0x01) so an
// attacker cannot pass an internal node off as a leaf (a second-preimage
// attack). Odd nodes are promoted unchanged to the next level rather than
// duplicated, avoiding the well-known duplicate-leaf malleability.
const (
	leafPrefix = 0x00
	nodePrefix = 0x01
)

// leafHash hashes a single entry hash into a Merkle leaf.
func leafHash(h [32]byte) [32]byte {
	s := sha256.New()
	s.Write([]byte{leafPrefix})
	s.Write(h[:])
	var out [32]byte
	copy(out[:], s.Sum(nil))
	return out
}

// nodeHash hashes two child digests into a parent digest.
func nodeHash(l, r [32]byte) [32]byte {
	s := sha256.New()
	s.Write([]byte{nodePrefix})
	s.Write(l[:])
	s.Write(r[:])
	var out [32]byte
	copy(out[:], s.Sum(nil))
	return out
}

// MerkleRoot computes the root over entry hashes. An empty set hashes to the
// SHA-256 of the empty string (a fixed, well-defined sentinel).
func MerkleRoot(entryHashes [][32]byte) [32]byte {
	if len(entryHashes) == 0 {
		return sha256.Sum256(nil)
	}
	level := make([][32]byte, len(entryHashes))
	for i, h := range entryHashes {
		level[i] = leafHash(h)
	}
	for len(level) > 1 {
		next := make([][32]byte, 0, (len(level)+1)/2)
		for i := 0; i < len(level); i += 2 {
			if i+1 == len(level) {
				next = append(next, level[i]) // promote the odd one
			} else {
				next = append(next, nodeHash(level[i], level[i+1]))
			}
		}
		level = next
	}
	return level[0]
}
