package cdcgo

import (
	"crypto/sha1"
	"crypto/sha256"
	"fmt"
	"hash"

	"github.com/zeebo/blake3"
)

// Hasher is a factory for hash.Hash based on a named algorithm.
type Hasher struct {
	Name string // "sha256", "sha1", "blake3", etc.
}

// New creates a fresh hash.Hash instance for the chosen algorithm.
func (h Hasher) New() (hash.Hash, error) {
	switch h.Name {
	case "sha256":
		return sha256.New(), nil
	case "sha1":
		return sha1.New(), nil
	case "blake3":
		return blake3.New(), nil
	default:
		return nil, fmt.Errorf("unsupported hash algorithm: %s", h.Name)
	}
}
