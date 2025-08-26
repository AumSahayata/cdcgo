package model

import (
	"bytes"
	"encoding/hex"
	"fmt"
)

// Chunk represents a contiguous piece of input data.
//
// Fields:
//   - Offset: byte offset of the chunk within the original input
//   - Size:   length of the chunk in bytes
//   - Hash:   cryptographic hash (e.g., SHA-256) of the chunk’s data
type Chunk struct {
	Offset int64
	Size   int
	Hash   []byte
}

// HexHash returns the hash in hex string form.
func (c Chunk) HexHash() string {
	return hex.EncodeToString(c.Hash)
}

// Equal reports whether two chunks have the same hash.
// Optionally also check size to guard against collisions.
func (c Chunk) Equal(other Chunk) bool {
	return bytes.Equal(c.Hash, other.Hash) && c.Size == other.Size
}

// String implements fmt.Stringer for convenient printing.
func (c Chunk) String() string {
	return fmt.Sprintf("Chunk {offset=%d, size=%d, hash=%s}", c.Offset, c.Size, c.HexHash())
}

// VerifyChunk checks that the provided data matches the chunk's metadata.
//
// Parameters:
//   - data: the byte slice representing the chunk's contents.
//   - hashAlgo: the hash algorithm used (e.g., "sha256").
//
// Returns an error if:
//   - The computed hash of the data does not match the stored Hash.
//   - The length of data does not match the stored Size.
func (c *Chunk) VerifyChunk(data []byte, hashAlgo string) error {
	h := Hasher{Name: hashAlgo}
	hasher, err := h.New()
	if err != nil {
		return err
	}

	// Compute hash
	hasher.Write(data)
	newHash := hasher.Sum(nil)

	// Compare hash
	if !bytes.Equal(c.Hash, newHash) {
		return fmt.Errorf("chunk hash mismatch: expected %x, got %x", c.Hash, newHash)
	}

	// Compare size
	if c.Size != len(data) {
		return fmt.Errorf("chunk size mismatch: expected %d, got %d", c.Size, len(data))
	}

	return nil
}
