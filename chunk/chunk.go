package chunk

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
