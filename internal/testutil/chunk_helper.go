package testutil

import (
	"crypto/sha256"

	"github.com/AumSahayata/cdcgo/chunk"
)

// TestChunk creates a test chunk with given data.
func TestChunk(data []byte, size int) chunk.Chunk {
	hash := sha256.Sum256(data)
	return chunk.Chunk{
		Offset: 0,
		Size:   size,
		Hash:   hash[:],
	}
}
