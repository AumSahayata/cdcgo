package testutil

import (
	"crypto/sha256"

	"github.com/AumSahayata/cdcgo"
)

// TestChunk creates a test chunk with given data.
func TestChunk(data []byte, size int) cdcgo.Chunk {
	hash := sha256.Sum256(data)
	return cdcgo.Chunk{
		Offset: 0,
		Size:   size,
		Hash:   hash[:],
	}
}
