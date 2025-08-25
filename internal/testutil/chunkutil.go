package testutil

import (
	"crypto/sha256"

	"github.com/AumSahayata/cdcgo/model"
)

// TestChunk creates a test chunk with given data.
func TestChunk(data []byte, size int) model.Chunk {
	hash := sha256.Sum256(data)
	return model.Chunk{
		Offset: 0,
		Size:   size,
		Hash:   hash[:],
	}
}
