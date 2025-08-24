package storage

import (
	"sync"

	"github.com/AumSahayata/cdcgo/types"
)

// Storage interface defines backend behavior.
type Storage interface {
	Save(chunk types.Chunk, data []byte) error
	Load(hash string) ([]byte, error)
}

// BaseStorage provides shared logic for all storage backends.
type BaseStorage struct {
	index Index
	mu    sync.Mutex
}

// chunkExists checks if a chunk exists in the index.
func (b *BaseStorage) chunkExists(hash string) (bool, error) {
	// If the index implements PersistentIndex, use ExistsWithErr
	if pi, ok := b.index.(PersistentIndex); ok {
		return pi.ExistsWithErr(hash)
	}

	// Otherwise, use normal Exists
	return b.index.Exists(hash), nil
}

// chunkGet fetches the metadata of a chunk from the index.
func (b *BaseStorage) chunkGet(hash string) (types.Chunk, bool, error) {
	// Check if the index implements PersistentIndex
	if pi, ok := b.index.(PersistentIndex); ok {
		return pi.GetWithErr(hash)
	}

	// Fallback for normal Index
	ch, ok := b.index.Get(hash)
	return ch, ok, nil
}
