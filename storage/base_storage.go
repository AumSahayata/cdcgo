package storage

import (
	"sync"

	"github.com/AumSahayata/cdcgo/chunk"
	"github.com/AumSahayata/cdcgo/model"
)

// Storage defines the minimal behavior for a chunk storage backend.
// Backends should guarantee deduplication and safe persistence.
type Storage interface {
	Save(chunk model.Chunk, data []byte) error
	Load(hash string) ([]byte, error)
	VerifyIntegrity() error
}

// BaseStorage provides shared helpers for storage backends.
// It embeds an Index for deduplication and a mutex for safe access.
type BaseStorage struct {
	index chunk.Index
	mu    sync.Mutex
}

// ChunkExists checks if a chunk exists in the index.
func (b *BaseStorage) ChunkExists(hash string) (bool, error) {
	// If the index implements PersistentIndex, use ExistsWithErr
	if pi, ok := b.index.(chunk.PersistentIndex); ok {
		return pi.ExistsWithErr(hash)
	}

	// Otherwise, use normal Exists
	return b.index.Exists(hash), nil
}

// ChunkGet fetches the metadata of a chunk from the index.
// func (b *BaseStorage) ChunkGet(hash string) (types.Chunk, bool, error) {
// 	// Check if the index implements PersistentIndex
// 	if pi, ok := b.index.(PersistentIndex); ok {
// 		return pi.GetWithErr(hash)
// 	}

// 	// Fallback for normal Index
// 	ch, ok := b.index.Get(hash)
// 	return ch, ok, nil
// }
