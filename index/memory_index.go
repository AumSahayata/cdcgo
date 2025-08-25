package index

import (
	"encoding/hex"
	"sync"

	"github.com/AumSahayata/cdcgo/chunk"
)

// MemoryIndex is a simple in-memory implementation of Index.
// It uses a sync.RWMutex to allow safe concurrent access.
//
// This is best suited for testing, prototyping, or single-node use.
// It should not be used in large-scale production environments
// where durability or distributed access is required.
type MemoryIndex struct {
	store map[string]chunk.Chunk
	mu    sync.RWMutex
}

// NewMemoryIndex creates an empty MemoryIndex.
func NewMemoryIndex() *MemoryIndex {
	return &MemoryIndex{
		store: make(map[string]chunk.Chunk),
	}
}

// Add inserts a chunk into the index.
// The hash is used as the key, encoded in hex.
func (m *MemoryIndex) Add(ch chunk.Chunk) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.store[hex.EncodeToString(ch.Hash)] = ch
	return nil
}

// Exists reports whether a chunk with the given hash exists in the index.
func (m *MemoryIndex) Exists(hash string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, ok := m.store[hash]
	return ok
}

// Get retrieves a chunk by its hash.
// Returns (chunk, true) if found, otherwise (zero, false).
func (m *MemoryIndex) Get(hash string) (chunk.Chunk, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ch, ok := m.store[hash]
	return ch, ok
}
