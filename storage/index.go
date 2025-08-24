package storage

import (
	"encoding/hex"
	"sync"

	"github.com/AumSahayata/cdcgo/types"
)

// Index defines the minimal interface for deduplication metadata storage.
// It is intended for in-memory or simple backends that are guaranteed to succeed.
//
// Implementations are expected to:
//   - Record unique chunks via Add()
//   - Allow fast existence checks via Exists()
//   - Optionally retrieve chunk metadata via Get()
//
// This interface is safe for local and lightweight usage where failures are not expected.
type Index interface {
	Add(chunk types.Chunk) error         // record a new chunk
	Exists(hash string) bool             // check if chunk exists
	Get(hash string) (types.Chunk, bool) // retrieve chunk info if needed
}

// PersistentIndex extends Index to support backends where storage operations
// may fail (e.g. databases, distributed systems, remote services).
//
// Unlike Index, methods here explicitly return errors to allow callers to
// handle network, I/O, or persistence failures gracefully.
//
// Implementations should be concurrency-safe and resilient to transient errors.
type PersistentIndex interface {
	Index
	ExistsWithErr(hash string) (bool, error)           // Check if chunk exists, with error reporting
	GetWithErr(hash string) (types.Chunk, bool, error) // Retrieve chunk metadata, with error reporting
}

// MemoryIndex is a simple in-memory implementation of Index.
// It uses a sync.RWMutex to allow safe concurrent access.
//
// This is best suited for testing, prototyping, or single-node use.
// It should not be used in large-scale production environments
// where durability or distributed access is required.
type MemoryIndex struct {
	store map[string]types.Chunk
	mu    sync.RWMutex
}

// NewMemoryIndex creates an empty MemoryIndex.
func NewMemoryIndex() *MemoryIndex {
	return &MemoryIndex{
		store: make(map[string]types.Chunk),
	}
}

// Add inserts a chunk into the index.
// The hash is used as the key, encoded in hex.
func (m *MemoryIndex) Add(ch types.Chunk) error {
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
func (m *MemoryIndex) Get(hash string) (types.Chunk, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ch, ok := m.store[hash]
	return ch, ok
}
