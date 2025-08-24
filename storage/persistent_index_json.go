package storage

import (
	"encoding/hex"
	"encoding/json"
	"maps"
	"os"
	"sync"

	"github.com/AumSahayata/cdcgo/types"
)

// PersistentIndexJSON is a JSON-backed implementation of PersistentIndex.
// It maintains a map of hash → Chunk in memory, and periodically flushes to disk.
//
// It implements the PersistentIndex interface, allowing chunk metadata
// to be stored and retrieved across program runs.
//
// Concurrency:
//   - Safe for concurrent use via an internal RWMutex.
//   - Each mutation is flushed to disk to ensure durability.
//
// Notes:
//   - Best for small/medium datasets.
//   - For high scale, prefer BoltDB/SQLite implementations.
type PersistentIndexJSON struct {
	path  string                 // file path on disk
	store map[string]types.Chunk // in-memory representation
	mu    sync.RWMutex           // concurrency control
}

// NewPersistentIndexJSON creates (or loads) a JSON-backed persistent index.
//
// If the file already exists, it will be loaded into memory.
// If not, an empty index will be created.
//
// Parameters:
//   - path: file path to the JSON index file
//
// Returns:
//   - *PersistentIndexJSON instance
//   - error if the file cannot be read or parsed
func NewPersistentIndexJSON(path string) (*PersistentIndexJSON, error) {
	idx := &PersistentIndexJSON{
		path:  path,
		store: make(map[string]types.Chunk),
	}

	// Check if the file exists
	if _, err := os.Stat(path); err == nil {
		// File exists → load it
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}

		// Unmarshal JSON into the store map
		if err := json.Unmarshal(data, &idx.store); err != nil {
			return nil, err
		}
	} else if !os.IsNotExist(err) {
		// Other error besides "file not found"
		return nil, err
	}
	return idx, nil
}

// Add inserts a chunk into the index and persists the update to disk.
//
// If the chunk already exists, it is silently ignored.
// Errors during disk flush are returned.
func (p *PersistentIndexJSON) Add(ch types.Chunk) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	newStore := make(map[string]types.Chunk)
	maps.Copy(newStore, p.store)
	newStore[hex.EncodeToString(ch.Hash)] = ch

	// Serialize to JSON
	data, err := json.MarshalIndent(newStore, "", " ")
	if err != nil {
		return err
	}

	// Write to temp file.
	tmpPath := p.path + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.Write(data); err != nil {
		return err
	}
	if err := f.Sync(); err != nil { // ensure durability
		return err
	}

	// Close file before renaming
	if err := f.Close(); err != nil {
		return err
	}

	// Atomic rename.
	if err := os.Rename(tmpPath, p.path); err != nil {
		return err
	}

	// Commit to memory.
	p.store = newStore

	return nil
}

// Exists checks if a chunk with the given hash exists in the index.
//
// Returns:
//   - true if present
//   - false otherwise
//
// This method never fails since it only consults in-memory state.
func (p *PersistentIndexJSON) Exists(hash string) bool {
	ok, _ := p.ExistsWithErr(hash) // ignore error
	return ok
}

// ExistsWithErr checks if a chunk exists in the persistent index.
//
// Returns:
//   - bool: true if chunk exists
//   - error: if the underlying store is unavailable or corrupted
func (p *PersistentIndexJSON) ExistsWithErr(hash string) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check in-memory
	if _, ok := p.store[hash]; ok {
		return ok, nil
	}

	// Reload from JSON file
	p.load()

	_, ok := p.store[hash]
	return ok, nil
}

// Get retrieves a chunk by hash if available.
//
// Returns:
//   - chunk
//   - true if found
//   - false if not found
func (p *PersistentIndexJSON) Get(hash string) (types.Chunk, bool) {
	ch, ok, _ := p.GetWithErr(hash)
	return ch, ok
}

// GetWithErr retrieves a chunk by its hash from the JSON-backed index.
// It first checks the in-memory map, and if not found, reloads from disk.
//
// Returns:
//   - chunk
//   - boolean
//   - error
func (p *PersistentIndexJSON) GetWithErr(hash string) (types.Chunk, bool, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Check in-memory
	if ch, ok := p.store[hash]; ok {
		return ch, true, nil
	}

	// Reload from JSON file
	p.load()

	ch, ok := p.store[hash]
	if !ok {
		return types.Chunk{}, false, nil
	}

	return ch, true, nil
}

// load loads the JSON file into the in-memory map.
//
// Called at initialization, and can be used to refresh state.
func (p *PersistentIndexJSON) load() error {
	data, err := os.ReadFile(p.path)
	if err != nil {
		return err
	}

	tmp := make(map[string]types.Chunk)
	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}

	// Replace in-memory store with fresh state
	p.store = tmp
	return nil
}
