package storage

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/AumSahayata/cdcgo/chunk"
	"github.com/AumSahayata/cdcgo/index"
	"github.com/AumSahayata/cdcgo/model"
)

// FSStorage provides file-backed chunk storage combined with a Index.
// Chunks are stored as files on disk, named by their hash.
// A metadata index ensures deduplication and optional recovery.
type FSStorage struct {
	BaseStorage
	rootDir string // base directory for chunk files
}

// NewFSStorage(root string, idx Index) (*FSStorage, error)
//   - Ensure root directory exists
//   - Initialize FSStorage with given index (e.g., MemoryIndex)
//   - If idx is nil, create a new MemoryIndex (In memory)
func NewFSStorage(root string, idx chunk.Index) (*FSStorage, error) {
	// Ensure root directory exists
	if err := os.MkdirAll(root, 0755); err != nil {
		return nil, fmt.Errorf("failed to create root directory: %w", err)
	}

	// If no index provided, create default in-memory index
	if idx == nil {
		idx = index.NewMemoryIndex()
	}

	return &FSStorage{
		BaseStorage: BaseStorage{
			index: idx,
		},
		rootDir: root,
	}, nil
}

// Save writes a chunk to the filesystem if it does not already exist.
// It uses the provided Index to check for duplicates.
//
// Parameters:
//   - chunk: the chunk metadata (hash, offset, size)
//   - data: the actual chunk bytes
//
// Returns an error if writing to disk fails.
func (fs *FSStorage) Save(chunk model.Chunk, data []byte) error {
	// Lock for concurrent writes
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Convert hash to string key
	key := chunk.HexHash()

	// Check if chunk exists in the index
	exists, err := fs.ChunkExists(key)
	if err != nil {
		return err
	}

	if exists {
		return nil // skip writing duplicates
	}

	// Build file path: RootDir/<hash>
	filePath := filepath.Join(fs.rootDir, key)

	// Write data to a temporary file
	tmpPath := filePath + ".tmp"

	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write chunk: %w", err)
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("failed to sync chunk: %w", err)
	}

	// Close file before renaming
	if err := f.Close(); err != nil {
		return err
	}

	err = os.Rename(tmpPath, filePath)
	if err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	if err := fs.index.Add(chunk); err != nil {
		_ = os.Remove(filePath)
		return fmt.Errorf("failed to update index: %w", err)
	}

	return nil
}

// Load reads the data for a given chunk from the filesystem.
//
// Parameters:
//   - hash: the hex-encoded hash of the chunk to retrieve.
//
// Returns:
//   - []byte containing the chunk data
//   - error if the chunk does not exist or cannot be read
func (fs *FSStorage) Load(hash string) ([]byte, error) {
	// Check the index for existence
	exists, err := fs.ChunkExists(hash)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, os.ErrNotExist
	}

	// Construct the file path
	filePath := filepath.Join(fs.rootDir, hash)

	// Read the file from disk
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read chunk %s: %w", hash, err)
	}

	return data, nil
}
