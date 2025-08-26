package model

import (
	"encoding/json"
	"os"
	"sync"
)

// Manifest represents a file’s chunk composition for deduplication.
//
// Each file gets one manifest. The manifest contains only metadata (Chunk info),
// not the actual chunk data. It can be serialized to JSON for storage, transfer, or reassembly.
type Manifest struct {
	FileName      string  `json:"file_name"`      // Original file name
	FileSize      int64   `json:"file_size"`      // Total size of the file
	HashAlgorithm string  `json:"hash_algorithm"` // e.g., "sha256"
	Chunks        []Chunk `json:"chunks"`         // Ordered list of chunks

	mu sync.Mutex // protects Chunks for concurrent access

}

// NewManifest creates a manifest for a given file.
func NewManifest(filename string, fileSize int64, hashAlgo string) *Manifest {
	return &Manifest{
		FileName:      filename,
		FileSize:      fileSize,
		HashAlgorithm: hashAlgo,
		Chunks:        make([]Chunk, 0),
	}
}

// Add adds a chunk metadata to the manifest.
func (m *Manifest) Save(path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	data, err := json.MarshalIndent(m, "", " ")
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0644)
}

// LoadManifest loads a manifest from JSON on disk.
func LoadManifest(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var m Manifest

	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}

	return &m, nil
}

// Flush writes the manifest incrementally (can be used during long-running chunking)
func (m *Manifest) Flush(path string) error {
	return m.Save(path)
}

// ChunkLoader defines a function that, given a chunk hash, returns the chunk's data.
type ChunkLoader func(hash string) ([]byte, error)

// VerifyFile validates all chunks listed in the manifest against their actual data.
//
// Parameters:
//   - loader: a function that takes a chunk hash string and returns the chunk data ([]byte) and any error.
//
// Returns an error if any chunk fails verification (hash or size mismatch).
func (m *Manifest) VerifyFile(load ChunkLoader) error {
	for _, ch := range m.Chunks {
		data, err := load(ch.HexHash())
		if err != nil {
			return err
		}

		err = ch.VerifyChunk(data, m.HashAlgorithm)
		if err != nil {
			return err
		}
	}

	return nil
}
