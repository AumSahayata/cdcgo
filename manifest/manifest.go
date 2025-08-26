package manifest

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/AumSahayata/cdcgo"
	"github.com/AumSahayata/cdcgo/storage"
)

// Manifest represents a file’s chunk composition for deduplication.
//
// Each file gets one manifest. The manifest contains only metadata (Chunk info),
// not the actual chunk data. It can be serialized to JSON for storage, transfer, or reassembly.
type Manifest struct {
	FileName      string        `json:"file_name"`      // Original file name
	FileSize      int64         `json:"file_size"`      // Total size of the file
	HashAlgorithm string        `json:"hash_algorithm"` // e.g., "sha256"
	Chunks        []cdcgo.Chunk `json:"chunks"`         // Ordered list of chunks

	mu sync.Mutex // protects Chunks for concurrent access

}

// NewManifest creates a manifest for a given file.
func NewManifest(filename string, fileSize int64, hashAlgo string) *Manifest {
	return &Manifest{
		FileName:      filename,
		FileSize:      fileSize,
		HashAlgorithm: hashAlgo,
		Chunks:        make([]cdcgo.Chunk, 0),
	}
}

// ChunkLoader defines a function that, given a chunk hash, returns the chunk's data.
type ChunkLoader func(hash string) ([]byte, error)

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

// Load loads a manifest from JSON on disk.
func Load(path string) (*Manifest, error) {
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

// VerifyFileWithLoader validates all chunks using a custom loader.
func (m *Manifest) VerifyFileWithLoader(load ChunkLoader) error {
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

// VerifyFile validates all chunks listed in the manifest against their actual data.
//
// Parameters:
//   - loader: a function that takes a chunk hash string and returns the chunk data ([]byte) and any error.
//
// Returns an error if any chunk fails verification (hash or size mismatch).
func (m *Manifest) VerifyFile(s storage.Storage) error {
	return m.VerifyFileWithLoader(s.Load)
}

// ReassembleWithLoader reassembles all chunks using a custom loader.
func (m *Manifest) ReassembleWithLoader(load ChunkLoader, w io.Writer) error {
	for _, ch := range m.Chunks {
		data, err := load(ch.HexHash())
		if err != nil {
			return fmt.Errorf("load chunk %s: %w", ch.HexHash(), err)
		}
		if err := ch.VerifyChunk(data, m.HashAlgorithm); err != nil {
			return fmt.Errorf("verify chunk %s: %w", ch.HexHash(), err)
		}
		if _, err := w.Write(data); err != nil {
			return fmt.Errorf("write chunk %s: %w", ch.HexHash(), err)
		}
	}

	return nil
}

// Reassemble restores the original file contents from chunks.
//
// It takes a load function (hash -> []byte) and an io.Writer to stream the
// reassembled data. It also verifies each chunk before writing.
func (m *Manifest) Reassemble(s storage.Storage, w io.Writer) error {
	return m.ReassembleWithLoader(s.Load, w)
}

// RestoreFileWithLoader restores the file using a custom loader.
func (m *Manifest) RestoreFileWithLoader(load ChunkLoader, dir string) error {
	// ensure directory exists
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create restore dir: %w", err)
	}

	dstPath := filepath.Join(dir, m.FileName)

	f, err := os.Create(dstPath)
	if err != nil {
		return fmt.Errorf("create file %s: %w", dstPath, err)
	}
	defer f.Close()

	return m.ReassembleWithLoader(load, f)
}

// RestoreFile restores the file described by the manifest into the given directory.
// The final file path will be dir/m.FileName.
func (m *Manifest) RestoreFile(s storage.Storage, dir string) error {
	return m.RestoreFileWithLoader(s.Load, dir)
}
