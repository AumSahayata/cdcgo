package chunk

import (
	"io"
	"sync"
)

// ChunkWriter writes chunks to an underlying storage.
// It optionally supports deduplication using an in-memory index.
type ChunkWriter struct {
	w io.Writer  // underlying storage
	index map[string]int64 // optional dedupe: hash (hex or []byte) -> offset
	offset int64 // write position
	mu     sync.Mutex // protect concurrent writes (optional)
}

// NewChunkWriter creates a new ChunkWriter for the given io.Writer.
func NewChunkWriter(w io.Writer) *ChunkWriter {
	return &ChunkWriter{
		w: w,
		index: make(map[string]int64),
	}
}

// WriteChunk writes a chunk to the underlying writer.
// If the chunk is already present in the index, it can skip writing.
// Returns:
//   - written: number of bytes written (0 if duplicate)
//   - duplicate: true if chunk was skipped
//   - err: any I/O error
func (cw *ChunkWriter) WriteChunk(chunk Chunk, data []byte) (written int, duplicate bool, err error) {
	cw.mu.Lock()
	defer cw.mu.Unlock()

	hashkey := string(chunk.Hash)

	if _, exists := cw.index[hashkey]; exists {
		// Chunk already written; skip writing
		return 0, true, nil
	}

	// Write chunk data
	n, err := cw.w.Write(data)
	if err != nil {
		return n, false, err
	}

	// Update index and offset
	cw.index[hashkey] = cw.offset
	cw.offset += int64(n)

	return n, false, nil
}

// Flush is optional for buffered writers
func (cw *ChunkWriter) Flush() error {
	if f, ok := cw.w.(interface{ Flush() error}); ok {
		return f.Flush()
	}

	return nil
}