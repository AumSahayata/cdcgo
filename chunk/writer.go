package chunk

import (
	"encoding/hex"
	"io"
	"sync"

	"github.com/AumSahayata/cdcgo"
	"github.com/AumSahayata/cdcgo/index"
)

// ChunkWriter writes chunks to an underlying storage
// and avoids duplicates using an Index.
type ChunkWriter struct {
	w      io.Writer // underlying storage
	index  Index     // dedupe index
	offset int64     // write position
	mu     sync.Mutex
}

// NewChunkWriter creates a new ChunkWriter.
// If no index is provided, a MemoryIndex will be used.
func NewChunkWriter(w io.Writer, idx Index) *ChunkWriter {
	if idx == nil {
		idx = index.NewMemoryIndex()
	}

	return &ChunkWriter{
		w:     w,
		index: idx,
	}
}

// WriteChunk writes a chunk’s data to the underlying writer if it is unique.
// Duplicate chunks are skipped.
//
// Returns:
//   - n: number of bytes written
//   - duplicate: true if the chunk was already written
//   - err: any underlying write error
func (cw *ChunkWriter) WriteChunk(chunk cdcgo.Chunk, data []byte) (written int, duplicate bool, err error) {
	cw.mu.Lock()
	defer cw.mu.Unlock()

	hashkey := hex.EncodeToString(chunk.Hash)

	if cw.index.Exists(hashkey) {
		// Chunk already written; skip writing
		return 0, true, nil
	}

	// Write chunk data
	n, err := cw.w.Write(data)
	if err != nil {
		return n, false, err
	}

	// Update index and offset
	err = cw.index.Add(chunk)
	if err != nil {
		return n, false, err
	}
	cw.offset += int64(n)

	return n, false, nil
}

// Flush flushes the underlying writer if supported.
func (cw *ChunkWriter) Flush() error {
	if f, ok := cw.w.(interface{ Flush() error }); ok {
		return f.Flush()
	}

	return nil
}
