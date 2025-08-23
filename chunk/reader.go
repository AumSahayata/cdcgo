package chunk

import (
	"hash"
	"io"
)

// ChunkReader implements a streaming API for splitting data into chunks.
// It reads from an io.Reader, breaks the input into fixed-size chunks,
// and computes a cryptographic hash for each chunk.
//
// In later versions, this fixed-size strategy will be replaced with
// Content-Defined Chunking (FastCDC).
type ChunkReader struct {
	r      io.Reader // the source
	hasher hash.Hash // chosen hash algorithm
	buf    []byte    // reusable buffer for reading chunks
	offset int64     // where we are in the stream
}

// NewChunkReader creates a new ChunkReader.
//
// Parameters:
//   - r: the input source (e.g. file, network, buffer)
//   - hasher: the chosen hash function (e.g. sha256.New())
//   - bufSize: the target chunk size in bytes
//
// The ChunkReader will reuse an internal buffer of size bufSize
// for efficiency, so bufSize also defines the maximum chunk size.
func NewChunkReader(r io.Reader, hasher hash.Hash, bufSize int) *ChunkReader {
	return &ChunkReader{
		r:      r,
		hasher: hasher,
		buf:    make([]byte, bufSize),
	}
}

// Next reads the next chunk from the underlying stream.
//
// It returns:
//   - A Chunk containing the offset, size, and hash of the data.
//   - io.EOF when no more data is available.
//   - Any other error encountered during reading.
//
// Each call to Next advances the internal offset. The returned
// Chunk is safe to use after the call; the underlying buffer may
// be reused for subsequent chunks.
func (cr *ChunkReader) Next() (Chunk, error) {
	off := cr.offset

	// Read into buffer
	n, err := cr.r.Read(cr.buf)
	if n > 0 {
		cr.offset += int64(n)

		// Reset hasher for each chunk
		cr.hasher.Reset()
		cr.hasher.Write(cr.buf[:])
		hash := cr.hasher.Sum(nil)

		return Chunk{
			Offset: off,
			Size:   n,
			Hash:   hash,
		}, nil
	}

	// End of stream
	if err == io.EOF {
		return Chunk{}, io.EOF
	}

	// Propagate other errors
	if err != nil {
		return Chunk{}, err
	}

	return Chunk{}, nil
}
