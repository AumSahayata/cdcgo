package chunk

import (
	"hash"
	"io"

	"github.com/AumSahayata/cdcgo/fastcdc"
	"github.com/AumSahayata/cdcgo/types"
)

// ChunkReader implements a streaming API for splitting data into chunks.
// It reads from an io.Reader, breaks the input into fixed-size chunks,
// and computes a cryptographic hash for each chunk.
type ChunkReader struct {
	r        io.Reader        // the source
	hasher   hash.Hash        // chosen hash algorithm
	buf      []byte           // reusable buffer for reading chunks
	offset   int64            // where we are in the stream
	chunker  *fastcdc.Chunker // FastCDC chunker
	leftover int              // number of bytes from previous read
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
func NewChunkReader(r io.Reader, hasher hash.Hash, bufSize int, chunker *fastcdc.Chunker) *ChunkReader {
	return &ChunkReader{
		r:       r,
		hasher:  hasher,
		buf:     make([]byte, bufSize),
		chunker: chunker,
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
func (cr *ChunkReader) Next() (types.Chunk, error) {
	off := cr.offset

	// Fill buffer if there's space
	n, err := cr.r.Read(cr.buf[cr.leftover:])
	total := cr.leftover + n

	// If there is leftover data at EOF, emit it as the final chunk
	if total > 0 && err == io.EOF {
		cut := total
		chunkData := cr.buf[:cut]

		cr.hasher.Reset()
		cr.hasher.Write(chunkData)
		hash := cr.hasher.Sum(nil)

		cr.leftover = 0
		cr.offset += int64(cut)

		return types.Chunk{
			Offset: off,
			Size:   cut,
			Hash:   hash,
		}, nil
	}

	// If no data read and other error, propagate
	if total == 0 && err != nil {
		return types.Chunk{}, err
	}

	// propagate other errors
	if n == 0 && err != nil {
		// no data read, other errors
		return types.Chunk{}, err
	}

	// Determine chunk boundary
	cut := cr.chunker.NextBoundary(cr.buf[:total])
	chunkData := cr.buf[:cut]

	// Compute hash
	cr.hasher.Reset()
	cr.hasher.Write(chunkData)
	hash := cr.hasher.Sum(nil)

	// Shift leftover bytes to start of buffer
	copy(cr.buf[0:], cr.buf[cut:total])
	cr.leftover = total - cut
	cr.offset += int64(cut)

	return types.Chunk{
		Offset: off,
		Size:   cut,
		Hash:   hash,
	}, nil
}
