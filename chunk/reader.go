package chunk

import (
	"fmt"
	"io"

	"github.com/AumSahayata/cdcgo"
	"github.com/AumSahayata/cdcgo/fastcdc"
)

// ChunkReader implements a streaming API for splitting data into chunks.
// It reads from an io.Reader, breaks the input into fixed-size chunks,
// and computes a cryptographic hash for each chunk.
type ChunkReader struct {
	r        io.Reader        // the source
	hashAlgo string           // chosen hash algorithm
	buf      []byte           // reusable buffer for reading chunks
	offset   int64            // where we are in the stream
	chunker  *fastcdc.Chunker // FastCDC chunker
	leftover int              // number of bytes from previous read
}

// NewChunkReader creates a new ChunkReader.
//
// Parameters:
//
//   - r: the input source (e.g., file, network, buffer).
//
//   - hashAlgo: the name of the hash function to use for chunking.
//     If empty, "sha256" is used by default.
//     Supported hash algorithms include:
//
//   - "sha256" — Secure Hash Algorithm 256-bit (default).
//
//   - "sha1"   — Secure Hash Algorithm 160-bit (legacy, less secure).
//
//   - "blake3" — High-performance, cryptographically strong hash.
//
//   - bufSize: the target buffer size in bytes. The internal buffer is reused
//     for efficiency, and bufSize also represents the maximum chunk size.
//
//   - chunker: a FastCDC chunker object used to determine variable-sized
//     content-defined chunk boundaries.
//
// Returns:
//   - A new ChunkReader instance ready to stream chunks from r.
//   - An error if the requested hash algorithm is unsupported.
//
// Notes:
//   - Each call to Next() reads the next chunk, computes its hash, and
//     advances the internal offset.
//   - This design allows efficient streaming, deduplication, and manifest
//     generation without loading entire files into memory.
func NewChunkReader(r io.Reader, hashAlgo string, bufSize int, chunker *fastcdc.Chunker) (*ChunkReader, error) {
	if bufSize <= 0 {
		return nil, fmt.Errorf("bufSize must be > 0")
	}

	if hashAlgo == "" {
		hashAlgo = "sha256"
	}

	return &ChunkReader{
		r:        r,
		hashAlgo: hashAlgo,
		buf:      make([]byte, bufSize),
		chunker:  chunker,
	}, nil
}

// Next reads the next chunk from the underlying stream.
//
// Returns:
//   - A Chunk containing the offset, size, and hash of the data.
//   - io.EOF when no more data is available.
//   - Any other error encountered during reading.
//
// Each call to Next advances the internal offset. The returned
// Chunk is safe to use after the call; the underlying buffer may
// be reused for subsequent chunks.
func (cr *ChunkReader) Next() (cdcgo.Chunk, []byte, error) {
	off := cr.offset

	// Fill buffer if there's space
	n, err := cr.r.Read(cr.buf[cr.leftover:])
	total := cr.leftover + n

	// If there is leftover data at EOF, emit it as the final chunk
	if total > 0 && err == io.EOF {
		cut := total
		chunkData := cr.buf[:cut]

		// Setup hasher
		h := cdcgo.Hasher{Name: cr.hashAlgo}
		hasher, err := h.New()
		if err != nil {
			return cdcgo.Chunk{}, nil, err
		}

		// Compute hash
		hasher.Reset()
		hasher.Write(chunkData)
		hash := hasher.Sum(nil)

		cr.leftover = 0
		cr.offset += int64(cut)

		return cdcgo.Chunk{
			Offset: off,
			Size:   cut,
			Hash:   hash[:],
		}, chunkData, nil
	}

	// If no data read and other error, propagate
	if total == 0 && err != nil {
		return cdcgo.Chunk{}, []byte{}, err
	}

	// propagate other errors
	if n == 0 && err != nil {
		// no data read, other errors
		return cdcgo.Chunk{}, []byte{}, err
	}

	// Determine chunk boundary
	cut := cr.chunker.NextBoundary(cr.buf[:total])
	chunkData := cr.buf[:cut]

	// Setup hasher
	h := cdcgo.Hasher{Name: cr.hashAlgo}
	hasher, err := h.New()
	if err != nil {
		return cdcgo.Chunk{}, nil, err
	}

	// Compute hash
	hasher.Reset()
	hasher.Write(chunkData)
	hash := hasher.Sum(nil)

	// Shift leftover bytes to start of buffer
	copy(cr.buf[0:], cr.buf[cut:total])
	cr.leftover = total - cut
	cr.offset += int64(cut)

	return cdcgo.Chunk{
		Offset: off,
		Size:   cut,
		Hash:   hash[:],
	}, chunkData, nil
}
