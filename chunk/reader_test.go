package chunk

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"testing"

	"github.com/AumSahayata/cdcgo/fastcdc"
	"github.com/AumSahayata/cdcgo/types"
)

// TestChunkReader_HashBasic ensures that ChunkReader produces correct chunks
// and computes SHA-256 hashes as expected.
func TestChunkReader_HashBasic(t *testing.T) {
	// Test input
	input := []byte("Hello, World! This is test data")
	r := bytes.NewReader(input)
	params := fastcdc.NewParams(10, 20, 50, nil)

	// Create ChunkReader with chunk size = 8 bytes
	cr := NewChunkReader(r, sha256.New(), 8, fastcdc.NewChunker(params))

	// Read all chunks until EOF
	var chunks []types.Chunk
	for {
		ch, err := cr.Next()
		if err == io.EOF {
			break
		}

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		chunks = append(chunks, ch)
	}

	// Check that chunks cover the whole input
	var total int
	for _, ch := range chunks {
		total += ch.Size
	}

	if total != len(input) {
		t.Errorf("total chunked size = %d, want %d", total, len(input))
	}

	// Verify first chunk hash matches manual SHA-256
	firstChunk := input[:8]
	wantHash := sha256.Sum256(firstChunk)

	if !bytes.Equal(chunks[0].Hash, wantHash[:]) {
		t.Errorf("hash mismatch: got %x, want %x", chunks[0].Hash, wantHash)
	}
}

// TestChunkReader_Normal verifies that ChunkReader correctly reads a stream
// of data using FastCDC chunking. It ensures that all data is chunked
// and that each chunk size respects the MinSize/MaxSize constraints.
func TestChunkReader_Normal(t *testing.T) {
	data := bytes.Repeat([]byte{0xAA}, 1024)
	params := fastcdc.NewParams(50, 100, 200, nil)
	chunker := fastcdc.NewChunker(params)
	cr := NewChunkReader(bytes.NewReader(data), sha256.New(), 256, chunker)

	offset := 0
	for {
		ch, err := cr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// Last chunk may be smaller than MinSize
		if offset+ch.Size < len(data) { // not the last chunk
			if ch.Size < params.MinSize || ch.Size > params.MaxSize {
				t.Errorf("chunk size out of bounds: %d", ch.Size)
			}
		}
		offset += ch.Size
	}

	if offset != len(data) {
		t.Errorf("total bytes read %d, expected %d", offset, len(data))
	}
}

// TestChunkReader_LeftoverEOF verifies that leftover bytes in the buffer
// are correctly returned as a final chunk when the reader reaches EOF.
func TestChunkReader_LeftoverEOF(t *testing.T) {
	data := bytes.Repeat([]byte{0xAB}, 150) // smaller than buffer
	params := fastcdc.NewParams(50, 100, 200, nil)
	chunker := fastcdc.NewChunker(params)

	cr := NewChunkReader(bytes.NewReader(data), sha256.New(), 128, chunker)

	totalRead := 0

	for {
		ch, err := cr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		totalRead += ch.Size
	}

	if totalRead != len(data) {
		t.Errorf("leftover not processed correctly, got %d, want %d", totalRead, len(data))
	}
}

// errorReader is a helper reader that always returns an error.
type errorReader struct{}

func (e *errorReader) Read(p []byte) (int, error) {
	return 0, fmt.Errorf("simulated read error")
}

// TestChunkReader_ReadError ensures that ChunkReader correctly propagates
// read errors when no bytes are read from the underlying reader.
func TestChunkReader_ReadError(t *testing.T) {

	params := fastcdc.NewParams(50, 100, 200, nil)
	chunker := fastcdc.NewChunker(params)
	cr := NewChunkReader(&errorReader{}, sha256.New(), 128, chunker)

	_, err := cr.Next()
	if err == nil || err.Error() != "simulated read error" {
		t.Fatalf("expected read error, got %v", err)
	}
}

func BenchmarkChunkReader(b *testing.B) {
	// 16MB input
	data := bytes.Repeat([]byte("abcdef1234567890"), 1<<19)
	dataSize := int64(len(data))

	sizes := []int{4 << 10, 64 << 10, 1 << 20}             // 4KB, 64KB, 1MB
	params := fastcdc.NewParams(4<<10, 8<<10, 16<<10, nil) // chunk sizes in bytes

	for _, sz := range sizes {
		b.Run(fmt.Sprintf("size=%d", sz), func(b *testing.B) {
			b.SetBytes(dataSize) // tells Go the size of input per iteration
			for b.Loop() {
				// Important: create a new reader each iteration
				cr := NewChunkReader(bytes.NewReader(data), sha256.New(), sz, fastcdc.NewChunker(params))
				// Consume all chunks
				for {
					_, err := cr.Next()
					if err == io.EOF {
						break
					}
					if err != nil {
						b.Fatalf("unexpected error: %v", err)
					}
				}
			}
		})
	}
}
