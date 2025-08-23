package chunk

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"testing"
)

// TestChunkReader_Basic ensures that ChunkReader produces correct chunks
// and computes SHA-256 hashes as expected
func TestChunkReader_Basic(t *testing.T) {
	// Test input
	input := []byte("Hello, World! This is test data")
	r := bytes.NewReader(input)

	// Create ChunkReader with chunk size = 8 bytes
	cr := NewChunkReader(r, sha256.New(), 8)

	// Read all chunks until EOF
	var chunks []Chunk
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

func BenchmarkChunkReader(b *testing.B) {
	// 16MB input
	data := bytes.Repeat([]byte("abcdef1234567890"), 1<<19)
	dataSize := int64(len(data))

	sizes := []int{4 << 10, 64 << 10, 1 << 20} // 4KB, 64KB, 1MB

	for _, sz := range sizes {
		b.Run(fmt.Sprintf("size=%d", sz), func(b *testing.B) {
			b.SetBytes(dataSize) // tells Go the size of input per iteration
			for b.Loop() {
				// Important: create a new reader each iteration
				cr := NewChunkReader(bytes.NewReader(data), sha256.New(), sz)
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
