package chunk_test

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"testing"

	"github.com/AumSahayata/cdcgo"
	"github.com/AumSahayata/cdcgo/chunk"
	"github.com/AumSahayata/cdcgo/fastcdc"
)

// TestChunkWriter_Basic tests basic ChunkWriter functionality.
//
// Checks:
// 1. Writing sequential chunks to underlying storage.
// 2. Deduplication: duplicate chunks are skipped.
// 3. Correct number of bytes written.
// 4. Final buffer content matches expected concatenation.
func TestChunkWriter_Basic(t *testing.T) {
	// Prepare a buffer to act as underlying storage
	buf := &bytes.Buffer{}
	cw := chunk.NewChunkWriter(buf, nil)

	data1 := []byte("Chunk1")
	data2 := []byte("Chunk2")

	hash1 := sha256.Sum256(data1)
	ch1 := cdcgo.Chunk{Hash: hash1[:]}

	hash2 := sha256.Sum256(data2)
	ch2 := cdcgo.Chunk{Hash: hash2[:]}

	// Write first chunk
	n, dup, err := cw.WriteChunk(ch1, data1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dup {
		t.Fatalf("first chunk should not be duplicate")
	}
	if n != len(data1) {
		t.Errorf("bytes written = %d, want %d", n, len(data1))
	}

	// Write second chunk
	n, dup, err = cw.WriteChunk(ch2, data2)
	if dup || err != nil || n != len(data2) {
		t.Errorf("writing second chunk failed")
	}

	// Write duplicate of first chunk
	n, dup, _ = cw.WriteChunk(ch1, data1)
	if !dup || n != 0 {
		t.Errorf("duplicate chunk was not skipped correctly")
	}

	// Verify buffer content
	expected := append(data1, data2...)
	if !bytes.Equal(buf.Bytes(), expected) {
		t.Errorf("buffer content mismatch: got %v, want %v", buf.Bytes(), expected)
	}
}

// BenchmarkChunkWriter measures throughput and allocations of ChunkWriter.
//
// It repeatedly writes 16MB of sample data split into FastCDC chunks
// using a sha256 hasher. Benchmarks are performed using io.Discard to
// avoid actual I/O overhead and isolate processing performance.
func BenchmarkChunkWriter(b *testing.B) {
	// Prepare 16MB of test data
	data := bytes.Repeat([]byte("abcdef1234567890"), 1<<19) // 16 MB
	dataSize := int64(len(data))

	// FastCDC parameters
	params := fastcdc.NewParams(4<<10, 8<<10, 16<<10, nil) // min=4KB, avg=8KB, max=16KB
	chunker := fastcdc.NewChunker(&params)

	// Test multiple buffer sizes for ChunkReader
	bufferSizes := []int{4 << 10, 64 << 10, 1 << 20} // 4KB, 64KB, 1MB

	for _, bufSize := range bufferSizes {
		b.Run(fmt.Sprintf("bufSize=%d", bufSize), func(b *testing.B) {
			b.SetBytes(dataSize) // allows Go to report MB/s
			for b.Loop() {
				reader, err := chunk.NewChunkReader(bytes.NewReader(data), "", bufSize, chunker)
				if err != nil {
					b.Fatalf("failed to create chunk reader: %v", err)
				}
				writer := chunk.NewChunkWriter(io.Discard, nil)

				for {
					ch, _, err := reader.Next()
					if err == io.EOF {
						break
					}
					if err != nil {
						b.Fatalf("unexpected error: %v", err)
					}

					// Slice the chunk data correctly
					chunkData := data[ch.Offset : ch.Offset+int64(ch.Size)]
					_, _, _ = writer.WriteChunk(ch, chunkData)
				}
			}
		})
	}
}
