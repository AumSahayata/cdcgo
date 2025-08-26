// Package benchmark contains end-to-end performance benchmarks and tests
// for the cdcgo library. Benchmarks exercise the full pipeline:
//   - ChunkReader: content-defined chunking
//   - Storage: deduplication and persistence
//   - Manifest: file reassembly and integrity verification
//
// Benchmarks measure throughput, memory usage, and deduplication efficiency.
//
// Example usage:
//
//	go test -bench=. ./benchmark
package benchmark

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/AumSahayata/cdcgo/chunk"
	"github.com/AumSahayata/cdcgo/fastcdc"
	"github.com/AumSahayata/cdcgo/index"
	"github.com/AumSahayata/cdcgo/manifest"
	"github.com/AumSahayata/cdcgo/storage"
)

func TestPipeline_Full(t *testing.T) {
	root := t.TempDir()
	data := []byte("The quick brown fox jumps over the lazy dog")
	r := bytes.NewReader(data)

	p := fastcdc.NewParams(5, 10, 20, nil)
	chunker := fastcdc.NewChunker(&p)

	hashAlgo := "sha256"
	cr, err := chunk.NewChunkReader(r, hashAlgo, 16, chunker)
	if err != nil {
		t.Fatalf("failed to create chunk reader: %v", err)
	}

	fs, err := storage.NewFSStorage(root, index.NewMemoryIndex())
	if err != nil {
		t.Fatalf("failed to create file storage: %v", err)
	}

	m := manifest.NewManifest("example.txt", int64(len(data)), hashAlgo)

	for {
		ch, d, err := cr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("chunk reader error: %v", err)
		}

		if err := fs.Save(ch, d); err != nil {
			t.Fatalf("fs storage save error: %v", err)
		}

		m.Chunks = append(m.Chunks, ch)
	}

	if err := m.VerifyFile(fs); err != nil {
		t.Fatalf("manifest verification failed: %v", err)
	}
}

func BenchmarkPipeline_SaveChunks(b *testing.B) {
	root := b.TempDir()
	hashAlgo := "sha256"

	// Read all files from testdata folder
	files, err := os.ReadDir("testdata")
	if err != nil {
		b.Fatalf("failed to read testdata: %v", err)
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		fname := filepath.Join("testdata", file.Name())

		b.Run(file.Name(), func(b *testing.B) {
			data, err := os.ReadFile(fname)
			if err != nil {
				b.Fatalf("failed to read file %s: %v", fname, err)
			}

			p := fastcdc.NewParams(32*1024, 128*1024, 512*1024, nil)
			chunker := fastcdc.NewChunker(&p)

			fs, err := storage.NewFSStorage(root, index.NewMemoryIndex())
			if err != nil {
				b.Fatalf("failed to create FSStorage: %v", err)
			}

			b.SetBytes(int64(len(data)))
			b.ResetTimer()

			for b.N > 0 {
				cr, err := chunk.NewChunkReader(bytes.NewReader(data), hashAlgo, 512*1024, chunker)
				if err != nil {
					b.Fatalf("failed to create ChunkReader: %v", err)
				}

				totalChunks := 0
				uniqueChunks := 0

				for {
					ch, chunkData, err := cr.Next()
					if err == io.EOF {
						break
					}
					if err != nil {
						b.Fatalf("chunk reader error: %v", err)
					}

					totalChunks++

					exists, _ := fs.ChunkExists(ch.HexHash())
					if !exists {
						uniqueChunks++
					}

					_ = fs.Save(ch, chunkData)
				}

				// Report dedupe ratio
				dedupeRatio := float64(totalChunks) / float64(uniqueChunks)
				b.ReportMetric(dedupeRatio, "dedupe_ratio")
				break // Process each file once per benchmark iteration
			}
		})
	}
}
