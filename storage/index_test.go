package storage

import (
	"crypto/sha256"
	"sync/atomic"
	"testing"

	"github.com/AumSahayata/cdcgo/types"
)

// helperChunk creates a test chunk with given data.
func helperChunk(data []byte, size int) types.Chunk {
	hash := sha256.Sum256(data)
	return types.Chunk{
		Offset: 0,
		Size:   size,
		Hash:   hash[:],
	}
}

// TestMemoryIndex_AddAndExists verifies that a chunk can be added
// and later found using Exists().
func TestMemoryIndex_AddAndExists(t *testing.T) {
	ch := helperChunk([]byte("test-data"), 9)

	mi := NewMemoryIndex()

	// Add the chunk
	if err := mi.Add(ch); err != nil {
		t.Fatalf("unexpected error adding chunk: %v", err)
	}

	// Check existence
	if !mi.Exists(ch.HexHash()) {
		t.Errorf("expected chunk to exist after Add, but it does not")
	}
}

// TestMemoryIndex_Get verifies retrieval of a chunk by its hash.
func TestMemoryIndex_Get(t *testing.T) {
	ch := helperChunk([]byte("chunks"), 6)

	mi := NewMemoryIndex()
	if err := mi.Add(ch); err != nil {
		t.Fatalf("unexpected error adding chunk: %v", err)
	}

	got, ok := mi.Get(ch.HexHash())
	if !ok {
		t.Fatalf("expected chunk to be retrievable, but it was not found")
	}

	if got.HexHash() != ch.HexHash() {
		t.Errorf("retrieved chunk hash mismatch: got=%s want=%s", got.HexHash(), ch.HexHash())
	}
}

// TestMemoryIndex_NonExistent ensures Exists() and Get() behave correctly
// when the chunk is not in the index.
func TestMemoryIndex_NonExistent(t *testing.T) {
	mi := NewMemoryIndex()

	if mi.Exists("pokemon") {
		t.Errorf("expected Exists() to return false for unknown hash")
	}

	if _, ok := mi.Get("pokemon"); ok {
		t.Errorf("expected Get() to fail for unknown hash")
	}
}

// TestMemoryIndex_Concurrent verifies that MemoryIndex
// is safe for concurrent use.
func TestMemoryIndex_Concurrent(t *testing.T) {
	mi := NewMemoryIndex()
	ch := helperChunk([]byte("concurrent"), 10)

	done := make(chan bool)

	// Writer goroutine
	go func() {
		for range 1000 {
			_ = mi.Add(ch)
		}
		done <- true
	}()

	// Reader goroutine
	go func() {
		for range 1000 {
			_ = mi.Exists(ch.HexHash())
			_, _ = mi.Get(ch.HexHash())
		}
		done <- true
	}()

	<-done
	<-done
}

// BenchmarkMemoryIndex_Add measures write throughput (Add only).
func BenchmarkMemoryIndex_Add(b *testing.B) {
	idx := NewMemoryIndex()
	chunkSize := 1024 // 1 KB chunks
	b.SetBytes(int64(chunkSize))

	for i := 0; b.Loop(); i++ {
		// prepare unique data per iteration
		data := make([]byte, chunkSize)
		data[0] = byte(i) // ensure different hash each loop
		ch := helperChunk(data, chunkSize)

		_ = idx.Add(ch)
	}
}

// BenchmarkMemoryIndex_Exists measures lookup throughput (Exists only).
func BenchmarkMemoryIndex_Exists(b *testing.B) {
	idx := NewMemoryIndex()
	chunkSize := 1024
	b.SetBytes(int64(chunkSize))

	ch := helperChunk([]byte("zoro"), chunkSize)
	_ = idx.Add(ch)

	b.ResetTimer()
	for b.Loop() {
		_ = idx.Exists(ch.HexHash())
	}
}

// BenchmarkMemoryIndex_AddAndExists measures mixed workload (Add + Exists).
func BenchmarkMemoryIndex_AddAndExists(b *testing.B) {
	idx := NewMemoryIndex()
	chunkSize := 1024
	b.SetBytes(int64(chunkSize))

	for i := 0; b.Loop(); i++ {
		// prepare unique data per iteration
		data := make([]byte, chunkSize)
		data[0] = byte(i) // ensure different hash each loop

		ch := helperChunk(data, chunkSize)
		_ = idx.Add(ch)
		_ = idx.Exists(ch.HexHash())
	}
}

// BenchmarkMemoryIndex_Parallel measures concurrent Add+Exists workload.
// Uses atomic counter for unique data per goroutine.
func BenchmarkMemoryIndex_Parallel(b *testing.B) {
	idx := NewMemoryIndex()
	chunkSize := 1024
	b.SetBytes(int64(chunkSize))

	var counter uint64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := atomic.AddUint64(&counter, 1)
			data := make([]byte, chunkSize)
			data[0] = byte(i)
			ch := helperChunk(data, chunkSize)
			_ = idx.Add(ch)
			_ = idx.Exists(ch.HexHash())
		}
	})
}
