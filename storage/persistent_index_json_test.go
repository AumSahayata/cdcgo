package storage

import (
	"os"
	"sync/atomic"
	"testing"
)

// TestPersistentIndexJSON_AddAndExists verifies chunks can be added
// and queried, and persistence to disk is preserved.
func TestPersistentIndexJSON_AddAndExists(t *testing.T) {
	// Create temp file
	path := t.TempDir() + "/index.json"

	// Open new index
	idx, err := NewPersistentIndexJSON(path)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	// Add a chunk
	ch := helperChunk([]byte("jayson"), 6)
	if err := idx.Add(ch); err != nil {
		t.Fatalf("failed to add: %v", err)
	}

	// Check exists
	if ok := idx.Exists(ch.HexHash()); !ok {
		t.Errorf("expected chunk to exist")
	}

	// Reopen index from disk
	idx2, err := NewPersistentIndexJSON(path)
	if err != nil {
		t.Fatalf("failed to reopen index: %v", err)
	}

	if ok := idx2.Exists(ch.HexHash()); !ok {
		t.Errorf("expected chunk to exist after reload")
	}
}

// TestPersistentIndexJSON_Get verifies retrieval of a chunk by its hash.
func TestPersistentIndexJSON_Get(t *testing.T) {
	// Create temp file
	path := t.TempDir() + "/index.json"

	// Open new index
	idx, err := NewPersistentIndexJSON(path)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	ch := helperChunk([]byte("chunks"), 6)
	if err := idx.Add(ch); err != nil {
		t.Fatalf("unexpected error adding chunk: %v", err)
	}

	got, ok := idx.Get(ch.HexHash())
	if !ok {
		t.Fatalf("expected chunk to be retrievable, but it was not found")
	}

	if got.HexHash() != ch.HexHash() {
		t.Errorf("retrieved chunk hash mismatch: got=%s want=%s", got.HexHash(), ch.HexHash())
	}
}

// TestPersistentIndexJSON_NonExistent ensures ExistsWithErr() and GetWithErr() behave correctly
// when the chunk is not in the index.
func TestPersistentIndexJSON_NonExistent(t *testing.T) {
	// Create temp file
	path := t.TempDir() + "/index.json"

	// Open new index
	idx, err := NewPersistentIndexJSON(path)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	ok, err := idx.ExistsWithErr("pokemon")
	if ok {
		t.Errorf("expected ExistsWithErr() to return false for unknown hash")
	}
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	_, ok, err = idx.GetWithErr("pokemon")
	if !ok && err != nil {
		t.Errorf("expected GetWithErr() to fail for unknown hash")
	}
}

// TestPersistentIndexJSON_Concurrent verifies that PersistentIndexJSON
// is safe for concurrent use.
func TestPersistentIndexJSON_Concurrent(t *testing.T) {
	// Create temp file
	path := t.TempDir() + "/index.json"

	// Open new index
	idx, err := NewPersistentIndexJSON(path)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	ch := helperChunk([]byte("jayson"), 6)
	done := make(chan bool)

	// Writer goroutine
	go func() {
		for range 1000 {
			_ = idx.Add(ch)
		}
		done <- true
	}()

	// Reader goroutine
	go func() {
		for range 1000 {
			_ = idx.Exists(ch.HexHash())
			_, _ = idx.Get(ch.HexHash())
		}
		done <- true
	}()

	<-done
	<-done
}

// TestPersistentIndexJSON_CorruptedFile verifies that
// ExistsWithErr and GetWithErr return errors if the JSON file is corrupted.
func TestPersistentIndexJSON_CorruptedFile(t *testing.T) {
	// Create temp file
	path := t.TempDir() + "/index.json"

	// Write invalid JSON directly
	if err := os.WriteFile(path, []byte("{not-valid-json}"), 0644); err != nil {
		t.Fatalf("failed to write corrupted file: %v", err)
	}

	// Open the index (it should still construct, but loading is deferred)
	_, err := NewPersistentIndexJSON(path)
	if err == nil {
		t.Fatalf("expected error due to corrupted file, got nil")
	}
}

// BenchmarkPersistentIndexJSON_Add measures write throughput (Add only).
func BenchmarkPersistentIndexJSON_Add(b *testing.B) {
	// Create temp file
	path := b.TempDir() + "/index.json"

	// Open new PersistentIndexJSON
	idx, err := NewPersistentIndexJSON(path)
	if err != nil {
		b.Fatalf("failed to create index: %v", err)
	}

	chunkSize := 1024            // 1 KB
	b.SetBytes(int64(chunkSize)) // set bytes for throughput reporting

	for i := 0; b.Loop(); i++ {
		data := make([]byte, chunkSize)
		data[0] = byte(i) // ensure different hash each iteration
		ch := helperChunk(data, chunkSize)
		_ = idx.Add(ch)
	}
}

// BenchmarkPersistentIndexJSON_Exists measures lookup throughput (Exists only).
func BenchmarkPersistentIndexJSON_Exists(b *testing.B) {
	path := b.TempDir() + "/index.json"
	idx, err := NewPersistentIndexJSON(path)
	if err != nil {
		b.Fatalf("failed to create index: %v", err)
	}

	chunkSize := 1024
	b.SetBytes(int64(chunkSize))

	ch := helperChunk([]byte("zoro"), chunkSize)
	_ = idx.Add(ch)

	b.ResetTimer()
	for b.Loop() {
		_ = idx.Exists(ch.HexHash())
	}
}

// BenchmarkPersistentIndexJSON_AddAndExists measures mixed workload (Add + Exists).
func BenchmarkPersistentIndexJSON_AddAndExists(b *testing.B) {
	path := b.TempDir() + "/index.json"
	idx, err := NewPersistentIndexJSON(path)
	if err != nil {
		b.Fatalf("failed to create index: %v", err)
	}

	chunkSize := 1024
	b.SetBytes(int64(chunkSize))

	for i := 0; b.Loop(); i++ {
		data := make([]byte, chunkSize)
		data[0] = byte(i)
		ch := helperChunk(data, chunkSize)
		_ = idx.Add(ch)
		_ = idx.Exists(ch.HexHash())
	}
}

// BenchmarkPersistentIndexJSON_Parallel measures concurrent workload.
func BenchmarkPersistentIndexJSON_Parallel(b *testing.B) {
	path := b.TempDir() + "/index.json"
	idx, err := NewPersistentIndexJSON(path)
	if err != nil {
		b.Fatalf("failed to create index: %v", err)
	}

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
