package storage

import (
	"bytes"
	"errors"
	"os"
	"sync/atomic"
	"testing"
)

// TestFSStorage_SaveAndLoad verifies that saved data can be retrieved correctly.
func TestFSStorage_SaveAndLoad(t *testing.T) {
	tmpDir := t.TempDir()
	fs, err := NewFSStorage(tmpDir, nil)
	if err != nil {
		t.Fatalf("failed to create FSStorage: %v", err)
	}
	data := []byte("test-data")
	ch := helperChunk(data, 9)

	if err := fs.Save(ch, data); err != nil {
		t.Fatalf("failed to save chunk: %v", err)
	}

	ldch, err := fs.Load(ch.HexHash())
	if err != nil {
		t.Fatalf("failed to load chunk: %v", err)
	}

	if !bytes.Equal(ldch, data) {
		t.Errorf("chunk data does not match")
	}
}

// TestFSStorage_LoadNonExistent ensures FSStorage handles missing chunks gracefully.
func TestFSStorage_LoadNonExistent(t *testing.T) {
	tmpDir := t.TempDir()
	fs, err := NewFSStorage(tmpDir, nil)
	if err != nil {
		t.Fatalf("failed to create FSStorage: %v", err)
	}

	_, err = fs.Load("nonexistent")

	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("expected os.ErrNotExist, got: %v", err)
	}
}

// TestFSStorage_SaveDuplicate verifies that saving the same chunk twice
// does not create duplicate files and the index prevents redundant writes.
func TestFSStorage_SaveDuplicate(t *testing.T) {
	tmpDir := t.TempDir()
	fs, err := NewFSStorage(tmpDir, nil)
	if err != nil {
		t.Fatalf("failed to create FSStorage: %v", err)
	}

	data := []byte("duplicate-test")
	ch := helperChunk(data, len(data))

	// Save chunk first time
	if err := fs.Save(ch, data); err != nil {
		t.Fatalf("failed to save chunk first time: %v", err)
	}

	// Save chunk second time (should skip writing)
	if err := fs.Save(ch, data); err != nil {
		t.Fatalf("failed to save chunk second time: %v", err)
	}

	loadedData, err := fs.Load(ch.HexHash())
	if err != nil {
		t.Fatalf("failed to load chunk: %v", err)
	}
	if !bytes.Equal(loadedData, data) {
		t.Errorf("chunk content mismatch: got %v, want %v", loadedData, data)
	}

	// Check that only one file exists in directory
	files, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("failed to list temp dir: %v", err)
	}
	if len(files) != 1 {
		t.Errorf("expected 1 file in storage, got %d", len(files))
	}
}

// BenchmarkFSStorage_Save measures the throughput of writing chunks to FSStorage.
// This benchmark simulates sequential writes.
func BenchmarkFSStorage_Save(b *testing.B) {
	tmpDir := b.TempDir()
	fs, err := NewFSStorage(tmpDir, nil)
	if err != nil {
		b.Fatalf("failed to create FSStorage: %v", err)
	}

	chunkSize := 1024
	data := make([]byte, chunkSize)
	b.SetBytes(int64(chunkSize))

	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		data[0] = byte(i) // ensure unique hash each iteration
		ch := helperChunk(data, chunkSize)
		_ = fs.Save(ch, data)
	}
}

// BenchmarkFSStorage_Load measures the throughput of reading chunks from FSStorage.
// It first populates FSStorage with a single chunk, then repeatedly reads it.
func BenchmarkFSStorage_Load(b *testing.B) {
	tmpDir := b.TempDir()
	fs, err := NewFSStorage(tmpDir, nil)
	if err != nil {
		b.Fatalf("failed to create FSStorage: %v", err)
	}

	chunkSize := 1024
	data := make([]byte, chunkSize)
	b.SetBytes(int64(chunkSize))
	ch := helperChunk(data, chunkSize)
	_ = fs.Save(ch, data)

	b.ResetTimer()
	for b.Loop() {
		_, _ = fs.Load(ch.HexHash())
	}
}

// BenchmarkFSStorage_Parallel simulates concurrent Save and Load operations.
// It uses b.RunParallel to spawn multiple goroutines.
func BenchmarkFSStorage_Parallel(b *testing.B) {
	tmpDir := b.TempDir()
	fs, err := NewFSStorage(tmpDir, nil)
	if err != nil {
		b.Fatalf("failed to create FSStorage: %v", err)
	}

	chunkSize := 1024
	b.SetBytes(int64(chunkSize))
	b.ResetTimer()

	var counter uint64
	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			i := atomic.AddUint64(&counter, 1)
			data := make([]byte, chunkSize)
			data[0] = byte(i)
			ch := helperChunk(data, chunkSize)
			_ = fs.Save(ch, data)
			_, _ = fs.Load(ch.HexHash())
		}
	})
}
