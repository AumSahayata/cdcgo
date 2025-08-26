package manifest_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/AumSahayata/cdcgo"
	"github.com/AumSahayata/cdcgo/internal/testutil"
	"github.com/AumSahayata/cdcgo/manifest"
)

// helper to create fake chunks with predictable data
func makeTestChunks(t *testing.T) ([]cdcgo.Chunk, map[string][]byte) {
	t.Helper()

	data1 := []byte("hello world")
	data2 := []byte("foo bar baz")

	ch1 := testutil.TestChunk(data1, len(data1))
	ch2 := testutil.TestChunk(data2, len(data2))

	store := map[string][]byte{
		ch1.HexHash(): data1,
		ch2.HexHash(): data2,
	}
	return []cdcgo.Chunk{ch1, ch2}, store
}

// fake loader implementing manifest.ChunkLoader
func makeLoader(store map[string][]byte) manifest.ChunkLoader {
	return func(hash string) ([]byte, error) {
		d, ok := store[hash]
		if !ok {
			return nil, fmt.Errorf("chunk not found: %s", hash)
		}
		return d, nil
	}
}

// func TestManifest_SaveAndLoad ensures that a manifest can correctly
// save to disk and load into memory
func TestManifest_SaveAndLoad(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "m.json")

	chunks := []cdcgo.Chunk{testutil.TestChunk([]byte("chunk1"), 6),
		testutil.TestChunk([]byte("chunk2"), 6)}

	m := manifest.NewManifest("testfile.txt", 1234, "sha256")
	m.Chunks = chunks

	if err := m.Save(path); err != nil {
		t.Fatalf("failed to save manifest: %v", err)
	}

	loaded, err := manifest.Load(path)
	if err != nil {
		t.Fatalf("failed to load manifest: %v", err)
	}

	if loaded.FileName != m.FileName || loaded.FileSize != m.FileSize || loaded.HashAlgorithm != m.HashAlgorithm {
		t.Errorf("Loaded manifest metadata mismatch: got %+v, want %+v", loaded, m)
	}

	if len(loaded.Chunks) != len(m.Chunks) {
		t.Errorf("Loaded manifest chunks mismatch: got %d, want %d", len(loaded.Chunks), len(m.Chunks))
	}
}

// TestManifest_VerifyFileWithLoader ensures that a manifest can correctly
// verify all of its chunks when using a custom chunk loader.
func TestManifest_VerifyFileWithLoader(t *testing.T) {
	chunks, store := makeTestChunks(t)

	m := manifest.NewManifest("verify.txt", int64(len(store)), "sha256")
	m.Chunks = chunks

	err := m.VerifyFileWithLoader(makeLoader(store))
	if err != nil {
		t.Errorf("VerifyFileWithLoader failed: %v", err)
	}
}

// TestManifest_ReassembleWithLoader ensures that a manifest can correctly
// re-assemble all of its chunks when using a custom chunk loader
func TestManifest_ReassembleWithLoader(t *testing.T) {
	chunks, store := makeTestChunks(t)

	m := manifest.NewManifest("reasm.txt", 11+11, "sha256")
	m.Chunks = chunks

	var buf bytes.Buffer
	if err := m.ReassembleWithLoader(makeLoader(store), &buf); err != nil {
		t.Fatalf("ReassembleWithLoader failed: %v", err)
	}

	expected := append(store[chunks[0].HexHash()], store[chunks[1].HexHash()]...)
	if !bytes.Equal(buf.Bytes(), expected) {
		t.Errorf("Reassembled data mismatch: got %q, want %q", buf.Bytes(), expected)
	}
}

// TestManifest_ReassembleWithLoader ensures that a manifest can correctly
// re-create original file when using a custom chunk loader
func TestManifest_RestoreFileWithLoader(t *testing.T) {
	chunks, store := makeTestChunks(t)
	m := manifest.NewManifest("restored.txt", 11+11, "sha256")
	m.Chunks = chunks

	tmpDir := t.TempDir()
	if err := m.RestoreFileWithLoader(makeLoader(store), tmpDir); err != nil {
		t.Fatalf("RestoreFileWithLoader failed: %v", err)
	}

	path := filepath.Join(tmpDir, "restored.txt")
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading restored file failed: %v", err)
	}

	expected := append(store[chunks[0].HexHash()], store[chunks[1].HexHash()]...)
	if !bytes.Equal(got, expected) {
		t.Errorf("Restored file mismatch: got %q, want %q", got, expected)
	}
}
