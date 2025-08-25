package chunk

import "github.com/AumSahayata/cdcgo/model"

// Index defines the minimal interface for deduplication metadata storage.
// It is intended for in-memory or simple backends that are guaranteed to succeed.
//
// Implementations are expected to:
//   - Record unique chunks via Add()
//   - Allow fast existence checks via Exists()
//   - Optionally retrieve chunk metadata via Get()
//
// This interface is safe for local and lightweight usage where failures are not expected.
type Index interface {
	Add(chunk model.Chunk) error         // record a new chunk
	Exists(hash string) bool             // check if chunk exists
	Get(hash string) (model.Chunk, bool) // retrieve chunk info if needed
}

// PersistentIndex extends Index to support backends where storage operations
// may fail (e.g. databases, distributed systems, remote services).
//
// Unlike Index, methods here explicitly return errors to allow callers to
// handle network, I/O, or persistence failures gracefully.
//
// Implementations should be concurrency-safe and resilient to transient errors.
type PersistentIndex interface {
	Index
	ExistsWithErr(hash string) (bool, error)           // Check if chunk exists, with error reporting
	GetWithErr(hash string) (model.Chunk, bool, error) // Retrieve chunk metadata, with error reporting
}
