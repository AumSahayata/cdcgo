package fastcdc

// Chunker holds state for FastCDC.
type Chunker struct {
	P *Params
}

// NewChunkReader creates a ChunkReader to read content-defined chunks from r.
// bufSize is the internal buffer size in bytes. Must be >= MaxSize of the chunker.
func NewChunker(params *Params) *Chunker {
	return &Chunker{P: params}
}

// NextBoundary finds the next chunk boundary given a buffer of data.
// Returns the next chunk boundary as an offset in bytes relative to the buffer start.
func (c *Chunker) NextBoundary(buf []byte) int {
	size := 0
	var hash uint64 = 0
	table := GetGear(c.P)

	for _, b := range buf {
		size++
		hash = (hash << 1) + table[b]

		if size < c.P.MinSize {
			continue
		}

		if size >= c.P.AvgSize && (hash&c.P.Mask) == 0 {
			return size
		}

		if size >= c.P.MaxSize {
			return size
		}
	}

	return size
}
