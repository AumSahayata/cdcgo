package fastcdc

// Params defines chunking parameters.
type Params struct {
	MinSize int
	AvgSize int
	MaxSize int
	Mask    uint64
	Gear    *[256]uint64 // optional custom gear table
}

// NewParams creates a new FastCDC parameter set.
// min, avg, max sizes are in bytes and define the chunk size distribution.
func NewParams(min, avg, max int, gear *[256]uint64) Params {
	// Mask is chosen based on avg size
	// e.g. if avg = 64KB, then mask ~ (1 << 16) - 1
	var bits uint
	for (1 << bits) < avg {
		bits++
	}
	mask := uint64((1 << bits) - 1)
	return Params{
		MinSize: min,
		AvgSize: avg,
		MaxSize: max,
		Mask:    mask,
		Gear:    gear, // can be nil -> default table
	}
}
