package fastcdc

// Params defines the configuration for FastCDC chunking.
//
// The parameters control how the content-defined chunking operates:
//   - MinSize: minimum chunk size in bytes.
//   - AvgSize: target/average chunk size in bytes.
//   - MaxSize: maximum chunk size in bytes.
//   - Mask: bitmask derived from AvgSize used for boundary detection.
//   - Gear: optional pointer to a GearTable. If nil, the default
//     precomputed table is used.
type Params struct {
	MinSize int
	AvgSize int
	MaxSize int
	Mask    uint64
	Gear    *GearTable
}

// NewParams creates a new FastCDC parameter set with optional custom GearTable.
//
// min, avg, max are in bytes. The mask is automatically calculated from avg size.
// If gear is nil, the default GearTable is used for rolling hash.
func NewParams(min, avg, max int, gear *GearTable) Params {
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
