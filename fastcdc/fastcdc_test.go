package fastcdc

import (
	"bytes"
	"testing"
)

func TestNextBoundary_Basic(t *testing.T) {
	// Simple repeating data
	data := bytes.Repeat([]byte{0xAB}, 1000)

	// FastCDC params
	params := NewParams(50, 100, 200, nil) // Min=50, Avg=100, Max=200
	chunker := NewChunker(params)

	offset := 0

	for offset < len(data) {
		cut := chunker.NextBoundary(data[offset:])
		if cut < chunker.params.MinSize {
			t.Errorf("chunk too small: got %d, min %d", cut, params.MinSize)
		}
		if cut > chunker.params.MaxSize {
			t.Errorf("chunk too big: got %d, max %d", cut, params.MaxSize)
		}
		offset += cut
	}
}

func TestNextBoundary_Deterministic(t *testing.T) {
	data := bytes.Repeat([]byte{0x01, 0x02, 0x03}, 500)

	params := NewParams(50, 100, 200, nil)
	chunker := NewChunker(params)

	// Collect first pass cuts
	var firstCuts []int
	offset := 0
	for offset < len(data) {
		cut := chunker.NextBoundary(data[offset:])
		firstCuts = append(firstCuts, cut)
		offset += cut
	}

	// Collect second pass cuts (should be identical)
	chunker2 := NewChunker(params)
	var secondCuts []int
	offset = 0
	for offset < len(data) {
		cut := chunker2.NextBoundary(data[offset:])
		secondCuts = append(secondCuts, cut)
		offset += cut
	}

	for i := range firstCuts {
		if firstCuts[i] != secondCuts[i] {
			t.Errorf("cuts not deterministic at chunk %d: %d vs %d", i, firstCuts[i], secondCuts[i])
		}
	}
}
