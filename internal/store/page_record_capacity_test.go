package store

import (
	"bytes"
	"testing"
)

func TestPageRecordCapacityPreservesEncodingAndLimits(t *testing.T) {
	encode := func(e *binaryEncoder) { e.u(128); e.str("page payload"); e.ids([]uint64{1, 127, 128, 65536}) }
	for _, limit := range []uint64{8, 64, 1024} {
		want, wantErr := encodePageRecordBounded(5, limit, encode)
		for _, capacity := range []int{0, 1, 64, 1024} {
			got, err := encodePageRecordCapacity(5, limit, capacity, encode)
			if (err == nil) != (wantErr == nil) || !bytes.Equal(got, want) {
				t.Fatalf("limit=%d capacity=%d got=%x err=%v want=%x err=%v", limit, capacity, got, err, want, wantErr)
			}
		}
	}
}
