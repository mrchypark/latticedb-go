//go:build goexperiment.simd && arm64

package search

import (
	"fmt"
	"math"
	"testing"
)

func vectorPair(n int) ([]float32, []float32) {
	left, right := make([]float32, n), make([]float32, n)
	for i := range left {
		left[i] = math.Float32frombits(uint32(0x3f000000 + (i*17)%0x007fffff))
		right[i] = math.Float32frombits(uint32(0x3e000000 + (i*29)%0x007fffff))
	}
	return left, right
}

func TestSquaredVectorDistanceSIMDBitwiseParity(t *testing.T) {
	for _, n := range []int{0, 1, 2, 3, 4, 5, 127, 128, 129, 255, 256, 257} {
		left, right := vectorPair(n)
		assertDistanceParity(t, fmt.Sprintf("regular/%d", n), left, right)
	}
	edges := []float32{
		math.Float32frombits(0x80000000), 0,
		math.SmallestNonzeroFloat32, -math.SmallestNonzeroFloat32,
		math.Float32frombits(0x00800000), -math.Float32frombits(0x00800000),
		math.Float32frombits(0x0c800000), -math.Float32frombits(0x0c800000),
		math.Float32frombits(0x3f800000), -math.Float32frombits(0x3f800000),
		math.Float32frombits(0x71800000), -math.Float32frombits(0x71800000),
		math.MaxFloat32, -math.MaxFloat32,
	}
	rightEdges := []float32{
		0, math.Float32frombits(0x80000000),
		-math.SmallestNonzeroFloat32, math.SmallestNonzeroFloat32,
		-math.Float32frombits(0x00800000), math.Float32frombits(0x00800000),
		-math.Float32frombits(0x0c800000), math.Float32frombits(0x0c800000),
		-math.Float32frombits(0x3f800000), math.Float32frombits(0x3f800000),
		-math.Float32frombits(0x71800000), math.Float32frombits(0x71800000),
		math.Nextafter32(math.MaxFloat32, 0), math.Nextafter32(-math.MaxFloat32, 0),
	}
	for _, n := range []int{1, 2, 3, 4, 5, 13, 14} {
		assertDistanceParity(t, fmt.Sprintf("edges/%d", n), edges[:n], rightEdges[:n])
	}
	_, mismatch := SquaredVectorDistance([]float32{1}, nil)
	_, referenceMismatch := squaredVectorDistanceReference([]float32{1}, nil)
	if errorText(mismatch) != errorText(referenceMismatch) {
		t.Fatalf("mismatch error = %v, want %v", mismatch, referenceMismatch)
	}
	_, overflow := SquaredVectorDistance([]float32{math.MaxFloat32}, []float32{-math.MaxFloat32})
	_, referenceOverflow := squaredVectorDistanceReference([]float32{math.MaxFloat32}, []float32{-math.MaxFloat32})
	if errorText(overflow) != errorText(referenceOverflow) {
		t.Fatalf("overflow error = %v, want %v", overflow, referenceOverflow)
	}
}

func assertDistanceParity(t *testing.T, name string, left, right []float32) {
	t.Helper()
	want, wantErr := squaredVectorDistanceReference(left, right)
	got, gotErr := SquaredVectorDistance(left, right)
	if errorText(gotErr) != errorText(wantErr) || math.Float64bits(got) != math.Float64bits(want) {
		t.Fatalf("%s got=(%x, %v), want=(%x, %v)", name, math.Float64bits(got), gotErr, math.Float64bits(want), wantErr)
	}
}

func TestSquaredVectorDistanceSIMDRoundingParity(t *testing.T) {
	state := uint32(42)
	next := func() float32 {
		state = state*1664525 + 1013904223
		return math.Float32frombits((state & 0x807fffff) | ((100 + (state>>24)%50) << 23))
	}
	for sample := 0; sample < 10000; sample++ {
		var left, right [128]float32
		for i := range left {
			left[i] = next()
			right[i] = next()
		}
		assertDistanceParity(t, fmt.Sprintf("rounding/%d", sample), left[:], right[:])
	}
}
