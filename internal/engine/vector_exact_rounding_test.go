package engine

import (
	"math"
	"math/rand/v2"
	"testing"
)

func TestExactRoundedL2Classifier(t *testing.T) {
	check := func(s float64) {
		t.Helper()
		want := math.Float32bits(float32(math.Sqrt(s)))
		if got := roundedL2Bits(s); got != want {
			t.Fatalf("s=%x got=%08x want=%08x", math.Float64bits(s), got, want)
		}
	}
	check(0)
	check(0x1p-298)
	check(float64(math.MaxFloat32) * float64(math.MaxFloat32))
	random := rand.New(rand.NewPCG(116, 47))
	for range 10000 {
		exponent := random.IntN(554) - 298
		s := math.Ldexp(1+random.Float64(), exponent)
		if s <= float64(math.MaxFloat32)*float64(math.MaxFloat32) {
			check(s)
		}
		a := random.Uint32() % 0x7f7fffff
		midpoint := (float64(math.Float32frombits(a)) + float64(math.Float32frombits(a+1))) / 2
		square := midpoint * midpoint
		check(square)
		below, above := square, square
		for range 3 {
			below = math.Nextafter(below, 0)
			above = math.Nextafter(above, math.Inf(1))
			check(below)
			check(above)
		}
	}
}

func TestExactSquaredComparatorMatchesRoundedOracle(t *testing.T) {
	random := rand.New(rand.NewPCG(32, 116))
	for range 10000 {
		exponent := random.IntN(554) - 298
		a := math.Ldexp(1+random.Float64(), exponent)
		b := a * (1 + (random.Float64()-.5)*1e-5)
		if a > float64(math.MaxFloat32)*float64(math.MaxFloat32) || b > float64(math.MaxFloat32)*float64(math.MaxFloat32) {
			continue
		}
		left, right := vectorCandidate{id: 2, distance: a}, vectorCandidate{id: 1, distance: b}
		got := compareExactVectorCandidate(left, right)
		want := compareVectorResult(VectorSearchResult{NodeID: 2, Distance: float32(math.Sqrt(a))}, VectorSearchResult{NodeID: 1, Distance: float32(math.Sqrt(b))})
		if got != want {
			t.Fatalf("a=%g b=%g order=%d want=%d", a, b, got, want)
		}
	}
	// Different squared distances both round to the smallest positive float32.
	a, b := vectorCandidate{id: 2, distance: 0x1p-298}, vectorCandidate{id: 1, distance: 0x1p-297}
	if compareExactVectorCandidate(a, b) != 1 {
		t.Fatal("subnormal rounded tie changed order")
	}
}
