package engine

import (
	"cmp"
	"math"
	"math/bits"
)

// A float32 L2 rounding bin spans less than this relative squared-distance
// interval for normal float32 results. Subnormal scores need the slow path.
// Nonzero sums of squared float32 differences are at least 2^-298.
const exactSquaredRoundingMargin = 1.0 / (1 << 19)
const smallestNormalL2Squared = 0x1p-252

func compareExactVectorCandidate(a, b vectorCandidate) int {
	if a.distance != b.distance && (math.Min(a.distance, b.distance) < smallestNormalL2Squared || math.Abs(a.distance-b.distance) <= math.Max(a.distance, b.distance)*exactSquaredRoundingMargin) {
		if roundedL2Bits(a.distance) == roundedL2Bits(b.distance) {
			return cmp.Compare(a.id, b.id)
		}
	}
	return compareVectorCandidate(a, b)
}

// roundedL2Bits classifies a squared distance by the existing float32(Sqrt(s))
// rounding bins, without taking a square root. Only near-tie comparisons need
// this slow path; returned scores still use the original math.Sqrt operation.
// ponytail: binary search for ambiguous ties; cache keys if tie-heavy workloads dominate.
func roundedL2Bits(squared float64) uint32 {
	if squared == 0 {
		return 0
	}
	low, high := uint32(0), uint32(0x7f7fffff)
	for low < high {
		middle := low + (high-low)/2
		a, b := float64(math.Float32frombits(middle)), float64(math.Float32frombits(middle+1))
		midpoint := (a + b) / 2
		m := math.Float64bits(midpoint)
		// The midpoint has at most 25 significant bits, so its float64 mantissa
		// is even. Include the half-ULP64 interval that first rounds to midpoint.
		mantissa := ((m & ((1 << 52) - 1)) | (1 << 52)) * 2
		even := middle&1 == 0
		if even {
			mantissa++
		} else {
			mantissa--
		}
		hi, lo := bits.Mul64(mantissa, mantissa)
		s := math.Float64bits(squared)
		sm := (s & ((1 << 52) - 1)) | (1 << 52)
		shift := int((s>>52)&0x7ff) - 2*int((m>>52)&0x7ff) + 1077
		// Compare s with the exact 108-bit square of midpoint +/- half ULP64.
		order := cmp.Compare(53+shift, 64+bits.Len64(hi))
		if order == 0 {
			sh, sl := sm>>uint(64-shift), sm<<uint(shift)
			order = cmp.Compare(sh, hi)
			if order == 0 {
				order = cmp.Compare(sl, lo)
			}
		}
		if order < 0 || (order == 0 && even) {
			high = middle
		} else {
			low = middle + 1
		}
	}
	return low
}
