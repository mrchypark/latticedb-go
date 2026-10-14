//go:build goexperiment.simd && arm64

package search

import (
	"errors"
	"fmt"
	"math"
	"simd/archsimd"
)

// SquaredVectorDistance preserves the scalar ARM64 FMA and four-accumulator order.
func SquaredVectorDistance(left, right []float32) (float64, error) {
	if len(left) != len(right) {
		return 0, fmt.Errorf("vector length mismatch: %d != %d", len(left), len(right))
	}
	var lowSum, highSum archsimd.Float64x2
	i := 0
	for ; i+3 < len(left); i += 4 {
		leftVector := archsimd.LoadFloat32x4(left[i:])
		rightVector := archsimd.LoadFloat32x4(right[i:])
		low := leftVector.ConvertLo2ToFloat64().Sub(rightVector.ConvertLo2ToFloat64())
		high := leftVector.HiToLo().ConvertLo2ToFloat64().Sub(rightVector.HiToLo().ConvertLo2ToFloat64())
		lowSum = low.MulAdd(low, lowSum)
		highSum = high.MulAdd(high, highSum)
	}
	sum0, sum1 := lowSum.GetElem(0), lowSum.GetElem(1)
	sum2, sum3 := highSum.GetElem(0), highSum.GetElem(1)
	total := (sum0 + sum1) + (sum2 + sum3)
	for ; i < len(left); i++ {
		diff := float64(left[i]) - float64(right[i])
		total += diff * diff
	}
	if total > float64(math.MaxFloat32)*float64(math.MaxFloat32) {
		return 0, errors.New("vector distance exceeds float32 range")
	}
	return total, nil
}
