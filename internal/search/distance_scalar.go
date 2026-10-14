//go:build !goexperiment.simd || !arm64

package search

import (
	"errors"
	"fmt"
	"math"
)

// SquaredVectorDistance avoids sqrt when callers only compare distances.
func SquaredVectorDistance(left []float32, right []float32) (float64, error) {
	if len(left) != len(right) {
		return 0, fmt.Errorf("vector length mismatch: %d != %d", len(left), len(right))
	}
	var sum0, sum1, sum2, sum3 float64
	i := 0
	for ; i+3 < len(left); i += 4 {
		diff := float64(left[i]) - float64(right[i])
		sum0 += diff * diff
		diff = float64(left[i+1]) - float64(right[i+1])
		sum1 += diff * diff
		diff = float64(left[i+2]) - float64(right[i+2])
		sum2 += diff * diff
		diff = float64(left[i+3]) - float64(right[i+3])
		sum3 += diff * diff
	}
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
