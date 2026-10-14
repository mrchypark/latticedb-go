package search

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
)

func squaredVectorDistanceReference(left []float32, right []float32) (float64, error) {
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

func squaredVectorDistanceContextReference(ctx context.Context, left []float32, right []float32) (float64, error) {
	if len(left) != len(right) {
		return 0, fmt.Errorf("vector length mismatch: %d != %d", len(left), len(right))
	}
	var sum0, sum1, sum2, sum3 float64
	i := 0
	for ; i+3 < len(left); i += 4 {
		if i&255 == 0 {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
		}
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
		if i&255 == 0 {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
		}
		diff := float64(left[i]) - float64(right[i])
		total += diff * diff
	}
	if total > float64(math.MaxFloat32)*float64(math.MaxFloat32) {
		return 0, errors.New("vector distance exceeds float32 range")
	}
	return total, nil
}

func distanceBenchmarkVectors(n int) ([]float32, []float32) {
	left, right := make([]float32, n), make([]float32, n)
	for i := range left {
		left[i] = float32((i*17)%101-50) / 7
		right[i] = float32((i*29)%97-48) / 11
	}
	return left, right
}

func TestSquaredVectorDistanceBitwiseParity(t *testing.T) {
	for _, n := range []int{0, 1, 2, 3, 4, 5, 127, 128, 129, 255, 256, 257} {
		left, right := distanceBenchmarkVectors(n)
		want, wantErr := squaredVectorDistanceReference(left, right)
		got, gotErr := SquaredVectorDistance(left, right)
		if errorText(gotErr) != errorText(wantErr) || math.Float64bits(got) != math.Float64bits(want) {
			t.Fatalf("n=%d got=(%x, %v) want=(%x, %v)", n, math.Float64bits(got), gotErr, math.Float64bits(want), wantErr)
		}
	}
	if _, err := SquaredVectorDistance([]float32{1}, nil); errorText(err) != errorText(mustDistanceError(t, squaredVectorDistanceReference, []float32{1}, nil)) {
		t.Fatalf("length mismatch error = %v", err)
	}
	if _, err := SquaredVectorDistance([]float32{math.MaxFloat32}, []float32{-math.MaxFloat32}); errorText(err) != errorText(mustDistanceError(t, squaredVectorDistanceReference, []float32{math.MaxFloat32}, []float32{-math.MaxFloat32})) {
		t.Fatalf("overflow error = %v", err)
	}
	for _, n := range []int{128, 260} {
		left, right := distanceBenchmarkVectors(n)
		wantCtx := &cancelAfterNErrs{Context: context.Background(), n: 1}
		gotCtx := &cancelAfterNErrs{Context: context.Background(), n: 1}
		want, wantErr := squaredVectorDistanceContextReference(wantCtx, left, right)
		got, gotErr := SquaredVectorDistanceContext(gotCtx, left, right)
		if errorText(gotErr) != errorText(wantErr) || math.Float64bits(got) != math.Float64bits(want) || gotCtx.call != wantCtx.call {
			t.Fatalf("context n=%d got=(%x, %v, %d) want=(%x, %v, %d)", n, math.Float64bits(got), gotErr, gotCtx.call, math.Float64bits(want), wantErr, wantCtx.call)
		}
	}
}

func mustDistanceError(t *testing.T, fn func([]float32, []float32) (float64, error), left, right []float32) error {
	t.Helper()
	_, err := fn(left, right)
	return err
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func BenchmarkSquaredVectorDistance128(b *testing.B) {
	benchmarkSquaredVectorDistance(b, 128, SquaredVectorDistance)
}
func BenchmarkSquaredVectorDistance127(b *testing.B) {
	benchmarkSquaredVectorDistance(b, 127, SquaredVectorDistance)
}
func BenchmarkSquaredVectorDistance129(b *testing.B) {
	benchmarkSquaredVectorDistance(b, 129, SquaredVectorDistance)
}

func BenchmarkSquaredVectorDistanceReference128(b *testing.B) {
	benchmarkSquaredVectorDistance(b, 128, squaredVectorDistanceReference)
}
func BenchmarkSquaredVectorDistanceReference127(b *testing.B) {
	benchmarkSquaredVectorDistance(b, 127, squaredVectorDistanceReference)
}
func BenchmarkSquaredVectorDistanceReference129(b *testing.B) {
	benchmarkSquaredVectorDistance(b, 129, squaredVectorDistanceReference)
}

func benchmarkSquaredVectorDistance(b *testing.B, n int, fn func([]float32, []float32) (float64, error)) {
	left, right := distanceBenchmarkVectors(n)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := fn(left, right); err != nil {
			b.Fatal(err)
		}
	}
}
