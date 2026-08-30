package search

import (
	"context"
	"errors"
	"math"
	"slices"
	"strings"
	"testing"
)

func TestTokenizeContextMatchesAndCancels(t *testing.T) {
	text := "Hello, 세계 １２３ café"
	tokens, err := TokenizeContext(context.Background(), text)
	if err != nil {
		t.Fatal(err)
	}
	if want := Tokenize(text); !slices.Equal(tokens, want) {
		t.Fatalf("tokens = %q, want %q", tokens, want)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := TokenizeContext(canceled, text); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation = %v", err)
	}
}

func TestTokenizeContextPreflightsHighCardinality(t *testing.T) {
	text := strings.Repeat("A ", 100_000)
	if _, err := TokenizeContextWithLimit(context.Background(), text, uint64(len(text))*8); !errors.Is(err, ErrTokenizationLimit) {
		t.Fatalf("high-cardinality limit = %v", err)
	}
	tokens, err := TokenizeContextWithLimit(context.Background(), text, ^uint64(0))
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 100_000 {
		t.Fatalf("tokens = %d", len(tokens))
	}
}

func TestVectorDistanceAvoidsFloat32Overflow(t *testing.T) {
	if _, err := VectorDistance([]float32{math.MaxFloat32}, []float32{-math.MaxFloat32}); err == nil {
		t.Fatal("expected unrepresentable distance to fail")
	}
	distance, err := VectorDistance([]float32{3, 4}, []float32{0, 0})
	if err != nil {
		t.Fatal(err)
	}
	if distance != 5 {
		t.Fatalf("distance = %v, want 5", distance)
	}
}

func TestSquaredVectorDistance(t *testing.T) {
	left, right := []float32{3, 4}, []float32{0, 0}
	distance, err := SquaredVectorDistance(left, right)
	if err != nil || distance != 25 {
		t.Fatalf("SquaredVectorDistance = %v, %v", distance, err)
	}
	contextDistance, err := SquaredVectorDistanceContext(context.Background(), left, right)
	if err != nil || contextDistance != distance {
		t.Fatalf("SquaredVectorDistanceContext = %v, %v", contextDistance, err)
	}
}
