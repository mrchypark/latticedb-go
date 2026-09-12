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

// cancelAfterNErrs returns context.Canceled after n successful Err() calls.
type cancelAfterNErrs struct {
	context.Context
	n    int
	call int
}

func (c *cancelAfterNErrs) Err() error {
	c.call++
	if c.call > c.n {
		return context.Canceled
	}
	return c.Context.Err()
}

func TestCancellationPolling(t *testing.T) {
	// Adversarial string: 'a' + 1000 é. Rune byte offsets are
	// 0,1,3,5,...,1999 — all odd after 0, so old offset&255==0 fires
	// only once. New runeCount&63==0 fires every 64 runes (~16 times).
	const n = 1000
	text := "a" + strings.Repeat("é", n)
	if n+1 != len([]rune(text)) {
		t.Fatalf("rune count = %d, want %d", len([]rune(text)), n+1)
	}

	// Verify construction: no rune byte offset (except 0) is a multiple of 256.
	for offset := range text {
		if offset > 0 && offset%256 == 0 {
			t.Fatalf("rune byte offset %d is multiple of 256 in adversarial string", offset)
		}
	}

	pollFns := []struct {
		name string
		fn   func(ctx context.Context, s string) error
	}{
		{"TokenizeContext", func(ctx context.Context, s string) error {
			_, err := TokenizeContext(ctx, s)
			return err
		}},
		{"tokenizationLogicalBytes", func(ctx context.Context, s string) error {
			_, err := tokenizationLogicalBytes(ctx, s)
			return err
		}},
	}

	for _, tt := range pollFns {
		t.Run(tt.name+"/cancel_on_nth_err", func(t *testing.T) {
			// Cancel after 5 Err() calls. Old code makes ~1 call,
			// new code makes ~16. This proves midstream detection.
			cc := &cancelAfterNErrs{Context: context.Background(), n: 5}
			if err := tt.fn(cc, text); !errors.Is(err, context.Canceled) {
				t.Fatalf("expected midstream cancellation, got %v (polls: %d)", err, cc.call)
			}
		})

		t.Run(tt.name+"/poll_count_bounded", func(t *testing.T) {
			// Upper bound catches runeCount++ below continue (polls every rune = 1001).
			// Lower bound catches old byte-offset code (polls once).
			cc := &cancelAfterNErrs{Context: context.Background(), n: 999999}
			tt.fn(cc, text)
			if cc.call < 10 {
				t.Fatalf("poll count %d < 10: old byte-offset check?", cc.call)
			}
			if cc.call > 20 {
				t.Fatalf("poll count %d > 20: runeCount++ not advancing?", cc.call)
			}
		})
	}
}

func TestTokenizationSemanticsMixedWidth(t *testing.T) {
	tests := []struct {
		name string
		text string
	}{
		{"ascii", "hello world 123"},
		{"two_byte", strings.Repeat("é", 200)},
		{"three_byte", strings.Repeat("가", 200)},
		{"four_byte", strings.Repeat("𐌰", 200)},
		{"mixed", "aé가𐌰 x123"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := Tokenize(tt.text)
			got, err := TokenizeContext(context.Background(), tt.text)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got, want) {
				t.Fatalf("TokenizeContext = %v, want %v", got, want)
			}
			got2, err := TokenizeContextWithLimit(context.Background(), tt.text, ^uint64(0))
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got2, want) {
				t.Fatalf("TokenizeContextWithLimit = %v, want %v", got2, want)
			}
		})
	}
}
