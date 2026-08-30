package store

import (
	"errors"
	"math"
	"slices"
	"testing"
)

func TestStringPostingsForkIsolationAcrossPromotion(t *testing.T) {
	base := NewStringPostings()
	for id := uint64(1); id <= smallPostingLimit; id++ {
		base.Add("term", id)
	}
	fork := base.Fork()
	fork.Add("term", smallPostingLimit+1)
	fork.Remove("term", 1)
	if base.Len("term") != smallPostingLimit || fork.Len("term") != smallPostingLimit {
		t.Fatalf("posting lengths = base %d, fork %d", base.Len("term"), fork.Len("term"))
	}
	if !slices.Equal(base.Get("term"), idsFromOne(smallPostingLimit)) {
		t.Fatal("fork mutation changed base postings")
	}
	for id := uint64(2); id <= smallPostingLimit+1; id++ {
		fork.Remove("term", id)
	}
	if keys := slices.Collect(fork.Keys()); len(keys) != 0 {
		t.Fatalf("empty posting keys = %v", keys)
	}
	if base.Len("term") != smallPostingLimit {
		t.Fatal("posting cleanup changed base")
	}
}

func TestPagedMapForkIsolationAndHighIDs(t *testing.T) {
	base := NewPagedMap[uint64]()
	for _, id := range []uint64{1, 64, 65, 1 << 48} {
		base.Set(id, id)
	}
	fork := base.Fork()
	fork.CloneShardOnce(65)
	fork.Set(65, 99)
	fork.CloneShardOnce(1 << 48)
	fork.Delete(1 << 48)
	if base.Get(65) != 65 || !base.Has(1<<48) {
		t.Fatal("fork mutation changed base pages")
	}
	if fork.Get(65) != 99 || fork.Has(1<<48) || fork.Len() != 3 {
		t.Fatalf("fork state = value %d, high %v, len %d", fork.Get(65), fork.Has(1<<48), fork.Len())
	}
	fork = base.ForkSet(65, 100)
	fork = fork.ForkSet(66, 101)
	if base.Get(65) != 65 || fork.Get(65) != 100 || fork.Get(66) != 101 || fork.Len() != 5 {
		t.Fatalf("fork-set state = base %d, values %d/%d, len %d", base.Get(65), fork.Get(65), fork.Get(66), fork.Len())
	}
}

func idsFromOne(count int) []uint64 {
	ids := make([]uint64, count)
	for index := range ids {
		ids[index] = uint64(index + 1)
	}
	return ids
}

func TestNormalizeValueRejectsCyclesAndNonFiniteNumbers(t *testing.T) {
	cyclicMap := map[string]any{}
	cyclicMap["self"] = cyclicMap
	cyclicSlice := make([]any, 1)
	cyclicSlice[0] = cyclicSlice

	for name, value := range map[string]any{
		"map cycle":   cyclicMap,
		"slice cycle": cyclicSlice,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NormalizeValue(value); !errors.Is(err, ErrValueCycle) {
				t.Fatalf("error = %v, want ErrValueCycle", err)
			}
		})
	}
	for name, value := range map[string]any{
		"nan":    math.NaN(),
		"posinf": math.Inf(1),
		"vector": []float32{float32(math.Inf(1))},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NormalizeValue(value); err == nil {
				t.Fatal("expected non-finite value to fail")
			}
		})
	}
}

func TestNormalizeValueUnsignedBoundsDepthAndSharedValues(t *testing.T) {
	if ^uint(0)>>63 != 0 {
		value, err := NormalizeValue(uint(math.MaxInt64))
		if err != nil || value != int64(math.MaxInt64) {
			t.Fatalf("MaxInt64 uint = %v, %v", value, err)
		}
		if _, err := NormalizeValue(uint(math.MaxInt64) + 1); err == nil {
			t.Fatal("uint above MaxInt64 accepted")
		}
	}

	shared := map[string]any{"value": 1}
	if _, err := NormalizeValue(map[string]any{"left": shared, "right": shared}); err != nil {
		t.Fatalf("shared acyclic value rejected: %v", err)
	}
	backing := make([]any, 2)
	backing[0] = "x"
	backing[1] = backing[:1]
	if _, err := NormalizeValue(backing); err != nil {
		t.Fatalf("overlapping acyclic slices rejected: %v", err)
	}
	value := any("leaf")
	for range maxValueDepth {
		value = []any{value}
	}
	if _, err := NormalizeValue(value); err != nil {
		t.Fatalf("value at maximum depth rejected: %v", err)
	}
	value = []any{value}
	if _, err := NormalizeValue(value); !errors.Is(err, ErrValueLimit) {
		t.Fatalf("value above maximum depth error = %v, want ErrValueLimit", err)
	}
}
