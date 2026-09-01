package store

import (
	"errors"
	"maps"
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
	fork.Add("term", 99)
	if got := fork.Get("term"); !slices.Equal(got, []uint64{99}) {
		t.Fatalf("re-added posting = %v", got)
	}
	if base.Len("term") != smallPostingLimit {
		t.Fatal("posting cleanup changed base")
	}
}

func TestShardMapDeleteOccupiedShardsInInsertionOrder(t *testing.T) {
	base := NewShardMap[uint64]()
	for id := uint64(0); id < 10_000; id++ {
		base.Set(id, id)
	}
	fork := base.Fork()
	for id := uint64(0); id < 10_000; id++ {
		fork.CloneShardOnce(id)
		fork.Delete(id)
	}
	if fork.Len() != 0 || base.Len() != 10_000 {
		t.Fatalf("lengths after deletion = fork %d, base %d", fork.Len(), base.Len())
	}
	for range fork.All() {
		t.Fatal("deleted shard remained active")
	}
	fork.CloneShardOnce(20_000)
	fork.Set(20_000, 1)
	if fork.Get(20_000) != 1 || base.Has(20_000) {
		t.Fatal("reusing emptied shard map changed the base")
	}
	for id, value := range fork.All() {
		if id != 20_000 || value != 1 {
			t.Fatalf("reused shard iteration = %d:%d", id, value)
		}
		return
	}
	t.Fatal("reused shard was absent from iteration")
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

func TestPagedMapDifferentialAcrossForks(t *testing.T) {
	base := NewPagedMap[uint64]()
	wantBase := map[uint64]uint64{}
	seed := uint64(1)
	for range 10_000 {
		seed = seed*6364136223846793005 + 1
		id := seed & ((1 << 50) - 1)
		base.Set(id, seed)
		wantBase[id] = seed
	}
	fork := base.Fork()
	wantFork := maps.Clone(wantBase)
	for index := range 5_000 {
		seed = seed*6364136223846793005 + 1
		id := seed & ((1 << 50) - 1)
		fork.CloneShardOnce(id)
		if index%3 == 0 {
			fork.Delete(id)
			delete(wantFork, id)
		} else {
			fork.Set(id, seed)
			wantFork[id] = seed
		}
	}
	collect := func(values PagedMap[uint64]) map[uint64]uint64 {
		got := make(map[uint64]uint64, values.Len())
		for id, value := range values.All() {
			got[id] = value
		}
		return got
	}
	if got := collect(base); !maps.Equal(got, wantBase) {
		t.Fatalf("base differs: got %d entries, want %d", len(got), len(wantBase))
	}
	if got := collect(fork); !maps.Equal(got, wantFork) {
		t.Fatalf("fork differs: got %d entries, want %d", len(got), len(wantFork))
	}
}

func TestPagedMapDeleteLastPageThenReuseFork(t *testing.T) {
	for _, ids := range [][]uint64{{1, 64}, {1, 1 << 20}, {1 << 48, 1<<48 + 64}} {
		base := NewPagedMap[uint64]()
		base.Set(ids[0], ids[0])
		fork := base.Fork()
		fork.CloneShardOnce(ids[0])
		fork.Delete(ids[0])
		fork.CloneShardOnce(ids[1])
		fork.Set(ids[1], ids[1])
		if !base.Has(ids[0]) || fork.Has(ids[0]) || fork.Get(ids[1]) != ids[1] {
			t.Fatalf("ids %v: base/fork isolation failed", ids)
		}
	}
	base := NewPagedMap[uint64]()
	base.Set(1, 1)
	base.Set(1<<13, 1<<13)
	fork := base.Fork()
	fork.CloneShardOnce(1)
	fork.Delete(1)
	fork.CloneShardOnce(64)
	fork.Set(64, 64)
	if !base.Has(1) || !fork.Has(1<<13) || fork.Get(64) != 64 {
		t.Fatal("reuse with another live bucket failed")
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

func TestNormalizeValueRejectsInvalidUTF8(t *testing.T) {
	invalid := string([]byte{0xff})
	for name, value := range map[string]any{
		"string":        invalid,
		"nested string": []any{invalid},
		"map key":       map[string]any{invalid: "value"},
		"typed map key": map[string]string{invalid: "value"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NormalizeValue(value); err == nil {
				t.Fatal("invalid UTF-8 accepted")
			}
		})
	}
	if _, err := NormalizeProperties(map[string]any{invalid: "value"}); err == nil {
		t.Fatal("invalid UTF-8 property key accepted")
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
