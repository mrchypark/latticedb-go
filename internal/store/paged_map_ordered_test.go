package store

import (
	"slices"
	"testing"
)

func TestPagedMapOrderedDenseRootZeroAndEarlyStop(t *testing.T) {
	values := NewPagedMap[uint64]()
	for id := uint64(100_000); id > 0; id-- {
		values.Set(id-1, id-1)
	}

	var first []uint64
	for id := range values.Ordered() {
		first = append(first, id)
		if len(first) == 3 {
			break
		}
	}
	if !slices.Equal(first, []uint64{0, 1, 2}) {
		t.Fatalf("early ordered prefix = %v", first)
	}

	ordered := make([]uint64, 0, values.Len())
	for id := range values.Ordered() {
		ordered = append(ordered, id)
	}
	want := make([]uint64, 100_000)
	for id := range want {
		want[id] = uint64(id)
	}
	if !slices.Equal(ordered, want) {
		t.Fatalf("dense ordered IDs differ at %d entries", len(ordered))
	}
	allocs := testing.AllocsPerRun(10, func() {
		count := 0
		for range values.Ordered() {
			count++
		}
		if count != len(want) {
			t.Fatalf("dense allocation check count = %d", count)
		}
	})
	if allocs != 0 {
		t.Fatalf("dense ordered allocations = %f, want 0", allocs)
	}
}

func TestPagedMapOrderedSparseReverseRootsAndMaxID(t *testing.T) {
	values := NewPagedMap[uint64]()
	maxRoot := ^uint64(0) >> 20
	roots := []uint64{maxRoot, 7_000, 1, 4_096, 2}
	var want []uint64
	for index := len(roots) - 1; index >= 0; index-- {
		id := roots[index]<<20 | uint64(index)
		values.Set(id, id)
		want = append(want, id)
	}
	values.Set(9, 9)
	want = append(want, 9)
	values.Set(^uint64(0), ^uint64(0))
	want = append(want, ^uint64(0))
	slices.Sort(want)

	var got []uint64
	for id, value := range values.Ordered() {
		if id != value {
			t.Fatalf("ordered value at %d = %d", id, value)
		}
		got = append(got, id)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("sparse ordered IDs = %v, want %v", got, want)
	}

	var prefix []uint64
	for id := range values.Ordered() {
		prefix = append(prefix, id)
		break
	}
	if !slices.Equal(prefix, want[:1]) {
		t.Fatalf("sparse early prefix = %v, want %v", prefix, want[:1])
	}
}

func TestPagedMapOrderedTwoPageSmallActiveAndFork(t *testing.T) {
	base := NewPagedMap[uint64]()
	base.Set(1<<14|3, 1<<14|3)
	base.Set(2, 2)
	base.Set(1<<14|1, 1<<14|1)
	wantBase := []uint64{2, 1<<14 | 1, 1<<14 | 3}
	if got := collectPagedMapIDs(base); !slices.Equal(got, wantBase) {
		t.Fatalf("two-page ordered IDs = %v, want %v", got, wantBase)
	}

	fork := base.Fork()
	fork.CloneShardOnce(1 << 14)
	fork.Set(1<<14|1, 11)
	fork.Set(1<<20|5, 1<<20|5)
	wantFork := append(slices.Clone(wantBase), 1<<20|5)
	slices.Sort(wantFork)
	if got := collectPagedMapIDs(fork); !slices.Equal(got, wantFork) {
		t.Fatalf("fork ordered IDs = %v, want %v", got, wantFork)
	}
	if base.Get(1<<14|1) != 1<<14|1 || base.Has(1<<20|5) {
		t.Fatal("ordered fork setup changed base")
	}
}

func TestPagedMapOrderedManySparseRootsUsesCompleteBatches(t *testing.T) {
	values := NewPagedMap[uint64]()
	const roots = 4_200
	for index := roots - 1; index >= 0; index-- {
		id := uint64(index+1) << 20
		values.Set(id, id)
	}
	var previous uint64
	count := 0
	for id := range values.Ordered() {
		if count != 0 && id <= previous {
			t.Fatalf("sparse order at %d: %d after %d", count, id, previous)
		}
		previous = id
		count++
	}
	if count != roots {
		t.Fatalf("ordered sparse count = %d, want %d", count, roots)
	}
}

func collectPagedMapIDs(values PagedMap[uint64]) []uint64 {
	ids := make([]uint64, 0, values.Len())
	for id := range values.Ordered() {
		ids = append(ids, id)
	}
	return ids
}
