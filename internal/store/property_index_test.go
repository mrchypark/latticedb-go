package store

import (
	"context"
	"errors"
	"math"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
)

func TestPropertyIndexesTypedValuesAndForkIsolation(t *testing.T) {
	definition := PropertyIndexDefinition{Scope: "Item", Property: "value"}
	base := NewPropertyIndexes()
	if !base.Create(definition) {
		t.Fatal("create property index")
	}
	values := []any{
		nil,
		true,
		int64(7),
		float64(7),
		"seven",
		[]byte{7},
		[]float32{7, 0},
		[]any{int64(7), "seven"},
		map[string]any{"a": int64(7), "b": true},
	}
	for index, value := range values {
		if err := base.Add(definition, value, uint64(index+1)); err != nil {
			t.Fatal(err)
		}
	}
	for index, value := range values {
		ids, exists, err := base.Lookup(definition, value)
		if err != nil || !exists || !slices.Equal(ids, []uint64{uint64(index + 1)}) {
			t.Fatalf("lookup %T = %v, %v, %v", value, ids, exists, err)
		}
	}
	if err := base.Add(definition, math.Copysign(0, -1), 99); err != nil {
		t.Fatal(err)
	}
	ids, _, err := base.Lookup(definition, float64(0))
	if err != nil || !slices.Equal(ids, []uint64{99}) {
		t.Fatalf("signed-zero lookup = %v, %v", ids, err)
	}
	fork := base.Fork()
	if err := fork.Remove(definition, "seven", 5); err != nil {
		t.Fatal(err)
	}
	if err := fork.Add(definition, "seven", 100); err != nil {
		t.Fatal(err)
	}
	baseIDs, _, _ := base.Lookup(definition, "seven")
	forkIDs, _, _ := fork.Lookup(definition, "seven")
	if !slices.Equal(baseIDs, []uint64{5}) || !slices.Equal(forkIDs, []uint64{100}) {
		t.Fatalf("fork isolation = base %v, fork %v", baseIDs, forkIDs)
	}
}

func TestPropertyIndexesCardinality(t *testing.T) {
	indexes := NewPropertyIndexes()
	definition := PropertyIndexDefinition{Scope: "Item", Property: "kind"}
	if !indexes.Create(definition) {
		t.Fatal("Create returned false")
	}
	for id, value := range []any{"rare", "common", "common"} {
		if err := indexes.Add(definition, value, uint64(id)); err != nil {
			t.Fatal(err)
		}
	}
	if got, found, err := indexes.Cardinality(definition, "common"); err != nil || !found || got != 2 {
		t.Fatalf("common cardinality = %d, %t, %v", got, found, err)
	}
	if got, found, err := indexes.Cardinality(definition, "missing"); err != nil || !found || got != 0 {
		t.Fatalf("missing cardinality = %d, %t, %v", got, found, err)
	}
	if got, found, err := indexes.Cardinality(PropertyIndexDefinition{Scope: "missing"}, "common"); err != nil || found || got != 0 {
		t.Fatalf("missing definition cardinality = %d, %t, %v", got, found, err)
	}
}

func TestPropertyIndexLookupLimitReturnsOrderedPrefixAcrossMutations(t *testing.T) {
	indexes := NewPropertyIndexes()
	definition := PropertyIndexDefinition{Scope: "Item", Property: "kind"}
	indexes.Create(definition)
	for id := uint64(1_000); id > 0; id-- {
		if err := indexes.Add(definition, "common", id); err != nil {
			t.Fatal(err)
		}
	}
	full, found, err := indexes.Lookup(definition, "common")
	if err != nil || !found || len(full) != 1_000 {
		t.Fatalf("full lookup = %d IDs, found=%v, err=%v", len(full), found, err)
	}
	for _, limit := range []uint{1, 3, 128} {
		ids, found, err := indexes.LookupLimit(definition, "common", limit)
		if err != nil || !found || !slices.Equal(ids, full[:limit]) {
			t.Fatalf("limit %d = %v, want %v, found=%v, err=%v", limit, ids, full[:limit], found, err)
		}
	}
	fork := indexes.Fork()
	if err := fork.Remove(definition, "common", 1); err != nil {
		t.Fatal(err)
	}
	if err := fork.Remove(definition, "common", 2); err != nil {
		t.Fatal(err)
	}
	if err := fork.Add(definition, "common", 2); err != nil {
		t.Fatal(err)
	}
	forkIDs, found, err := fork.LookupLimit(definition, "common", 3)
	if err != nil || !found || !slices.Equal(forkIDs, []uint64{2, 3, 4}) {
		t.Fatalf("fork limited lookup = %v, found=%v err=%v", forkIDs, found, err)
	}
	baseIDs, found, err := indexes.Lookup(definition, "common")
	if err != nil || !found || len(baseIDs) != 1_000 {
		t.Fatalf("base lookup after fork delete = %d, found=%v, err=%v", len(baseIDs), found, err)
	}

	// IDs in later radix roots must still follow the first root, independent of
	// map iteration order.
	sparse := NewPropertyIndexes()
	sparse.Create(definition)
	for _, id := range []uint64{2<<20 + 1, 1 << 20, 2, 1<<20 + 1} {
		if err := sparse.Add(definition, "sparse", id); err != nil {
			t.Fatal(err)
		}
	}
	got, found, err := sparse.LookupLimit(definition, "sparse", 3)
	if err != nil || !found || !slices.Equal(got, []uint64{2, 1 << 20, 1<<20 + 1}) {
		t.Fatalf("sparse limited lookup = %v, found=%v, err=%v", got, found, err)
	}
	for root := uint64(3); root <= 128; root++ {
		if err := sparse.Add(definition, "sparse", root<<20); err != nil {
			t.Fatal(err)
		}
	}
	got, found, err = sparse.LookupLimit(definition, "sparse", 1)
	if err != nil || !found || !slices.Equal(got, []uint64{2}) {
		t.Fatalf("many-root limited lookup = %v, found=%v, err=%v", got, found, err)
	}
	sparseFork := sparse.Fork()
	if err := sparseFork.Remove(definition, "sparse", 2); err != nil {
		t.Fatal(err)
	}
	got, found, err = sparseFork.LookupLimit(definition, "sparse", 1)
	if err != nil || !found || !slices.Equal(got, []uint64{1 << 20}) {
		t.Fatalf("sparse fork limited lookup = %v, found=%v, err=%v", got, found, err)
	}
	got, found, err = sparse.LookupLimit(definition, "sparse", 1)
	if err != nil || !found || !slices.Equal(got, []uint64{2}) {
		t.Fatalf("base sparse lookup after fork delete = %v, found=%v, err=%v", got, found, err)
	}

	// A previously dense radix posting must shed high-root overhead when later
	// writes spread it across many roots.
	dense := NewPropertyIndexes()
	dense.Create(definition)
	for id := uint64(1); id <= smallPostingLimit+1; id++ {
		if err := dense.Add(definition, "dense-wide", id); err != nil {
			t.Fatal(err)
		}
	}
	for root := uint64(1); root <= 8; root++ {
		if err := dense.Add(definition, "dense-wide", root<<20|7); err != nil {
			t.Fatal(err)
		}
	}
	denseKey, err := makePropertyValueKey("dense-wide")
	if err != nil {
		t.Fatal(err)
	}
	densePosting := dense.definitions[definition].values.Get(hashPropertyValueKey(denseKey))[denseKey]
	if densePosting.large != nil || densePosting.chunks == nil {
		t.Fatalf("wide radix posting did not convert to chunks: %#v", densePosting)
	}
}

func TestPropertyIndexesDefinitionsFor(t *testing.T) {
	indexes := NewPropertyIndexes()
	for _, definition := range []PropertyIndexDefinition{
		{Scope: "Item", Property: "key"},
		{Scope: "Item", Property: "other"},
		{Scope: "Other", Property: "key"},
	} {
		indexes.Create(definition)
	}
	var got []PropertyIndexDefinition
	for definition := range indexes.DefinitionsFor([]string{"Item"}, map[string]any{"key": int64(1)}) {
		got = append(got, definition)
	}
	if !slices.Equal(got, []PropertyIndexDefinition{{Scope: "Item", Property: "key"}}) {
		t.Fatalf("DefinitionsFor = %v", got)
	}
	got = nil
	for definition := range indexes.DefinitionsFor([]string{"Item", "Other"}, map[string]any{"key": int64(1), "other": int64(2)}) {
		got = append(got, definition)
	}
	slices.SortFunc(got, func(left, right PropertyIndexDefinition) int {
		if left.Scope != right.Scope {
			return strings.Compare(left.Scope, right.Scope)
		}
		return strings.Compare(left.Property, right.Property)
	})
	want := []PropertyIndexDefinition{{Scope: "Item", Property: "key"}, {Scope: "Item", Property: "other"}, {Scope: "Other", Property: "key"}}
	if !slices.Equal(got, want) {
		t.Fatalf("DefinitionsFor = %v, want %v", got, want)
	}
	indexes = NewPropertyIndexes()
	definition := PropertyIndexDefinition{Scope: "Item", Property: "key"}
	indexes.Create(definition)
	for i := 0; i < 16; i++ {
		indexes.Create(PropertyIndexDefinition{Scope: "Item", Property: "unused" + strconv.Itoa(i)})
	}
	scopes := []string{"Item"}
	for i := 1; i < 32; i++ {
		scopes = append(scopes, "Other"+strconv.Itoa(i))
	}
	got = nil
	for definition := range indexes.DefinitionsFor(scopes, map[string]any{"key": int64(1)}) {
		got = append(got, definition)
	}
	if !slices.Equal(got, []PropertyIndexDefinition{definition}) {
		t.Fatalf("DefinitionsFor multiple scopes = %v", got)
	}
}

func TestPropertyIndexReclaimsHistoricalValues(t *testing.T) {
	indexes := NewPropertyIndexes()
	definition := PropertyIndexDefinition{Scope: "Item", Property: "version"}
	if !indexes.Create(definition) {
		t.Fatal("create property index")
	}
	for value := int64(0); value < 100_000; value++ {
		if err := indexes.Add(definition, value, 1); err != nil {
			t.Fatal(err)
		}
		if err := indexes.Remove(definition, value, 1); err != nil {
			t.Fatal(err)
		}
	}
	data := indexes.definitions[definition]
	if got := data.values.Len(); got != 0 {
		t.Fatalf("retained empty value buckets = %d, want 0", got)
	}
	if got := len(data.values.clonedShards); got != 0 {
		t.Fatalf("retained cloned shard markers = %d, want 0", got)
	}
}

func TestPropertyIndexReaddAfterEmptyBucketPreservesForkIsolation(t *testing.T) {
	definition := PropertyIndexDefinition{Scope: "Item", Property: "version"}
	base := NewPropertyIndexes()
	base.Create(definition)
	if err := base.Add(definition, int64(1), 1); err != nil {
		t.Fatal(err)
	}
	fork := base.Fork()
	if err := fork.Remove(definition, int64(1), 1); err != nil {
		t.Fatal(err)
	}
	if err := fork.Add(definition, int64(1), 2); err != nil {
		t.Fatal(err)
	}
	baseIDs, _, _ := base.Lookup(definition, int64(1))
	forkIDs, _, _ := fork.Lookup(definition, int64(1))
	if !slices.Equal(baseIDs, []uint64{1}) || !slices.Equal(forkIDs, []uint64{2}) {
		t.Fatalf("fork isolation = base %v, fork %v", baseIDs, forkIDs)
	}
}

func TestPropertyIndexRebuildChargesSparseScans(t *testing.T) {
	snapshot := persistedState{DatabaseID: "00000000000000000000000000000001", NextNodeID: 1_001}
	for id := uint64(1); id <= 1_000; id++ {
		snapshot.Nodes = append(snapshot.Nodes, persistedNode{ID: id, Labels: []string{"Item"}})
	}
	minimum := minimumDecodeWork(t, snapshot)
	snapshot.NodeIndexes = []persistedPropertyIndexDefinition{{Scope: "Item", Property: "missing"}}
	if _, _, _, _, err := decodePersistedStateContext(context.Background(), snapshot, minimum, ^uint64(0)); !errors.Is(err, ErrDerivedIndexResourceLimit) {
		t.Fatalf("sparse index rebuild at base budget = %v", err)
	}
}

func minimumDecodeWork(t *testing.T, snapshot persistedState) uint64 {
	t.Helper()
	high := uint64(1)
	for {
		if _, _, _, _, err := decodePersistedStateContext(context.Background(), snapshot, high, ^uint64(0)); err == nil {
			break
		} else if !errors.Is(err, ErrDerivedIndexResourceLimit) {
			t.Fatal(err)
		}
		high *= 2
	}
	low := high / 2
	for low+1 < high {
		middle := low + (high-low)/2
		if _, _, _, _, err := decodePersistedStateContext(context.Background(), snapshot, middle, ^uint64(0)); err == nil {
			high = middle
		} else if errors.Is(err, ErrDerivedIndexResourceLimit) {
			low = middle
		} else {
			t.Fatal(err)
		}
	}
	return high
}

func BenchmarkPropertyValueKeyComposite(b *testing.B) {
	value := map[string]any{"a": int64(7), "b": []any{"seven", true}}
	b.ReportAllocs()
	for range b.N {
		if _, err := makePropertyValueKey(value); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPropertyIndexLookupLimitCommonPosting(b *testing.B) {
	indexes := NewPropertyIndexes()
	definition := PropertyIndexDefinition{Scope: "Item", Property: "kind"}
	indexes.Create(definition)
	for id := uint64(1); id <= 100_000; id++ {
		if err := indexes.Add(definition, "common", id); err != nil {
			b.Fatal(err)
		}
	}
	for _, limit := range []uint{1, 10, 100_000} {
		b.Run(strconv.FormatUint(uint64(limit), 10), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if ids, found, err := indexes.LookupLimit(definition, "common", limit); err != nil || !found || len(ids) != int(limit) {
					b.Fatalf("LookupLimit ids=%d found=%v err=%v", len(ids), found, err)
				}
			}
		})
	}
}

func BenchmarkPropertyIndexForkAddCommonPosting(b *testing.B) {
	indexes := NewPropertyIndexes()
	definition := PropertyIndexDefinition{Scope: "Item", Property: "kind"}
	indexes.Create(definition)
	for id := uint64(1); id <= 100_000; id++ {
		if err := indexes.Add(definition, "common", id); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		fork := indexes.Fork()
		if err := fork.Add(definition, "common", uint64(100_001+i)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPropertyIndexLookupLimitSparseRoots(b *testing.B) {
	indexes := NewPropertyIndexes()
	definition := PropertyIndexDefinition{Scope: "Item", Property: "kind"}
	indexes.Create(definition)
	for root := uint64(1); root <= 10_000; root++ {
		if err := indexes.Add(definition, "common", root<<20); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		ids, found, err := indexes.LookupLimit(definition, "common", 1)
		if err != nil || !found || !slices.Equal(ids, []uint64{1 << 20}) {
			b.Fatalf("LookupLimit ids=%v found=%v err=%v", ids, found, err)
		}
	}
}

func BenchmarkPropertyIndexBuildUniqueValues(b *testing.B) {
	definition := PropertyIndexDefinition{Scope: "Item", Property: "kind"}
	for _, sparse := range []bool{false, true} {
		name := "dense"
		if sparse {
			name = "sparse"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				indexes := NewPropertyIndexes()
				indexes.Create(definition)
				for id := uint64(1); id <= 10_000; id++ {
					postingID := id
					if sparse {
						postingID <<= 20
					}
					if err := indexes.Add(definition, int64(id), postingID); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

func BenchmarkPropertyIndexRetainedHeap(b *testing.B) {
	definition := PropertyIndexDefinition{Scope: "Item", Property: "kind"}
	for _, scenario := range []struct {
		name  string
		value func(uint64) any
		id    func(uint64) uint64
	}{
		{name: "common", value: func(uint64) any { return "common" }, id: func(id uint64) uint64 { return id }},
		{name: "unique", value: func(id uint64) any { return int64(id) }, id: func(id uint64) uint64 { return id }},
		{name: "sparse", value: func(uint64) any { return "common" }, id: func(id uint64) uint64 { return id << 20 }},
	} {
		b.Run(scenario.name, func(b *testing.B) {
			b.StopTimer()
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			indexes := NewPropertyIndexes()
			indexes.Create(definition)
			for id := uint64(1); id <= 10_000; id++ {
				if err := indexes.Add(definition, scenario.value(id), scenario.id(id)); err != nil {
					b.Fatal(err)
				}
			}
			runtime.GC()
			runtime.ReadMemStats(&after)
			runtime.KeepAlive(indexes)
			// Report the GC heap delta, not timing or an RSS limit. Subtract as
			// signed floating-point values so background GC noise cannot wrap.
			b.ReportMetric(float64(after.HeapAlloc)-float64(before.HeapAlloc), "retained-B")
			b.StartTimer()
			for range b.N {
				runtime.KeepAlive(indexes)
			}
		})
	}
}

func BenchmarkPropertyIndexSparsePostingWrites(b *testing.B) {
	definition := PropertyIndexDefinition{Scope: "Item", Property: "kind"}
	b.Run("build_reverse", func(b *testing.B) {
		for range b.N {
			indexes := NewPropertyIndexes()
			indexes.Create(definition)
			for index := uint64(100_000); index > 0; index-- {
				if err := indexes.Add(definition, "common", sparsePostingID(index)); err != nil {
					b.Fatal(err)
				}
			}
		}
	})

	indexes := NewPropertyIndexes()
	indexes.Create(definition)
	for index := uint64(1); index <= 100_000; index++ {
		if err := indexes.Add(definition, "common", sparsePostingID(index)); err != nil {
			b.Fatal(err)
		}
	}
	b.Run("fork_add", func(b *testing.B) {
		for range b.N {
			fork := indexes.Fork()
			if err := fork.Add(definition, "common", sparsePostingID(100_001)); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("fork_delete", func(b *testing.B) {
		for range b.N {
			fork := indexes.Fork()
			if err := fork.Remove(definition, "common", sparsePostingID(50_000)); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func sparsePostingID(index uint64) uint64 {
	return index<<20 | index&0xffff
}
