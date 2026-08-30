package store

import (
	"context"
	"errors"
	"math"
	"slices"
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
