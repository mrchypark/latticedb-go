package store

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"slices"
	"testing"
)

func TestPropertyDeltaRemovalOrderDoesNotMutateKeys(t *testing.T) {
	keys := []string{"z", "present", "a"}
	change, err := buildPersistedPropertyChange(1, keys, PropertiesFromMap(map[string]any{"present": int64(1)}))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(change.Remove, []string{"a", "z"}) || !slices.Equal(keys, []string{"z", "present", "a"}) {
		t.Fatalf("remove=%v, caller keys=%v", change.Remove, keys)
	}
}

func TestPropertyDeltaChargesNewScalarKeys(t *testing.T) {
	set := make(map[string]persistedValue, 100)
	for i := 0; i < 100; i++ {
		set[fmt.Sprint(i)] = persistedValue{Kind: "int", Int: int64(i)}
	}
	for _, node := range []bool{true, false} {
		accumulator := &walAccumulator{
			nodes: map[uint64]persistedNode{1: {ID: 1}},
			edges: map[uint64]persistedEdge{1: {ID: 1}},
		}
		delta := persistedDelta{}
		changes := []persistedPropertyChange{{ID: 1, Set: set}}
		if node {
			delta.NodePropertyChanges = changes
		} else {
			delta.EdgePropertyChanges = changes
		}
		work, err := propertyDeltaWork(context.Background(), accumulator, delta)
		if err != nil {
			t.Fatal(err)
		}
		budget := &recoveryBudget{limits: RecoveryLimits{MaxWork: 50}}
		if err := budget.replayWork(work); !errors.Is(err, ErrLoadResourceLimit) {
			t.Fatalf("node=%v scalar patch work=%d, budget error=%v", node, work, err)
		}
	}
}

func TestPropertyTotalsStayEqualToRecount(t *testing.T) {
	ctx := context.Background()
	accumulator, err := newWALAccumulator(ctx, persistedState{DatabaseID: "00000000000000000000000000000001", NextNodeID: 2, NextEdgeID: 1, Nodes: []persistedNode{{ID: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	random := rand.New(rand.NewSource(67))
	for step := 0; step < 100; step++ {
		next := func() persistedDelta {
			return persistedDelta{DatabaseID: accumulator.state.DatabaseID, CommitID: accumulator.state.CommitID + 1, NextNodeID: 2, NextEdgeID: 1}
		}
		if step%17 == 0 {
			full := next()
			full.UpsertNodes = []persistedNode{{ID: 1, Properties: map[string]persistedValue{"fresh": {Kind: "string", String: "value"}}}}
			if err := accumulator.apply(full); err != nil {
				t.Fatal(err)
			}
		}
		key := fmt.Sprintf("key%d", random.Intn(5))
		change := persistedPropertyChange{ID: 1}
		if step%3 == 0 {
			change.Remove = []string{key}
		} else {
			list := make([]persistedValue, random.Intn(5))
			for i := range list {
				list[i] = persistedValue{Kind: "bytes", Bytes: make([]byte, i+1)}
			}
			change.Set = map[string]persistedValue{key: {Kind: "map", Map: map[string]persistedValue{"nested": {Kind: "list", List: list}}}}
		}
		delta := next()
		delta.NodePropertyChanges = []persistedPropertyChange{change}
		if _, err := propertyDeltaWork(ctx, accumulator, delta); err != nil {
			t.Fatal(err)
		}
		prepared, err := preparePropertyDelta(ctx, accumulator, delta)
		if err != nil {
			t.Fatal(err)
		}
		if len(prepared) != 1 {
			t.Fatalf("prepared count=%d", len(prepared))
		}
		fresh, _, err := derivePropertyTotals(ctx, prepared[0].properties)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(prepared[0].totals, fresh) {
			t.Fatalf("step %d cached totals differ: got=%+v want=%+v", step, prepared[0].totals, fresh)
		}
		delta.NodePropertyChanges = nil
		if err := accumulator.apply(delta); err != nil {
			t.Fatal(err)
		}
		node := accumulator.nodes[1]
		node.Properties = prepared[0].properties
		accumulator.nodes[1] = node
		accumulator.nodePropertyTotals[1] = prepared[0].totals
	}
}
