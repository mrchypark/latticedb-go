package store

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func propertyTestGraph() *GraphState {
	graph := NewGraphState()
	graph.DatabaseID = "00000000000000000000000000000001"
	graph.Nodes.Set(1, &NodeRecord{ID: 1, Properties: map[string]any{"keep": int64(1), "change": "old"}})
	return graph
}

func TestBuildPersistedDeltaPropertyPatch(t *testing.T) {
	delta, err := buildPersistedDelta(propertyTestGraph(), 2, 1, 1, GraphDelta{
		UpsertNodes:      []uint64{1},
		NodePropertyKeys: map[uint64][]string{1: {"change", "missing"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(delta.UpsertNodes) != 0 || len(delta.NodePropertyChanges) != 1 {
		t.Fatalf("delta = %+v, want one property patch and no full upsert", delta)
	}
	change := delta.NodePropertyChanges[0]
	if change.Remove[0] != "missing" || change.Set["change"].String != "old" {
		t.Fatalf("property change = %+v", change)
	}
}

func TestBuildPersistedPropertyChangeRejectsCombinedAggregateLimit(t *testing.T) {
	wide := make([]any, maxValueElements-1)
	properties := map[string]any{"wide": wide, "changed": "valid"}
	if _, err := encodePropertyMap(map[string]any{"changed": "valid"}); err != nil {
		t.Fatalf("individual changed property was rejected: %v", err)
	}
	if _, err := buildPersistedPropertyChange(1, []string{"changed"}, properties); !errors.Is(err, ErrValueLimit) {
		t.Fatalf("combined property map error = %v, want ErrValueLimit", err)
	}
}

func TestBuildPersistedPropertyChangeAggregateBytesBoundary(t *testing.T) {
	large := strings.Repeat("x", maxValueBytes-1)
	if _, err := buildPersistedPropertyChange(1, []string{"x"}, map[string]any{"x": large}); err != nil {
		t.Fatalf("exact aggregate byte boundary rejected: %v", err)
	}
	if _, err := encodePropertyMap(map[string]any{"y": "valid"}); err != nil {
		t.Fatalf("individual changed property was rejected: %v", err)
	}
	if _, err := buildPersistedPropertyChange(1, []string{"y"}, map[string]any{"x": large, "y": "valid"}); !errors.Is(err, ErrValueLimit) {
		t.Fatalf("combined property map error = %v, want ErrValueLimit", err)
	}
}

func TestPreparePropertyDeltaPreservesAndClonesProperties(t *testing.T) {
	accumulator, err := newWALAccumulator(context.Background(), persistedState{
		DatabaseID: "00000000000000000000000000000001", CommitID: 0, NextNodeID: 2, NextEdgeID: 1,
		Nodes: []persistedNode{{ID: 1, Properties: map[string]persistedValue{"keep": {Kind: "int", Int: 1}, "change": {Kind: "string", String: "old"}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	delta := persistedDelta{DatabaseID: accumulator.state.DatabaseID, CommitID: 1, NextNodeID: 2, NextEdgeID: 1,
		NodePropertyChanges: []persistedPropertyChange{{ID: 1, Set: map[string]persistedValue{"change": {Kind: "string", String: "new"}}, Remove: []string{"missing"}}}}
	prepared, err := preparePropertyDelta(context.Background(), accumulator, delta)
	if err != nil {
		t.Fatal(err)
	}
	if len(prepared) != 1 || prepared[0].properties["keep"].Int != 1 || prepared[0].properties["change"].String != "new" {
		t.Fatalf("prepared = %+v", prepared)
	}
	prepared[0].properties["change"] = persistedValue{Kind: "string", String: "mutated"}
	if accumulator.nodes[1].Properties["change"].String != "old" {
		t.Fatal("property expansion aliased accumulator state")
	}
}

func TestPropertyDeltaRejectsOldKindAndEmptyNewKind(t *testing.T) {
	delta := persistedDelta{DatabaseID: propertyTestGraph().DatabaseID, CommitID: 1, NextNodeID: 2, NextEdgeID: 1,
		NodePropertyChanges: []persistedPropertyChange{{ID: 1, Set: map[string]persistedValue{"x": {Kind: "int", Int: 2}}}}}
	for _, kind := range []string{"delta", "property_delta"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "wal")
			if err := AppendWALCommit(path, propertyTestGraph(), 2, 1, 0); err != nil {
				t.Fatal(err)
			}
			base, err := os.OpenFile(walFilePath(path), os.O_RDWR|os.O_APPEND, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer base.Close()
			payloadDelta := delta
			if kind == "property_delta" {
				payloadDelta.NodePropertyChanges = nil
			}
			payload, err := json.Marshal(walPayload{Kind: kind, Delta: &payloadDelta})
			if err != nil {
				t.Fatal(err)
			}
			header, err := encodeWALHeader(delta.DatabaseID, delta.CommitID, payload)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := base.Write(header[:]); err != nil {
				t.Fatal(err)
			}
			if _, err := base.Write(payload); err != nil {
				t.Fatal(err)
			}
			if _, err := base.Seek(0, 0); err != nil {
				t.Fatal(err)
			}
			_, err = loadLatestWALV2ContextWithRecoveryBudget(context.Background(), base, maxWALFrameBytes, nil, &recoveryBudget{})
			if kind == "delta" && err == nil {
				t.Fatal("old delta with patch fields was accepted")
			}
			if kind == "property_delta" && err == nil {
				t.Fatal("empty property delta was accepted")
			}
		})
	}
}

func TestPropertyDeltaRoundTripsThroughRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	graph := propertyTestGraph()
	if err := AppendWALCommit(path, graph, 2, 1, 0); err != nil {
		t.Fatal(err)
	}
	graph.Nodes.Get(1).Properties["change"] = "new"
	if err := AppendWALDelta(path, graph, 2, 1, 1, GraphDelta{UpsertNodes: []uint64{1}, NodePropertyKeys: map[uint64][]string{1: {"change"}}}); err != nil {
		t.Fatal(err)
	}
	state, err := loadLatestWALSnapshot(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Nodes) != 1 || state.Nodes[0].Properties["change"].String != "new" {
		t.Fatalf("recovered state = %+v", state.Nodes)
	}
}

func TestPropertyDeltaWorkBoundsDepthAndContext(t *testing.T) {
	value := persistedValue{Kind: "string", String: "x"}
	for i := 0; i <= maxValueDepth; i++ {
		value = persistedValue{Kind: "map", Map: map[string]persistedValue{"nested": value}}
	}
	accumulator := &walAccumulator{nodes: map[uint64]persistedNode{1: {ID: 1, Properties: map[string]persistedValue{}}}}
	delta := persistedDelta{NodePropertyChanges: []persistedPropertyChange{{ID: 1, Set: map[string]persistedValue{"x": value}}}}
	if _, err := propertyDeltaWork(context.Background(), accumulator, delta); err == nil {
		t.Fatal("deep raw property value was accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := propertyDeltaWork(ctx, accumulator, delta); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled property work error = %v", err)
	}
}

func TestPropertyDeltaSetIntoEmptyProperties(t *testing.T) {
	accumulator, err := newWALAccumulator(context.Background(), persistedState{DatabaseID: "00000000000000000000000000000001", CommitID: 0, NextNodeID: 2, NextEdgeID: 1, Nodes: []persistedNode{{ID: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := preparePropertyDelta(context.Background(), accumulator, persistedDelta{NodePropertyChanges: []persistedPropertyChange{{ID: 1, Set: map[string]persistedValue{"x": {Kind: "string", String: "ok"}}}}})
	if err != nil || len(prepared) != 1 || prepared[0].properties["x"].String != "ok" {
		t.Fatalf("prepared empty property map = %+v, err=%v", prepared, err)
	}
}

func TestPropertyDeltaTotalsInvalidateAcrossFullUpsertAndDelete(t *testing.T) {
	old := persistedValue{Kind: "list", List: make([]persistedValue, 100)}
	for i := range old.List {
		old.List[i] = persistedValue{Kind: "map", Map: map[string]persistedValue{"v": {Kind: "string", String: "old"}}}
	}
	accumulator, err := newWALAccumulator(context.Background(), persistedState{
		DatabaseID: "00000000000000000000000000000001", CommitID: 0, NextNodeID: 2, NextEdgeID: 1,
		Nodes: []persistedNode{{ID: 1, Properties: map[string]persistedValue{"nested": old}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	first := persistedDelta{
		DatabaseID: accumulator.state.DatabaseID, CommitID: 1, NextNodeID: 2, NextEdgeID: 1,
		NodePropertyChanges: []persistedPropertyChange{{ID: 1, Set: map[string]persistedValue{"a": {Kind: "string", String: "one"}}}},
	}
	if _, err := propertyDeltaWork(context.Background(), accumulator, first); err != nil {
		t.Fatal(err)
	}
	prepared, err := preparePropertyDelta(context.Background(), accumulator, first)
	if err != nil {
		t.Fatal(err)
	}
	stripped := first
	stripped.NodePropertyChanges = nil
	if err := accumulator.apply(stripped); err != nil {
		t.Fatal(err)
	}
	accumulator.nodes[1] = persistedNode{ID: 1, Properties: prepared[0].properties}
	accumulator.nodePropertyTotals[1] = prepared[0].totals
	full := persistedDelta{
		DatabaseID: accumulator.state.DatabaseID, CommitID: 2, NextNodeID: 2, NextEdgeID: 1,
		UpsertNodes: []persistedNode{{ID: 1, Properties: map[string]persistedValue{"small": {Kind: "string", String: "x"}}}},
	}
	if err := accumulator.apply(full); err != nil {
		t.Fatal(err)
	}
	if _, ok := accumulator.nodePropertyTotals[1]; ok {
		t.Fatal("full upsert did not invalidate property totals")
	}
	second := persistedDelta{
		DatabaseID: accumulator.state.DatabaseID, CommitID: 3, NextNodeID: 2, NextEdgeID: 1,
		NodePropertyChanges: []persistedPropertyChange{{ID: 1, Set: map[string]persistedValue{"new": {Kind: "string", String: "y"}}}},
	}
	if _, err := propertyDeltaWork(context.Background(), accumulator, second); err != nil {
		t.Fatal(err)
	}
	prepared, err = preparePropertyDelta(context.Background(), accumulator, second)
	if err != nil {
		t.Fatal(err)
	}
	if prepared[0].totals.elements != 2 {
		t.Fatalf("fresh totals elements = %d, want 2", prepared[0].totals.elements)
	}
	if err := accumulator.apply(persistedDelta{
		DatabaseID: accumulator.state.DatabaseID, CommitID: 3, NextNodeID: 2, NextEdgeID: 1,
		DeleteNodes: []uint64{1},
	}); err != nil {
		t.Fatal(err)
	}
	if _, ok := accumulator.nodePropertyTotals[1]; ok {
		t.Fatal("delete did not invalidate property totals")
	}
}

func TestPropertyDeltaWorkDoesNotRescanUnchangedNestedValues(t *testing.T) {
	nested := persistedValue{Kind: "list", List: make([]persistedValue, 2000)}
	for i := range nested.List {
		nested.List[i] = persistedValue{Kind: "map", Map: map[string]persistedValue{
			"value": {Kind: "string", String: "unchanged"},
		}}
	}
	accumulator, err := newWALAccumulator(context.Background(), persistedState{
		DatabaseID: "00000000000000000000000000000001", NextNodeID: 2, NextEdgeID: 1,
		Nodes: []persistedNode{{ID: 1, Properties: map[string]persistedValue{"nested": nested}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	patch := func(key string) persistedDelta {
		return persistedDelta{NodePropertyChanges: []persistedPropertyChange{{
			ID: 1, Set: map[string]persistedValue{key: {Kind: "string", String: key}},
		}}}
	}
	first := patch("first")
	firstWork, err := propertyDeltaWork(context.Background(), accumulator, first)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := preparePropertyDelta(context.Background(), accumulator, first)
	if err != nil {
		t.Fatal(err)
	}
	accumulator.nodes[1] = persistedNode{ID: 1, Properties: prepared[0].properties}
	accumulator.nodePropertyTotals[1] = prepared[0].totals
	secondWork, err := propertyDeltaWork(context.Background(), accumulator, patch("second"))
	if err != nil {
		t.Fatal(err)
	}
	if secondWork >= firstWork {
		t.Fatalf("second patch work = %d, first = %d; unchanged nested value was rescanned", secondWork, firstWork)
	}
	if firstWork <= 10 || secondWork > 10 {
		t.Fatalf("patch work = first %d, second %d; expected only a small cached second patch", firstWork, secondWork)
	}
	budget := &recoveryBudget{limits: RecoveryLimits{MaxWork: 10}}
	if err := budget.replayWork(secondWork); err != nil {
		t.Fatalf("cached second patch exceeded tight recovery budget: %v", err)
	}
}

func TestAdjustPropertyTotalsRejectsAggregateLimitOverflow(t *testing.T) {
	for _, test := range []struct {
		name   string
		totals propertyTotals
	}{
		{name: "elements", totals: propertyTotals{
			elements: maxValueElements, values: map[string]propertyValueTotals{"old": {}},
		}},
		{name: "bytes", totals: propertyTotals{
			bytes: maxValueBytes, values: map[string]propertyValueTotals{"old": {}},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := adjustPropertyTotals(context.Background(), test.totals, persistedPropertyChange{
				ID: 1, Set: map[string]persistedValue{"new": {Kind: "int", Int: 1}},
			})
			if !errors.Is(err, ErrValueLimit) {
				t.Fatalf("adjustPropertyTotals error = %v, want ErrValueLimit", err)
			}
		})
	}
}
