package store

import "testing"

func vectorNamespaceFixture() (*GraphState, VectorNamespace) {
	graph := NewGraphState()
	namespace := VectorNamespace{Property: "embedding", Scope: "article", Dimensions: 3, Metric: VectorMetricL2}
	index := NewVectorIndex()
	index.EntryID = 1
	index.MaxLevel = 2
	index.Nodes.Set(1, &VectorIndexNode{Level: 1, Neighbors: [][]uint64{{2, 3}}, Vector: []float32{1, 2, 3}})
	tombstones := NewPagedMap[[]float32]()
	tombstones.Set(2, []float32{4, 5, 6})
	graph.VectorNamespaces = map[VectorNamespace]VectorNamespaceState{
		namespace: {Index: index, Tombstones: tombstones, LiveCount: 7, Mutations: 8},
	}
	graph.VectorNamespace = &namespace
	return graph, namespace
}

func TestVectorNamespaceShallowCloneForksContainers(t *testing.T) {
	base, namespace := vectorNamespaceFixture()
	clone := CloneGraphStateShallow(base)
	state := clone.VectorNamespaces[namespace]

	state.Index.Nodes.CloneShardOnce(1)
	state.Index.Nodes.Set(1, &VectorIndexNode{
		Level: 1, Neighbors: [][]uint64{{99, 3}}, Vector: []float32{9, 2, 3},
	})
	state.Tombstones.CloneShardOnce(2)
	state.Tombstones.Set(2, []float32{40, 5, 6})
	state.LiveCount = 10
	clone.VectorNamespaces[namespace] = state

	oldState := base.VectorNamespaces[namespace]
	oldNode := oldState.Index.Nodes.Get(1)
	if oldNode.Neighbors[0][0] != 2 || oldNode.Vector[0] != 1 {
		t.Fatal("shallow namespace clone mutated the old index")
	}
	if oldState.Tombstones.Get(2)[0] != 4 {
		t.Fatal("shallow namespace clone mutated old tombstones")
	}
	if oldState.LiveCount != 7 {
		t.Fatal("shallow namespace clone mutated old counters")
	}
}

func TestVectorNamespaceDeepCloneCopiesNestedValues(t *testing.T) {
	base, namespace := vectorNamespaceFixture()
	clone := CloneGraphState(base)
	state := clone.VectorNamespaces[namespace]
	clonedNode := state.Index.Nodes.Get(1)
	clonedNode.Neighbors[0][0] = 99
	clonedNode.Vector[0] = 9
	clonedTombstone := state.Tombstones.Get(2)
	clonedTombstone[0] = 40

	oldState := base.VectorNamespaces[namespace]
	oldNode := oldState.Index.Nodes.Get(1)
	if oldNode.Neighbors[0][0] != 2 || oldNode.Vector[0] != 1 {
		t.Fatal("deep namespace clone aliased nested index values")
	}
	if oldState.Tombstones.Get(2)[0] != 4 {
		t.Fatal("deep namespace clone aliased tombstone values")
	}
}

func TestVectorNamespaceClonesKeepEmptyMapNilAndSelector(t *testing.T) {
	base := NewGraphState()
	selector := VectorNamespace{Property: "embedding", Dimensions: 3, Metric: VectorMetricL2}
	base.VectorNamespace = &selector

	shallow := CloneGraphStateShallow(base)
	deep := CloneGraphState(base)
	if shallow.VectorNamespaces != nil || deep.VectorNamespaces != nil {
		t.Fatal("empty vector namespace map was allocated")
	}
	if shallow.VectorNamespace != base.VectorNamespace || deep.VectorNamespace != base.VectorNamespace {
		t.Fatal("transient namespace selector was not retained")
	}
}
