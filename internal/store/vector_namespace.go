package store

import "slices"

// VectorMetric identifies the distance function used by a derived vector
// namespace. The initial namespace implementation supports L2 only.
type VectorMetric uint8

const VectorMetricL2 VectorMetric = 0

// VectorNamespace identifies one derived vector index opened by the caller.
// It is intentionally not persisted; callers recreate open namespaces.
type VectorNamespace struct {
	Property   string
	Scope      string
	Dimensions uint16
	Metric     VectorMetric
}

// VectorNamespaceState contains the derived state for one open namespace.
type VectorNamespaceState struct {
	Index      VectorIndex
	Tombstones PagedMap[[]float32]
	LiveCount  uint64
	Mutations  uint64
}

func cloneVectorNamespacesShallow(namespaces map[VectorNamespace]VectorNamespaceState) map[VectorNamespace]VectorNamespaceState {
	if len(namespaces) == 0 {
		return nil
	}
	cloned := make(map[VectorNamespace]VectorNamespaceState, len(namespaces))
	for namespace, state := range namespaces {
		state.Index = state.Index.Fork()
		state.Tombstones = state.Tombstones.Fork()
		cloned[namespace] = state
	}
	return cloned
}

func cloneVectorIndexDeep(index VectorIndex) VectorIndex {
	cloned := NewVectorIndex()
	cloned.EntryID = index.EntryID
	cloned.MaxLevel = index.MaxLevel
	for id, node := range index.Nodes.All() {
		copyNode := &VectorIndexNode{Level: node.Level, Neighbors: make([][]uint64, len(node.Neighbors)), Vector: slices.Clone(node.Vector)}
		for level := range node.Neighbors {
			copyNode.Neighbors[level] = slices.Clone(node.Neighbors[level])
		}
		cloned.Nodes.Set(id, copyNode)
	}
	return cloned
}

func cloneVectorTombstonesDeep(tombstones PagedMap[[]float32]) PagedMap[[]float32] {
	cloned := NewPagedMap[[]float32]()
	for id, vector := range tombstones.All() {
		cloned.Set(id, slices.Clone(vector))
	}
	return cloned
}

func cloneVectorNamespacesDeep(namespaces map[VectorNamespace]VectorNamespaceState) map[VectorNamespace]VectorNamespaceState {
	if len(namespaces) == 0 {
		return nil
	}
	cloned := make(map[VectorNamespace]VectorNamespaceState, len(namespaces))
	for namespace, state := range namespaces {
		state.Index = cloneVectorIndexDeep(state.Index)
		state.Tombstones = cloneVectorTombstonesDeep(state.Tombstones)
		cloned[namespace] = state
	}
	return cloned
}
