package engine

import (
	"cmp"
	"fmt"
	"slices"

	"github.com/mrchypark/latticedb-go/internal/store"
)

type VectorMetric = store.VectorMetric
type VectorNamespace = store.VectorNamespace
type VectorNamespaceState = store.VectorNamespaceState

const VectorMetricL2 = store.VectorMetricL2

func normalizeVectorNamespace(namespace VectorNamespace, dimensions uint16) (VectorNamespace, error) {
	if namespace.Property == "" {
		return VectorNamespace{}, fmt.Errorf("%w: vector namespace property is required", ErrInvalidArgument)
	}
	if err := store.ValidatePropertyKey(namespace.Property); err != nil {
		return VectorNamespace{}, err
	}
	if namespace.Scope != "" {
		if err := store.ValidateCreateLabels([]string{namespace.Scope}); err != nil {
			return VectorNamespace{}, err
		}
	}
	if namespace.Dimensions == 0 {
		namespace.Dimensions = dimensions
	}
	if dimensions == 0 || namespace.Dimensions != dimensions {
		return VectorNamespace{}, fmt.Errorf("vector namespace dimensions %d do not match configured dimensions %d", namespace.Dimensions, dimensions)
	}
	if namespace.Metric != VectorMetricL2 {
		return VectorNamespace{}, fmt.Errorf("%w: vector metric %d is unsupported", ErrUnsupportedOption, namespace.Metric)
	}
	return namespace, nil
}

func normalizeVectorNamespaces(namespaces []VectorNamespace, dimensions uint16) ([]VectorNamespace, error) {
	if len(namespaces) == 0 {
		return nil, nil
	}
	normalized := make([]VectorNamespace, len(namespaces))
	for i, namespace := range namespaces {
		var err error
		normalized[i], err = normalizeVectorNamespace(namespace, dimensions)
		if err != nil {
			return nil, err
		}
	}
	slices.SortFunc(normalized, compareVectorNamespace)
	for i := 1; i < len(normalized); i++ {
		if normalized[i] == normalized[i-1] {
			return nil, fmt.Errorf("%w: duplicate vector namespace", ErrInvalidArgument)
		}
	}
	return normalized, nil
}

func compareVectorNamespace(a, b VectorNamespace) int {
	if c := cmp.Compare(a.Scope, b.Scope); c != 0 {
		return c
	}
	if c := cmp.Compare(a.Property, b.Property); c != 0 {
		return c
	}
	if c := cmp.Compare(a.Dimensions, b.Dimensions); c != 0 {
		return c
	}
	return cmp.Compare(a.Metric, b.Metric)
}

func emptyVectorNamespaceStates(namespaces []VectorNamespace) map[VectorNamespace]VectorNamespaceState {
	if len(namespaces) == 0 {
		return nil
	}
	states := make(map[VectorNamespace]VectorNamespaceState, len(namespaces))
	for _, namespace := range namespaces {
		states[namespace] = VectorNamespaceState{Index: store.NewVectorIndex(), Tombstones: store.NewPagedMap[[]float32]()}
	}
	return states
}

func vectorNamespaceFacade(graph *store.GraphState, namespace VectorNamespace) *store.GraphState {
	state := graph.VectorNamespaces[namespace]
	view := *graph
	view.VectorNamespace = &namespace
	view.VectorNamespaces = nil
	view.VectorIndex = state.Index
	view.VectorTombstones = state.Tombstones
	view.VectorLiveCount = state.LiveCount
	view.VectorMutations = state.Mutations
	return &view
}

func writeVectorNamespaceFacade(graph *store.GraphState, namespace VectorNamespace, view *store.GraphState) {
	state := graph.VectorNamespaces[namespace]
	state.Index = view.VectorIndex
	state.Tombstones = view.VectorTombstones
	state.LiveCount = view.VectorLiveCount
	state.Mutations = view.VectorMutations
	if graph.VectorNamespaces == nil {
		graph.VectorNamespaces = make(map[VectorNamespace]VectorNamespaceState)
	}
	graph.VectorNamespaces[namespace] = state
}

func resolveVectorNamespace(graph *store.GraphState, requested *VectorNamespace) (*VectorNamespace, error) {
	if requested == nil {
		return nil, nil
	}
	if graph == nil {
		return nil, fmt.Errorf("%w: vector namespace is unavailable", ErrUnsupportedOption)
	}
	namespace, err := normalizeVectorNamespace(*requested, graph.VectorDimensions)
	if err != nil {
		return nil, err
	}
	if _, ok := graph.VectorNamespaces[namespace]; !ok {
		return nil, fmt.Errorf("%w: vector namespace is not configured", ErrUnsupportedOption)
	}
	return &namespace, nil
}

func selectedNamespaceVector(graph *store.GraphState, node *store.NodeRecord) ([]float32, bool) {
	if graph == nil || node == nil || graph.VectorNamespace == nil {
		return nil, false
	}
	if graph.VectorNamespace.Scope != "" && !slices.Contains(node.Labels, graph.VectorNamespace.Scope) {
		return nil, false
	}
	vector, ok := node.Properties[graph.VectorNamespace.Property].([]float32)
	return vector, ok && len(vector) == int(graph.VectorNamespace.Dimensions)
}

func sameVectorNamespace(a, b *VectorNamespace) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func cloneVectorNamespace(namespace *VectorNamespace) *VectorNamespace {
	if namespace == nil {
		return nil
	}
	copy := *namespace
	return &copy
}

func retainedVectorIndexBytes(graph *store.GraphState, target *VectorNamespace) uint64 {
	var bytes uint64
	if target != nil {
		bytes = saturatingAdd(bytes, estimateVectorIndexBytes(uint64(graph.VectorIndex.Nodes.Len()), graph.VectorDimensions))
		bytes = saturatingAdd(bytes, saturatingMul(uint64(graph.VectorTombstones.Len()), uint64(graph.VectorDimensions)*4))
	}
	for namespace, state := range graph.VectorNamespaces {
		if target != nil && namespace == *target {
			continue
		}
		bytes = saturatingAdd(bytes, estimateVectorIndexBytes(uint64(state.Index.Nodes.Len()), namespace.Dimensions))
		bytes = saturatingAdd(bytes, saturatingMul(uint64(state.Tombstones.Len()), uint64(namespace.Dimensions)*4))
	}
	return bytes
}

func sortedVectorNamespaces(states map[VectorNamespace]VectorNamespaceState) []VectorNamespace {
	namespaces := make([]VectorNamespace, 0, len(states))
	for namespace := range states {
		namespaces = append(namespaces, namespace)
	}
	slices.SortFunc(namespaces, compareVectorNamespace)
	return namespaces
}
