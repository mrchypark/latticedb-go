package engine

import (
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/mrchypark/latticedb-go/internal/search"
	"github.com/mrchypark/latticedb-go/internal/store"
)

// searchVectorGraph searches an already namespace-selected graph. The caller
// owns option namespace resolution and budget construction.
func searchVectorGraph(graph *store.GraphState, vector []float32, opts VectorSearchOptions, budget *directSearchBudget, disableIndex bool) ([]VectorSearchResult, bool, error) {
	if graph == nil {
		return nil, false, errors.New("vector search requires a graph")
	}
	if budget == nil {
		return nil, false, errors.New("vector search requires a budget")
	}
	if graph.VectorDimensions > 0 && len(vector) != int(graph.VectorDimensions) {
		return nil, false, fmt.Errorf("vector length %d does not match configured dimensions %d", len(vector), graph.VectorDimensions)
	}
	for _, value := range vector {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return nil, false, errors.New("vector contains non-finite value")
		}
	}

	limit := uint64(opts.K)
	if limit == 0 {
		limit = 10
	}
	queryVector := vector
	capacity := limit
	if !opts.Exact && !disableIndex && graph.VectorIndex.Nodes.Len() > 0 {
		capacity = min(limit, graph.VectorLiveCount)
	} else if nodeCount := uint64(graph.Nodes.Len()); capacity > nodeCount {
		capacity = nodeCount
	}
	results := make([]VectorSearchResult, 0, int(capacity))
	exactFallbackUsed := false
	if !opts.Exact && !disableIndex && graph.VectorIndex.Nodes.Len() > 0 {
		entry := graph.VectorIndex.EntryID
		for level := graph.VectorIndex.MaxLevel; level > 0; level-- {
			var err error
			entry, err = vectorGreedyBudget(graph, queryVector, entry, level, budget)
			if err != nil {
				return nil, false, err
			}
		}
		ef := int(opts.EfSearch)
		if ef == 0 {
			ef = vectorIndexSearchEF
		}
		ef = max(ef, int(capacity))
		maxVisited := uint64(graph.VectorIndex.Nodes.Len())
		if byWork := budget.maxWork/uint64(max(1, len(queryVector))) + 1; maxVisited > byWork {
			maxVisited = byWork
		}
		// Visited entries are charged as they are discovered so small-Ef searches are
		// not rejected solely because the index is large.
		scratchBytes := saturatingAdd(256, saturatingMul(uint64(ef), 32))
		if err := budget.reserveBytes(scratchBytes); err != nil {
			return nil, false, err
		}
		budget.annVisitedLimit = (budget.maxBytes - budget.bytes) / 80
		if budget.annVisitedLimit == 0 {
			return nil, false, fmt.Errorf("%w: search memory exceeds budget", ErrResourceLimit)
		}
		annBytesBefore := budget.bytes
		scratch := acquireVectorSearchScratch(int(maxVisited), int(maxVisited), ef*2)
		candidates, searchErr := vectorSearchLayerBudget(graph, queryVector, entry, 0, ef, 0, scratch, budget)
		if searchErr != nil {
			releaseVectorSearchScratch(scratch)
			return nil, false, searchErr
		}
		budget.bytes += uint64(len(scratch.visited)) * 80
		for _, candidate := range candidates {
			results = pushVectorResult(results, VectorSearchResult{NodeID: candidate.id, Distance: float32(math.Sqrt(candidate.distance))}, int(capacity))
		}
		releaseVectorSearchScratch(scratch)
		if len(results) == int(capacity) {
			if err := budget.check(); err != nil {
				return nil, false, err
			}
			slices.SortFunc(results, compareVectorResult)
			return results, false, nil
		}
		budget.releaseBytes(budget.bytes - annBytesBefore + scratchBytes)
		// Disconnected or degenerate ANN graphs fall back to exact search to honor K.
		exactFallbackUsed = true
		results = results[:0]
	}
	for _, node := range graph.Nodes.All() {
		vectorValue, ok := selectedVector(graph, node)
		if !ok {
			if err := budget.add(1); err != nil {
				return nil, false, err
			}
			continue
		}
		if err := budget.add(uint64(len(queryVector))); err != nil {
			return nil, false, err
		}
		var distance float32
		var err error
		if len(queryVector) < 256 {
			distance, err = search.VectorDistance(vectorValue, queryVector)
		} else {
			distance, err = search.VectorDistanceContext(budget.ctx, vectorValue, queryVector)
		}
		if err != nil {
			return nil, false, err
		}
		results = pushVectorResult(results, VectorSearchResult{NodeID: node.ID, Distance: distance}, int(capacity))
	}
	if err := budget.check(); err != nil {
		return nil, false, err
	}
	slices.SortFunc(results, compareVectorResult)
	return results, exactFallbackUsed, nil
}
