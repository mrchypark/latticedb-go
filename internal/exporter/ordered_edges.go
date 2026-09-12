package exporter

import (
	"container/heap"
	"context"
	"slices"

	"github.com/mrchypark/latticedb-go/internal/store"
)

const orderedEdgeBatchSize = 16384

type orderedEdgeMaxHeap []*store.EdgeRecord

func (h orderedEdgeMaxHeap) Len() int { return len(h) }
func (h orderedEdgeMaxHeap) Less(i, j int) bool {
	return compareCanonicalEdges(h[i], h[j]) > 0
}
func (h orderedEdgeMaxHeap) Swap(i, j int)   { h[i], h[j] = h[j], h[i] }
func (h *orderedEdgeMaxHeap) Push(value any) { *h = append(*h, value.(*store.EdgeRecord)) }
func (h *orderedEdgeMaxHeap) Pop() any {
	old := *h
	value := old[len(old)-1]
	*h = old[:len(old)-1]
	return value
}

func compareCanonicalEdges(left, right *store.EdgeRecord) int {
	switch {
	case left.SourceID < right.SourceID:
		return -1
	case left.SourceID > right.SourceID:
		return 1
	case left.TargetID < right.TargetID:
		return -1
	case left.TargetID > right.TargetID:
		return 1
	case left.Type < right.Type:
		return -1
	case left.Type > right.Type:
		return 1
	case left.ID < right.ID:
		return -1
	case left.ID > right.ID:
		return 1
	default:
		return 0
	}
}

// ponytail: rescanning edges in 16384-entry batches bounds sorting storage;
// O(E*ceil(E/16384)) candidate scans trade throughput for memory. Add a
// canonical index if export throughput makes this ceiling material.
func forEachCanonicalEdge(ctx context.Context, graph *store.GraphState, visit func(*store.EdgeRecord) error) error {
	selected := make(orderedEdgeMaxHeap, 0, min(orderedEdgeBatchSize, graph.Edges.Len()))
	var last *store.EdgeRecord
	for {
		selected = selected[:0]
		for _, edge := range graph.Edges.All() {
			if err := ctx.Err(); err != nil {
				return err
			}
			if last != nil && compareCanonicalEdges(edge, last) <= 0 {
				continue
			}
			if len(selected) < orderedEdgeBatchSize {
				selected = append(selected, edge)
				if len(selected) == orderedEdgeBatchSize {
					heap.Init(&selected)
				}
				continue
			}
			if compareCanonicalEdges(edge, selected[0]) < 0 {
				selected[0] = edge
				heap.Fix(&selected, 0)
			}
		}
		if len(selected) == 0 {
			return nil
		}
		slices.SortFunc(selected, compareCanonicalEdges)
		for _, edge := range selected {
			if err := visit(edge); err != nil {
				return err
			}
			last = edge
		}
	}
}
