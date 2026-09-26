package store

import (
	"context"
	"io"
	"iter"
	"slices"
)

// visitOverlay merges a bounded transaction write set with a disk cursor.
// Only visitor EOF is an early stop; storage errors retain their identity.
func visitOverlay[V any](ctx context.Context, overlay *PagedMap[V], deleted *PagedMap[bool], base func(func(uint64, V) error) error, visit func(V) error) error {
	next, stop := iter.Pull2(overlay.Ordered())
	defer stop()
	id, value, ok := next()
	stopped := false
	emit := func(v V) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := visit(v)
		if err == io.EOF {
			stopped = true
		}
		return err
	}
	if base != nil {
		err := base(func(baseID uint64, baseValue V) error {
			for ok && id < baseID {
				if !deleted.Get(id) {
					if err := emit(value); err != nil {
						return err
					}
				}
				id, value, ok = next()
			}
			if ok && id == baseID {
				baseValue = value
				id, value, ok = next()
			}
			if deleted.Get(baseID) {
				return nil
			}
			return emit(baseValue)
		})
		if err != nil {
			if stopped && err == io.EOF {
				return nil
			}
			return err
		}
		if stopped {
			return nil
		}
	}
	for ok {
		if !deleted.Get(id) {
			if err := emit(value); err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}
		}
		id, value, ok = next()
	}
	return ctx.Err()
}

func (graph *GraphState) ReadNode(id uint64) (*NodeRecord, error) {
	if graph.DeletedNodes.Get(id) {
		return nil, nil
	}
	if value := graph.Nodes.Get(id); value != nil {
		return value, nil
	}
	if graph.PageBase != nil {
		return graph.PageBase.GetNode(id)
	}
	return nil, nil
}
func (graph *GraphState) VisitNodes(ctx context.Context, visit func(*NodeRecord) error) error {
	var base func(func(uint64, *NodeRecord) error) error
	if graph.PageBase != nil {
		base = func(fn func(uint64, *NodeRecord) error) error {
			return graph.PageBase.VisitNodes(ctx, func(record *NodeRecord) error { return fn(record.ID, record) })
		}
	}
	return visitOverlay(ctx, &graph.Nodes, &graph.DeletedNodes, base, visit)
}
func (graph *GraphState) NodeCount() (uint64, error) {
	if graph.PageBase == nil {
		return uint64(graph.Nodes.Len()), nil
	}
	count, err := graph.PageBase.count(pageNodes)
	if err != nil {
		return 0, err
	}
	for id, deleted := range graph.DeletedNodes.All() {
		if !deleted {
			continue
		}
		record, err := graph.PageBase.GetNode(id)
		if err != nil {
			return 0, err
		}
		if record != nil {
			count--
		}
	}
	for id := range graph.Nodes.All() {
		if graph.DeletedNodes.Get(id) {
			continue
		}
		record, err := graph.PageBase.GetNode(id)
		if err != nil {
			return 0, err
		}
		if record == nil {
			count++
		}
	}
	return count, nil
}

func (graph *GraphState) ReadEdge(id uint64) (*EdgeRecord, error) {
	if graph.DeletedEdges.Get(id) {
		return nil, nil
	}
	if value := graph.Edges.Get(id); value != nil {
		return value, nil
	}
	if graph.PageBase != nil {
		return graph.PageBase.GetEdge(id)
	}
	return nil, nil
}
func (graph *GraphState) VisitEdges(ctx context.Context, visit func(*EdgeRecord) error) error {
	var base func(func(uint64, *EdgeRecord) error) error
	if graph.PageBase != nil {
		base = func(fn func(uint64, *EdgeRecord) error) error {
			return graph.PageBase.VisitEdges(ctx, func(record *EdgeRecord) error { return fn(record.ID, record) })
		}
	}
	return visitOverlay(ctx, &graph.Edges, &graph.DeletedEdges, base, visit)
}
func (graph *GraphState) EdgeCount() (uint64, error) {
	if graph.PageBase == nil {
		return uint64(graph.Edges.Len()), nil
	}
	count, err := graph.PageBase.count(pageEdges)
	if err != nil {
		return 0, err
	}
	for id, deleted := range graph.DeletedEdges.All() {
		if !deleted {
			continue
		}
		record, err := graph.PageBase.GetEdge(id)
		if err != nil {
			return 0, err
		}
		if record != nil {
			count--
		}
	}
	for id := range graph.Edges.All() {
		if graph.DeletedEdges.Get(id) {
			continue
		}
		record, err := graph.PageBase.GetEdge(id)
		if err != nil {
			return 0, err
		}
		if record == nil {
			count++
		}
	}
	return count, nil
}

func (graph *GraphState) VisitLabel(ctx context.Context, label string, visit func(uint64) error) error {
	if graph.PageBase == nil {
		for id := range graph.Labels.All(label) {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := visit(id); err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}
		}
		return nil
	}
	var selected PagedMap[uint64]
	for id, record := range graph.Nodes.All() {
		if slices.Contains(record.Labels, label) {
			selected.Set(id, id)
		}
	}
	var base func(func(uint64, uint64) error) error
	if graph.PageBase != nil {
		base = func(fn func(uint64, uint64) error) error {
			return graph.PageBase.VisitLabel(ctx, label, func(id uint64) error {
				if record := graph.Nodes.Get(id); record != nil && !(slices.Contains(record.Labels, label)) {
					return nil
				}
				return fn(id, id)
			})
		}
	}
	return visitOverlay(ctx, &selected, &graph.DeletedNodes, base, visit)
}
func (graph *GraphState) LabelCount(label string) (uint64, error) {
	return graph.LabelCountContext(context.Background(), label, nil)
}

// LabelCountContext counts a label posting with cancellation and optional
// bounded-work charging. Resident maps answer in O(1); page-backed maps stream.
func (graph *GraphState) LabelCountContext(ctx context.Context, label string, charge func() error) (uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if graph.PageBase == nil {
		return uint64(graph.Labels.Len(label)), nil
	}
	var count uint64
	err := graph.PageBase.VisitLabel(ctx, label, func(id uint64) error {
		if charge != nil {
			if err := charge(); err != nil {
				return err
			}
		}
		if graph.DeletedNodes.Get(id) {
			return nil
		}
		if record := graph.Nodes.Get(id); record != nil && !slices.Contains(record.Labels, label) {
			return nil
		}
		count++
		return nil
	})
	if err != nil {
		return 0, err
	}
	for id, record := range graph.Nodes.Ordered() {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if charge != nil {
			if err := charge(); err != nil {
				return 0, err
			}
		}
		if graph.DeletedNodes.Get(id) || !slices.Contains(record.Labels, label) {
			continue
		}
		base, err := graph.PageBase.GetNode(id)
		if err != nil {
			return 0, err
		}
		if base == nil || !slices.Contains(base.Labels, label) {
			count++
		}
	}
	return count, err
}

func (graph *GraphState) VisitEdgeType(ctx context.Context, kind string, visit func(uint64) error) error {
	if graph.PageBase == nil {
		for id := range graph.EdgeTypes.All(kind) {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := visit(id); err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}
		}
		return nil
	}
	var selected PagedMap[uint64]
	for id, record := range graph.Edges.All() {
		if record.Type == kind {
			selected.Set(id, id)
		}
	}
	var base func(func(uint64, uint64) error) error
	if graph.PageBase != nil {
		base = func(fn func(uint64, uint64) error) error {
			return graph.PageBase.VisitEdgeType(ctx, kind, func(id uint64) error {
				if record := graph.Edges.Get(id); record != nil && !(record.Type == kind) {
					return nil
				}
				return fn(id, id)
			})
		}
	}
	return visitOverlay(ctx, &selected, &graph.DeletedEdges, base, visit)
}
func (graph *GraphState) EdgeTypeCount(kind string) (uint64, error) {
	return graph.EdgeTypeCountContext(context.Background(), kind, nil)
}

// EdgeTypeCountContext counts an edge-type posting with cancellation and
// optional bounded-work charging. Resident maps answer in O(1); page-backed
// maps stream their posting list.
func (graph *GraphState) EdgeTypeCountContext(ctx context.Context, kind string, charge func() error) (uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if graph.PageBase == nil {
		return uint64(graph.EdgeTypes.Len(kind)), nil
	}
	var count uint64
	err := graph.PageBase.VisitEdgeType(ctx, kind, func(id uint64) error {
		if charge != nil {
			if err := charge(); err != nil {
				return err
			}
		}
		if graph.DeletedEdges.Get(id) {
			return nil
		}
		if record := graph.Edges.Get(id); record != nil && record.Type != kind {
			return nil
		}
		count++
		return nil
	})
	if err != nil {
		return 0, err
	}
	for id, record := range graph.Edges.Ordered() {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if charge != nil {
			if err := charge(); err != nil {
				return 0, err
			}
		}
		if graph.DeletedEdges.Get(id) || record.Type != kind {
			continue
		}
		base, err := graph.PageBase.GetEdge(id)
		if err != nil {
			return 0, err
		}
		if base == nil || base.Type != kind {
			count++
		}
	}
	return count, err
}

func (graph *GraphState) VisitOutgoing(ctx context.Context, nodeID uint64, visit func(uint64) error) error {
	if graph.PageBase == nil {
		list := graph.Outgoing.Get(nodeID)
		for chunk := range list.Chunks() {
			for _, id := range chunk {
				if err := ctx.Err(); err != nil {
					return err
				}
				if list.IsRemoved(id) {
					continue
				}
				if err := visit(id); err != nil {
					if err == io.EOF {
						return nil
					}
					return err
				}
			}
		}
		return nil
	}
	var selected PagedMap[uint64]
	for id, record := range graph.Edges.All() {
		if record.SourceID == nodeID {
			selected.Set(id, id)
		}
	}
	var base func(func(uint64, uint64) error) error
	if graph.PageBase != nil {
		base = func(fn func(uint64, uint64) error) error {
			return graph.PageBase.VisitOutgoing(ctx, nodeID, func(id uint64) error {
				if record := graph.Edges.Get(id); record != nil && !(record.SourceID == nodeID) {
					return nil
				}
				return fn(id, id)
			})
		}
	}
	return visitOverlay(ctx, &selected, &graph.DeletedEdges, base, visit)
}
func (graph *GraphState) OutgoingCount(nodeID uint64) (uint64, error) {
	return graph.OutgoingCountContext(context.Background(), nodeID, nil)
}

// OutgoingCountContext streams the adjacency posting with cancellation and
// optional per-posting work charging.
func (graph *GraphState) OutgoingCountContext(ctx context.Context, nodeID uint64, charge func() error) (uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var count uint64
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if graph.PageBase == nil {
		list := graph.Outgoing.Get(nodeID)
		for chunk := range list.Chunks() {
			for _, id := range chunk {
				if err := ctx.Err(); err != nil {
					return 0, err
				}
				if charge != nil {
					if err := charge(); err != nil {
						return 0, err
					}
				}
				if !list.IsRemoved(id) {
					count++
				}
			}
		}
		return count, nil
	}
	err := graph.PageBase.VisitOutgoing(ctx, nodeID, func(id uint64) error {
		if charge != nil {
			if err := charge(); err != nil {
				return err
			}
		}
		if graph.DeletedEdges.Get(id) {
			return nil
		}
		if edge := graph.Edges.Get(id); edge != nil && edge.SourceID != nodeID {
			return nil
		}
		count++
		return nil
	})
	if err != nil {
		return 0, err
	}
	for id, edge := range graph.Edges.Ordered() {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if charge != nil {
			if err := charge(); err != nil {
				return 0, err
			}
		}
		if graph.DeletedEdges.Get(id) || edge.SourceID != nodeID {
			continue
		}
		base, err := graph.PageBase.GetEdge(id)
		if err != nil {
			return 0, err
		}
		if base == nil || base.SourceID != nodeID {
			count++
		}
	}
	return count, nil
}

func (graph *GraphState) VisitIncoming(ctx context.Context, nodeID uint64, visit func(uint64) error) error {
	if graph.PageBase == nil {
		list := graph.Incoming.Get(nodeID)
		for chunk := range list.Chunks() {
			for _, id := range chunk {
				if err := ctx.Err(); err != nil {
					return err
				}
				if list.IsRemoved(id) {
					continue
				}
				if err := visit(id); err != nil {
					if err == io.EOF {
						return nil
					}
					return err
				}
			}
		}
		return nil
	}
	var selected PagedMap[uint64]
	for id, record := range graph.Edges.All() {
		if record.TargetID == nodeID {
			selected.Set(id, id)
		}
	}
	var base func(func(uint64, uint64) error) error
	if graph.PageBase != nil {
		base = func(fn func(uint64, uint64) error) error {
			return graph.PageBase.VisitIncoming(ctx, nodeID, func(id uint64) error {
				if record := graph.Edges.Get(id); record != nil && !(record.TargetID == nodeID) {
					return nil
				}
				return fn(id, id)
			})
		}
	}
	return visitOverlay(ctx, &selected, &graph.DeletedEdges, base, visit)
}
func (graph *GraphState) IncomingCount(nodeID uint64) (uint64, error) {
	return graph.IncomingCountContext(context.Background(), nodeID, nil)
}

// IncomingCountContext streams the adjacency posting with cancellation and
// optional per-posting work charging.
func (graph *GraphState) IncomingCountContext(ctx context.Context, nodeID uint64, charge func() error) (uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var count uint64
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if graph.PageBase == nil {
		list := graph.Incoming.Get(nodeID)
		for chunk := range list.Chunks() {
			for _, id := range chunk {
				if err := ctx.Err(); err != nil {
					return 0, err
				}
				if charge != nil {
					if err := charge(); err != nil {
						return 0, err
					}
				}
				if !list.IsRemoved(id) {
					count++
				}
			}
		}
		return count, nil
	}
	err := graph.PageBase.VisitIncoming(ctx, nodeID, func(id uint64) error {
		if charge != nil {
			if err := charge(); err != nil {
				return err
			}
		}
		if graph.DeletedEdges.Get(id) {
			return nil
		}
		if edge := graph.Edges.Get(id); edge != nil && edge.TargetID != nodeID {
			return nil
		}
		count++
		return nil
	})
	if err != nil {
		return 0, err
	}
	for id, edge := range graph.Edges.Ordered() {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if charge != nil {
			if err := charge(); err != nil {
				return 0, err
			}
		}
		if graph.DeletedEdges.Get(id) || edge.TargetID != nodeID {
			continue
		}
		base, err := graph.PageBase.GetEdge(id)
		if err != nil {
			return 0, err
		}
		if base == nil || base.TargetID != nodeID {
			count++
		}
	}
	return count, nil
}
