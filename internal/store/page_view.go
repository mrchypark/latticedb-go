package store

import (
	"context"
	"encoding/binary"
	"errors"
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

// VisitNode admits disk allocations for the callback lifetime. Overlay records
// are already resident and are not charged as newly decoded page storage.
func (graph *GraphState) VisitNode(ctx context.Context, id uint64, visit func(*NodeRecord) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if graph.DeletedNodes.Get(id) {
		return visit(nil)
	}
	if value := graph.Nodes.Get(id); value != nil {
		return visit(value)
	}
	if graph.PageBase != nil {
		return graph.PageBase.VisitNode(ctx, id, visit)
	}
	return visit(nil)
}
func (graph *GraphState) VisitEdge(ctx context.Context, id uint64, visit func(*EdgeRecord) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if graph.DeletedEdges.Get(id) {
		return visit(nil)
	}
	if value := graph.Edges.Get(id); value != nil {
		return visit(value)
	}
	if graph.PageBase != nil {
		return graph.PageBase.VisitEdge(ctx, id, visit)
	}
	return visit(nil)
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
	return graph.NodeCountContext(context.Background())
}
func (graph *GraphState) NodeCountContext(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if graph.PageBase == nil {
		return uint64(graph.Nodes.Len()), nil
	}
	if graph.Nodes.Len() == 0 && graph.DeletedNodes.Len() == 0 {
		return graph.PageBase.count(pageNodes)
	}
	var count uint64
	err := graph.VisitNodeIDs(ctx, func(uint64) error { count++; return nil })
	return count, err
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
	return graph.EdgeCountContext(context.Background())
}
func (graph *GraphState) EdgeCountContext(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if graph.PageBase == nil {
		return uint64(graph.Edges.Len()), nil
	}
	if graph.Edges.Len() == 0 && graph.DeletedEdges.Len() == 0 {
		return graph.PageBase.count(pageEdges)
	}
	var count uint64
	err := graph.VisitEdgeIDs(ctx, func(uint64) error { count++; return nil })
	return count, err
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
		err := graph.PageBase.VisitNode(ctx, id, func(base *NodeRecord) error {
			if base == nil || !slices.Contains(base.Labels, label) {
				count++
			}
			return nil
		})
		if err != nil {
			return 0, err
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
		err := graph.PageBase.VisitEdge(ctx, id, func(base *EdgeRecord) error {
			if base == nil || base.Type != kind {
				count++
			}
			return nil
		})
		if err != nil {
			return 0, err
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
		err := graph.PageBase.VisitEdge(ctx, id, func(base *EdgeRecord) error {
			if base == nil || base.SourceID != nodeID {
				count++
			}
			return nil
		})
		if err != nil {
			return 0, err
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
		err := graph.PageBase.VisitEdge(ctx, id, func(base *EdgeRecord) error {
			if base == nil || base.TargetID != nodeID {
				count++
			}
			return nil
		})
		if err != nil {
			return 0, err
		}
	}
	return count, nil
}

func (graph *GraphState) ReadNodeOwned(ctx context.Context, id uint64) (*NodeRecord, *PageReadLease, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if graph.DeletedNodes.Get(id) {
		return nil, nil, nil
	}
	if n := graph.Nodes.Get(id); n != nil {
		return n, nil, nil
	}
	if graph.PageBase != nil {
		return graph.PageBase.ReadNodeOwned(ctx, id)
	}
	return nil, nil, nil
}
func (graph *GraphState) ReadEdgeOwned(ctx context.Context, id uint64) (*EdgeRecord, *PageReadLease, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if graph.DeletedEdges.Get(id) {
		return nil, nil, nil
	}
	if e := graph.Edges.Get(id); e != nil {
		return e, nil, nil
	}
	if graph.PageBase != nil {
		return graph.PageBase.ReadEdgeOwned(ctx, id)
	}
	return nil, nil, nil
}

// Merge keys before loading canonical values. Shadowed or deleted page records
// must not consume a query's source allowance when only the overlay is needed.
func visitGraphIDs[V any](ctx context.Context, page *PageGraph, bucket string, overlay *PagedMap[V], deleted *PagedMap[bool], visit func(uint64) error) error {
	next, stop := iter.Pull2(overlay.Ordered())
	defer stop()
	id, _, ok := next()
	emit := func(id uint64) error {
		if deleted.Get(id) {
			return nil
		}
		return visit(id)
	}
	if page != nil {
		budget := pageReadBudgetFromContext(ctx)
		charge := func(size uint64) error {
			if budget != nil {
				return budget.ReservePageRead(1, 2*size)
			}
			return nil
		}
		stopped := false
		err := page.Tx.ScanKeysWithCharge(ctx, bucket, nil, nil, 8, charge, func(key []byte) error {
			if budget != nil {
				defer budget.ReleasePageRead(uint64(2 * len(key)))
			}
			if len(key) != 8 {
				return errors.New("invalid page record key")
			}
			baseID := binary.BigEndian.Uint64(key)
			for ok && id < baseID {
				if err := emit(id); err != nil {
					stopped = err == io.EOF
					return err
				}
				id, _, ok = next()
			}
			if ok && id == baseID {
				id, _, ok = next()
			}
			err := emit(baseID)
			stopped = err == io.EOF
			return err
		})
		if err != nil {
			return err
		}
		if stopped {
			return nil
		}
	}
	for ok {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := emit(id); err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		id, _, ok = next()
	}
	return ctx.Err()
}
func (graph *GraphState) VisitNodeIDs(ctx context.Context, visit func(uint64) error) error {
	return visitGraphIDs(ctx, graph.PageBase, pageNodes, &graph.Nodes, &graph.DeletedNodes, visit)
}
func (graph *GraphState) VisitEdgeIDs(ctx context.Context, visit func(uint64) error) error {
	return visitGraphIDs(ctx, graph.PageBase, pageEdges, &graph.Edges, &graph.DeletedEdges, visit)
}
