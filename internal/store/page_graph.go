package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/mrchypark/latticedb-go/internal/pagestore"
)

const (
	pageNodes     = "nodes"
	pageEdges     = "edges"
	pageOutgoing  = "outgoing"
	pageIncoming  = "incoming"
	pageLabels    = "labels"
	pageEdgeTypes = "edge-types"
)

// PageGraph is scoped to one storage transaction. It never caches the graph;
// returned records own their data and scans decode one record at a time.
type PageGraph struct {
	Tx             *pagestore.Tx
	MaxRecordBytes uint64
}

func pageID(id uint64) []byte { var key [8]byte; binary.BigEndian.PutUint64(key[:], id); return key[:] }
func pagePair(first, second uint64) []byte {
	key := make([]byte, 16)
	binary.BigEndian.PutUint64(key, first)
	binary.BigEndian.PutUint64(key[8:], second)
	return key
}
func pageStringPrefix(value string) []byte {
	key := make([]byte, 4+len(value))
	binary.BigEndian.PutUint32(key, uint32(len(value)))
	copy(key[4:], value)
	return key
}
func pageStringID(value string, id uint64) []byte {
	return append(pageStringPrefix(value), pageID(id)...)
}
func pagePrefixEnd(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 255 {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

func (graph *PageGraph) recordLimit() uint64 {
	if graph.MaxRecordBytes != 0 {
		return graph.MaxRecordBytes
	}
	return maxValueBytes + 1<<20
}

func (graph *PageGraph) GetNode(id uint64) (*NodeRecord, error) {
	data, err := graph.Tx.Get(pageNodes, pageID(id))
	if err != nil || data == nil {
		return nil, err
	}
	return decodePageNode(data, id, graph.recordLimit())
}
func (graph *PageGraph) GetEdge(id uint64) (*EdgeRecord, error) {
	data, err := graph.Tx.Get(pageEdges, pageID(id))
	if err != nil || data == nil {
		return nil, err
	}
	return decodePageEdge(data, id, graph.recordLimit())
}
func (graph *PageGraph) VisitNodes(ctx context.Context, visit func(*NodeRecord) error) error {
	return graph.Tx.Scan(ctx, pageNodes, nil, nil, func(key, value []byte) error {
		if len(key) != 8 {
			return errors.New("invalid page node key")
		}
		node, err := decodePageNode(value, binary.BigEndian.Uint64(key), graph.recordLimit())
		if err != nil {
			return err
		}
		return visit(node)
	})
}
func (graph *PageGraph) VisitEdges(ctx context.Context, visit func(*EdgeRecord) error) error {
	return graph.Tx.Scan(ctx, pageEdges, nil, nil, func(key, value []byte) error {
		if len(key) != 8 {
			return errors.New("invalid page edge key")
		}
		edge, err := decodePageEdge(value, binary.BigEndian.Uint64(key), graph.recordLimit())
		if err != nil {
			return err
		}
		return visit(edge)
	})
}
func (graph *PageGraph) visitIDs(ctx context.Context, bucket string, prefix []byte, visit func(uint64) error) error {
	return graph.Tx.Scan(ctx, bucket, prefix, pagePrefixEnd(prefix), func(key, value []byte) error {
		if len(key) != len(prefix)+8 {
			return fmt.Errorf("invalid %s posting key", bucket)
		}
		id := binary.BigEndian.Uint64(key[len(prefix):])
		if err := ValidateEntityID(id); err != nil {
			return err
		}
		return visit(id)
	})
}
func (graph *PageGraph) VisitLabel(ctx context.Context, label string, visit func(uint64) error) error {
	return graph.visitIDs(ctx, pageLabels, pageStringPrefix(label), visit)
}
func (graph *PageGraph) VisitEdgeType(ctx context.Context, kind string, visit func(uint64) error) error {
	return graph.visitIDs(ctx, pageEdgeTypes, pageStringPrefix(kind), visit)
}
func (graph *PageGraph) VisitOutgoing(ctx context.Context, id uint64, visit func(uint64) error) error {
	return graph.visitIDs(ctx, pageOutgoing, pageID(id), visit)
}
func (graph *PageGraph) VisitIncoming(ctx context.Context, id uint64, visit func(uint64) error) error {
	return graph.visitIDs(ctx, pageIncoming, pageID(id), visit)
}

// PutNode and PutEdge update their lookup entries in the same physical
// transaction. A caller must roll back the transaction after a write error.
func (graph *PageGraph) PutNode(node *NodeRecord) error {
	data, err := encodePageNode(node)
	if err != nil {
		return err
	}
	if uint64(len(data)) > graph.recordLimit() {
		return fmt.Errorf("%w: page node exceeds limit", ErrLoadResourceLimit)
	}
	old, err := graph.GetNode(node.ID)
	if err != nil {
		return err
	}
	if old != nil {
		for _, label := range old.Labels {
			if err := graph.Tx.Delete(pageLabels, pageStringID(label, node.ID)); err != nil {
				return err
			}
		}
	}
	if err := graph.UpdateNodePropertyIndexes(old, node); err != nil {
		return err
	}
	if old == nil {
		if err := graph.changeCount(pageNodes, true); err != nil {
			return err
		}
	}
	if err := graph.Tx.Put(pageNodes, pageID(node.ID), data); err != nil {
		return err
	}
	for _, label := range node.Labels {
		if err := graph.Tx.Put(pageLabels, pageStringID(label, node.ID), []byte{}); err != nil {
			return err
		}
	}
	return nil
}
func (graph *PageGraph) PutEdge(edge *EdgeRecord) error {
	data, err := encodePageEdge(edge)
	if err != nil {
		return err
	}
	if uint64(len(data)) > graph.recordLimit() {
		return fmt.Errorf("%w: page edge exceeds limit", ErrLoadResourceLimit)
	}
	for _, id := range []uint64{edge.SourceID, edge.TargetID} {
		node, err := graph.GetNode(id)
		if err != nil {
			return err
		}
		if node == nil {
			return errors.New("page edge endpoint is missing")
		}
	}
	old, err := graph.GetEdge(edge.ID)
	if err != nil {
		return err
	}
	if old != nil {
		if err := graph.removeEdgePostings(old); err != nil {
			return err
		}
	}
	if err := graph.UpdateEdgePropertyIndexes(old, edge); err != nil {
		return err
	}
	if old == nil {
		if err := graph.changeCount(pageEdges, true); err != nil {
			return err
		}
	}
	if err := graph.Tx.Put(pageEdges, pageID(edge.ID), data); err != nil {
		return err
	}
	for _, item := range []struct {
		bucket string
		key    []byte
	}{{pageOutgoing, pagePair(edge.SourceID, edge.ID)}, {pageIncoming, pagePair(edge.TargetID, edge.ID)}, {pageEdgeTypes, pageStringID(edge.Type, edge.ID)}, {"edge-order", pageCanonicalEdgeKey(edge)}} {
		if err := graph.Tx.Put(item.bucket, item.key, []byte{}); err != nil {
			return err
		}
	}
	return nil
}
func (graph *PageGraph) removeEdgePostings(edge *EdgeRecord) error {
	for _, item := range []struct {
		bucket string
		key    []byte
	}{{pageOutgoing, pagePair(edge.SourceID, edge.ID)}, {pageIncoming, pagePair(edge.TargetID, edge.ID)}, {pageEdgeTypes, pageStringID(edge.Type, edge.ID)}, {"edge-order", pageCanonicalEdgeKey(edge)}} {
		if err := graph.Tx.Delete(item.bucket, item.key); err != nil {
			return err
		}
	}
	return nil
}
func (graph *PageGraph) DeleteEdge(id uint64) error {
	edge, err := graph.GetEdge(id)
	if err != nil || edge == nil {
		return err
	}
	if err := graph.UpdateEdgePropertyIndexes(edge, nil); err != nil {
		return err
	}
	if err := graph.removeEdgePostings(edge); err != nil {
		return err
	}
	if err := graph.changeCount(pageEdges, false); err != nil {
		return err
	}
	return graph.Tx.Delete(pageEdges, pageID(id))
}
func (graph *PageGraph) DeleteNode(ctx context.Context, id uint64) error {
	node, err := graph.GetNode(id)
	if err != nil || node == nil {
		return err
	}
	// Scan a bounded batch before mutation: bbolt cursor advancement across
	// deletion is not used to decide which relationships remain.
	for _, bucket := range []string{pageOutgoing, pageIncoming} {
		for {
			ids := make([]uint64, 0, 128)
			err := graph.visitIDs(ctx, bucket, pageID(id), func(edgeID uint64) error {
				ids = append(ids, edgeID)
				if len(ids) == cap(ids) {
					return io.EOF
				}
				return nil
			})
			if err != nil {
				return err
			}
			if len(ids) == 0 {
				break
			}
			for _, edgeID := range ids {
				edge, err := graph.GetEdge(edgeID)
				if err != nil {
					return err
				}
				if edge == nil {
					return errors.New("page adjacency references missing edge")
				}
				if err := graph.DeleteEdge(edgeID); err != nil {
					return err
				}
			}
		}
	}
	for _, label := range node.Labels {
		if err := graph.Tx.Delete(pageLabels, pageStringID(label, id)); err != nil {
			return err
		}
	}
	if err := graph.UpdateNodePropertyIndexes(node, nil); err != nil {
		return err
	}
	if err := graph.PutFTS(id, nil); err != nil {
		return err
	}
	if err := graph.changeCount(pageNodes, false); err != nil {
		return err
	}
	return graph.Tx.Delete(pageNodes, pageID(id))
}

func (graph *PageGraph) count(bucket string) (uint64, error) {
	data, err := graph.Tx.Get("counts", []byte(bucket))
	if err != nil {
		return 0, err
	}
	if data == nil {
		return 0, nil
	}
	if len(data) != 8 {
		return 0, errors.New("invalid page count")
	}
	return binary.BigEndian.Uint64(data), nil
}
func (graph *PageGraph) changeCount(bucket string, add bool) error {
	count, err := graph.count(bucket)
	if err != nil {
		return err
	}
	if add {
		if count == ^uint64(0) {
			return errors.New("page count overflow")
		}
		count++
	} else {
		if count == 0 {
			return errors.New("page count underflow")
		}
		count--
	}
	return graph.Tx.Put("counts", []byte(bucket), pageID(count))
}

func pageCanonicalEdgeKey(edge *EdgeRecord) []byte {
	key := pagePair(edge.SourceID, edge.TargetID)
	for _, value := range []byte(edge.Type) {
		key = append(key, value)
		if value == 0 {
			key = append(key, 1)
		}
	}
	key = append(key, 0, 0)
	return append(key, pageID(edge.ID)...)
}

// VisitCanonicalEdges uses the maintained export order instead of retaining
// decoded relationship payloads for an in-memory sort.
func (graph *PageGraph) VisitCanonicalEdges(ctx context.Context, visit func(*EdgeRecord) error) error {
	return graph.Tx.Scan(ctx, "edge-order", nil, nil, func(key, value []byte) error {
		if len(key) < 26 {
			return errors.New("invalid canonical edge key")
		}
		id := binary.BigEndian.Uint64(key[len(key)-8:])
		edge, err := graph.GetEdge(id)
		if err != nil {
			return err
		}
		if edge == nil || !bytes.Equal(key, pageCanonicalEdgeKey(edge)) {
			return errors.New("canonical edge index disagrees with record")
		}
		return visit(edge)
	})
}
