package engine

import (
	"github.com/mrchypark/latticedb-go/internal/store"
)

// Reads are immutable. The cache lives for one search or writable transaction;
// writes reach the page transaction before publishing an owned cached snapshot.
type pageVectorAccess struct {
	store.PageVectorIndex
	nodes       map[uint64]*pageVectorCachedNode
	head, tail  *pageVectorCachedNode
	budget      *directSearchBudget
	limit, used uint64
	scratch     *vectorSearchScratch
}

const pageVectorScratchBytes = 512 << 10

type pageVectorCachedNode struct {
	prev, next *pageVectorCachedNode
	id         uint64
	node       *store.PageVectorNode
	cost       uint64
}

func newPageVectorAccess(index store.PageVectorIndex, budget *directSearchBudget, building bool) *pageVectorAccess {
	a := &pageVectorAccess{PageVectorIndex: index, budget: budget}
	if budget != nil {
		share, ceiling := uint64(16), uint64(64<<20)
		if building {
			share, ceiling = 4, 256<<20
		}
		a.limit = min(ceiling, budget.RemainingPageReadBytes()/share)
		if a.limit < 4096 {
			a.limit = 0
		}
		_ = budget.reserveBytes(a.limit)
		if building && a.limit >= 4*pageVectorScratchBytes {
			a.scratch = &vectorSearchScratch{}
		}
	}
	return a
}

func (a *pageVectorAccess) close() {
	if a.budget != nil {
		a.budget.releaseBytes(a.limit)
	}
	a.reset()
	a.nodes = nil
	a.scratch = nil
	a.limit = 0
}

func (a *pageVectorAccess) Get(id uint64) (*store.PageVectorNode, error) {
	if entry := a.nodes[id]; entry != nil {
		a.moveToFront(entry)
		return entry.node, nil
	}
	n, err := a.PageVectorIndex.Get(id)
	if err != nil || n == nil || a.limit == 0 {
		return n, err
	}
	a.admit(id, n, nil)
	return n, nil
}

func (a *pageVectorAccess) admit(id uint64, n *store.PageVectorNode, entry *pageVectorCachedNode) {
	// Include map/list slots, node headers, and allocation rounding.
	cost := uint64(256 + 8*cap(n.Vector) + 48*cap(n.Neighbors))
	for _, neighbors := range n.Neighbors {
		cost += uint64(16 * cap(neighbors))
	}
	nodeLimit := a.limit / 2
	if a.scratch != nil {
		nodeLimit = (a.limit - pageVectorScratchBytes) / 2
	}
	if cost > nodeLimit {
		if entry != nil {
			a.remove(entry)
		}
		return
	}
	if entry != nil {
		a.unlink(entry)
		a.used -= entry.cost
	}
	// Keep hot upper-layer nodes when the bounded cache fills.
	for cost > nodeLimit-a.used {
		a.remove(a.tail)
	}
	if a.nodes == nil {
		a.nodes = make(map[uint64]*pageVectorCachedNode)
	}
	if entry == nil {
		entry = &pageVectorCachedNode{id: id}
		a.nodes[id] = entry
	}
	entry.node, entry.cost = n, cost
	a.pushFront(entry)
	a.used += cost
}

func (a *pageVectorAccess) remove(entry *pageVectorCachedNode) {
	if entry == nil {
		return
	}
	a.unlink(entry)
	delete(a.nodes, entry.id)
	a.used -= entry.cost
	entry.node = nil
	entry.cost = 0
}

func (a *pageVectorAccess) moveToFront(entry *pageVectorCachedNode) {
	if entry != a.head {
		a.unlink(entry)
		a.pushFront(entry)
	}
}

func (a *pageVectorAccess) pushFront(entry *pageVectorCachedNode) {
	entry.prev, entry.next = nil, a.head
	if a.head != nil {
		a.head.prev = entry
	} else {
		a.tail = entry
	}
	a.head = entry
}

func (a *pageVectorAccess) unlink(entry *pageVectorCachedNode) {
	if entry.prev != nil {
		entry.prev.next = entry.next
	} else {
		a.head = entry.next
	}
	if entry.next != nil {
		entry.next.prev = entry.prev
	} else {
		a.tail = entry.prev
	}
	entry.prev, entry.next = nil, nil
}

func (a *pageVectorAccess) Put(id uint64, node *store.PageVectorNode) error {
	var previous *store.PageVectorNode
	if entry := a.nodes[id]; entry != nil {
		previous = entry.node
	}
	unchangedVector := previous != nil && node != nil && len(node.Vector) != 0 &&
		len(previous.Vector) == len(node.Vector) && &previous.Vector[0] == &node.Vector[0]
	var written bool
	var err error
	if unchangedVector {
		written, err = a.PageVectorIndex.PutLinks(id, node)
	}
	if err == nil && !written {
		err = a.PageVectorIndex.Put(id, node)
	}
	if err != nil {
		a.remove(a.nodes[id])
		return err
	}
	if a.limit == 0 {
		a.remove(a.nodes[id])
		return nil
	}
	// Only publish writes that are certain to pass the bounded disk reader.
	readLimit := a.MaxRecordBytes
	if readLimit == 0 {
		readLimit = 1 << 20
	}
	readBytes := uint64(64 + 5*len(node.Vector) + 24*len(node.Neighbors))
	for _, neighbors := range node.Neighbors {
		readBytes += uint64(10 + 10*len(neighbors))
		for _, neighbor := range neighbors {
			if neighbor == 0 {
				a.remove(a.nodes[id])
				return nil
			}
		}
	}
	if len(node.Vector) == 0 || readBytes > readLimit/3 {
		a.remove(a.nodes[id])
		return nil
	}
	copyNode := *node
	// A shared vector from an immutable cached read needs no second copy.
	if unchangedVector {
		copyNode.Vector = previous.Vector
	} else {
		copyNode.Vector = make([]float32, len(node.Vector))
		copy(copyNode.Vector, node.Vector)
	}
	copyNode.Neighbors = make([][]uint64, len(node.Neighbors))
	for level := range copyNode.Neighbors {
		copyNode.Neighbors[level] = make([]uint64, len(node.Neighbors[level]))
		copy(copyNode.Neighbors[level], node.Neighbors[level])
	}
	a.admit(id, &copyNode, a.nodes[id])
	return nil
}

func (a *pageVectorAccess) reset() {
	clear(a.nodes)
	a.head, a.tail = nil, nil
	a.used = 0
	if a.scratch != nil {
		*a.scratch = vectorSearchScratch{}
	}
}
