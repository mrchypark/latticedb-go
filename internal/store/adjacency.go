package store

import (
	"iter"
	"slices"
)

const (
	adjacencyChunkSize   = 64
	adjacencyInlineLimit = 256
)

// EdgeList is an immutable adjacency list. Small lists stay inline; larger
// lists append by copying only their last fixed-size chunk.
type EdgeList struct {
	small   []uint64
	chunks  PagedMap[[]uint64]
	removed PagedMap[struct{}]
	count   int
	total   int
}

func (list EdgeList) Len() int { return list.count }

func (list EdgeList) IsInline() bool { return list.chunks.root0 == nil && list.chunks.roots == nil }

func (list EdgeList) InlineIDs() []uint64 { return list.small }

func (list EdgeList) HasRemovals() bool { return list.removed.Len() != 0 }

func (list EdgeList) IsRemoved(id uint64) bool { return list.removed.Has(id) }

func (list EdgeList) Chunks() iter.Seq[[]uint64] {
	return func(yield func([]uint64) bool) {
		if list.IsInline() {
			if len(list.small) != 0 {
				yield(list.small)
			}
			return
		}
		for index := 0; index*adjacencyChunkSize < list.total; index++ {
			if !yield(list.chunks.Get(uint64(index))) {
				return
			}
		}
	}
}

func (list EdgeList) Append(id uint64) EdgeList {
	if list.IsInline() && len(list.small) < adjacencyInlineLimit {
		list.small = append(slices.Clone(list.small), id)
		list.count++
		list.total++
		return list
	}
	owned := false
	if list.IsInline() {
		list.chunks = NewPagedMap[[]uint64]()
		for start := 0; start < len(list.small); start += adjacencyChunkSize {
			end := min(start+adjacencyChunkSize, len(list.small))
			list.chunks.Set(uint64(start/adjacencyChunkSize), list.small[start:end])
		}
		list.small = nil
		owned = true
	}
	chunk := uint64(list.total / adjacencyChunkSize)
	ids := list.chunks.Get(chunk)
	updated := make([]uint64, len(ids)+1)
	copy(updated, ids)
	updated[len(ids)] = id
	if owned {
		list.chunks.Set(chunk, updated)
	} else {
		list.chunks = list.chunks.ForkSet(chunk, updated)
	}
	list.count++
	list.total++
	return list
}

func (list EdgeList) Remove(id uint64) EdgeList {
	if list.IsInline() {
		for index, edgeID := range list.small {
			if edgeID == id {
				list.small = append(slices.Clone(list.small[:index]), list.small[index+1:]...)
				list.count--
				return list
			}
		}
		return list
	}
	if list.removed.Has(id) {
		return list
	}
	list.removed = list.removed.Fork()
	list.removed.CloneShardOnce(id)
	list.removed.Set(id, struct{}{})
	list.count--
	if list.removed.Len() > adjacencyChunkSize && list.removed.Len() > list.count {
		list = list.compact()
	}
	return list
}

func (list EdgeList) All() iter.Seq[uint64] {
	return func(yield func(uint64) bool) {
		if list.IsInline() {
			for _, id := range list.small {
				if !yield(id) {
					return
				}
			}
			return
		}
		hasRemoved := list.removed.Len() != 0
		for chunk := range list.Chunks() {
			for _, id := range chunk {
				if (!hasRemoved || !list.removed.Has(id)) && !yield(id) {
					return
				}
			}
		}
	}
}

func (list EdgeList) IDs() []uint64 {
	ids := make([]uint64, 0, list.count)
	for id := range list.All() {
		ids = append(ids, id)
	}
	return ids
}

func (list EdgeList) compact() EdgeList {
	compacted := EdgeList{}
	for id := range list.All() {
		compacted = compacted.Append(id)
	}
	return compacted
}
