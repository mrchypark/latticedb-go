package store

import (
	"iter"
	"slices"
)

const (
	adjacencyChunkSize        = 64
	adjacencySyncCompactLimit = adjacencyChunkSize * 4
	// Keep at most 63 IDs inline so the 64th append fills the first chunk.
	adjacencyInlineLimit = adjacencyChunkSize - 1
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

func (list *EdgeList) Len() int {
	if list == nil {
		return 0
	}
	return list.count
}

func (list *EdgeList) IsInline() bool {
	return list == nil || list.chunks.root0 == nil && list.chunks.roots == nil
}

func (list *EdgeList) InlineIDs() []uint64 {
	if list == nil {
		return nil
	}
	return list.small
}

func (list *EdgeList) HasRemovals() bool { return list != nil && list.removed.Len() != 0 }

func (list *EdgeList) IsRemoved(id uint64) bool { return list != nil && list.removed.Has(id) }

func (list *EdgeList) Chunks() iter.Seq[[]uint64] {
	return func(yield func([]uint64) bool) {
		if list == nil {
			return
		}
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

func (list *EdgeList) Append(id uint64) *EdgeList {
	result := EdgeList{}
	if list != nil {
		result = *list
	}
	if result.IsInline() && len(result.small) < adjacencyInlineLimit {
		result.small = append(slices.Clone(result.small), id)
		result.count++
		result.total++
		return &result
	}
	owned := false
	if result.IsInline() {
		result.total = len(result.small)
		result.chunks = NewPagedMap[[]uint64]()
		for start := 0; start < len(result.small); start += adjacencyChunkSize {
			end := min(start+adjacencyChunkSize, len(result.small))
			result.chunks.Set(uint64(start/adjacencyChunkSize), result.small[start:end])
		}
		result.small = nil
		owned = true
	}
	chunk := uint64(result.total / adjacencyChunkSize)
	ids := result.chunks.Get(chunk)
	updatedIDs := make([]uint64, len(ids)+1)
	copy(updatedIDs, ids)
	updatedIDs[len(ids)] = id
	if owned {
		result.chunks.Set(chunk, updatedIDs)
	} else {
		result.chunks = result.chunks.ForkSet(chunk, updatedIDs)
	}
	result.count++
	result.total++
	return &result
}

func (list *EdgeList) Remove(id uint64) *EdgeList {
	if list == nil {
		return nil
	}
	result := *list
	if result.IsInline() {
		for index, edgeID := range result.small {
			if edgeID == id {
				result.small = append(slices.Clone(result.small[:index]), result.small[index+1:]...)
				result.count--
				result.total--
				return &result
			}
		}
		return list
	}
	if result.removed.Has(id) {
		return list
	}
	for chunk := range result.Chunks() {
		if slices.Contains(chunk, id) {
			goto found
		}
	}
	return list

found:
	return list.RemoveKnown(id)
}

// RemoveKnown skips the chunk membership scan when the caller already proved
// the edge belongs to this list.
func (list *EdgeList) RemoveKnown(id uint64) *EdgeList {
	if list == nil || list.IsInline() {
		return list.Remove(id)
	}
	result := *list
	if result.removed.Has(id) {
		return list
	}
	result.removed = result.removed.Fork()
	result.removed.CloneShardOnce(id)
	result.removed.Set(id, struct{}{})
	result.count--
	if result.total <= adjacencySyncCompactLimit && result.removed.Len() > adjacencyChunkSize && result.removed.Len() > result.count {
		return result.compact()
	}
	// ponytail: lists over 256 retain tombstones until reopen rebuilds adjacency; add incremental/background compaction if read amplification is measurable.
	return &result
}

func (list *EdgeList) All() iter.Seq[uint64] {
	return func(yield func(uint64) bool) {
		if list == nil {
			return
		}
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

func (list *EdgeList) IDs() []uint64 {
	ids := make([]uint64, 0, list.Len())
	for id := range list.All() {
		ids = append(ids, id)
	}
	return ids
}

func (list *EdgeList) compact() *EdgeList {
	var compacted *EdgeList
	for id := range list.All() {
		compacted = compacted.Append(id)
	}
	return compacted
}
