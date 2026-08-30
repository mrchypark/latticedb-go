package store

import (
	"iter"
	"maps"
	"math/bits"
)

const (
	pagedMapFanout = 128
	pagedMapSlots  = 64
)

type valuePage[V any] struct {
	occupied uint64
	values   [pagedMapSlots]V
}

type pageBucket[V any] [pagedMapFanout]*valuePage[V]
type pageRoot[V any] [pagedMapFanout]*pageBucket[V]

// PagedMap is a copy-on-write radix table for sequential uint64 IDs. Sparse
// high ID ranges use separate roots without paying for empty value slots.
type PagedMap[V any] struct {
	root0         *pageRoot[V]
	roots         map[uint64]*pageRoot[V]
	length        int
	root0Cloned   bool
	rootsCloned   bool
	clonedRoots   map[uint64]struct{}
	clonedBuckets map[uint64]struct{}
	clonedPages   map[uint64]struct{}
}

func NewPagedMap[V any]() PagedMap[V] {
	return PagedMap[V]{}
}

func (m *PagedMap[V]) Get(id uint64) V {
	page, slot := m.page(id)
	if page == nil || page.occupied&(uint64(1)<<slot) == 0 {
		var zero V
		return zero
	}
	return page.values[slot]
}

func (m *PagedMap[V]) Has(id uint64) bool {
	page, slot := m.page(id)
	return page != nil && page.occupied&(uint64(1)<<slot) != 0
}

func (m *PagedMap[V]) Len() int { return m.length }

func (m *PagedMap[V]) All() iter.Seq2[uint64, V] {
	return func(yield func(uint64, V) bool) {
		yieldRoot := func(high uint64, root *pageRoot[V]) bool {
			if root == nil {
				return true
			}
			for bucketIndex, bucket := range root {
				if bucket == nil {
					continue
				}
				for shard, page := range bucket {
					if page == nil {
						continue
					}
					key := high<<14 | uint64(bucketIndex)<<7 | uint64(shard)
					for occupied := page.occupied; occupied != 0; occupied &= occupied - 1 {
						slot := uint(bits.TrailingZeros64(occupied))
						if !yield(key<<6|uint64(slot), page.values[slot]) {
							return false
						}
					}
				}
			}
			return true
		}
		if !yieldRoot(0, m.root0) {
			return
		}
		for high, root := range m.roots {
			if !yieldRoot(high, root) {
				return
			}
		}
	}
}

func (m *PagedMap[V]) Set(id uint64, value V) {
	key, slot := id>>6, uint(id&63)
	page := m.ensurePage(key)
	mask := uint64(1) << slot
	if page.occupied&mask == 0 {
		page.occupied |= mask
		m.length++
	}
	page.values[slot] = value
}

func (m *PagedMap[V]) Delete(id uint64) {
	key, slot := id>>6, uint(id&63)
	page := m.pageByKey(key)
	mask := uint64(1) << slot
	if page == nil || page.occupied&mask == 0 {
		return
	}
	var zero V
	page.values[slot] = zero
	page.occupied &^= mask
	m.length--
	if page.occupied != 0 {
		return
	}
	high, bucket, shard := pageIndexes(key)
	root := m.root(high)
	root[bucket][shard] = nil
	if pageBucketEmpty(root[bucket]) {
		root[bucket] = nil
	}
	if pageRootEmpty(root) {
		if high == 0 {
			m.root0 = nil
		} else {
			delete(m.roots, high)
		}
	}
}

func (m PagedMap[V]) Fork() PagedMap[V] {
	return PagedMap[V]{root0: m.root0, roots: m.roots, length: m.length}
}

func (m PagedMap[V]) ForkSet(id uint64, value V) PagedMap[V] {
	key, slot := id>>6, uint(id&63)
	high, bucketIndex, shard := pageIndexes(key)
	result := m.Fork()
	root := new(pageRoot[V])
	if source := m.root(high); source != nil {
		*root = *source
	}
	if high == 0 {
		result.root0 = root
	} else {
		result.roots = maps.Clone(m.roots)
		if result.roots == nil {
			result.roots = map[uint64]*pageRoot[V]{}
		}
		result.roots[high] = root
	}
	bucket := new(pageBucket[V])
	if root[bucketIndex] != nil {
		*bucket = *root[bucketIndex]
	}
	root[bucketIndex] = bucket
	page := new(valuePage[V])
	if bucket[shard] != nil {
		*page = *bucket[shard]
	}
	bucket[shard] = page
	mask := uint64(1) << slot
	if page.occupied&mask == 0 {
		page.occupied |= mask
		result.length++
	}
	page.values[slot] = value
	return result
}

func (m *PagedMap[V]) CloneShardOnce(id uint64) {
	key := id >> 6
	if _, cloned := m.clonedPages[key]; cloned && m.pageByKey(key) != nil {
		return
	}
	if m.clonedPages == nil {
		m.clonedRoots = map[uint64]struct{}{}
		m.clonedBuckets = map[uint64]struct{}{}
		m.clonedPages = map[uint64]struct{}{}
	}
	high, bucket, shard := pageIndexes(key)
	if high == 0 && (!m.root0Cloned || m.root0 == nil) {
		root := new(pageRoot[V])
		if m.root0 != nil {
			*root = *m.root0
		}
		m.root0 = root
		m.root0Cloned = true
	} else if high != 0 {
		if !m.rootsCloned {
			m.roots = maps.Clone(m.roots)
			m.rootsCloned = true
		}
		if _, cloned := m.clonedRoots[high]; !cloned || m.roots[high] == nil {
			root := new(pageRoot[V])
			if m.roots[high] != nil {
				*root = *m.roots[high]
			}
			if m.roots == nil {
				m.roots = map[uint64]*pageRoot[V]{}
			}
			m.roots[high] = root
			m.clonedRoots[high] = struct{}{}
		}
	}
	bucketKey := high<<8 | uint64(bucket)
	root := m.root(high)
	if _, cloned := m.clonedBuckets[bucketKey]; !cloned || root[bucket] == nil {
		clonedBucket := new(pageBucket[V])
		if root[bucket] != nil {
			*clonedBucket = *root[bucket]
		}
		root[bucket] = clonedBucket
		m.clonedBuckets[bucketKey] = struct{}{}
	}
	root = m.root(high)
	if source := root[bucket][shard]; source != nil {
		page := new(valuePage[V])
		*page = *source
		root[bucket][shard] = page
	}
	m.clonedPages[key] = struct{}{}
}

func (m *PagedMap[V]) page(id uint64) (*valuePage[V], uint) {
	return m.pageByKey(id >> 6), uint(id & 63)
}

func (m *PagedMap[V]) pageByKey(key uint64) *valuePage[V] {
	high, bucket, shard := pageIndexes(key)
	root := m.root(high)
	if root == nil || root[bucket] == nil {
		return nil
	}
	return root[bucket][shard]
}

func (m *PagedMap[V]) ensurePage(key uint64) *valuePage[V] {
	high, bucket, shard := pageIndexes(key)
	root := m.root(high)
	if root == nil {
		root = new(pageRoot[V])
		if high == 0 {
			m.root0 = root
		} else {
			if m.roots == nil {
				m.roots = map[uint64]*pageRoot[V]{}
			}
			m.roots[high] = root
		}
	}
	if root[bucket] == nil {
		root[bucket] = new(pageBucket[V])
	}
	if root[bucket][shard] == nil {
		root[bucket][shard] = new(valuePage[V])
	}
	return root[bucket][shard]
}

func (m *PagedMap[V]) root(high uint64) *pageRoot[V] {
	if high == 0 {
		return m.root0
	}
	return m.roots[high]
}

func pageIndexes(key uint64) (uint64, uint8, uint8) {
	return key >> 14, uint8(key>>7) & (pagedMapFanout - 1), uint8(key) & (pagedMapFanout - 1)
}

func pageBucketEmpty[V any](bucket *pageBucket[V]) bool {
	if bucket == nil {
		return true
	}
	for _, page := range bucket {
		if page != nil {
			return false
		}
	}
	return true
}

func pageRootEmpty[V any](root *pageRoot[V]) bool {
	if root == nil {
		return true
	}
	for _, bucket := range root {
		if bucket != nil {
			return false
		}
	}
	return true
}
