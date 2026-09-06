package store

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"iter"
	"maps"
	"math"
	"reflect"
	"slices"
)

type PropertyIndexDefinition struct {
	Scope    string
	Property string
}

type propertyValueKey struct {
	kind   uint8
	number uint64
	text   string
	digest [sha256.Size]byte
}

type propertyIndexData struct {
	values       ShardMap[map[propertyValueKey]propertyPosting]
	clonedHashes map[uint64]struct{}
	clonedValues map[propertyValueKey]struct{}
}

type PropertyIndexes struct {
	definitions map[PropertyIndexDefinition]propertyIndexData
	rootCloned  bool
	cloned      map[PropertyIndexDefinition]struct{}
}

func NewPropertyIndexes() PropertyIndexes {
	return PropertyIndexes{definitions: map[PropertyIndexDefinition]propertyIndexData{}}
}

// Fork creates a writable child of an immutable source generation, matching
// PagedMap and ShardMap: keep the source unchanged while the child is in use.
func (indexes PropertyIndexes) Fork() PropertyIndexes {
	return PropertyIndexes{definitions: indexes.definitions}
}

func (indexes PropertyIndexes) Has(definition PropertyIndexDefinition) bool {
	_, ok := indexes.definitions[definition]
	return ok
}

func (indexes PropertyIndexes) Len() int {
	return len(indexes.definitions)
}

func (indexes PropertyIndexes) Definitions() iter.Seq[PropertyIndexDefinition] {
	return func(yield func(PropertyIndexDefinition) bool) {
		for definition := range indexes.definitions {
			if !yield(definition) {
				return
			}
		}
	}
}

// DefinitionsFor visits definitions whose scope and property are both present.
// Definition scans compare every scope; reverse lookups do so only per property.
func (indexes PropertyIndexes) DefinitionsFor(scopes []string, properties map[string]any) iter.Seq[PropertyIndexDefinition] {
	return func(yield func(PropertyIndexDefinition) bool) {
		if len(scopes) == 0 || len(properties) == 0 {
			return
		}
		if indexes.Len() <= len(properties) {
			for definition := range indexes.Definitions() {
				if slices.Contains(scopes, definition.Scope) {
					if _, ok := properties[definition.Property]; ok && !yield(definition) {
						return
					}
				}
			}
			return
		}
		for _, scope := range scopes {
			for property := range properties {
				definition := PropertyIndexDefinition{Scope: scope, Property: property}
				if indexes.Has(definition) && !yield(definition) {
					return
				}
			}
		}
	}
}

func (indexes *PropertyIndexes) Create(definition PropertyIndexDefinition) bool {
	if indexes.Has(definition) {
		return false
	}
	indexes.cloneRoot()
	indexes.definitions[definition] = propertyIndexData{values: NewShardMap[map[propertyValueKey]propertyPosting]()}
	indexes.cloned[definition] = struct{}{}
	return true
}

func (indexes *PropertyIndexes) Drop(definition PropertyIndexDefinition) bool {
	if !indexes.Has(definition) {
		return false
	}
	indexes.cloneRoot()
	delete(indexes.definitions, definition)
	delete(indexes.cloned, definition)
	return true
}

func (indexes *PropertyIndexes) Add(definition PropertyIndexDefinition, value any, id uint64) error {
	key, err := makePropertyValueKey(value)
	if err != nil {
		return err
	}
	data, ok := indexes.writableDefinition(definition)
	if !ok {
		return nil
	}
	hashed := hashPropertyValueKey(key)
	bucket := data.writableBucket(hashed)
	list := bucket[key]
	if _, cloned := data.clonedValues[key]; !cloned {
		list = forkPropertyPosting(list)
		data.clonedValues[key] = struct{}{}
	}
	list.add(id)
	bucket[key] = list
	indexes.definitions[definition] = data
	return nil
}

func (indexes *PropertyIndexes) Remove(definition PropertyIndexDefinition, value any, id uint64) error {
	key, err := makePropertyValueKey(value)
	if err != nil {
		return err
	}
	data, ok := indexes.definitions[definition]
	if !ok {
		return nil
	}
	hashed := hashPropertyValueKey(key)
	list := data.values.Get(hashed)[key]
	if !list.has(id) {
		return nil
	}
	data, _ = indexes.writableDefinition(definition)
	bucket := data.writableBucket(hashed)
	list = bucket[key]
	if _, cloned := data.clonedValues[key]; !cloned {
		list = forkPropertyPosting(list)
		data.clonedValues[key] = struct{}{}
	}
	list.remove(id)
	if list.len() == 0 {
		delete(bucket, key)
		delete(data.clonedValues, key)
		if len(bucket) == 0 {
			data.values.Delete(hashed)
			delete(data.clonedHashes, hashed)
		}
	} else {
		bucket[key] = list
	}
	indexes.definitions[definition] = data
	return nil
}

func (indexes PropertyIndexes) Lookup(definition PropertyIndexDefinition, value any) ([]uint64, bool, error) {
	data, ok := indexes.definitions[definition]
	if !ok {
		return nil, false, nil
	}
	key, err := makePropertyValueKey(value)
	if err != nil {
		return nil, true, err
	}
	list := data.values.Get(hashPropertyValueKey(key))[key]
	ids := make([]uint64, 0, list.len())
	for id := range list.all() {
		ids = append(ids, id)
	}
	return ids, true, nil
}

// Visit calls visit for each posting without materializing the posting list.
func (indexes PropertyIndexes) Visit(definition PropertyIndexDefinition, value any, visit func(uint64) bool) (bool, error) {
	data, ok := indexes.definitions[definition]
	if !ok {
		return false, nil
	}
	key, err := makePropertyValueKey(value)
	if err != nil {
		return true, err
	}
	for id := range data.values.Get(hashPropertyValueKey(key))[key].all() {
		if !visit(id) {
			break
		}
	}
	return true, nil
}

// Cardinality returns the number of IDs in a value posting without allocating.
func (indexes PropertyIndexes) Cardinality(definition PropertyIndexDefinition, value any) (int, bool, error) {
	data, ok := indexes.definitions[definition]
	if !ok {
		return 0, false, nil
	}
	key, err := makePropertyValueKey(value)
	if err != nil {
		return 0, true, err
	}
	return data.values.Get(hashPropertyValueKey(key))[key].len(), true, nil
}

func (indexes PropertyIndexes) LookupLimit(definition PropertyIndexDefinition, value any, limit uint) ([]uint64, bool, error) {
	if limit == ^uint(0) {
		return indexes.Lookup(definition, value)
	}
	data, ok := indexes.definitions[definition]
	if !ok {
		return nil, false, nil
	}
	key, err := makePropertyValueKey(value)
	if err != nil {
		return nil, true, err
	}
	if limit == 0 {
		return nil, true, nil
	}
	list := data.values.Get(hashPropertyValueKey(key))[key]
	capacity := list.len()
	if limit < uint(capacity) {
		capacity = int(limit)
	}
	ids := make([]uint64, 0, capacity)
	for id := range list.all() {
		ids = append(ids, id)
		if uint(len(ids)) == limit {
			break
		}
	}
	return ids, true, nil
}

func PropertyValuesEqual(left, right any) bool {
	return reflect.DeepEqual(left, right)
}

func EstimatePropertyIndexValueBytes(value any) uint64 {
	return estimateValueBytes(value)
}

func (indexes *PropertyIndexes) cloneRoot() {
	if indexes.rootCloned {
		return
	}
	indexes.definitions = maps.Clone(indexes.definitions)
	if indexes.definitions == nil {
		indexes.definitions = map[PropertyIndexDefinition]propertyIndexData{}
	}
	indexes.rootCloned = true
	indexes.cloned = map[PropertyIndexDefinition]struct{}{}
}

func (indexes *PropertyIndexes) writableDefinition(definition PropertyIndexDefinition) (propertyIndexData, bool) {
	data, ok := indexes.definitions[definition]
	if !ok {
		return propertyIndexData{}, false
	}
	indexes.cloneRoot()
	if _, cloned := indexes.cloned[definition]; !cloned {
		data.values = data.values.Fork()
		data.clonedHashes = nil
		data.clonedValues = nil
		indexes.definitions[definition] = data
		indexes.cloned[definition] = struct{}{}
	}
	return indexes.definitions[definition], true
}

func (data *propertyIndexData) writableBucket(hashed uint64) map[propertyValueKey]propertyPosting {
	data.values.CloneShardOnce(hashed)
	if data.clonedHashes == nil {
		data.clonedHashes = map[uint64]struct{}{}
		data.clonedValues = map[propertyValueKey]struct{}{}
	}
	if _, cloned := data.clonedHashes[hashed]; !cloned {
		bucket := maps.Clone(data.values.Get(hashed))
		if bucket == nil {
			bucket = map[propertyValueKey]propertyPosting{}
		}
		data.values.Set(hashed, bucket)
		data.clonedHashes[hashed] = struct{}{}
	}
	return data.values.Get(hashed)
}

// propertyPosting keeps the common small posting compact and ordered. Larger
// postings promote to a copy-on-write radix table, whose ordered iterator can
// stop after a requested low-ID prefix.
type propertyPosting struct {
	small  []uint64
	chunks *propertyPostingChunks
	large  *propertyPostingRadix
}

const propertyPostingChunkSize = 256

type propertyPostingRadix struct {
	ids         PagedMap[struct{}]
	roots       []uint64
	rootsCloned bool
}

type propertyPostingChunks struct {
	chunks       []*propertyPostingChunk
	length       int
	directoryCOW bool
	cloned       map[int]struct{}
}

type propertyPostingChunk struct {
	ids []uint64
}

func (posting propertyPosting) has(id uint64) bool {
	if posting.large != nil {
		return posting.large.ids.Has(id)
	}
	if posting.chunks != nil {
		return posting.chunks.has(id)
	}
	_, found := slices.BinarySearch(posting.small, id)
	return found
}

func (posting propertyPosting) len() int {
	if posting.large != nil {
		return posting.large.ids.Len()
	}
	if posting.chunks != nil {
		return posting.chunks.length
	}
	return len(posting.small)
}

func (posting propertyPosting) all() iter.Seq[uint64] {
	return func(yield func(uint64) bool) {
		if posting.large != nil {
			for id := range posting.large.ids.orderedRoots(posting.large.roots) {
				if !yield(id) {
					return
				}
			}
			return
		}
		if posting.chunks != nil {
			for _, chunk := range posting.chunks.chunks {
				for _, id := range chunk.ids {
					if !yield(id) {
						return
					}
				}
			}
			return
		}
		for _, id := range posting.small {
			if !yield(id) {
				return
			}
		}
	}
}

func forkPropertyPosting(posting propertyPosting) propertyPosting {
	if posting.large != nil {
		forked := *posting.large
		forked.ids = posting.large.ids.Fork()
		forked.rootsCloned = false
		posting.large = &forked
		return posting
	}
	if posting.chunks != nil {
		forked := *posting.chunks
		forked.directoryCOW = false
		forked.cloned = nil
		posting.chunks = &forked
		return posting
	}
	posting.small = slices.Clone(posting.small)
	return posting
}

func (posting *propertyPosting) add(id uint64) {
	if posting.large != nil {
		posting.large.add(id)
		if posting.large.shouldUseChunks() {
			posting.chunks = newPropertyPostingChunksFromRadix(posting.large)
			posting.large = nil
		}
		return
	}
	if posting.chunks != nil {
		posting.chunks.add(id)
		return
	}
	index, found := slices.BinarySearch(posting.small, id)
	if found {
		return
	}
	posting.small = append(posting.small, 0)
	copy(posting.small[index+1:], posting.small[index:])
	posting.small[index] = id
	if len(posting.small) <= smallPostingLimit {
		return
	}
	if !propertyPostingShouldPromote(posting.small) {
		posting.chunks = newPropertyPostingChunks(posting.small)
		posting.small = nil
		return
	}
	large := &propertyPostingRadix{}
	for _, postingID := range posting.small {
		large.add(postingID)
	}
	posting.small = nil
	posting.large = large
}

// Sparse IDs would allocate one radix root per distant range. Keep those
// postings as a compact sorted slice; dense postings promote for bounded
// ordered prefix reads.
func propertyPostingShouldPromote(ids []uint64) bool {
	return (ids[len(ids)-1]>>20)-(ids[0]>>20) < 8
}

func newPropertyPostingChunks(ids []uint64) *propertyPostingChunks {
	posting := &propertyPostingChunks{length: len(ids), directoryCOW: true}
	for len(ids) > 0 {
		length := min(len(ids), propertyPostingChunkSize)
		chunk := &propertyPostingChunk{ids: make([]uint64, length, propertyPostingChunkSize)}
		copy(chunk.ids, ids[:length])
		posting.chunks = append(posting.chunks, chunk)
		ids = ids[length:]
	}
	return posting
}

func newPropertyPostingChunksFromRadix(radix *propertyPostingRadix) *propertyPostingChunks {
	ids := make([]uint64, 0, radix.ids.Len())
	for id := range radix.ids.orderedRoots(radix.roots) {
		ids = append(ids, id)
	}
	return newPropertyPostingChunks(ids)
}

func (posting *propertyPostingChunks) has(id uint64) bool {
	index := posting.index(id)
	if index == len(posting.chunks) {
		return false
	}
	_, found := slices.BinarySearch(posting.chunks[index].ids, id)
	return found
}

func (posting *propertyPostingChunks) add(id uint64) {
	index := posting.index(id)
	if index == len(posting.chunks) {
		index--
	}
	chunk := posting.writableChunk(index)
	insert, found := slices.BinarySearch(chunk.ids, id)
	if found {
		return
	}
	if len(chunk.ids) < propertyPostingChunkSize {
		chunk.ids = append(chunk.ids, 0)
		copy(chunk.ids[insert+1:], chunk.ids[insert:])
		chunk.ids[insert] = id
		posting.length++
		return
	}
	merged := make([]uint64, len(chunk.ids)+1)
	copy(merged, chunk.ids[:insert])
	merged[insert] = id
	copy(merged[insert+1:], chunk.ids[insert:])
	leftLength := len(merged) / 2
	left := &propertyPostingChunk{ids: make([]uint64, leftLength, propertyPostingChunkSize)}
	right := &propertyPostingChunk{ids: make([]uint64, len(merged)-leftLength, propertyPostingChunkSize)}
	copy(left.ids, merged[:leftLength])
	copy(right.ids, merged[leftLength:])
	posting.cloneDirectory()
	posting.chunks[index] = left
	posting.chunks = append(posting.chunks, nil)
	copy(posting.chunks[index+2:], posting.chunks[index+1:])
	posting.chunks[index+1] = right
	posting.cloned = nil
	posting.length++
}

func (posting *propertyPostingChunks) remove(id uint64) {
	index := posting.index(id)
	if index == len(posting.chunks) {
		return
	}
	chunk := posting.writableChunk(index)
	remove, found := slices.BinarySearch(chunk.ids, id)
	if !found {
		return
	}
	copy(chunk.ids[remove:], chunk.ids[remove+1:])
	chunk.ids = chunk.ids[:len(chunk.ids)-1]
	posting.length--
	if len(chunk.ids) != 0 {
		return
	}
	posting.cloneDirectory()
	copy(posting.chunks[index:], posting.chunks[index+1:])
	posting.chunks[len(posting.chunks)-1] = nil
	posting.chunks = posting.chunks[:len(posting.chunks)-1]
	posting.cloned = nil
}

func (posting *propertyPostingChunks) index(id uint64) int {
	low, high := 0, len(posting.chunks)
	for low < high {
		middle := low + (high-low)/2
		if posting.chunks[middle].ids[len(posting.chunks[middle].ids)-1] < id {
			low = middle + 1
		} else {
			high = middle
		}
	}
	return low
}

func (posting *propertyPostingChunks) writableChunk(index int) *propertyPostingChunk {
	posting.cloneDirectory()
	if posting.cloned == nil {
		posting.cloned = map[int]struct{}{}
	}
	if _, cloned := posting.cloned[index]; !cloned {
		source := posting.chunks[index]
		chunk := &propertyPostingChunk{ids: make([]uint64, len(source.ids), propertyPostingChunkSize)}
		copy(chunk.ids, source.ids)
		posting.chunks[index] = chunk
		posting.cloned[index] = struct{}{}
	}
	return posting.chunks[index]
}

func (posting *propertyPostingChunks) cloneDirectory() {
	if posting.directoryCOW {
		return
	}
	// ponytail: the first sparse write copies O(chunks) directory pointers;
	// use a persistent directory only if larger posting benchmarks justify it.
	posting.chunks = slices.Clone(posting.chunks)
	posting.directoryCOW = true
}

func (posting *propertyPosting) remove(id uint64) {
	if posting.large != nil {
		posting.large.remove(id)
		if posting.large.shouldUseChunks() {
			posting.chunks = newPropertyPostingChunksFromRadix(posting.large)
			posting.large = nil
		}
		return
	}
	if posting.chunks != nil {
		posting.chunks.remove(id)
		return
	}
	index, found := slices.BinarySearch(posting.small, id)
	if !found {
		return
	}
	copy(posting.small[index:], posting.small[index+1:])
	posting.small = posting.small[:len(posting.small)-1]
}

func (posting *propertyPostingRadix) add(id uint64) {
	high := id >> 20
	newRoot := high != 0 && posting.ids.root(high) == nil
	posting.ids.CloneShardOnce(id)
	posting.ids.Set(id, struct{}{})
	if newRoot {
		posting.cloneRoots()
		index, _ := slices.BinarySearch(posting.roots, high)
		posting.roots = append(posting.roots, 0)
		copy(posting.roots[index+1:], posting.roots[index:])
		posting.roots[index] = high
	}
}

func (posting *propertyPostingRadix) remove(id uint64) {
	high := id >> 20
	posting.ids.CloneShardOnce(id)
	posting.ids.Delete(id)
	if high == 0 || posting.ids.root(high) != nil {
		return
	}
	posting.cloneRoots()
	index, found := slices.BinarySearch(posting.roots, high)
	if !found {
		return
	}
	copy(posting.roots[index:], posting.roots[index+1:])
	posting.roots = posting.roots[:len(posting.roots)-1]
}

func (posting *propertyPostingRadix) cloneRoots() {
	if posting.rootsCloned {
		return
	}
	posting.roots = slices.Clone(posting.roots)
	posting.rootsCloned = true
}

func (posting *propertyPostingRadix) shouldUseChunks() bool {
	return len(posting.roots) >= 8 && len(posting.roots)*propertyPostingChunkSize > posting.ids.Len()
}

func makePropertyValueKey(value any) (propertyValueKey, error) {
	switch typed := value.(type) {
	case nil:
		return propertyValueKey{kind: 1}, nil
	case bool:
		return propertyValueKey{kind: 2, number: uint64(boolByte(typed))}, nil
	case int64:
		return propertyValueKey{kind: 3, number: uint64(typed)}, nil
	case float64:
		if typed == 0 {
			typed = 0
		}
		return propertyValueKey{kind: 4, number: math.Float64bits(typed)}, nil
	case string:
		return propertyValueKey{kind: 5, text: typed}, nil
	case []byte:
		return propertyValueKey{kind: 6, digest: sha256.Sum256(typed)}, nil
	case []float32, []any, map[string]any:
		hasher := sha256.New()
		if err := writePropertyValueHash(hasher, typed); err != nil {
			return propertyValueKey{}, err
		}
		var digest [sha256.Size]byte
		hasher.Sum(digest[:0])
		return propertyValueKey{kind: propertyKind(typed), digest: digest}, nil
	default:
		return propertyValueKey{}, errors.New("property index value is not normalized")
	}
}

func propertyKind(value any) uint8 {
	switch value.(type) {
	case []float32:
		return 7
	case []any:
		return 8
	case map[string]any:
		return 9
	default:
		return 0
	}
}

func writePropertyValueHash(hasher hash.Hash, value any) error {
	var number [8]byte
	switch typed := value.(type) {
	case nil:
		hasher.Write([]byte{1})
	case bool:
		hasher.Write([]byte{2, boolByte(typed)})
	case int64:
		hasher.Write([]byte{3})
		binary.BigEndian.PutUint64(number[:], uint64(typed))
		hasher.Write(number[:])
	case float64:
		hasher.Write([]byte{4})
		if typed == 0 {
			typed = 0
		}
		binary.BigEndian.PutUint64(number[:], math.Float64bits(typed))
		hasher.Write(number[:])
	case string:
		hasher.Write([]byte{5})
		binary.BigEndian.PutUint64(number[:], uint64(len(typed)))
		hasher.Write(number[:])
		hasher.Write([]byte(typed))
	case []byte:
		hasher.Write([]byte{6})
		binary.BigEndian.PutUint64(number[:], uint64(len(typed)))
		hasher.Write(number[:])
		hasher.Write(typed)
	case []float32:
		hasher.Write([]byte{7})
		binary.BigEndian.PutUint64(number[:], uint64(len(typed)))
		hasher.Write(number[:])
		var bits [4]byte
		for _, item := range typed {
			if item == 0 {
				item = 0
			}
			binary.BigEndian.PutUint32(bits[:], math.Float32bits(item))
			hasher.Write(bits[:])
		}
	case []any:
		hasher.Write([]byte{8})
		binary.BigEndian.PutUint64(number[:], uint64(len(typed)))
		hasher.Write(number[:])
		for _, item := range typed {
			if err := writePropertyValueHash(hasher, item); err != nil {
				return err
			}
		}
	case map[string]any:
		hasher.Write([]byte{9})
		keys := slices.Sorted(maps.Keys(typed))
		binary.BigEndian.PutUint64(number[:], uint64(len(keys)))
		hasher.Write(number[:])
		for _, name := range keys {
			binary.BigEndian.PutUint64(number[:], uint64(len(name)))
			hasher.Write(number[:])
			hasher.Write([]byte(name))
			if err := writePropertyValueHash(hasher, typed[name]); err != nil {
				return err
			}
		}
	default:
		return errors.New("property index value is not normalized")
	}
	return nil
}

func hashPropertyValueKey(key propertyValueKey) uint64 {
	hash := uint64(14695981039346656037) ^ uint64(key.kind)
	hash = (hash ^ key.number) * 1099511628211
	for index := range len(key.text) {
		hash = (hash ^ uint64(key.text[index])) * 1099511628211
	}
	for _, value := range key.digest {
		hash = (hash ^ uint64(value)) * 1099511628211
	}
	return hash
}

func boolByte(value bool) byte {
	if value {
		return 1
	}
	return 0
}
