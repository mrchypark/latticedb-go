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
	values       ShardMap[map[propertyValueKey]postingList]
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

func (indexes *PropertyIndexes) Create(definition PropertyIndexDefinition) bool {
	if indexes.Has(definition) {
		return false
	}
	indexes.cloneRoot()
	indexes.definitions[definition] = propertyIndexData{values: NewShardMap[map[propertyValueKey]postingList]()}
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
		list = forkPostingList(list)
		data.clonedValues[key] = struct{}{}
	}
	addPosting(&list, id)
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
		list = forkPostingList(list)
		data.clonedValues[key] = struct{}{}
	}
	removePosting(&list, id)
	if list.len() == 0 {
		delete(bucket, key)
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
	slices.Sort(ids)
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
	capacity := 64
	if limit < uint(capacity) {
		capacity = int(limit)
	}
	ids := make([]uint64, 0, capacity)
	for id := range data.values.Get(hashPropertyValueKey(key))[key].all() {
		ids = insertPostingID(ids, id, limit)
	}
	return ids, true, nil
}

func insertPostingID(ids []uint64, id uint64, limit uint) []uint64 {
	if uint(len(ids)) == limit && id >= ids[len(ids)-1] {
		return ids
	}
	index, found := slices.BinarySearch(ids, id)
	if found {
		return ids
	}
	if uint(len(ids)) == limit {
		ids = ids[:len(ids)-1]
	}
	ids = append(ids, 0)
	copy(ids[index+1:], ids[index:])
	ids[index] = id
	return ids
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

func (data *propertyIndexData) writableBucket(hashed uint64) map[propertyValueKey]postingList {
	data.values.CloneShardOnce(hashed)
	if data.clonedHashes == nil {
		data.clonedHashes = map[uint64]struct{}{}
		data.clonedValues = map[propertyValueKey]struct{}{}
	}
	if _, cloned := data.clonedHashes[hashed]; !cloned {
		bucket := maps.Clone(data.values.Get(hashed))
		if bucket == nil {
			bucket = map[propertyValueKey]postingList{}
		}
		data.values.Set(hashed, bucket)
		data.clonedHashes[hashed] = struct{}{}
	}
	return data.values.Get(hashed)
}

func forkPostingList(list postingList) postingList {
	if list.large.root != nil {
		list.large = list.large.Fork()
	} else if list.small == nil {
		list.small = map[uint64]struct{}{}
	} else {
		list.small = maps.Clone(list.small)
	}
	return list
}

func addPosting(list *postingList, id uint64) {
	if list.large.root != nil {
		list.large.CloneShardOnce(id)
		list.large.Set(id, struct{}{})
		return
	}
	if list.small == nil {
		list.small = map[uint64]struct{}{}
	}
	list.small[id] = struct{}{}
	if len(list.small) <= smallPostingLimit {
		return
	}
	list.large = newPostingShardMap()
	for postingID := range list.small {
		list.large.Set(postingID, struct{}{})
	}
	list.small = nil
}

func removePosting(list *postingList, id uint64) {
	if list.large.root != nil {
		list.large.CloneShardOnce(id)
		list.large.Delete(id)
		return
	}
	delete(list.small, id)
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
