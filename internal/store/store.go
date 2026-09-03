package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"maps"
	"math"
	"math/bits"
	"path/filepath"
	"reflect"
	"slices"
	"unicode/utf8"
)

const shardFanout = 256

type shardBucket[V any] struct {
	values [shardFanout]map[uint64]V
	active [shardFanout / 64]uint64
}

type ShardMap[V any] struct {
	root          *[shardFanout]*shardBucket[V]
	length        int
	clonedBuckets [shardFanout / 64]uint64
	clonedShards  map[uint16]struct{}
	activeBuckets [shardFanout / 64]uint64
	smallActive   [2]uint16
	smallCount    uint8
	shardShift    uint8
}

type StringPostings struct {
	buckets      ShardMap[map[string]postingList]
	clonedHashes map[uint64]struct{}
	clonedKeys   map[string]struct{}
}

const smallPostingLimit = 64

type postingList struct {
	small map[uint64]struct{}
	large ShardMap[struct{}]
}

func NewStringPostings() StringPostings {
	return StringPostings{buckets: NewShardMap[map[string]postingList]()}
}

func (postings StringPostings) Fork() StringPostings {
	return StringPostings{buckets: postings.buckets.Fork()}
}

func (postings StringPostings) Get(key string) []uint64 {
	list := postings.buckets.Get(hashString(key))[key]
	ids := make([]uint64, 0, list.len())
	for id := range list.all() {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func (postings StringPostings) Len(key string) int {
	return postings.buckets.Get(hashString(key))[key].len()
}

func (postings StringPostings) All(key string) iter.Seq[uint64] {
	list := postings.buckets.Get(hashString(key))[key]
	if list.large.root != nil {
		large := list.large
		return func(yield func(uint64) bool) {
			for id := range large.All() {
				if !yield(id) {
					return
				}
			}
		}
	}
	small := list.small
	return func(yield func(uint64) bool) {
		for id := range small {
			if !yield(id) {
				return
			}
		}
	}
}

func (postings StringPostings) Keys() iter.Seq[string] {
	return func(yield func(string) bool) {
		for _, bucket := range postings.buckets.All() {
			for key := range bucket {
				if !yield(key) {
					return
				}
			}
		}
	}
}

func (postings *StringPostings) Add(key string, id uint64) {
	list := postings.writable(key)
	if list.large.root != nil {
		list.large.CloneShardOnce(id)
		list.large.Set(id, struct{}{})
	} else {
		list.small[id] = struct{}{}
		if len(list.small) > smallPostingLimit {
			list.large = newPostingShardMap()
			for postingID := range list.small {
				list.large.Set(postingID, struct{}{})
			}
			list.small = nil
		}
	}
	postings.buckets.Get(hashString(key))[key] = list
}

func (postings *StringPostings) Remove(key string, id uint64) {
	hash := hashString(key)
	list := postings.buckets.Get(hash)[key]
	if !list.has(id) {
		return
	}
	list = postings.writable(key)
	if list.large.root != nil {
		list.large.CloneShardOnce(id)
		list.large.Delete(id)
	} else {
		delete(list.small, id)
	}
	bucket := postings.buckets.Get(hash)
	if list.len() == 0 {
		delete(bucket, key)
		delete(postings.clonedKeys, key)
		if len(bucket) == 0 {
			postings.buckets.Delete(hash)
			delete(postings.clonedHashes, hash)
		}
		return
	}
	bucket[key] = list
}

func (postings *StringPostings) writable(key string) postingList {
	hash := hashString(key)
	postings.buckets.CloneShardOnce(hash)
	if postings.clonedHashes == nil {
		postings.clonedHashes = map[uint64]struct{}{}
		postings.clonedKeys = map[string]struct{}{}
	}
	if _, cloned := postings.clonedHashes[hash]; !cloned {
		bucket := maps.Clone(postings.buckets.Get(hash))
		if bucket == nil {
			bucket = map[string]postingList{}
		}
		postings.buckets.Set(hash, bucket)
		postings.clonedHashes[hash] = struct{}{}
	}
	bucket := postings.buckets.Get(hash)
	if _, cloned := postings.clonedKeys[key]; !cloned {
		list := bucket[key]
		if list.large.root != nil {
			list.large = list.large.Fork()
		} else if list.small == nil {
			list.small = map[uint64]struct{}{}
		} else {
			list.small = maps.Clone(list.small)
		}
		bucket[key] = list
		postings.clonedKeys[key] = struct{}{}
	}
	return bucket[key]
}

func (list postingList) len() int {
	if list.large.root != nil {
		return list.large.Len()
	}
	return len(list.small)
}

func (list postingList) has(id uint64) bool {
	if list.large.root != nil {
		return list.large.Has(id)
	}
	_, ok := list.small[id]
	return ok
}

func (list postingList) all() iter.Seq[uint64] {
	return func(yield func(uint64) bool) {
		if list.large.root != nil {
			for id := range list.large.All() {
				if !yield(id) {
					return
				}
			}
			return
		}
		for id := range list.small {
			if !yield(id) {
				return
			}
		}
	}
}

func hashString(value string) uint64 {
	const offset = uint64(14695981039346656037)
	const prime = uint64(1099511628211)
	hash := offset
	for index := range len(value) {
		hash ^= uint64(value[index])
		hash *= prime
	}
	return hash
}

func NewShardMap[V any]() ShardMap[V] {
	return ShardMap[V]{root: new([shardFanout]*shardBucket[V])}
}

func newPostingShardMap() ShardMap[struct{}] {
	return ShardMap[struct{}]{root: new([shardFanout]*shardBucket[struct{}]), shardShift: 8}
}

func (m ShardMap[V]) Get(id uint64) V {
	bucket, shard := m.indexes(id)
	if m.root[bucket] == nil {
		var zero V
		return zero
	}
	return m.root[bucket].values[shard][id]
}

func (m ShardMap[V]) Has(id uint64) bool {
	bucket, shard := m.indexes(id)
	if m.root == nil || m.root[bucket] == nil {
		return false
	}
	_, exists := m.root[bucket].values[shard][id]
	return exists
}

func (m ShardMap[V]) Len() int { return m.length }

func (m ShardMap[V]) All() iter.Seq2[uint64, V] {
	return func(yield func(uint64, V) bool) {
		if m.root == nil {
			return
		}
		if m.smallCount <= uint8(len(m.smallActive)) {
			for _, index := range m.smallActive[:m.smallCount] {
				bucket, shard := uint8(index>>8), uint8(index)
				for id, value := range m.root[bucket].values[shard] {
					if !yield(id, value) {
						return
					}
				}
			}
			return
		}
		for bucketWord, activeBuckets := range m.activeBuckets {
			for activeBuckets != 0 {
				bucket := uint8(bucketWord*64 + bits.TrailingZeros64(activeBuckets))
				item := m.root[bucket]
				for shardWord, activeShards := range item.active {
					for activeShards != 0 {
						shard := uint8(shardWord*64 + bits.TrailingZeros64(activeShards))
						for id, value := range item.values[shard] {
							if !yield(id, value) {
								return
							}
						}
						activeShards &= activeShards - 1
					}
				}
				activeBuckets &= activeBuckets - 1
			}
		}
	}
}

func (m *ShardMap[V]) Set(id uint64, value V) {
	bucket, shard := m.indexes(id)
	if m.root[bucket] == nil {
		m.root[bucket] = new(shardBucket[V])
	}
	m.activeBuckets[bucket/64] |= uint64(1) << (bucket % 64)
	if m.root[bucket].values[shard] == nil {
		m.root[bucket].values[shard] = map[uint64]V{}
		m.root[bucket].active[shard/64] |= uint64(1) << (shard % 64)
		if m.smallCount < uint8(len(m.smallActive)) {
			m.smallActive[m.smallCount] = uint16(bucket)<<8 | uint16(shard)
			m.smallCount++
		} else {
			m.smallCount = uint8(len(m.smallActive)) + 1
		}
	}
	if _, exists := m.root[bucket].values[shard][id]; !exists {
		m.length++
	}
	m.root[bucket].values[shard][id] = value
}

func (m *ShardMap[V]) Delete(id uint64) {
	bucket, shard := m.indexes(id)
	if m.root[bucket] == nil {
		return
	}
	if _, exists := m.root[bucket].values[shard][id]; exists {
		delete(m.root[bucket].values[shard], id)
		m.length--
		if len(m.root[bucket].values[shard]) == 0 {
			delete(m.clonedShards, uint16(bucket)<<8|uint16(shard))
			m.root[bucket].values[shard] = nil
			m.root[bucket].active[shard/64] &^= uint64(1) << (shard % 64)
			if m.smallCount <= uint8(len(m.smallActive)) {
				index := uint16(bucket)<<8 | uint16(shard)
				for activeIndex, active := range m.smallActive[:m.smallCount] {
					if active == index {
						m.smallCount--
						m.smallActive[activeIndex] = m.smallActive[m.smallCount]
						break
					}
				}
			} else if m.length == 0 {
				m.smallCount = 0
			}
			if m.root[bucket].active == [shardFanout / 64]uint64{} {
				m.root[bucket] = nil
				m.activeBuckets[bucket/64] &^= uint64(1) << (bucket % 64)
			}
		}
	}
}

func (m ShardMap[V]) Fork() ShardMap[V] {
	return ShardMap[V]{root: m.root, length: m.length, activeBuckets: m.activeBuckets, smallActive: m.smallActive, smallCount: m.smallCount, shardShift: m.shardShift}
}

func (m *ShardMap[V]) CloneShardOnce(id uint64) {
	bucket, shard := m.indexes(id)
	index := uint16(bucket)<<8 | uint16(shard)
	if _, cloned := m.clonedShards[index]; cloned {
		return
	}
	if m.clonedShards == nil {
		root := new([shardFanout]*shardBucket[V])
		*root = *m.root
		m.root = root
		m.clonedShards = map[uint16]struct{}{}
	}
	word, bit := bucket/64, uint64(1)<<(bucket%64)
	if m.clonedBuckets[word]&bit == 0 || m.root[bucket] == nil {
		cloned := new(shardBucket[V])
		if m.root[bucket] != nil {
			*cloned = *m.root[bucket]
		}
		m.root[bucket] = cloned
		m.clonedBuckets[word] |= bit
	}
	m.root[bucket].values[shard] = maps.Clone(m.root[bucket].values[shard])
	m.clonedShards[index] = struct{}{}
}

func shardIndexes(id uint64) (uint8, uint8) {
	index := uint16(id)
	return uint8(index >> 8), uint8(index)
}

func (m ShardMap[V]) indexes(id uint64) (uint8, uint8) {
	return shardIndexes(id >> m.shardShift)
}

const (
	maxValueDepth    = 64
	maxValueElements = 1_000_000
	maxValueBytes    = 64 << 20
)

var (
	ErrValueCycle = errors.New("value contains a cycle")
	ErrValueLimit = errors.New("value exceeds resource limit")
)

type valueWalk struct {
	active   map[valueVisit]struct{}
	elements int
	bytes    int
}

type valueVisit struct {
	kind reflect.Kind
	ptr  uintptr
	len  int
	cap  int
}

const (
	stateFileName = "state.json"
	walFileName   = "wal.log"
	idsFileName   = "ids.json"
)

type GraphState struct {
	DatabaseID       string
	VectorDimensions uint16
	SnapshotBytes    uint64
	AppMetadata      map[string][]byte
	Nodes            PagedMap[*NodeRecord]
	Edges            PagedMap[*EdgeRecord]
	FTS              PagedMap[*FTSRecord]
	Outgoing         PagedMap[*EdgeList]
	Incoming         PagedMap[*EdgeList]
	Labels           StringPostings
	EdgeTypes        StringPostings
	FTSTokens        StringPostings
	NodeProperties   PropertyIndexes
	EdgeProperties   PropertyIndexes
	VectorIndex      VectorIndex
	VectorTombstones PagedMap[[]float32]
	VectorMutations  uint64
	// DerivedIndexWork/LogicalBytes are rebuilt on recovery and maintained by mutations.
	DerivedIndexWork         uint64
	DerivedIndexLogicalBytes uint64
	Streams                  StreamStore
}

// VectorIndex is derived from node properties and is intentionally not persisted.
type VectorIndex struct {
	EntryID  uint64
	MaxLevel int
	Nodes    PagedMap[*VectorIndexNode]
}

type VectorIndexNode struct {
	Level     int
	Neighbors [][]uint64
	Vector    []float32
}

func NewVectorIndex() VectorIndex {
	return VectorIndex{Nodes: NewPagedMap[*VectorIndexNode]()}
}

func (index VectorIndex) Fork() VectorIndex {
	index.Nodes = index.Nodes.Fork()
	return index
}

type FTSRecord struct {
	Text   string
	Tokens []string
}

type NodeRecord struct {
	ID         uint64
	Labels     []string
	Properties map[string]any
}

type EdgeRecord struct {
	ID         uint64
	SourceID   uint64
	TargetID   uint64
	Type       string
	Properties map[string]any
}

type persistedState struct {
	DatabaseID       string                             `json:"database_id,omitempty"`
	VectorDimensions uint16                             `json:"vector_dimensions,omitempty"`
	CommitID         uint64                             `json:"commit_id"`
	NextNodeID       uint64                             `json:"next_node_id"`
	NextEdgeID       uint64                             `json:"next_edge_id"`
	AppMetadata      []persistedAppMetadata             `json:"app_metadata,omitempty"`
	Nodes            []persistedNode                    `json:"nodes"`
	Edges            []persistedEdge                    `json:"edges"`
	FTS              []persistedFTS                     `json:"fts,omitempty"`
	NodeIndexes      []persistedPropertyIndexDefinition `json:"node_property_indexes,omitempty"`
	EdgeIndexes      []persistedPropertyIndexDefinition `json:"edge_property_indexes,omitempty"`
	Streams          persistedStreams                   `json:"streams,omitempty"`
}

type persistedEnvelope struct {
	Magic      string          `json:"magic"`
	Version    uint32          `json:"version"`
	DatabaseID string          `json:"database_id"`
	CommitID   uint64          `json:"commit_id"`
	Checksum   uint32          `json:"checksum"`
	Payload    json.RawMessage `json:"payload"`
}

type persistedIDs struct {
	Magic      string `json:"magic,omitempty"`
	Version    uint32 `json:"version,omitempty"`
	DatabaseID string `json:"database_id,omitempty"`
	NextNodeID uint64 `json:"next_node_id"`
	NextEdgeID uint64 `json:"next_edge_id"`
	Checksum   uint32 `json:"checksum,omitempty"`
}

type walPayload struct {
	Kind     string          `json:"kind"`
	Snapshot *persistedState `json:"snapshot,omitempty"`
	Delta    *persistedDelta `json:"delta,omitempty"`
}

type persistedDelta struct {
	DatabaseID        string                             `json:"database_id"`
	CommitID          uint64                             `json:"commit_id"`
	NextNodeID        uint64                             `json:"next_node_id"`
	NextEdgeID        uint64                             `json:"next_edge_id"`
	UpsertNodes       []persistedNode                    `json:"upsert_nodes,omitempty"`
	DeleteNodes       []uint64                           `json:"delete_nodes,omitempty"`
	UpsertEdges       []persistedEdge                    `json:"upsert_edges,omitempty"`
	DeleteEdges       []uint64                           `json:"delete_edges,omitempty"`
	UpsertFTS         []persistedFTS                     `json:"upsert_fts,omitempty"`
	DeleteFTS         []uint64                           `json:"delete_fts,omitempty"`
	AppMetadata       []persistedAppMetadataChange       `json:"app_metadata,omitempty"`
	Streams           *persistedStreams                  `json:"streams,omitempty"`
	StreamOperations  []persistedStreamOperation         `json:"stream_operations,omitempty"`
	CreateNodeIndexes []persistedPropertyIndexDefinition `json:"create_node_indexes,omitempty"`
	DropNodeIndexes   []persistedPropertyIndexDefinition `json:"drop_node_indexes,omitempty"`
	CreateEdgeIndexes []persistedPropertyIndexDefinition `json:"create_edge_indexes,omitempty"`
	DropEdgeIndexes   []persistedPropertyIndexDefinition `json:"drop_edge_indexes,omitempty"`
}

type GraphDelta struct {
	UpsertNodes       []uint64
	DeleteNodes       []uint64
	UpsertEdges       []uint64
	DeleteEdges       []uint64
	UpsertFTS         []uint64
	DeleteFTS         []uint64
	AppMetadata       []AppMetadataChange
	StreamsChanged    bool
	StreamOperations  []StreamOperation
	CreateNodeIndexes []PropertyIndexDefinition
	DropNodeIndexes   []PropertyIndexDefinition
	CreateEdgeIndexes []PropertyIndexDefinition
	DropEdgeIndexes   []PropertyIndexDefinition
}

type AppMetadataChange struct {
	Key    []byte
	Value  []byte
	Delete bool
}

type persistedAppMetadata struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

type persistedAppMetadataChange struct {
	Key    []byte `json:"key"`
	Value  []byte `json:"value,omitempty"`
	Delete bool   `json:"delete,omitempty"`
}

type persistedNode struct {
	ID         uint64                    `json:"id"`
	Labels     []string                  `json:"labels"`
	Properties map[string]persistedValue `json:"properties"`
}

type persistedEdge struct {
	ID         uint64                    `json:"id"`
	SourceID   uint64                    `json:"source_id"`
	TargetID   uint64                    `json:"target_id"`
	Type       string                    `json:"type"`
	Properties map[string]persistedValue `json:"properties"`
}

type persistedFTS struct {
	NodeID uint64 `json:"node_id"`
	Text   string `json:"text"`
}

type persistedPropertyIndexDefinition struct {
	Scope    string `json:"scope"`
	Property string `json:"property"`
}

type persistedValue struct {
	Kind   string                    `json:"kind"`
	Bool   bool                      `json:"bool,omitempty"`
	Int    int64                     `json:"int,omitempty"`
	Float  float64                   `json:"float,omitempty"`
	String string                    `json:"string,omitempty"`
	Bytes  []byte                    `json:"bytes,omitempty"`
	Vector []float32                 `json:"vector,omitempty"`
	List   []persistedValue          `json:"list,omitempty"`
	Map    map[string]persistedValue `json:"map,omitempty"`
}

func NewGraphState() *GraphState {
	return &GraphState{
		SnapshotBytes:    4096,
		AppMetadata:      map[string][]byte{},
		Nodes:            NewPagedMap[*NodeRecord](),
		Edges:            NewPagedMap[*EdgeRecord](),
		FTS:              NewPagedMap[*FTSRecord](),
		Outgoing:         NewPagedMap[*EdgeList](),
		Incoming:         NewPagedMap[*EdgeList](),
		Labels:           NewStringPostings(),
		EdgeTypes:        NewStringPostings(),
		FTSTokens:        NewStringPostings(),
		NodeProperties:   NewPropertyIndexes(),
		EdgeProperties:   NewPropertyIndexes(),
		VectorIndex:      NewVectorIndex(),
		VectorTombstones: NewPagedMap[[]float32](),
		Streams:          NewStreamStore(),
	}
}

func CloneGraphState(graph *GraphState) *GraphState {
	cloned := NewGraphState()
	cloned.DatabaseID = graph.DatabaseID
	cloned.VectorDimensions = graph.VectorDimensions
	cloned.SnapshotBytes = graph.SnapshotBytes
	cloned.AppMetadata = CloneAppMetadata(graph.AppMetadata)
	cloned.DerivedIndexWork = graph.DerivedIndexWork
	cloned.DerivedIndexLogicalBytes = graph.DerivedIndexLogicalBytes
	for id, node := range graph.Nodes.All() {
		cloned.Nodes.Set(id, &NodeRecord{
			ID:         node.ID,
			Labels:     slices.Clone(node.Labels),
			Properties: ClonePropertyMap(node.Properties),
		})
		for _, label := range node.Labels {
			cloned.Labels.Add(label, id)
		}
	}
	for id, edge := range graph.Edges.All() {
		cloned.Edges.Set(id, &EdgeRecord{
			ID:         edge.ID,
			SourceID:   edge.SourceID,
			TargetID:   edge.TargetID,
			Type:       edge.Type,
			Properties: ClonePropertyMap(edge.Properties),
		})
		cloned.EdgeTypes.Add(edge.Type, id)
	}
	for id, record := range graph.FTS.All() {
		cloned.FTS.Set(id, &FTSRecord{Text: record.Text, Tokens: slices.Clone(record.Tokens)})
		for _, token := range record.Tokens {
			cloned.FTSTokens.Add(token, id)
		}
	}
	for id, edges := range graph.Outgoing.All() {
		var list *EdgeList
		for chunk := range edges.Chunks() {
			for _, edgeID := range chunk {
				if !edges.IsRemoved(edgeID) {
					list = list.Append(edgeID)
				}
			}
		}
		cloned.Outgoing.Set(id, list)
	}
	for id, edges := range graph.Incoming.All() {
		var list *EdgeList
		for chunk := range edges.Chunks() {
			for _, edgeID := range chunk {
				if !edges.IsRemoved(edgeID) {
					list = list.Append(edgeID)
				}
			}
		}
		cloned.Incoming.Set(id, list)
	}
	cloned.VectorIndex.EntryID = graph.VectorIndex.EntryID
	cloned.VectorIndex.MaxLevel = graph.VectorIndex.MaxLevel
	cloned.VectorMutations = graph.VectorMutations
	cloned.Streams = graph.Streams.Fork()
	for definition := range graph.NodeProperties.Definitions() {
		cloned.NodeProperties.Create(definition)
		for id, node := range cloned.Nodes.All() {
			if slices.Contains(node.Labels, definition.Scope) {
				if value, ok := node.Properties[definition.Property]; ok {
					_ = cloned.NodeProperties.Add(definition, value, id)
				}
			}
		}
	}
	for definition := range graph.EdgeProperties.Definitions() {
		cloned.EdgeProperties.Create(definition)
		for id, edge := range cloned.Edges.All() {
			if edge.Type == definition.Scope {
				if value, ok := edge.Properties[definition.Property]; ok {
					_ = cloned.EdgeProperties.Add(definition, value, id)
				}
			}
		}
	}
	for id, node := range graph.VectorIndex.Nodes.All() {
		copyNode := &VectorIndexNode{Level: node.Level, Neighbors: make([][]uint64, len(node.Neighbors)), Vector: slices.Clone(node.Vector)}
		for level := range node.Neighbors {
			copyNode.Neighbors[level] = slices.Clone(node.Neighbors[level])
		}
		cloned.VectorIndex.Nodes.Set(id, copyNode)
	}
	for id, vector := range graph.VectorTombstones.All() {
		cloned.VectorTombstones.Set(id, slices.Clone(vector))
	}
	return cloned
}

func CloneGraphStateShallow(graph *GraphState) *GraphState {
	return &GraphState{
		DatabaseID:               graph.DatabaseID,
		VectorDimensions:         graph.VectorDimensions,
		SnapshotBytes:            graph.SnapshotBytes,
		AppMetadata:              graph.AppMetadata,
		Nodes:                    graph.Nodes.Fork(),
		Edges:                    graph.Edges.Fork(),
		FTS:                      graph.FTS.Fork(),
		Outgoing:                 graph.Outgoing.Fork(),
		Incoming:                 graph.Incoming.Fork(),
		Labels:                   graph.Labels.Fork(),
		EdgeTypes:                graph.EdgeTypes.Fork(),
		FTSTokens:                graph.FTSTokens.Fork(),
		NodeProperties:           graph.NodeProperties.Fork(),
		EdgeProperties:           graph.EdgeProperties.Fork(),
		VectorIndex:              graph.VectorIndex.Fork(),
		VectorTombstones:         graph.VectorTombstones.Fork(),
		VectorMutations:          graph.VectorMutations,
		DerivedIndexWork:         graph.DerivedIndexWork,
		DerivedIndexLogicalBytes: graph.DerivedIndexLogicalBytes,
		Streams:                  graph.Streams.Fork(),
	}
}

func CloneAppMetadata(metadata map[string][]byte) map[string][]byte {
	cloned := make(map[string][]byte, len(metadata))
	for key, value := range metadata {
		cloned[key] = slices.Clone(value)
	}
	return cloned
}

func ClonePropertyMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = CloneValue(value)
	}
	return out
}

func CloneValue(value any) any {
	switch v := value.(type) {
	case nil:
		return nil
	case []byte:
		return append([]byte(nil), v...)
	case []float32:
		return append([]float32(nil), v...)
	case []any:
		cloned := make([]any, len(v))
		for i, item := range v {
			cloned[i] = CloneValue(item)
		}
		return cloned
	case map[string]any:
		return ClonePropertyMap(v)
	default:
		rv := reflect.ValueOf(value)
		switch rv.Kind() {
		case reflect.Slice, reflect.Array:
			list := make([]any, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				list[i] = CloneValue(rv.Index(i).Interface())
			}
			return list
		case reflect.Map:
			if rv.Type().Key().Kind() != reflect.String {
				return value
			}
			out := make(map[string]any, rv.Len())
			iter := rv.MapRange()
			for iter.Next() {
				out[iter.Key().String()] = CloneValue(iter.Value().Interface())
			}
			return out
		default:
			return value
		}
	}
}

func NormalizeValue(value any) (any, error) {
	return normalizeValue(value, 0, newValueWalk())
}

func normalizeValue(value any, depth int, walk *valueWalk) (any, error) {
	if depth > maxValueDepth {
		return nil, fmt.Errorf("%w: nesting exceeds %d", ErrValueLimit, maxValueDepth)
	}
	switch v := value.(type) {
	case nil:
		return nil, nil
	case bool:
		return v, nil
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		if uint64(v) > math.MaxInt64 {
			return nil, fmt.Errorf("uint value %d exceeds int64 range", v)
		}
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		if v > math.MaxInt64 {
			return nil, fmt.Errorf("uint64 value %d exceeds int64 range", v)
		}
		return int64(v), nil
	case float32:
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			return nil, errors.New("non-finite float32 value")
		}
		return float64(v), nil
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, errors.New("non-finite float64 value")
		}
		return v, nil
	case string:
		if !utf8.ValidString(v) {
			return nil, errors.New("string contains invalid UTF-8")
		}
		if err := walk.addBytes(len(v)); err != nil {
			return nil, err
		}
		return v, nil
	case []byte:
		if err := walk.addBytes(len(v)); err != nil {
			return nil, err
		}
		return append([]byte(nil), v...), nil
	case []float32:
		if len(v) > maxValueBytes/4 {
			return nil, fmt.Errorf("%w: byte count exceeds %d", ErrValueLimit, maxValueBytes)
		}
		if err := walk.addBytes(len(v) * 4); err != nil {
			return nil, err
		}
		for _, item := range v {
			if math.IsNaN(float64(item)) || math.IsInf(float64(item), 0) {
				return nil, errors.New("vector contains non-finite value")
			}
		}
		return append([]float32(nil), v...), nil
	case []any:
		if err := walk.enter(reflect.ValueOf(v), len(v)); err != nil {
			return nil, err
		}
		defer walk.leave(reflect.ValueOf(v))
		list := make([]any, len(v))
		for i, item := range v {
			normalized, err := normalizeValue(item, depth+1, walk)
			if err != nil {
				return nil, err
			}
			list[i] = normalized
		}
		return list, nil
	case map[string]any:
		if err := walk.enter(reflect.ValueOf(v), len(v)); err != nil {
			return nil, err
		}
		defer walk.leave(reflect.ValueOf(v))
		out := make(map[string]any, len(v))
		for key, item := range v {
			if !utf8.ValidString(key) {
				return nil, errors.New("map key contains invalid UTF-8")
			}
			if err := walk.addBytes(len(key)); err != nil {
				return nil, err
			}
			normalized, err := normalizeValue(item, depth+1, walk)
			if err != nil {
				return nil, err
			}
			out[key] = normalized
		}
		return out, nil
	}

	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Invalid:
		return nil, nil
	case reflect.Slice, reflect.Array:
		if rv.Kind() == reflect.Slice {
			if err := walk.enter(rv, rv.Len()); err != nil {
				return nil, err
			}
			defer walk.leave(rv)
		} else if err := walk.add(rv.Len()); err != nil {
			return nil, err
		}
		list := make([]any, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			normalized, err := normalizeValue(rv.Index(i).Interface(), depth+1, walk)
			if err != nil {
				return nil, err
			}
			list[i] = normalized
		}
		return list, nil
	case reflect.Map:
		if rv.Type().Key().Kind() != reflect.String {
			return nil, fmt.Errorf("map key type %s is not supported", rv.Type().Key())
		}
		if err := walk.enter(rv, rv.Len()); err != nil {
			return nil, err
		}
		defer walk.leave(rv)
		out := make(map[string]any, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			key := iter.Key().String()
			if !utf8.ValidString(key) {
				return nil, errors.New("map key contains invalid UTF-8")
			}
			if err := walk.addBytes(len(key)); err != nil {
				return nil, err
			}
			normalized, err := normalizeValue(iter.Value().Interface(), depth+1, walk)
			if err != nil {
				return nil, err
			}
			out[key] = normalized
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported value type %T", value)
	}
}

func (walk *valueWalk) add(count int) error {
	walk.elements += count
	if walk.elements > maxValueElements {
		return fmt.Errorf("%w: element count exceeds %d", ErrValueLimit, maxValueElements)
	}
	return nil
}

func (walk *valueWalk) addBytes(count int) error {
	if count > maxValueBytes-walk.bytes {
		return fmt.Errorf("%w: byte count exceeds %d", ErrValueLimit, maxValueBytes)
	}
	walk.bytes += count
	return nil
}

func (walk *valueWalk) enter(value reflect.Value, count int) error {
	if err := walk.add(count); err != nil {
		return err
	}
	visit := valueIdentity(value)
	if visit.ptr == 0 {
		return nil
	}
	if _, ok := walk.active[visit]; ok {
		return ErrValueCycle
	}
	walk.active[visit] = struct{}{}
	return nil
}

func (walk *valueWalk) leave(value reflect.Value) {
	delete(walk.active, valueIdentity(value))
}

func valueIdentity(value reflect.Value) valueVisit {
	visit := valueVisit{kind: value.Kind(), ptr: value.Pointer()}
	if value.Kind() == reflect.Slice {
		visit.len, visit.cap = value.Len(), value.Cap()
	}
	return visit
}

func newValueWalk() *valueWalk {
	return &valueWalk{active: map[valueVisit]struct{}{}}
}

func NormalizeProperties(in map[string]any) (map[string]any, error) {
	if len(in) == 0 {
		return map[string]any{}, nil
	}
	out := make(map[string]any, len(in))
	walk := newValueWalk()
	if err := walk.add(len(in)); err != nil {
		return nil, err
	}
	for key, value := range in {
		if !utf8.ValidString(key) {
			return nil, fmt.Errorf("property %q: key contains invalid UTF-8", key)
		}
		if err := walk.addBytes(len(key)); err != nil {
			return nil, fmt.Errorf("property %q: %w", key, err)
		}
		normalized, err := normalizeValue(value, 0, walk)
		if err != nil {
			return nil, fmt.Errorf("property %q: %w", key, err)
		}
		out[key] = normalized
	}
	return out, nil
}

func SortedNodeIDs(graph *GraphState) []uint64 {
	ids := make([]uint64, 0, graph.Nodes.Len())
	for id := range graph.Nodes.All() {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func SortedEdgeIDs(graph *GraphState) []uint64 {
	ids := make([]uint64, 0, graph.Edges.Len())
	for id := range graph.Edges.All() {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func LabelsMatch(node *NodeRecord, required []string) bool {
	for _, label := range required {
		if !slices.Contains(node.Labels, label) {
			return false
		}
	}
	return true
}

func PropertiesMatch(actual map[string]any, required map[string]any) bool {
	for key, want := range required {
		got, ok := actual[key]
		if !ok {
			return false
		}
		if !reflect.DeepEqual(got, want) {
			return false
		}
	}
	return true
}

func ValidateCreateLabels(labels []string) error {
	seen := make(map[string]struct{}, len(labels))
	for _, label := range labels {
		if label == "" {
			return errors.New("labels must be non-empty")
		}
		if !utf8.ValidString(label) {
			return errors.New("labels must contain valid UTF-8")
		}
		if _, ok := seen[label]; ok {
			return fmt.Errorf("duplicate label %q", label)
		}
		seen[label] = struct{}{}
	}
	return nil
}

func ValidateEdgeType(edgeType string) error {
	if edgeType == "" {
		return errors.New("edge type must be non-empty")
	}
	if !utf8.ValidString(edgeType) {
		return errors.New("edge type must contain valid UTF-8")
	}
	return nil
}

func ValidatePropertyKey(key string) error {
	if !utf8.ValidString(key) {
		return errors.New("property key must contain valid UTF-8")
	}
	return nil
}

func ValidateFTSText(text string) error {
	if !utf8.ValidString(text) {
		return errors.New("FTS text must contain valid UTF-8")
	}
	return nil
}

type DatabaseFiles struct {
	Directory string
	State     string
	WAL       string
	WALBase   string
	IDs       string
}

func DirectoryDatabaseFiles(path string) DatabaseFiles {
	return DatabaseFiles{
		Directory: path,
		State:     filepath.Join(path, stateFileName),
		WAL:       filepath.Join(path, walFileName),
		WALBase:   filepath.Join(path, "wal.base"),
		IDs:       filepath.Join(path, idsFileName),
	}
}

func FlatDatabaseFiles(path string) DatabaseFiles {
	return DatabaseFiles{
		Directory: filepath.Dir(path),
		State:     path,
		WAL:       path + "-wal",
		WALBase:   path + "-wal.base",
		IDs:       path + "-ids",
	}
}

func stateFilePath(dbPath string) string { return DirectoryDatabaseFiles(dbPath).State }
func walFilePath(dbPath string) string   { return DirectoryDatabaseFiles(dbPath).WAL }
func walBaseFilePath(dbPath string) string {
	return DirectoryDatabaseFiles(dbPath).WALBase
}
func idsFilePath(dbPath string) string { return DirectoryDatabaseFiles(dbPath).IDs }

func encodePropertyMap(in map[string]any) (map[string]persistedValue, error) {
	if len(in) == 0 {
		return map[string]persistedValue{}, nil
	}
	normalized, err := NormalizeProperties(in)
	if err != nil {
		return nil, err
	}
	return encodeNormalizedPropertyMap(normalized)
}

func encodeNormalizedPropertyMap(in map[string]any) (map[string]persistedValue, error) {
	if len(in) == 0 {
		return map[string]persistedValue{}, nil
	}
	out := make(map[string]persistedValue, len(in))
	for key, value := range in {
		encoded, err := encodeValue(value)
		if err != nil {
			return nil, err
		}
		out[key] = encoded
	}
	return out, nil
}

func decodePropertyMap(in map[string]persistedValue) (map[string]any, error) {
	if len(in) == 0 {
		return map[string]any{}, nil
	}
	out, err := decodePropertyMapRaw(in)
	if err != nil {
		return nil, err
	}
	return NormalizeProperties(out)
}

func decodePropertyMapRaw(in map[string]persistedValue) (map[string]any, error) {
	if len(in) == 0 {
		return map[string]any{}, nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		decoded, err := decodeValue(value)
		if err != nil {
			return nil, err
		}
		out[key] = decoded
	}
	return out, nil
}

func encodeValue(value any) (persistedValue, error) {
	switch v := value.(type) {
	case nil:
		return persistedValue{Kind: "null"}, nil
	case bool:
		return persistedValue{Kind: "bool", Bool: v}, nil
	case int64:
		return persistedValue{Kind: "int", Int: v}, nil
	case float64:
		return persistedValue{Kind: "float", Float: v}, nil
	case string:
		return persistedValue{Kind: "string", String: v}, nil
	case []byte:
		return persistedValue{Kind: "bytes", Bytes: append([]byte(nil), v...)}, nil
	case []float32:
		return persistedValue{Kind: "vector", Vector: append([]float32(nil), v...)}, nil
	case []any:
		list := make([]persistedValue, len(v))
		for i, item := range v {
			encoded, err := encodeValue(item)
			if err != nil {
				return persistedValue{}, err
			}
			list[i] = encoded
		}
		return persistedValue{Kind: "list", List: list}, nil
	case map[string]any:
		mapped, err := encodeNormalizedPropertyMap(v)
		if err != nil {
			return persistedValue{}, err
		}
		return persistedValue{Kind: "map", Map: mapped}, nil
	default:
		normalized, err := NormalizeValue(value)
		if err != nil {
			return persistedValue{}, err
		}
		if reflect.TypeOf(normalized) == reflect.TypeOf(value) {
			return persistedValue{}, fmt.Errorf("unsupported normalized value type %T", value)
		}
		return encodeValue(normalized)
	}
}

func decodeValue(value persistedValue) (any, error) {
	switch value.Kind {
	case "null":
		return nil, nil
	case "bool":
		return value.Bool, nil
	case "int":
		return value.Int, nil
	case "float":
		return value.Float, nil
	case "string":
		return value.String, nil
	case "bytes":
		return append([]byte(nil), value.Bytes...), nil
	case "vector":
		return append([]float32(nil), value.Vector...), nil
	case "list":
		list := make([]any, len(value.List))
		for i, item := range value.List {
			decoded, err := decodeValue(item)
			if err != nil {
				return nil, err
			}
			list[i] = decoded
		}
		return list, nil
	case "map":
		return decodePropertyMapRaw(value.Map)
	default:
		return nil, fmt.Errorf("unknown stored value kind %q", value.Kind)
	}
}
