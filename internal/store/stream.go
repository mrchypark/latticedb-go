package store

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"unicode/utf8"
)

// StreamRecord is one durable, per-stream event.
type StreamRecord struct {
	Sequence uint64
	Kind     string
	Payload  any
}

// StreamOperation describes one ordered, transactional stream mutation for a
// WAL delta. It is internal to the repository packages despite being exported
// for the engine package.
type StreamOperation struct {
	Type     string
	Stream   string
	Consumer string
	Sequence uint64
	Kind     string
	Payload  any
}

const streamChunkSize = uint64(64)

type streamLog struct {
	tail  *streamChunk
	first uint64
	count uint64
	bytes uint64
}

type streamChunk struct {
	previous *streamChunk
	skips    []*streamChunk
	records  []StreamRecord
}

// StreamStore keeps system streams separate from graph data. Fork shares its
// immutable state; the first writer copies only the changed stream or offset.
type StreamStore struct {
	streams       map[string]streamLog
	next          map[string]uint64
	offsets       map[string]map[string]uint64
	streamsCloned bool
	nextCloned    bool
	offsetsCloned bool
	clonedStreams map[string]struct{}
	clonedOffsets map[string]struct{}
	logicalBytes  uint64
}

type persistedStreams struct {
	Streams []persistedStream       `json:"streams,omitempty"`
	Offsets []persistedStreamOffset `json:"offsets,omitempty"`
}

type persistedStream struct {
	Name    string                  `json:"name"`
	Next    uint64                  `json:"next"`
	Records []persistedStreamRecord `json:"records,omitempty"`
}

type persistedStreamRecord struct {
	Sequence uint64         `json:"sequence"`
	Kind     string         `json:"kind"`
	Payload  persistedValue `json:"payload"`
}

type persistedStreamOffset struct {
	Stream   string `json:"stream"`
	Consumer string `json:"consumer"`
	Sequence uint64 `json:"sequence"`
}

type persistedStreamOperation struct {
	Type     string         `json:"type"`
	Stream   string         `json:"stream"`
	Consumer string         `json:"consumer,omitempty"`
	Sequence uint64         `json:"sequence"`
	Kind     string         `json:"kind,omitempty"`
	Payload  persistedValue `json:"payload,omitempty"`
}

func NewStreamStore() StreamStore {
	return StreamStore{
		streams: map[string]streamLog{},
		next:    map[string]uint64{},
		offsets: map[string]map[string]uint64{},
	}
}

func ValidateStreamName(value string, allowReserved bool) error {
	if value == "" || len(value) > 255 || !utf8.ValidString(value) {
		return errorsNewStreamName()
	}
	if !allowReserved && strings.HasPrefix(value, "__lattice_") {
		return errorsNewStreamName()
	}
	return nil
}

func ValidateStreamKind(value string) error {
	if value == "" || len(value) > 255 || !utf8.ValidString(value) {
		return errorsNewStreamName()
	}
	return nil
}

func errorsNewStreamName() error { return fmt.Errorf("invalid stream name, kind, or consumer") }

func (store StreamStore) Fork() StreamStore {
	return StreamStore{
		streams:      store.streams,
		next:         store.next,
		offsets:      store.offsets,
		logicalBytes: store.logicalBytes,
	}
}

func (store StreamStore) Read(name string, after uint64, limit uint) []StreamRecord {
	log := store.streams[name]
	if limit == 0 || log.count == 0 {
		return []StreamRecord{}
	}
	if after >= log.first+log.count-1 {
		return []StreamRecord{}
	}
	sequence := after + 1
	if sequence < log.first {
		sequence = log.first
	}
	available := log.count
	if sequence > log.first {
		available -= min(available, sequence-log.first)
	}
	take := min(uint64(limit), available)
	end := sequence + take - 1
	endChunk := log.chunk(end)
	if endChunk == nil {
		return []StreamRecord{}
	}
	if sequence >= endChunk.records[0].Sequence {
		start := int(sequence - endChunk.records[0].Sequence)
		out := make([]StreamRecord, 0, take)
		for _, record := range endChunk.records[start : start+int(take)] {
			out = append(out, StreamRecord{Sequence: record.Sequence, Kind: record.Kind, Payload: CloneValue(record.Payload)})
		}
		return out
	}
	chunks := make([]*streamChunk, 0, (take+streamChunkSize-1)/streamChunkSize+1)
	for chunk := endChunk; chunk != nil && chunk.records[len(chunk.records)-1].Sequence >= sequence; chunk = chunk.previous {
		chunks = append(chunks, chunk)
	}
	slices.Reverse(chunks)
	out := make([]StreamRecord, 0, take)
	for _, chunk := range chunks {
		out = appendStreamRecords(out, chunk.records, sequence, end)
	}
	return out
}

func appendStreamRecords(out []StreamRecord, records []StreamRecord, first, last uint64) []StreamRecord {
	for _, record := range records {
		if record.Sequence >= first && record.Sequence <= last {
			out = append(out, StreamRecord{Sequence: record.Sequence, Kind: record.Kind, Payload: CloneValue(record.Payload)})
		}
	}
	return out
}

func (store StreamStore) StreamBytes(name string) uint64 { return store.streams[name].bytes }

func (store *StreamStore) TrimToBytes(name string, maxBytes uint64) (uint64, bool) {
	log := store.streams[name]
	if log.bytes <= maxBytes || log.count == 0 {
		return 0, false
	}
	target := maxBytes / 2
	var retained uint64
	through := log.first - 1
	for chunk := log.tail; chunk != nil; chunk = chunk.previous {
		for index := len(chunk.records) - 1; index >= 0; index-- {
			recordBytes := streamRecordBytes(chunk.records[index])
			if snapshotAdd(retained, recordBytes) > target {
				store.Trim(name, chunk.records[index].Sequence)
				return chunk.records[index].Sequence, true
			}
			retained = snapshotAdd(retained, recordBytes)
			through = chunk.records[index].Sequence - 1
		}
	}
	if through >= log.first {
		store.Trim(name, through)
		return through, true
	}
	return 0, false
}

func (store StreamStore) NextSequence(name string) uint64 {
	if next := store.next[name]; next != 0 {
		return next
	}
	return 1
}

func (store StreamStore) GetOffset(name, consumer string) (uint64, bool) {
	offset, ok := store.offsets[name][consumer]
	return offset, ok
}

func (store *StreamStore) Publish(name, kind string, payload any) uint64 {
	newStream := store.next[name] == 0
	store.cloneStream(name)
	sequence := store.next[name]
	if sequence == 0 {
		sequence = 1
	}
	store.next[name] = sequence + 1
	log := store.streams[name]
	record := StreamRecord{Sequence: sequence, Kind: kind, Payload: CloneValue(payload)}
	recordBytes := streamRecordBytes(record)
	if log.tail == nil || len(log.tail.records) == int(streamChunkSize) {
		log.tail = newStreamChunk(log.tail, []StreamRecord{record})
	} else {
		records := make([]StreamRecord, len(log.tail.records)+1)
		copy(records, log.tail.records)
		records[len(log.tail.records)] = record
		log.tail = &streamChunk{previous: log.tail.previous, skips: log.tail.skips, records: records}
	}
	if log.count == 0 {
		log.first = sequence
	}
	log.count++
	log.bytes = snapshotAdd(log.bytes, recordBytes)
	store.logicalBytes = snapshotAdd(store.logicalBytes, recordBytes)
	if newStream {
		store.logicalBytes = snapshotAdd(store.logicalBytes, uint64(len(name))+64)
	}
	store.streams[name] = log
	return sequence
}

func (store *StreamStore) SetOffset(name, consumer string, sequence uint64) {
	if store.next[name] == 0 {
		store.cloneStream(name)
		store.next[name] = 1
		store.logicalBytes = snapshotAdd(store.logicalBytes, uint64(len(name))+64)
	}
	_, existed := store.offsets[name][consumer]
	store.cloneOffset(name)
	store.offsets[name][consumer] = sequence
	if !existed {
		store.logicalBytes = snapshotAdd(store.logicalBytes, uint64(len(name)+len(consumer))+48)
	}
}

func (store *StreamStore) Trim(name string, through uint64) {
	store.cloneStream(name)
	if store.next[name] == 0 {
		store.next[name] = 1
		store.logicalBytes = snapshotAdd(store.logicalBytes, uint64(len(name))+64)
	}
	log := store.streams[name]
	if log.count == 0 || through < log.first {
		return
	}
	last := log.first + log.count - 1
	trimmedThrough := min(through, last)
	chunks := make([]*streamChunk, 0, (log.count+streamChunkSize-1)/streamChunkSize)
	for chunk := log.tail; chunk != nil && chunk.records[len(chunk.records)-1].Sequence > trimmedThrough; chunk = chunk.previous {
		chunks = append(chunks, chunk)
	}
	var tail *streamChunk
	var retainedBytes uint64
	for index := len(chunks) - 1; index >= 0; index-- {
		records := chunks[index].records
		start := 0
		for start < len(records) && records[start].Sequence <= trimmedThrough {
			start++
		}
		if start < len(records) {
			retained := slices.Clone(records[start:])
			tail = newStreamChunk(tail, retained)
			for _, record := range retained {
				retainedBytes = snapshotAdd(retainedBytes, streamRecordBytes(record))
			}
		}
	}
	removed := trimmedThrough - log.first + 1
	log.count -= removed
	log.first = trimmedThrough + 1
	if log.count == 0 {
		log.first = 0
	}
	log.tail = tail
	if store.logicalBytes == ^uint64(0) || log.bytes > store.logicalBytes {
		log.bytes = retainedBytes
		store.streams[name] = log
		store.logicalBytes = calculateStreamStoreBytes(*store)
		return
	}
	store.logicalBytes = snapshotAdd(store.logicalBytes-log.bytes, retainedBytes)
	log.bytes = retainedBytes
	store.streams[name] = log
}

func newStreamChunk(previous *streamChunk, records []StreamRecord) *streamChunk {
	chunk := &streamChunk{previous: previous, records: records}
	if previous == nil {
		return chunk
	}
	chunk.skips = append(chunk.skips, previous)
	for level := 1; ; level++ {
		ancestor := chunk.skips[level-1]
		if len(ancestor.skips) < level {
			break
		}
		chunk.skips = append(chunk.skips, ancestor.skips[level-1])
	}
	return chunk
}

func (log streamLog) chunk(sequence uint64) *streamChunk {
	chunk := log.tail
	if chunk == nil || sequence > chunk.records[len(chunk.records)-1].Sequence {
		return nil
	}
	for level := len(chunk.skips) - 1; level >= 0; level-- {
		if level >= len(chunk.skips) {
			continue
		}
		candidate := chunk.skips[level]
		if candidate.records[len(candidate.records)-1].Sequence >= sequence {
			chunk = candidate
		}
	}
	if sequence < chunk.records[0].Sequence {
		return nil
	}
	return chunk
}

func BuildPersistedStreamOperations(operations []StreamOperation) ([]persistedStreamOperation, error) {
	encoded := make([]persistedStreamOperation, 0, len(operations))
	for _, operation := range operations {
		if err := ValidateStreamName(operation.Stream, true); err != nil {
			return nil, err
		}
		persisted := persistedStreamOperation{Type: operation.Type, Stream: operation.Stream, Consumer: operation.Consumer, Sequence: operation.Sequence, Kind: operation.Kind}
		switch operation.Type {
		case "publish":
			if operation.Sequence == 0 {
				return nil, fmt.Errorf("invalid stream sequence")
			}
			if err := ValidateStreamKind(operation.Kind); err != nil {
				return nil, err
			}
			payload, err := encodeValue(operation.Payload)
			if err != nil {
				return nil, err
			}
			persisted.Payload = payload
		case "offset":
			if err := ValidateStreamName(operation.Consumer, true); err != nil {
				return nil, err
			}
		case "trim":
		default:
			return nil, fmt.Errorf("invalid stream operation")
		}
		encoded = append(encoded, persisted)
	}
	return encoded, nil
}

func ApplyPersistedStreamOperations(store StreamStore, operations []persistedStreamOperation) (StreamStore, error) {
	updated := store.Fork()
	for _, operation := range operations {
		if err := ValidateStreamName(operation.Stream, true); err != nil {
			return StreamStore{}, err
		}
		switch operation.Type {
		case "publish":
			if operation.Sequence == 0 || operation.Sequence != updated.NextSequence(operation.Stream) {
				return StreamStore{}, fmt.Errorf("invalid stream sequence")
			}
			if err := ValidateStreamKind(operation.Kind); err != nil {
				return StreamStore{}, err
			}
			payload, err := decodeValue(operation.Payload)
			if err != nil {
				return StreamStore{}, err
			}
			updated.Publish(operation.Stream, operation.Kind, payload)
		case "offset":
			if err := ValidateStreamName(operation.Consumer, true); err != nil {
				return StreamStore{}, err
			}
			updated.SetOffset(operation.Stream, operation.Consumer, operation.Sequence)
		case "trim":
			updated.Trim(operation.Stream, operation.Sequence)
		default:
			return StreamStore{}, fmt.Errorf("invalid stream operation")
		}
	}
	return updated, nil
}

func (store *StreamStore) cloneStream(name string) {
	if !store.streamsCloned {
		store.streams = maps.Clone(store.streams)
		store.streamsCloned = true
	}
	if !store.nextCloned {
		store.next = maps.Clone(store.next)
		store.nextCloned = true
	}
	if store.clonedStreams == nil {
		store.clonedStreams = map[string]struct{}{}
	}
	if _, ok := store.clonedStreams[name]; !ok {
		store.clonedStreams[name] = struct{}{}
	}
}

func (store *StreamStore) cloneOffset(name string) {
	if !store.offsetsCloned {
		store.offsets = maps.Clone(store.offsets)
		store.offsetsCloned = true
	}
	if store.clonedOffsets == nil {
		store.clonedOffsets = map[string]struct{}{}
	}
	if _, ok := store.clonedOffsets[name]; !ok {
		store.offsets[name] = maps.Clone(store.offsets[name])
		if store.offsets[name] == nil {
			store.offsets[name] = map[string]uint64{}
		}
		store.clonedOffsets[name] = struct{}{}
	}
}

func buildPersistedStreams(store StreamStore) (persistedStreams, error) {
	state := persistedStreams{Streams: make([]persistedStream, 0, len(store.next))}
	names := make([]string, 0, len(store.next))
	for name := range store.next {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		if err := ValidateStreamName(name, true); err != nil {
			return persistedStreams{}, err
		}
		log := store.streams[name]
		stream := persistedStream{Name: name, Next: store.next[name], Records: make([]persistedStreamRecord, 0, log.count)}
		var previous uint64
		chunks := make([]*streamChunk, 0, (log.count+streamChunkSize-1)/streamChunkSize)
		for chunk := log.tail; chunk != nil; chunk = chunk.previous {
			chunks = append(chunks, chunk)
		}
		for index := len(chunks) - 1; index >= 0; index-- {
			for _, record := range chunks[index].records {
				if record.Sequence == 0 || record.Sequence <= previous || record.Sequence >= stream.Next || previous != 0 && record.Sequence != previous+1 {
					return persistedStreams{}, fmt.Errorf("invalid stream sequence")
				}
				if err := ValidateStreamKind(record.Kind); err != nil {
					return persistedStreams{}, err
				}
				payload, err := encodeValue(record.Payload)
				if err != nil {
					return persistedStreams{}, err
				}
				stream.Records = append(stream.Records, persistedStreamRecord{Sequence: record.Sequence, Kind: record.Kind, Payload: payload})
				previous = record.Sequence
			}
		}
		if previous != 0 && stream.Next != previous+1 {
			return persistedStreams{}, fmt.Errorf("invalid stream next sequence")
		}
		if stream.Next == 0 {
			stream.Next = 1
		}
		state.Streams = append(state.Streams, stream)
	}
	for _, stream := range names {
		consumers := make([]string, 0, len(store.offsets[stream]))
		for consumer := range store.offsets[stream] {
			consumers = append(consumers, consumer)
		}
		slices.Sort(consumers)
		for _, consumer := range consumers {
			if err := ValidateStreamName(consumer, true); err != nil {
				return persistedStreams{}, err
			}
			state.Offsets = append(state.Offsets, persistedStreamOffset{Stream: stream, Consumer: consumer, Sequence: store.offsets[stream][consumer]})
		}
	}
	return state, nil
}

func decodePersistedStreams(state persistedStreams) (StreamStore, error) {
	store := NewStreamStore()
	for _, stream := range state.Streams {
		if err := ValidateStreamName(stream.Name, true); err != nil {
			return StreamStore{}, err
		}
		if _, exists := store.streams[stream.Name]; exists || stream.Next == 0 {
			return StreamStore{}, fmt.Errorf("invalid persisted stream")
		}
		var previous uint64
		records := make([]StreamRecord, 0, len(stream.Records))
		for _, record := range stream.Records {
			if record.Sequence == 0 || record.Sequence <= previous || record.Sequence >= stream.Next || previous != 0 && record.Sequence != previous+1 {
				return StreamStore{}, fmt.Errorf("invalid persisted stream sequence")
			}
			if err := ValidateStreamKind(record.Kind); err != nil {
				return StreamStore{}, err
			}
			payload, err := decodeValue(record.Payload)
			if err != nil {
				return StreamStore{}, err
			}
			records = append(records, StreamRecord{Sequence: record.Sequence, Kind: record.Kind, Payload: payload})
			previous = record.Sequence
		}
		if previous != 0 && stream.Next != previous+1 {
			return StreamStore{}, fmt.Errorf("invalid persisted stream next sequence")
		}
		var log streamLog
		for start := 0; start < len(records); start += int(streamChunkSize) {
			end := min(start+int(streamChunkSize), len(records))
			log.tail = newStreamChunk(log.tail, slices.Clone(records[start:end]))
		}
		if len(records) != 0 {
			log.first = records[0].Sequence
			log.count = uint64(len(records))
		}
		for _, record := range records {
			log.bytes = snapshotAdd(log.bytes, streamRecordBytes(record))
		}
		store.streams[stream.Name] = log
		store.next[stream.Name] = stream.Next
	}
	for _, offset := range state.Offsets {
		if err := ValidateStreamName(offset.Stream, true); err != nil {
			return StreamStore{}, err
		}
		if err := ValidateStreamName(offset.Consumer, true); err != nil {
			return StreamStore{}, err
		}
		if _, exists := store.streams[offset.Stream]; !exists {
			store.streams[offset.Stream] = streamLog{}
			store.next[offset.Stream] = 1
		}
		if store.offsets[offset.Stream] == nil {
			store.offsets[offset.Stream] = map[string]uint64{}
		}
		if _, exists := store.offsets[offset.Stream][offset.Consumer]; exists {
			return StreamStore{}, fmt.Errorf("duplicate persisted stream offset")
		}
		store.offsets[offset.Stream][offset.Consumer] = offset.Sequence
	}
	store.logicalBytes = calculateStreamStoreBytes(store)
	return store, nil
}

func streamStoreBytes(store StreamStore) uint64 {
	return store.logicalBytes
}

func calculateStreamStoreBytes(store StreamStore) uint64 {
	var size uint64
	for name, log := range store.streams {
		size = snapshotAdd(size, uint64(len(name))+64)
		for chunk := log.tail; chunk != nil; chunk = chunk.previous {
			for _, record := range chunk.records {
				size = snapshotAdd(size, snapshotAdd(uint64(len(record.Kind))+48, estimateValueBytes(record.Payload)))
			}
		}
	}
	for stream, consumers := range store.offsets {
		for consumer := range consumers {
			size = snapshotAdd(size, uint64(len(stream)+len(consumer))+48)
		}
	}
	return size
}

func streamRecordBytes(record StreamRecord) uint64 {
	return snapshotAdd(uint64(len(record.Kind))+48, estimateValueBytes(record.Payload))
}
