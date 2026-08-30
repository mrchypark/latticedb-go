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

// StreamStore keeps system streams separate from graph data. Fork shares its
// immutable state; the first writer copies only the changed stream or offset.
type StreamStore struct {
	streams       map[string][]StreamRecord
	next          map[string]uint64
	offsets       map[string]map[string]uint64
	streamsCloned bool
	nextCloned    bool
	offsetsCloned bool
	clonedStreams map[string]struct{}
	clonedOffsets map[string]struct{}
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
		streams: map[string][]StreamRecord{},
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
		streams: store.streams,
		next:    store.next,
		offsets: store.offsets,
	}
}

func (store StreamStore) Read(name string, after uint64, limit uint) []StreamRecord {
	records := store.streams[name]
	start := 0
	for start < len(records) && records[start].Sequence <= after {
		start++
	}
	end := len(records)
	if uint64(limit) < uint64(end-start) {
		end = start + int(limit)
	}
	out := make([]StreamRecord, 0, end-start)
	for _, record := range records[start:end] {
		out = append(out, StreamRecord{Sequence: record.Sequence, Kind: record.Kind, Payload: CloneValue(record.Payload)})
	}
	return out
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
	store.cloneStream(name)
	sequence := store.next[name]
	if sequence == 0 {
		sequence = 1
	}
	store.next[name] = sequence + 1
	store.streams[name] = append(store.streams[name], StreamRecord{Sequence: sequence, Kind: kind, Payload: CloneValue(payload)})
	return sequence
}

func (store *StreamStore) SetOffset(name, consumer string, sequence uint64) {
	store.cloneStream(name)
	if store.next[name] == 0 {
		store.streams[name] = nil
		store.next[name] = 1
	}
	store.cloneOffset(name)
	store.offsets[name][consumer] = sequence
}

func (store *StreamStore) Trim(name string, through uint64) {
	store.cloneStream(name)
	records := store.streams[name]
	index := 0
	for index < len(records) && records[index].Sequence <= through {
		index++
	}
	store.streams[name] = slices.Clone(records[index:])
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
		store.streams[name] = slices.Clone(store.streams[name])
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
	state := persistedStreams{Streams: make([]persistedStream, 0, len(store.streams))}
	names := make([]string, 0, len(store.streams))
	for name := range store.streams {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		if err := ValidateStreamName(name, true); err != nil {
			return persistedStreams{}, err
		}
		records := store.streams[name]
		stream := persistedStream{Name: name, Next: store.next[name], Records: make([]persistedStreamRecord, 0, len(records))}
		var previous uint64
		for _, record := range records {
			if record.Sequence == 0 || record.Sequence <= previous || record.Sequence >= stream.Next {
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
			if record.Sequence == 0 || record.Sequence <= previous || record.Sequence >= stream.Next {
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
		store.streams[stream.Name] = records
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
			store.streams[offset.Stream] = nil
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
	return store, nil
}

func streamStoreBytes(store StreamStore) uint64 {
	var size uint64
	for name, records := range store.streams {
		size = snapshotAdd(size, uint64(len(name))+64)
		for _, record := range records {
			size = snapshotAdd(size, snapshotAdd(uint64(len(record.Kind))+48, estimateValueBytes(record.Payload)))
		}
	}
	for stream, consumers := range store.offsets {
		for consumer := range consumers {
			size = snapshotAdd(size, uint64(len(stream)+len(consumer))+48)
		}
	}
	return size
}
