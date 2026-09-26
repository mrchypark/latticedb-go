package store

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
)

const (
	pageStreamCatalog = "stream-catalog"
	pageStreamRecords = "stream-records"
	pageStreamOffsets = "stream-offsets"
)

type pageStreamSource struct{ graph *PageGraph }

type pageStreamMeta struct {
	next, first, count, bytes, snapshotBytes uint64
}

func encodePageStreamMeta(meta pageStreamMeta) []byte {
	data, _ := encodePageRecord(4, func(e *binaryEncoder) {
		e.u(meta.next)
		e.u(meta.first)
		e.u(meta.count)
		e.u(meta.bytes)
		e.u(meta.snapshotBytes)
	})
	return data
}

func decodePageStreamMeta(data []byte) (pageStreamMeta, error) {
	d, err := decodePageRecord(data, 4, 64)
	if err != nil {
		return pageStreamMeta{}, fmt.Errorf("invalid page stream catalog record: %w", err)
	}
	meta := pageStreamMeta{next: d.u(), first: d.u(), count: d.u(), bytes: d.u(), snapshotBytes: d.u()}
	if err := d.finish(); err != nil {
		return pageStreamMeta{}, fmt.Errorf("invalid page stream catalog record: %w", err)
	}
	if meta.next == 0 || (meta.count == 0 && meta.first != 0) || (meta.count != 0 && (meta.first == 0 || meta.count > ^uint64(0)-meta.first || meta.first+meta.count != meta.next)) {
		return pageStreamMeta{}, errors.New("invalid page stream catalog range")
	}
	return meta, nil
}

func pageStreamRecordKey(name string, sequence uint64) []byte {
	return pageStringID(name, sequence)
}

func pageStreamOffsetKey(stream, consumer string) []byte {
	return append(pageStringPrefix(stream), pageStringPrefix(consumer)...)
}

func encodePageStreamRecord(record StreamRecord) ([]byte, error) {
	payload, err := encodeValue(record.Payload)
	if err != nil {
		return nil, err
	}
	return encodePageRecord(3, func(e *binaryEncoder) {
		e.u(record.Sequence)
		e.str(record.Kind)
		e.value(payload, 0)
	})
}

func decodePageStreamRecord(data []byte, name string, sequence, maxBytes uint64) (StreamRecord, error) {
	d, err := decodePageRecord(data, 3, maxBytes)
	if err != nil {
		return StreamRecord{}, err
	}
	record := StreamRecord{Sequence: d.u(), Kind: d.str()}
	payload := d.value(0)
	if err := d.finish(); err != nil {
		return StreamRecord{}, fmt.Errorf("decode page stream record: %w", err)
	}
	if record.Sequence != sequence || record.Sequence == 0 {
		return StreamRecord{}, errors.New("page stream key does not match record")
	}
	if err := ValidateStreamKind(record.Kind); err != nil {
		return StreamRecord{}, err
	}
	record.Payload, err = decodeStreamValue(name, payload)
	return record, err
}

// LoadStreams reads only stream names, offsets, and retention counters. Record
// payloads remain in the transaction and are fetched only when read.
func (graph *PageGraph) LoadStreams(ctx context.Context) (StreamStore, error) {
	if ctx == nil {
		return StreamStore{}, errors.New("nil stream load context")
	}
	store := NewStreamStore()
	store.page = &pageStreamSource{graph: graph}
	metas := make(map[string]pageStreamMeta)
	err := graph.Tx.Scan(ctx, pageStreamCatalog, nil, nil, func(key, value []byte) error {
		name := string(key)
		if err := ValidateStreamName(name, true); err != nil {
			return err
		}
		meta, err := decodePageStreamMeta(value)
		if err != nil {
			return err
		}
		metas[name] = meta
		store.next[name] = meta.next
		store.streams[name] = streamLog{first: meta.first, count: meta.count, diskCount: meta.count, bytes: meta.bytes, snapshotBytes: meta.snapshotBytes}
		store.logicalBytes = snapshotAdd(store.logicalBytes, uint64(len(name))+64)
		store.logicalBytes = snapshotAdd(store.logicalBytes, meta.bytes)
		store.snapshotBytes = snapshotAdd(store.snapshotBytes, streamSnapshotBytes(name))
		store.snapshotBytes = snapshotAdd(store.snapshotBytes, meta.snapshotBytes)
		return nil
	})
	if err != nil {
		return StreamStore{}, err
	}
	err = graph.Tx.Scan(ctx, pageStreamOffsets, nil, nil, func(key, value []byte) error {
		stream, consumer, err := decodePageStreamOffsetKey(key)
		if err != nil {
			return err
		}
		if _, ok := metas[stream]; !ok {
			return errors.New("page stream offset references missing stream")
		}
		if len(value) != 8 {
			return errors.New("invalid page stream offset")
		}
		if store.offsets[stream] == nil {
			store.offsets[stream] = map[string]uint64{}
		}
		store.offsets[stream][consumer] = binary.BigEndian.Uint64(value)
		store.logicalBytes = snapshotAdd(store.logicalBytes, uint64(len(stream)+len(consumer))+48)
		store.snapshotBytes = snapshotAdd(store.snapshotBytes, streamOffsetSnapshotBytes(stream, consumer))
		return nil
	})
	if err != nil {
		return StreamStore{}, err
	}
	return store, nil
}

func decodePageStreamOffsetKey(key []byte) (string, string, error) {
	if len(key) < 8 {
		return "", "", errors.New("invalid page stream offset key")
	}
	streamLength := int(binary.BigEndian.Uint32(key))
	if streamLength > len(key)-8 {
		return "", "", errors.New("invalid page stream offset key")
	}
	consumerStart := 4 + streamLength
	consumerLength := int(binary.BigEndian.Uint32(key[consumerStart:]))
	if consumerStart+4+consumerLength != len(key) {
		return "", "", errors.New("invalid page stream offset key")
	}
	stream, consumer := string(key[4:consumerStart]), string(key[consumerStart+4:])
	if err := ValidateStreamName(stream, true); err != nil {
		return "", "", err
	}
	if err := ValidateStreamName(consumer, true); err != nil {
		return "", "", err
	}
	return stream, consumer, nil
}

func (source *pageStreamSource) record(ctx context.Context, name string, sequence uint64) (StreamRecord, error) {
	if err := ctx.Err(); err != nil {
		return StreamRecord{}, err
	}
	data, err := source.graph.Tx.Get(pageStreamRecords, pageStreamRecordKey(name, sequence))
	if err != nil {
		return StreamRecord{}, err
	}
	if data == nil {
		return StreamRecord{}, fmt.Errorf("missing page stream record %q sequence %d", name, sequence)
	}
	if err := ctx.Err(); err != nil {
		return StreamRecord{}, err
	}
	return decodePageStreamRecord(data, name, sequence, source.graph.recordLimit())
}

func (store StreamStore) pageRecord(ctx context.Context, name string, sequence uint64) (StreamRecord, error) {
	log := store.streams[name]
	if sequence < log.first+log.diskCount {
		return store.page.record(ctx, name, sequence)
	}
	return store.overlayRecord(name, sequence)
}

func (store StreamStore) readPageBounded(ctx context.Context, name string, after uint64, limit uint, maxBytes uint64) (StreamReadResult, error) {
	log := store.streams[name]
	start := max(after+1, log.first)
	end := log.first + log.count - 1
	result := StreamReadResult{Records: make([]StreamRecord, 0, min(uint64(limit), log.count))}
	for sequence := start; sequence <= end && uint(len(result.Records)) < limit; sequence++ {
		record, err := store.pageRecord(ctx, name, sequence)
		if err != nil {
			return result, err
		}
		if err := result.appendCtx(ctx, record, maxBytes); err != nil {
			return result, err
		}
		if result.ByteLimited {
			break
		}
	}
	return result, ctx.Err()
}

func (store StreamStore) overlayRecord(name string, sequence uint64) (StreamRecord, error) {
	for chunk := store.streams[name].tail; chunk != nil; chunk = chunk.previous {
		for _, record := range chunk.records {
			if record.Sequence == sequence {
				return record, nil
			}
		}
	}
	return StreamRecord{}, fmt.Errorf("missing appended stream record %q sequence %d", name, sequence)
}

func (store StreamStore) visitStreamRecords(ctx context.Context, name string, visit func(StreamRecord) error) error {
	log := store.streams[name]
	for sequence := log.first; sequence < log.first+log.count; sequence++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		var record StreamRecord
		var err error
		if store.page != nil && sequence < log.first+log.diskCount {
			record, err = store.page.record(ctx, name, sequence)
		} else if store.page != nil {
			record, err = store.overlayRecord(name, sequence)
		} else if chunk := log.chunk(sequence); chunk == nil {
			err = errors.New("invalid stream chunk")
		} else {
			record = chunk.records[sequence-chunk.records[0].Sequence]
		}
		if err != nil {
			return err
		}
		if err := visit(record); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (store *StreamStore) trimPage(ctx context.Context, name string, through uint64) error {
	log := store.streams[name]
	if log.count == 0 || through < log.first {
		return nil
	}
	trimmedThrough := min(through, log.first+log.count-1)
	var removedBytes, removedSnapshot uint64
	for sequence := log.first; sequence <= trimmedThrough; sequence++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		record, err := store.pageRecord(ctx, name, sequence)
		if err != nil {
			return err
		}
		removedBytes = snapshotAdd(removedBytes, streamRecordBytes(record))
		removedSnapshot = snapshotAdd(removedSnapshot, streamRecordSnapshotBytes(record))
	}
	removed := trimmedThrough - log.first + 1
	log.count -= removed
	log.diskCount -= min(log.diskCount, removed)
	log.first = trimmedThrough + 1
	log.bytes -= min(log.bytes, removedBytes)
	log.snapshotBytes -= min(log.snapshotBytes, removedSnapshot)
	store.logicalBytes -= min(store.logicalBytes, removedBytes)
	store.snapshotBytes -= min(store.snapshotBytes, removedSnapshot)
	if log.count == 0 {
		log.first = 0
		log.tail = nil
	}
	store.streams[name] = log
	return nil
}

// ApplyStreamOperations writes stream deltas into the graph's existing physical
// transaction. On error, the caller must roll back that transaction.
func (graph *PageGraph) ApplyStreamOperations(ctx context.Context, operations []StreamOperation) error {
	if ctx == nil {
		return errors.New("nil stream apply context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	metas := map[string]pageStreamMeta{}
	for _, op := range operations {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := ValidateStreamName(op.Stream, true); err != nil {
			return err
		}
		meta, ok := metas[op.Stream]
		if !ok {
			data, err := graph.Tx.Get(pageStreamCatalog, []byte(op.Stream))
			if err != nil {
				return err
			}
			if data == nil {
				meta.next = 1
			} else if meta, err = decodePageStreamMeta(data); err != nil {
				return err
			}
		}
		switch op.Type {
		case "publish":
			if meta.next == ^uint64(0) || op.Sequence != meta.next {
				return fmt.Errorf("invalid stream sequence: got %d, want %d", op.Sequence, meta.next)
			}
			if err := ValidateStreamKind(op.Kind); err != nil {
				return err
			}
			data, err := encodePageStreamRecord(StreamRecord{Sequence: op.Sequence, Kind: op.Kind, Payload: op.Payload})
			if err != nil {
				return err
			}
			if err := graph.Tx.Put(pageStreamRecords, pageStreamRecordKey(op.Stream, op.Sequence), data); err != nil {
				return err
			}
			record := StreamRecord{Sequence: op.Sequence, Kind: op.Kind, Payload: op.Payload}
			meta.bytes = snapshotAdd(meta.bytes, streamRecordBytes(record))
			meta.snapshotBytes = snapshotAdd(meta.snapshotBytes, streamRecordSnapshotBytes(record))
			if meta.count == 0 {
				meta.first = op.Sequence
			}
			meta.count++
			meta.next++
		case "offset":
			if err := ValidateStreamName(op.Consumer, true); err != nil {
				return err
			}
			var value [8]byte
			binary.BigEndian.PutUint64(value[:], op.Sequence)
			if err := graph.Tx.Put(pageStreamOffsets, pageStreamOffsetKey(op.Stream, op.Consumer), value[:]); err != nil {
				return err
			}
		case "trim":
			last := meta.first + meta.count - 1
			if meta.count != 0 && op.Sequence >= meta.first {
				through := min(op.Sequence, last)
				for sequence := meta.first; sequence <= through; sequence++ {
					if err := ctx.Err(); err != nil {
						return err
					}
					data, err := graph.Tx.Get(pageStreamRecords, pageStreamRecordKey(op.Stream, sequence))
					if err != nil {
						return err
					}
					if data == nil {
						return fmt.Errorf("missing page stream record %q sequence %d", op.Stream, sequence)
					}
					record, err := decodePageStreamRecord(data, op.Stream, sequence, graph.recordLimit())
					if err != nil {
						return err
					}
					meta.bytes -= min(meta.bytes, streamRecordBytes(record))
					meta.snapshotBytes -= min(meta.snapshotBytes, streamRecordSnapshotBytes(record))
					if err := graph.Tx.Delete(pageStreamRecords, pageStreamRecordKey(op.Stream, sequence)); err != nil {
						return err
					}
				}
				meta.count -= through - meta.first + 1
				meta.first = through + 1
				if meta.count == 0 {
					meta.first = 0
				}
			}
		default:
			return fmt.Errorf("invalid stream operation %q", op.Type)
		}
		metas[op.Stream] = meta
		if err := graph.Tx.Put(pageStreamCatalog, []byte(op.Stream), encodePageStreamMeta(meta)); err != nil {
			return err
		}
	}
	return nil
}

// ImportStreams installs a complete initial stream state in this transaction.
// It is intended for the one-time migration from persisted in-memory streams.
func (graph *PageGraph) ImportStreams(ctx context.Context, store StreamStore) error {
	if ctx == nil {
		return errors.New("nil stream import context")
	}
	exists := false
	if err := graph.Tx.Scan(ctx, pageStreamCatalog, nil, nil, func(_, _ []byte) error {
		exists = true
		return io.EOF
	}); err != nil {
		return err
	}
	if exists {
		return errors.New("page streams already exist")
	}
	names := make([]string, 0, len(store.next))
	for name := range store.next {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := ValidateStreamName(name, true); err != nil {
			return err
		}
		log := store.streams[name]
		meta := pageStreamMeta{next: max(store.next[name], 1), first: log.first, count: log.count, bytes: log.bytes, snapshotBytes: log.snapshotBytes}
		if _, err := decodePageStreamMeta(encodePageStreamMeta(meta)); err != nil {
			return err
		}
		if err := store.visitStreamRecords(ctx, name, func(record StreamRecord) error {
			data, err := encodePageStreamRecord(record)
			if err != nil {
				return err
			}
			return graph.Tx.Put(pageStreamRecords, pageStreamRecordKey(name, record.Sequence), data)
		}); err != nil {
			return err
		}
		if err := graph.Tx.Put(pageStreamCatalog, []byte(name), encodePageStreamMeta(meta)); err != nil {
			return err
		}
	}
	for _, stream := range names {
		for consumer, sequence := range store.offsets[stream] {
			if err := ValidateStreamName(consumer, true); err != nil {
				return err
			}
			var value [8]byte
			binary.BigEndian.PutUint64(value[:], sequence)
			if err := graph.Tx.Put(pageStreamOffsets, pageStreamOffsetKey(stream, consumer), value[:]); err != nil {
				return err
			}
		}
	}
	return nil
}
