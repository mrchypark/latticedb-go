package store

import (
	"context"
	"fmt"
	"slices"
)

// streamStore writes one record at a time. Checkpoints and
// archive bases must not build a second copy of every stream payload.
func (e *binaryEncoder) streamStore(store StreamStore) {
	names := make([]string, 0, len(store.next))
	for name := range store.next {
		names = append(names, name)
	}
	slices.Sort(names)
	e.u(uint64(len(names)) + 1)
	var offsets uint64
	for _, name := range names {
		if e.err != nil {
			return
		}
		if e.err = ValidateStreamName(name, true); e.err != nil {
			return
		}
		log, next := store.streams[name], max(store.next[name], 1)
		e.str(name)
		e.u(next)
		e.u(log.count + 1)
		var previous uint64
		e.err = store.visitStreamRecords(context.Background(), name, func(record StreamRecord) error {
			if record.Sequence == 0 || record.Sequence != previous+1 && previous != 0 || record.Sequence >= next {
				return fmt.Errorf("invalid stream sequence")
			}
			if previous == 0 && record.Sequence != log.first {
				return fmt.Errorf("invalid stream first sequence")
			}
			if err := ValidateStreamKind(record.Kind); err != nil {
				return err
			}
			payload, err := encodeValue(record.Payload)
			if err != nil {
				return err
			}
			e.u(record.Sequence)
			e.str(record.Kind)
			e.value(payload, 0)
			if e.err != nil {
				return e.err
			}
			previous = record.Sequence
			return nil
		})
		if e.err != nil {
			return
		}
		if log.count != 0 && previous+1 != next {
			e.err = fmt.Errorf("invalid stream next sequence")
			return
		}
		offsets += uint64(len(store.offsets[name]))
	}
	if offsets == 0 {
		e.u(0)
		return
	}
	e.u(offsets + 1)
	for _, name := range names {
		consumers := make([]string, 0, len(store.offsets[name]))
		for consumer := range store.offsets[name] {
			consumers = append(consumers, consumer)
		}
		slices.Sort(consumers)
		for _, consumer := range consumers {
			if e.err != nil {
				return
			}
			if e.err = ValidateStreamName(consumer, true); e.err != nil {
				return
			}
			e.str(name)
			e.str(consumer)
			e.u(store.offsets[name][consumer])
		}
	}
}

// streams encodes a persistedStreams value.
func (e *binaryEncoder) streams(state persistedStreams) {
	encodeBinarySlice(e, state.Streams, func(stream persistedStream) {
		e.str(stream.Name)
		e.u(stream.Next)
		encodeBinarySlice(e, stream.Records, func(record persistedStreamRecord) {
			e.u(record.Sequence)
			e.str(record.Kind)
			e.value(record.Payload, 0)
		})
	})
	encodeBinarySlice(e, state.Offsets, func(offset persistedStreamOffset) {
		e.str(offset.Stream)
		e.str(offset.Consumer)
		e.u(offset.Sequence)
	})
}

// streams decodes a persistedStreams value.
func (d *binaryDecoder) streams() persistedStreams {
	var state persistedStreams
	state.Streams = decodeBinarySlice(d, 48, func() persistedStream {
		var stream persistedStream
		stream.Name = d.str()
		stream.Next = d.u()
		stream.Records = decodeBinarySlice(d, 160, func() persistedStreamRecord {
			var record persistedStreamRecord
			record.Sequence = d.u()
			record.Kind = d.str()
			record.Payload = d.value(0)
			return record
		})
		return stream
	})
	state.Offsets = decodeBinarySlice(d, 40, func() persistedStreamOffset {
		var offset persistedStreamOffset
		offset.Stream = d.str()
		offset.Consumer = d.str()
		offset.Sequence = d.u()
		return offset
	})
	return state
}

// streamOperations encodes persistedStreamOperations.
func (e *binaryEncoder) streamOperations(operations []persistedStreamOperation) {
	encodeBinarySlice(e, operations, func(op persistedStreamOperation) {
		e.str(op.Type)
		e.str(op.Stream)
		e.str(op.Consumer)
		e.u(op.Sequence)
		e.str(op.Kind)
		e.value(op.Payload, 0)
	})
}

// streamOperations decodes persistedStreamOperations.
func (d *binaryDecoder) streamOperations() []persistedStreamOperation {
	return decodeBinarySlice(d, 208, func() persistedStreamOperation {
		var op persistedStreamOperation
		op.Type = d.str()
		op.Stream = d.str()
		op.Consumer = d.str()
		op.Sequence = d.u()
		op.Kind = d.str()
		op.Payload = d.value(0)
		return op
	})
}
