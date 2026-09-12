package store

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
