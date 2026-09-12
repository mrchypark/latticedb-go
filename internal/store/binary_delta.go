package store

import "errors"

// binary_delta.go implements binary encoding/decoding for persistedDelta.
// Field order matches the struct declaration in store.go exactly.

func (e *binaryEncoder) delta(d persistedDelta) {
	e.str(d.DatabaseID)
	e.u(d.CommitID)
	e.u(d.NextNodeID)
	e.u(d.NextEdgeID)

	encodeBinarySlice(e, d.UpsertNodes, e.node)
	e.ids(d.DeleteNodes)
	encodeBinarySlice(e, d.UpsertEdges, e.edge)
	e.ids(d.DeleteEdges)
	encodeBinarySlice(e, d.UpsertFTS, e.fts)
	e.ids(d.DeleteFTS)

	encodeBinarySlice(e, d.AppMetadata, func(am persistedAppMetadataChange) {
		e.bytes(am.Key)
		e.bytes(am.Value)
		if am.Delete {
			e.tag(1)
		} else {
			e.tag(0)
		}
	})

	if d.Streams == nil {
		e.tag(0)
	} else {
		e.tag(1)
		e.streams(*d.Streams)
	}
	e.streamOperations(d.StreamOperations)

	e.indexes(d.CreateNodeIndexes)
	e.indexes(d.DropNodeIndexes)
	e.indexes(d.CreateEdgeIndexes)
	e.indexes(d.DropEdgeIndexes)

	encodeBinarySlice(e, d.NodePropertyChanges, func(pc persistedPropertyChange) {
		e.u(pc.ID)
		e.properties(pc.Set, 0)
		e.strings(pc.Remove)
	})
	encodeBinarySlice(e, d.EdgePropertyChanges, func(pc persistedPropertyChange) {
		e.u(pc.ID)
		e.properties(pc.Set, 0)
		e.strings(pc.Remove)
	})
}

func (d *binaryDecoder) delta() persistedDelta {
	var delta persistedDelta

	delta.DatabaseID = d.str()
	delta.CommitID = d.u()
	delta.NextNodeID = d.u()
	delta.NextEdgeID = d.u()
	if d.err != nil {
		return delta
	}

	delta.UpsertNodes = decodeBinarySlice(d, 40, d.node)
	delta.DeleteNodes = d.ids()
	delta.UpsertEdges = decodeBinarySlice(d, 48, d.edge)
	delta.DeleteEdges = d.ids()
	delta.UpsertFTS = decodeBinarySlice(d, 24, d.fts)
	delta.DeleteFTS = d.ids()

	delta.AppMetadata = decodeBinarySlice(d, 56, func() persistedAppMetadataChange {
		var am persistedAppMetadataChange
		am.Key = d.bytes()
		am.Value = d.bytes()
		if d.err != nil {
			return am
		}
		tag, err := d.ReadByte()
		if err != nil {
			return am
		}
		if tag > 1 {
			d.err = errors.New("invalid binary app metadata delete flag")
			return am
		}
		am.Delete = tag == 1
		return am
	})

	presence, err := d.ReadByte()
	if err != nil {
		return delta
	}
	if presence > 1 {
		d.err = errors.New("invalid binary streams presence flag")
		return delta
	}
	if presence == 1 {
		ps := d.streams()
		delta.Streams = &ps
	}

	delta.StreamOperations = d.streamOperations()

	delta.CreateNodeIndexes = d.indexes()
	delta.DropNodeIndexes = d.indexes()
	delta.CreateEdgeIndexes = d.indexes()
	delta.DropEdgeIndexes = d.indexes()

	delta.NodePropertyChanges = decodeBinarySlice(d, 40, func() persistedPropertyChange {
		var pc persistedPropertyChange
		pc.ID = d.u()
		pc.Set = d.properties(0)
		pc.Remove = d.strings()
		return pc
	})
	delta.EdgePropertyChanges = decodeBinarySlice(d, 40, func() persistedPropertyChange {
		var pc persistedPropertyChange
		pc.ID = d.u()
		pc.Set = d.properties(0)
		pc.Remove = d.strings()
		return pc
	})

	return delta
}
