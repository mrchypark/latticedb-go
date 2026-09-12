package store

// Encoder methods for persisted record types. Field order matches the
// struct declaration order in store.go exactly.

func (e *binaryEncoder) node(n persistedNode) {
	e.u(n.ID)
	e.strings(n.Labels)
	e.properties(n.Properties, 0)
}

func (e *binaryEncoder) edge(ed persistedEdge) {
	e.u(ed.ID)
	e.u(ed.SourceID)
	e.u(ed.TargetID)
	e.str(ed.Type)
	e.properties(ed.Properties, 0)
}

func (e *binaryEncoder) fts(f persistedFTS) {
	e.u(f.NodeID)
	e.str(f.Text)
}

func (e *binaryEncoder) indexes(defs []persistedPropertyIndexDefinition) {
	encodeBinarySlice(e, defs, func(def persistedPropertyIndexDefinition) { e.str(def.Scope); e.str(def.Property) })
}
func (e *binaryEncoder) metadata(entries []persistedAppMetadata) {
	encodeBinarySlice(e, entries, func(entry persistedAppMetadata) { e.bytes(entry.Key); e.bytes(entry.Value) })
}

// Decoder methods for persisted record types.

func (d *binaryDecoder) node() persistedNode {
	var n persistedNode
	n.ID = d.u()
	if d.err != nil {
		return n
	}
	n.Labels = d.strings()
	n.Properties = d.properties(0)
	return n
}

func (d *binaryDecoder) edge() persistedEdge {
	var ed persistedEdge
	ed.ID = d.u()
	ed.SourceID = d.u()
	ed.TargetID = d.u()
	ed.Type = d.str()
	if d.err != nil {
		return ed
	}
	ed.Properties = d.properties(0)
	return ed
}

func (d *binaryDecoder) fts() persistedFTS {
	var f persistedFTS
	f.NodeID = d.u()
	f.Text = d.str()
	return f
}

func (d *binaryDecoder) indexes() []persistedPropertyIndexDefinition {
	return decodeBinarySlice[persistedPropertyIndexDefinition](d, 32, func() persistedPropertyIndexDefinition {
		return persistedPropertyIndexDefinition{Scope: d.str(), Property: d.str()}
	})
}

func (d *binaryDecoder) metadata() []persistedAppMetadata {
	return decodeBinarySlice[persistedAppMetadata](d, 32, func() persistedAppMetadata {
		return persistedAppMetadata{Key: d.bytes(), Value: d.bytes()}
	})
}
