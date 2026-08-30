package engine

import (
	"fmt"
	"reflect"
	"slices"
	"time"

	"github.com/mrchypark/latticedb-go/internal/store"
)

const (
	changeStreamName           = "__lattice_changes"
	changefeedInlineValueBytes = 256 << 10
)

type StreamRecord = store.StreamRecord

func (db *DB) ReadStream(stream string, afterSequence uint64, limit uint, timeoutMS uint32) ([]StreamRecord, error) {
	if err := validateStreamName(stream, true); err != nil {
		return nil, err
	}
	if limit == 0 {
		return nil, fmt.Errorf("%w: stream read limit must be positive", ErrInvalidArgument)
	}
	deadline := time.NewTimer(time.Duration(timeoutMS) * time.Millisecond)
	defer deadline.Stop()
	for {
		db.mu.RLock()
		if db.closed {
			db.mu.RUnlock()
			return nil, ErrDatabaseClosed
		}
		if db.recoveryRequired {
			db.mu.RUnlock()
			return nil, ErrRecoveryRequired
		}
		records := db.graph.Streams.Read(stream, afterSequence, limit)
		notify := db.streamNotify
		db.mu.RUnlock()
		if len(records) != 0 || timeoutMS == 0 {
			return records, nil
		}
		select {
		case <-notify:
		case <-deadline.C:
			return records, nil
		}
	}
}

func (db *DB) GetStreamOffset(stream, consumer string) (uint64, bool, error) {
	if err := validateStreamName(stream, true); err != nil {
		return 0, false, err
	}
	if err := validateConsumer(consumer); err != nil {
		return 0, false, err
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return 0, false, ErrDatabaseClosed
	}
	if db.recoveryRequired {
		return 0, false, ErrRecoveryRequired
	}
	offset, ok := db.graph.Streams.GetOffset(stream, consumer)
	return offset, ok, nil
}

func (db *DB) Changes(afterSequence uint64, limit uint, timeoutMS uint32) ([]StreamRecord, error) {
	return db.ReadStream(changeStreamName, afterSequence, limit, timeoutMS)
}

func (tx *Tx) PublishStream(stream, kind string, payload any) error {
	_, err := tx.PublishStreamGetSequence(stream, kind, payload)
	return err
}

func (tx *Tx) PublishStreamGetSequence(stream, kind string, payload any) (uint64, error) {
	if err := tx.ensureWritable(); err != nil {
		return 0, err
	}
	if err := validateStreamName(stream, false); err != nil {
		return 0, err
	}
	if err := validateStreamKind(kind); err != nil {
		return 0, err
	}
	normalized, err := store.NormalizeValue(payload)
	if err != nil {
		return 0, err
	}
	if tx.graph.Streams.NextSequence(stream) == ^uint64(0) {
		return 0, fmt.Errorf("%w: stream sequence space exhausted", ErrResourceLimit)
	}
	sequence := tx.graph.Streams.Publish(stream, kind, normalized)
	tx.recordStreamOperation(store.StreamOperation{Type: "publish", Stream: stream, Sequence: sequence, Kind: kind, Payload: normalized})
	return sequence, nil
}

func (tx *Tx) SetStreamOffset(stream, consumer string, sequence uint64) error {
	if err := tx.ensureWritable(); err != nil {
		return err
	}
	if err := validateStreamName(stream, true); err != nil {
		return err
	}
	if err := validateConsumer(consumer); err != nil {
		return err
	}
	tx.graph.Streams.SetOffset(stream, consumer, sequence)
	tx.recordStreamOperation(store.StreamOperation{Type: "offset", Stream: stream, Consumer: consumer, Sequence: sequence})
	return nil
}

func (tx *Tx) TrimStream(stream string, beforeSequence uint64) error {
	if err := tx.ensureWritable(); err != nil {
		return err
	}
	if err := validateStreamName(stream, true); err != nil {
		return err
	}
	tx.graph.Streams.Trim(stream, beforeSequence)
	tx.recordStreamOperation(store.StreamOperation{Type: "trim", Stream: stream, Sequence: beforeSequence})
	return nil
}

func (db *DB) notifyStreamsLocked() {
	if db.streamNotify != nil {
		close(db.streamNotify)
	}
	db.streamNotify = make(chan struct{})
}

func validateStreamName(name string, allowReserved bool) error {
	if err := store.ValidateStreamName(name, allowReserved); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	return nil
}

func validateStreamKind(kind string) error {
	if err := store.ValidateStreamKind(kind); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}
	return nil
}

func validateConsumer(consumer string) error { return validateStreamName(consumer, true) }

func (tx *Tx) appendChangefeed() error {
	if tx.changes == nil || (!hasIDs(tx.changes.upsertNodes) && !hasIDs(tx.changes.deleteNodes) && !hasIDs(tx.changes.upsertEdges) && !hasIDs(tx.changes.deleteEdges)) {
		return nil
	}
	for _, id := range mapKeys(tx.changes.deleteEdges) {
		if edge := tx.base.Edges.Get(id); edge != nil {
			tx.appendChange("edge.delete", map[string]any{"edge_id": int64(id), "source_id": int64(edge.SourceID), "target_id": int64(edge.TargetID), "type": edge.Type})
		}
	}
	for _, id := range mapKeys(tx.changes.deleteNodes) {
		tx.appendChange("node.delete", map[string]any{"node_id": int64(id)})
	}
	for _, id := range mapKeys(tx.changes.upsertNodes) {
		tx.appendNodeChanges(tx.base.Nodes.Get(id), tx.graph.Nodes.Get(id))
	}
	for _, id := range mapKeys(tx.changes.upsertEdges) {
		tx.appendEdgeChanges(tx.base.Edges.Get(id), tx.graph.Edges.Get(id))
	}
	if through, trimmed := tx.graph.Streams.TrimToBytes(changeStreamName, tx.db.changefeedMaxBytes); trimmed {
		tx.recordStreamOperation(store.StreamOperation{Type: "trim", Stream: changeStreamName, Sequence: through})
	}
	return nil
}

func (tx *Tx) appendNodeChanges(before, after *store.NodeRecord) {
	if after == nil {
		return
	}
	if before == nil {
		tx.appendChange("node.insert", map[string]any{"node_id": int64(after.ID)})
	}
	tx.appendLabelChanges(after.ID, before, after)
	tx.appendPropertyChanges("node", after.ID, beforeProperties(before), after.Properties)
}

func (tx *Tx) appendEdgeChanges(before, after *store.EdgeRecord) {
	if after == nil {
		return
	}
	if before == nil {
		tx.appendChange("edge.insert", map[string]any{"edge_id": int64(after.ID), "source_id": int64(after.SourceID), "target_id": int64(after.TargetID), "type": after.Type})
	}
	tx.appendPropertyChanges("edge", after.ID, beforePropertiesEdge(before), after.Properties)
}

func (tx *Tx) appendLabelChanges(nodeID uint64, before, after *store.NodeRecord) {
	oldLabels := map[string]struct{}{}
	if before != nil {
		for _, label := range before.Labels {
			oldLabels[label] = struct{}{}
		}
	}
	newLabels := map[string]struct{}{}
	for _, label := range after.Labels {
		newLabels[label] = struct{}{}
	}
	for _, label := range sortedStringSet(newLabels) {
		if _, exists := oldLabels[label]; !exists {
			tx.appendChange("node.label_add", map[string]any{"node_id": int64(nodeID), "label": label})
		}
	}
	for _, label := range sortedStringSet(oldLabels) {
		if _, exists := newLabels[label]; !exists {
			tx.appendChange("node.label_remove", map[string]any{"node_id": int64(nodeID), "label": label})
		}
	}
}

func (tx *Tx) appendPropertyChanges(entity string, id uint64, before, after map[string]any) {
	keys := map[string]struct{}{}
	for key := range before {
		keys[key] = struct{}{}
	}
	for key := range after {
		keys[key] = struct{}{}
	}
	for _, key := range sortedStringSet(keys) {
		oldValue, oldOK := before[key]
		newValue, newOK := after[key]
		if oldOK && newOK && reflect.DeepEqual(oldValue, newValue) {
			continue
		}
		payload := map[string]any{"key": key}
		if entity == "node" {
			payload["node_id"] = int64(id)
		} else {
			payload["edge_id"] = int64(id)
		}
		if newOK {
			payload["new_value"] = tx.changefeedValue(newValue)
			if oldOK {
				payload["old_value"] = tx.changefeedValue(oldValue)
			}
			tx.appendChange(entity+".property_set", payload)
			continue
		}
		payload["old_value"] = tx.changefeedValue(oldValue)
		tx.appendChange(entity+".property_remove", payload)
	}
}

func (tx *Tx) changefeedValue(value any) any {
	encodedBytes := store.EstimatePropertyIndexValueBytes(value)
	inlineLimit := min(uint64(changefeedInlineValueBytes), tx.db.changefeedMaxBytes/4)
	if encodedBytes <= inlineLimit {
		return store.CloneValue(value)
	}
	return map[string]any{
		"__lattice_value_omitted": true,
		"type":                    changefeedValueType(value),
		"encoded_bytes":           int64(min(encodedBytes, uint64(^uint64(0)>>1))),
	}
}

func changefeedValueType(value any) string {
	switch value.(type) {
	case nil:
		return "null"
	case bool:
		return "bool"
	case int64:
		return "int"
	case float64:
		return "float"
	case string:
		return "string"
	case []byte:
		return "bytes"
	case []float32:
		return "vector"
	case []any:
		return "list"
	case map[string]any:
		return "map"
	default:
		return "unknown"
	}
}

func (tx *Tx) appendChange(kind string, payload map[string]any) {
	sequence := tx.graph.Streams.Publish(changeStreamName, kind, payload)
	tx.recordStreamOperation(store.StreamOperation{Type: "publish", Stream: changeStreamName, Sequence: sequence, Kind: kind, Payload: payload})
}

func (tx *Tx) recordStreamOperation(operation store.StreamOperation) {
	tx.changes.streamsChanged = true
	tx.changes.streamOperations = append(tx.changes.streamOperations, operation)
}

func hasIDs(values map[uint64]struct{}) bool { return len(values) != 0 }

func beforeProperties(node *store.NodeRecord) map[string]any {
	if node == nil {
		return nil
	}
	return node.Properties
}

func beforePropertiesEdge(edge *store.EdgeRecord) map[string]any {
	if edge == nil {
		return nil
	}
	return edge.Properties
}

func sortedStringSet(values map[string]struct{}) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}
