package store

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func TestSnapshotSizeAccountingBoundsEncodedMetadataIndexesAndStreams(t *testing.T) {
	graph := NewGraphState()
	graph.DatabaseID = "0123456789abcdef0123456789abcdef"
	graph.AppMetadata.Set(string(bytes.Repeat([]byte{0xff}, 96)), bytes.Repeat([]byte{0}, 32<<10))
	for index := range 32 {
		definition := PropertyIndexDefinition{Scope: fmt.Sprintf("node-%02d-%s", index, strings.Repeat("\n", 96)), Property: strings.Repeat("\n", 96)}
		graph.NodeProperties.Create(definition)
		graph.EdgeProperties.Create(definition)
	}
	for index := range 32 {
		name := fmt.Sprintf("%03d%s", index, strings.Repeat("\n", 240))
		graph.Streams.Publish(name, strings.Repeat("\n", 240), map[string]any{"value": "quoted\nvalue"})
		graph.Streams.SetOffset(name, strings.Repeat("\n", 240), 1)
	}

	assertSnapshotPayloadFitsEstimate(t, graph, 1)

	base := CloneGraphStateShallow(graph)
	base.SnapshotBytes, _ = EstimateSnapshotBytes(base)
	updated := CloneGraphStateShallow(base)
	updated.AppMetadata = updated.AppMetadata.Fork()
	updated.AppMetadata.Set("next", bytes.Repeat([]byte{1}, 98))
	newIndex := PropertyIndexDefinition{Scope: strings.Repeat("\n", 240), Property: strings.Repeat("\n", 240)}
	updated.NodeProperties.Create(newIndex)
	updated.Streams.Publish("000"+strings.Repeat("\n", 240), strings.Repeat("\n", 240), int64(2))
	estimate, err := ApplyDeltaSnapshotBytes(base, updated, GraphDelta{
		AppMetadata:       []AppMetadataChange{{Key: []byte("next")}},
		CreateNodeIndexes: []PropertyIndexDefinition{newIndex},
		StreamsChanged:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	fullEstimate, err := EstimateSnapshotBytes(updated)
	if err != nil {
		t.Fatal(err)
	}
	if estimate != fullEstimate {
		t.Fatalf("delta estimate = %d, full estimate = %d", estimate, fullEstimate)
	}
	assertSnapshotPayloadFitsLimit(t, updated, 2, estimate)
	if streamStoreSnapshotBytes(updated.Streams) != calculateStreamStoreSnapshotBytes(updated.Streams) {
		t.Fatal("stream snapshot accounting drifted after publish")
	}
	updated.SnapshotBytes = estimate

	trimmed := CloneGraphStateShallow(updated)
	trimmed.NodeProperties.Drop(newIndex)
	stream := "000" + strings.Repeat("\n", 240)
	trimmed.Streams.Trim(stream, 1)
	trimmed.Streams.SetOffset(stream, strings.Repeat("\n", 240), 2)
	trimmedEstimate, err := ApplyDeltaSnapshotBytes(updated, trimmed, GraphDelta{
		DropNodeIndexes: []PropertyIndexDefinition{newIndex},
		StreamsChanged:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	trimmedFullEstimate, err := EstimateSnapshotBytes(trimmed)
	if err != nil {
		t.Fatal(err)
	}
	if trimmedEstimate != trimmedFullEstimate {
		t.Fatalf("trimmed delta estimate = %d, full estimate = %d", trimmedEstimate, trimmedFullEstimate)
	}
	if streamStoreSnapshotBytes(trimmed.Streams) != calculateStreamStoreSnapshotBytes(trimmed.Streams) {
		t.Fatal("stream snapshot accounting drifted after trim and offset update")
	}
	assertSnapshotPayloadFitsLimit(t, trimmed, 3, trimmedEstimate)
}

func assertSnapshotPayloadFitsEstimate(t *testing.T, graph *GraphState, commitID uint64) {
	t.Helper()
	estimate, err := EstimateSnapshotBytes(graph)
	if err != nil {
		t.Fatal(err)
	}
	assertSnapshotPayloadFitsLimit(t, graph, commitID, estimate)
}

func assertSnapshotPayloadFitsLimit(t *testing.T, graph *GraphState, commitID, limit uint64) {
	t.Helper()
	serialized, err := SerializeGraphState(graph, 1, 1, commitID)
	if err != nil {
		t.Fatal(err)
	}
	if uint64(len(serialized)-stateHeaderSize) > limit {
		t.Fatalf("snapshot payload = %d, limit = %d", len(serialized)-stateHeaderSize, limit)
	}
	if _, _, _, _, err := DeserializeGraphState(serialized, limit, ^uint64(0), ^uint64(0)); err != nil {
		t.Fatalf("deserialize at accepted snapshot limit: %v", err)
	}
}
