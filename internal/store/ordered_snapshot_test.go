package store

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"testing"
)

func TestOrderedSnapshotPreservesExistingBytes(t *testing.T) {
	graph := NewGraphState()
	graph.DatabaseID = "0123456789abcdef0123456789abcdef"
	for _, id := range []uint64{3 << 20, 128, 1 << 20, 1, 64} {
		graph.Nodes.Set(id, &NodeRecord{ID: id, Labels: []string{"Item"}, Properties: map[string]any{"name": "한글"}})
		graph.Edges.Set(id, &EdgeRecord{ID: id, SourceID: id, TargetID: 1, Type: "LINK"})
		graph.FTS.Set(id, &FTSRecord{Text: "text"})
	}
	var output bytes.Buffer
	if err := writePersistedStateJSON(&output, graph, 4<<20, 4<<20, 1); err != nil {
		t.Fatal(err)
	}
	// Golden captured from the pre-Ordered implementation for this sparse fixture.
	const want = "88e7dad62a303cfde05823fd8f8ce8b450d78df7634892102ece61d8f5dda430"
	if got := fmt.Sprintf("%x", sha256.Sum256(output.Bytes())); got != want {
		t.Fatalf("snapshot bytes changed: SHA256=%s", got)
	}
}
