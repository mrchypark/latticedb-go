package engine

import (
	"errors"
	"path/filepath"
	"slices"
	"testing"
)

func TestVectorNamespaceSelectsPropertyAndScopeWithoutChangingLegacySearch(t *testing.T) {
	a := VectorNamespace{Property: "embeddingA", Scope: "A", Dimensions: 2}
	b := VectorNamespace{Property: "embeddingB", Scope: "B", Dimensions: 2}
	db, err := Open(filepath.Join(t.TempDir(), "namespaces"), OpenOptions{
		Create: true, EnableVector: true, VectorDimensions: 2,
		VectorIndexMode: VectorIndexHNSWSynchronous, VectorNamespaces: []VectorNamespace{b, a},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var ids [2]uint64
	if err := db.Update(func(tx *Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"A"}, Properties: map[string]any{"embeddingA": []float32{1, 0}}})
		if err != nil {
			return err
		}
		ids[0] = node.ID
		node, err = tx.CreateNode(CreateNodeOptions{Labels: []string{"B"}, Properties: map[string]any{"embeddingB": []float32{0, 1}}})
		if err != nil {
			return err
		}
		ids[1] = node.ID
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		namespace VectorNamespace
		want      []uint64
	}{{a, []uint64{ids[0]}}, {b, []uint64{ids[1]}}} {
		got, err := db.VectorSearch([]float32{1, 0}, VectorSearchOptions{Namespace: &test.namespace, Exact: true, K: 10})
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(test.want) || got[0].NodeID != test.want[0] {
			t.Fatalf("namespace %s: got %v want %v", test.namespace.Scope, got, test.want)
		}
	}
	unknown := VectorNamespace{Property: "embeddingA", Scope: "missing", Dimensions: 2}
	if _, err := db.VectorSearch([]float32{1, 0}, VectorSearchOptions{Namespace: &unknown, Exact: true}); !errors.Is(err, ErrUnsupportedOption) {
		t.Fatalf("unknown namespace error=%v", err)
	}
	legacy, err := db.VectorSearch([]float32{1, 0}, VectorSearchOptions{Exact: true, K: 10})
	if err != nil {
		t.Fatal(err)
	}
	gotIDs := []uint64{legacy[0].NodeID, legacy[1].NodeID}
	slices.Sort(gotIDs)
	if !slices.Equal(gotIDs, ids[:]) {
		t.Fatalf("legacy IDs=%v want=%v", gotIDs, ids)
	}
}

func TestVectorNamespaceExactOnlyReopenRefreshesLiveCount(t *testing.T) {
	namespace := VectorNamespace{Property: "embedding", Scope: "A", Dimensions: 2}
	path := filepath.Join(t.TempDir(), "exact-only")
	opts := OpenOptions{Create: true, EnableVector: true, VectorDimensions: 2, VectorIndexMode: VectorIndexHNSWSynchronous, VectorNamespaces: []VectorNamespace{namespace}}
	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()
	var id uint64
	if err := db.Update(func(tx *Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"A"}, Properties: map[string]any{"embedding": []float32{1, 0}}})
		if err == nil {
			id = node.ID
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	opts.Create = false
	opts.VectorIndexMode = VectorIndexExactOnly
	db, err = Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	stats, err := db.VectorIndexNamespaceStats(namespace)
	if err != nil {
		t.Fatal(err)
	}
	if stats.LiveEntries != 1 || stats.IndexEntries != 0 {
		t.Fatalf("reopened exact-only stats=%+v", stats)
	}
	if err := db.Update(func(tx *Tx) error { return tx.DeleteNode(id) }); err != nil {
		t.Fatal(err)
	}
	stats, err = db.VectorIndexNamespaceStats(namespace)
	if err != nil {
		t.Fatal(err)
	}
	if stats.LiveEntries != 0 {
		t.Fatalf("after delete live=%d", stats.LiveEntries)
	}
}
