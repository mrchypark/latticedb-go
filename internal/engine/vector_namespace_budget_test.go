package engine

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestVectorNamespaceBuildBudgetIncludesOverlappingIndexes(t *testing.T) {
	a := VectorNamespace{Property: "embedding", Scope: "A", Dimensions: 2}
	all := VectorNamespace{Property: "embedding", Dimensions: 2}
	path := filepath.Join(t.TempDir(), "namespace-budget")
	opts := OpenOptions{Create: true, EnableVector: true, VectorDimensions: 2}
	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 1000; i++ {
			if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"A"}, Properties: map[string]any{"embedding": []float32{float32(i), 0}}}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	oneIndexBudget := estimateVectorBuildLogicalBytes(db.graph, 1000) + (64 << 10)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	opts.Create = false
	opts.VectorIndexMode = VectorIndexHNSWSynchronous
	opts.VectorIndexBuildMaxLogicalBytes = oneIndexBudget
	db, err = Open(path, opts)
	if err != nil {
		t.Fatalf("single-index budget rejected legacy index: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	opts.VectorNamespaces = []VectorNamespace{a, all}
	db, err = Open(path, opts)
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("overlapping namespace open error=%v, want resource limit", err)
	}
	// The failed open must release the database lock and leave canonical data intact.
	opts.VectorIndexBuildMaxLogicalBytes = 0
	db, err = Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	view := *db.graph
	state := db.graph.VectorNamespaces[a]
	view.VectorIndex, view.VectorTombstones = state.Index, state.Tombstones
	view.VectorLiveCount, view.VectorMutations = state.LiveCount, state.Mutations
	view.VectorNamespace = &a
	db.vectorIndexBuildMaxLogicalBytes = estimateVectorBuildLogicalBytes(&view, state.LiveCount) + (64 << 10)
	for _, rebuild := range []func(context.Context) error{
		db.RebuildVectorIndexContext,
		func(ctx context.Context) error { return db.RebuildVectorIndexNamespaceContext(ctx, a) },
	} {
		if err := rebuild(context.Background()); !errors.Is(err, ErrResourceLimit) {
			t.Fatalf("rebuild omitted retained sibling indexes: %v", err)
		}
	}
	got, err := db.VectorSearch([]float32{500, 0}, VectorSearchOptions{Namespace: &a, K: 1})
	if err != nil || len(got) != 1 || got[0].Distance != 0 {
		t.Fatalf("failed rebuild changed searchable index: results=%v error=%v", got, err)
	}
}
