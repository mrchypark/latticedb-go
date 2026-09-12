package engine

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func sidecarPath(path string) string {
	return store.DirectoryDatabaseFiles(path).State + "-hnsw"
}

func seedVectors(t *testing.T, path string, n int) {
	t.Helper()
	db, err := Open(path, OpenOptions{
		Create: true, EnableVector: true,
		VectorDimensions: 2, VectorIndexMode: VectorIndexHNSWSynchronous,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < n; i++ {
			if _, err := tx.CreateNode(CreateNodeOptions{
				Properties: map[string]any{"v": []float32{float32(i), float32(i)}},
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func openUpdateVector(t *testing.T, path string) (*DB, uint64, []float32) {
	t.Helper()
	db, err := Open(path, OpenOptions{
		EnableVector: true, VectorDimensions: 2,
		VectorIndexMode: VectorIndexHNSWSynchronous,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	query := []float32{99, 99}
	var id uint64
	if err := db.Update(func(tx *Tx) error {
		n, err := tx.CreateNode(CreateNodeOptions{
			Properties: map[string]any{"v": query},
		})
		if err == nil {
			id = n.ID
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		return tx.SetVector(id, "v", []float32{99, 98})
	}); err != nil {
		t.Fatal(err)
	}
	return db, id, query
}

func assertNearest(t *testing.T, db *DB, query []float32, wantID uint64) {
	t.Helper()
	results, err := db.VectorSearch(query, VectorSearchOptions{K: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].NodeID != wantID {
		t.Fatalf("nearest: got %v, want node %d", results, wantID)
	}
}

func TestVectorCacheHitPreservesMutationDebt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hit")
	seedVectors(t, path, 16)

	db, id, query := openUpdateVector(t, path)
	before, err := db.VectorIndexStats()
	if err != nil {
		t.Fatal(err)
	}
	if before.MutationDebt == 0 {
		t.Fatal("expected nonzero mutation debt after vector update")
	}
	assertNearest(t, db, query, id)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path, OpenOptions{
		EnableVector: true, VectorDimensions: 2,
		VectorIndexMode: VectorIndexHNSWSynchronous,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	after, err := db.VectorIndexStats()
	if err != nil {
		t.Fatal(err)
	}
	if after.MutationDebt != before.MutationDebt {
		t.Fatalf("debt changed: before=%d after=%d", before.MutationDebt, after.MutationDebt)
	}
	if after.IndexEntries != before.IndexEntries {
		t.Fatalf("index entries changed: before=%d after=%d", before.IndexEntries, after.IndexEntries)
	}
	assertNearest(t, db, query, id)
}

func TestVectorCacheCorruptSidecarRebuildsResetsDebt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "corrupt")
	seedVectors(t, path, 16)

	db, id, query := openUpdateVector(t, path)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(sidecarPath(path), []byte("corrupt"), 0o644); err != nil {
		t.Fatal(err)
	}
	db, err := Open(path, OpenOptions{
		EnableVector: true, VectorDimensions: 2,
		VectorIndexMode: VectorIndexHNSWSynchronous,
	})
	if err != nil {
		t.Fatalf("corrupt sidecar rebuild: %v", err)
	}
	defer db.Close()
	stats, err := db.VectorIndexStats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.MutationDebt != 0 {
		t.Fatalf("debt not reset after corrupt rebuild: %d", stats.MutationDebt)
	}
	assertNearest(t, db, query, id)
}

func TestVectorCacheMissingSidecarRebuildsResetsDebt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "missing")
	seedVectors(t, path, 16)

	db, id, query := openUpdateVector(t, path)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if err := os.Remove(sidecarPath(path)); err != nil {
		t.Fatal(err)
	}
	db, err := Open(path, OpenOptions{
		EnableVector: true, VectorDimensions: 2,
		VectorIndexMode: VectorIndexHNSWSynchronous,
	})
	if err != nil {
		t.Fatalf("missing sidecar rebuild: %v", err)
	}
	defer db.Close()
	stats, err := db.VectorIndexStats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.MutationDebt != 0 {
		t.Fatalf("debt not reset after missing rebuild: %d", stats.MutationDebt)
	}
	assertNearest(t, db, query, id)
}

func TestVectorCacheNamespaceMismatchRebuilds(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ns-mismatch")
	seedVectors(t, path, 16)

	db, err := Open(path, OpenOptions{
		EnableVector: true, VectorDimensions: 2,
		VectorIndexMode:  VectorIndexHNSWSynchronous,
		VectorNamespaces: []VectorNamespace{{Property: "v", Dimensions: 2}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	results, err := db.VectorSearch([]float32{5, 5}, VectorSearchOptions{
		K: 3, Namespace: &VectorNamespace{Property: "v", Dimensions: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) == 0 {
		t.Fatal("expected namespaced search results after rebuild")
	}
}

func TestVectorCacheSidecarsAreDistinct(t *testing.T) {
	dir := t.TempDir()
	pathA := filepath.Join(dir, "a")
	pathB := filepath.Join(dir, "b")
	seedVectors(t, pathA, 8)
	seedVectors(t, pathB, 8)

	for _, source := range []*string{&pathA, &pathB} {
		db, err := Open(*source, OpenOptions{})
		if err != nil {
			t.Fatal(err)
		}
		payload, err := db.Serialize()
		closeErr := db.Close()
		if err != nil {
			t.Fatal(err)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
		*source += ".ltdb"
		if err := os.WriteFile(*source, payload, 0600); err != nil {
			t.Fatal(err)
		}
		db, err = Open(*source, OpenOptions{VectorIndexMode: VectorIndexHNSWSynchronous})
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	scA, scB := pathA+"-hnsw", pathB+"-hnsw"
	if _, err := os.Stat(scA); err != nil {
		t.Fatalf("sidecar A missing: %v", err)
	}
	if _, err := os.Stat(scB); err != nil {
		t.Fatalf("sidecar B missing: %v", err)
	}
	if scA == scB {
		t.Fatal("sidecar paths collide")
	}

	if err := os.WriteFile(scA, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	db, err := Open(pathB, OpenOptions{
		EnableVector: true, VectorDimensions: 2,
		VectorIndexMode: VectorIndexHNSWSynchronous,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	results, err := db.VectorSearch([]float32{5, 5}, VectorSearchOptions{K: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) == 0 {
		t.Fatal("B should be unaffected by corrupt A sidecar")
	}
}

func TestVectorCacheReadonlyDoesNotWriteSidecar(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "readonly")
	seedVectors(t, path, 16)

	sc := sidecarPath(path)
	if err := os.Remove(sc); err != nil {
		t.Fatal(err)
	}
	db, err := Open(path, OpenOptions{
		ReadOnly: true, EnableVector: true,
		VectorDimensions: 2, VectorIndexMode: VectorIndexHNSWSynchronous,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.VectorSearch([]float32{5, 5}, VectorSearchOptions{K: 3}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(sc); !os.IsNotExist(err) {
		t.Fatal("readonly Open+Close should not write sidecar")
	}
}
