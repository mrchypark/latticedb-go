package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestVectorIndexRecallAndDeletion(t *testing.T) {
	graph := store.NewGraphState()
	var random uint64 = 1
	for id := uint64(1); id <= 10_000; id++ {
		vector := make([]float32, 16)
		for dimension := range vector {
			random ^= random << 13
			random ^= random >> 7
			random ^= random << 17
			vector[dimension] = float32(random&0xffff) / 0xffff
		}
		graph.Nodes.Set(id, &store.NodeRecord{ID: id, Properties: map[string]any{"vector": vector}})
		insertVectorIndex(graph, id)
	}
	refreshVectorLiveCount(graph)
	db := &DB{graph: graph, enableVector: true, vectorDimensions: 16, queryCache: map[string]*queryPlan{}}
	if err := validateVectorIndex(graph); err != nil {
		t.Fatal(err)
	}
	query := graph.Nodes.Get(9_999).Properties["vector"].([]float32)
	exact, err := db.VectorSearch(query, VectorSearchOptions{K: 10, Exact: true})
	if err != nil {
		t.Fatal(err)
	}
	approximate, err := db.VectorSearch(query, VectorSearchOptions{K: 10})
	if err != nil {
		t.Fatal(err)
	}
	expected := make(map[uint64]struct{}, len(exact))
	for _, result := range exact {
		expected[result.NodeID] = struct{}{}
	}
	matches := 0
	for _, result := range approximate {
		if _, ok := expected[result.NodeID]; ok {
			matches++
		}
	}
	if matches < 9 {
		t.Fatalf("recall@10 = %d%%, want at least 90%%", matches*10)
	}

	entry := graph.VectorIndex.EntryID
	entryVector := graph.Nodes.Get(entry).Properties["vector"].([]float32)
	graph.Nodes.Delete(entry)
	tombstoneVectorIndex(graph, entry, entryVector)
	if err := validateVectorIndex(graph); err != nil {
		t.Fatal(err)
	}
	if graph.VectorIndex.EntryID != entry || graph.VectorTombstones.Get(entry) == nil {
		t.Fatalf("deleted entry %d was not retained as a routing tombstone", entry)
	}
	results, err := db.VectorSearch(query, VectorSearchOptions{K: 10})
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range results {
		if result.NodeID == entry {
			t.Fatalf("deleted routing tombstone %d was returned", entry)
		}
	}
}

func TestVectorNeighborPruningUsesUpdatedVector(t *testing.T) {
	graph := store.NewGraphState()
	for id, vector := range map[uint64][]float32{
		1: {0, 0},
		2: {-1, 0},
		3: {2, 0},
		4: {0, 3},
	} {
		graph.Nodes.Set(id, &store.NodeRecord{ID: id, Properties: map[string]any{"vector": vector}})
		graph.VectorIndex.Nodes.Set(id, &store.VectorIndexNode{Level: 0, Neighbors: [][]uint64{nil}, Vector: vector})
	}
	graph.VectorIndex.Nodes.Get(1).Neighbors[0] = []uint64{2, 4}
	graph.VectorIndex.Nodes.Get(3).Vector = []float32{0, 0} // Pre-update cached value.

	if err := connectVectorNeighbor(graph, 1, 3, []float32{2, 0}, 0, 2, true, nil); err != nil {
		t.Fatal(err)
	}
	if got := graph.VectorIndex.Nodes.Get(1).Neighbors[0]; !slices.Equal(got, []uint64{2, 3}) {
		t.Fatalf("pruned neighbors = %v, want updated-vector selection [2 3]", got)
	}
}

func TestVectorSelectionExactANNParityAndCOW(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vector-selection.ltdb")
	db, err := Open(path, OpenOptions{Create: true, EnableVector: true, VectorDimensions: 2, VectorIndexMode: VectorIndexHNSWSynchronous})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var id uint64
	if err := db.Update(func(tx *Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"z": []float32{10, 10}}})
		id = node.ID
		return err
	}); err != nil {
		t.Fatal(err)
	}
	reader, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	oldFingerprint := vectorIndexFingerprint(reader.graph)
	if err := db.Update(func(tx *Tx) error { return tx.SetProperty(id, "a", []float32{0, 0}) }); err == nil {
		t.Fatal("accepted an ambiguous second vector property")
	}
	if got := vectorIndexFingerprint(reader.graph); got != oldFingerprint {
		t.Fatal("published vector index changed through a later generation")
	}
	_ = reader.Rollback()
	assertVectorModesAgree(t, db, []float32{10, 10}, id)
	if err := db.Update(func(tx *Tx) error { return tx.SetVector(id, "z", []float32{20, 20}) }); err != nil {
		t.Fatal(err)
	}
	assertVectorModesAgree(t, db, []float32{0, 0}, id)
	if _, err := db.Query("MATCH (n) WHERE id(n) = $id REMOVE n.a", map[string]any{"id": int64(id)}); err != nil {
		t.Fatal(err)
	}
	assertVectorModesAgree(t, db, []float32{20, 20}, id)
	if _, err := db.Query("MATCH (n) WHERE id(n) = $id SET n.a = $vector", map[string]any{"id": int64(id), "vector": []float32{1}}); err == nil {
		t.Fatal("query accepted a malformed vector dimension")
	}
	if _, err := db.Query("MATCH (n) WHERE id(n) = $id REMOVE n.z", map[string]any{"id": int64(id)}); err != nil {
		t.Fatal(err)
	}
	for _, exact := range []bool{false, true} {
		results, err := db.VectorSearch([]float32{20, 20}, VectorSearchOptions{K: 1, Exact: exact})
		if err != nil || len(results) != 0 {
			t.Fatalf("exact=%v last-vector removal results=%#v, %v", exact, results, err)
		}
	}
	if _, err := db.Query("MATCH (n) WHERE id(n) = $id SET n.z = $vector", map[string]any{"id": int64(id), "vector": []float32{20, 20}}); err != nil {
		t.Fatal(err)
	}
	assertVectorModesAgree(t, db, []float32{20, 20}, id)
	if err := db.Update(func(tx *Tx) error { return tx.DeleteNode(id) }); err != nil {
		t.Fatal(err)
	}
	if err := validateVectorIndex(db.graph); err != nil {
		t.Fatal(err)
	}
	results, err := db.VectorSearch([]float32{20, 20}, VectorSearchOptions{K: 1})
	if err != nil || len(results) != 0 {
		t.Fatalf("deleted node search = %#v, %v", results, err)
	}
}

func TestVectorPropertyContract(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ambiguous.ltdb")
	db, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{
			"a": []float32{1, 0}, "b": []float32{0, 1},
		}})
		if err != nil {
			return err
		}
		return tx.SetProperty(node.ID, "c", []float32{1, 1})
	}); err != nil {
		t.Fatalf("vector properties should be ordinary values when disabled: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path, OpenOptions{EnableVector: true, VectorDimensions: 2}); err == nil {
		t.Fatal("opened database with ambiguous vector properties")
	}

	db, err = Open(filepath.Join(t.TempDir(), "enabled.ltdb"), OpenOptions{Create: true, EnableVector: true, VectorDimensions: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var id uint64
	if err := db.Update(func(tx *Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"a": []float32{1, 0}}})
		id = node.ID
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error { return tx.SetVector(id, "b", []float32{0, 1}) }); err == nil {
		t.Fatal("SetVector accepted a second vector property")
	}
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{
			"a": []float32{1, 0}, "b": []float32{0, 1},
		}})
		return err
	}); err == nil {
		t.Fatal("CreateNode accepted multiple vector properties")
	}

	graph := store.NewGraphState()
	graph.VectorDimensions = 2
	graph.Nodes.Set(2, &store.NodeRecord{ID: 2, Properties: map[string]any{"z": []float32{1, 0}, "a": []float32{0, 1}}})
	graph.Nodes.Set(1, &store.NodeRecord{ID: 1, Properties: map[string]any{"z": []float32{1, 0}, "a": []float32{0, 1}}})
	err = validateGraphVectors(graph)
	if err == nil || !strings.Contains(err.Error(), "node 1") {
		t.Fatalf("validation error = %v, want deterministic node 1 error", err)
	}
	if err := rebuildVectorIndexContext(context.Background(), graph); err == nil {
		t.Fatal("rebuild accepted ambiguous vector properties")
	}
}

func assertVectorModesAgree(t *testing.T, db *DB, query []float32, want uint64) {
	t.Helper()
	for _, exact := range []bool{false, true} {
		results, err := db.VectorSearch(query, VectorSearchOptions{K: 1, Exact: exact})
		if err != nil {
			t.Fatal(err)
		}
		if len(results) != 1 || results[0].NodeID != want {
			t.Fatalf("exact=%v results=%#v, want node %d", exact, results, want)
		}
	}
}

func vectorIndexFingerprint(graph *store.GraphState) string {
	ids := make([]uint64, 0, graph.VectorIndex.Nodes.Len())
	for id := range graph.VectorIndex.Nodes.All() {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	var out strings.Builder
	for _, id := range ids {
		node := graph.VectorIndex.Nodes.Get(id)
		fmt.Fprintf(&out, "%d:%d:%v;", id, node.Level, node.Neighbors)
	}
	return out.String()
}

func TestVectorIndexSnapshotAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vectors.ltdb")
	db, err := Open(path, OpenOptions{Create: true, EnableVector: true, VectorDimensions: 2, VectorIndexMode: VectorIndexHNSWSynchronous})
	if err != nil {
		t.Fatal(err)
	}
	var first uint64
	if err := db.Update(func(tx *Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"vector": []float32{0, 0}}})
		if err != nil {
			return err
		}
		first = node.ID
		_, err = tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"vector": []float32{10, 10}}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	reader, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		return tx.SetVector(first, "vector", []float32{20, 20})
	}); err != nil {
		t.Fatal(err)
	}
	old := vectorSearchLayer(reader.graph, []float32{0, 0}, reader.graph.VectorIndex.EntryID, 0, 10, 0)
	if len(old) == 0 || old[0].id != first {
		t.Fatalf("old snapshot nearest = %#v, want node %d", old, first)
	}
	if err := reader.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, OpenOptions{EnableVector: true, VectorDimensions: 2, VectorIndexMode: VectorIndexHNSWSynchronous})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	results, err := db.VectorSearch([]float32{20, 20}, VectorSearchOptions{K: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].NodeID != first {
		t.Fatalf("reopened nearest = %#v, want node %d", results, first)
	}
}

func TestDirectSearchResourceBounds(t *testing.T) {
	db := benchmarkSearchDB(100, true)
	if _, err := db.VectorSearch(make([]float32, 16), VectorSearchOptions{K: ^uint32(0)}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("large K error = %v", err)
	}
	if _, err := db.VectorSearch(make([]float32, 16), VectorSearchOptions{MaxBytes: 1}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("vector byte error = %v", err)
	}
	const annLogicalBytes = 16 + 256 + 80 + 16*32
	if _, err := db.VectorSearch(make([]float32, 16), VectorSearchOptions{K: 1, EfSearch: 16, MaxBytes: annLogicalBytes - 1}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("ANN visited byte boundary error = %v", err)
	}
	if _, err := db.VectorSearch(make([]float32, 16), VectorSearchOptions{K: 1, EfSearch: 16}); err != nil {
		t.Fatalf("default ANN budget rejected small-Ef search: %v", err)
	}
	if _, err := db.VectorSearch(make([]float32, 16), VectorSearchOptions{K: 10, Exact: true, MaxWork: 10}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("vector work error = %v", err)
	}
	results, err := db.VectorSearch(make([]float32, 16), VectorSearchOptions{K: 65, EfSearch: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 65 {
		t.Fatalf("K > EfSearch returned %d results, want 65", len(results))
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := db.VectorSearchContext(cancelled, make([]float32, 16), VectorSearchOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled vector error = %v", err)
	}
	if _, err := db.FTSSearchContext(cancelled, "common", FTSSearchOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled FTS error = %v", err)
	}
	if _, err := db.FTSSearch("common", FTSSearchOptions{MaxWork: 10}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("FTS work error = %v", err)
	}
	if _, err := db.FTSSearch(strings.Repeat("term ", 100), FTSSearchOptions{Limit: 1, MaxBytes: 128}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("FTS tokenization byte error = %v", err)
	}
	if results, err := db.FTSSearch("!!!", FTSSearchOptions{Limit: 1, MaxBytes: 256}); err != nil || len(results) != 0 {
		t.Fatalf("punctuation-only FTS results = %#v, %v", results, err)
	}
	if _, err := db.FTSSearch(strings.Repeat("!", 100), FTSSearchOptions{MaxWork: 1}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("FTS input scan work error = %v", err)
	}
	if _, err := db.FTSSearchContext(&cancelAfterChecks{}, strings.Repeat("term ", 1_000), FTSSearchOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("mid-tokenization cancellation error = %v", err)
	}
	if _, err := db.VectorSearch(make([]float32, 16), VectorSearchOptions{K: 65, EfSearch: 1, MaxWork: 100}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("ANN fallback work error = %v", err)
	}
	if _, err := db.VectorSearch(make([]float32, 16), VectorSearchOptions{EfSearch: ^uint16(0), MaxBytes: 1024}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("ANN scratch byte error = %v", err)
	}
	large := benchmarkSearchDB(1_000, false)
	if _, err := large.VectorSearchContext(&cancelAfterChecks{}, make([]float32, 16), VectorSearchOptions{Exact: true}); !errors.Is(err, context.Canceled) {
		t.Fatalf("mid-search cancellation error = %v", err)
	}
}

func TestANNExactFallbackSharesSingleByteBudget(t *testing.T) {
	db := benchmarkSearchDB(100, true)
	entry := db.graph.VectorIndex.EntryID
	db.graph.VectorIndex = store.NewVectorIndex()
	db.graph.VectorIndex.EntryID = entry
	db.graph.VectorIndex.Nodes.Set(entry, &store.VectorIndexNode{Level: 0, Neighbors: [][]uint64{nil}})
	const logicalBytes = 10*16 + 256 + 1*80 + 10*32
	if _, err := db.VectorSearch(make([]float32, 16), VectorSearchOptions{K: 10, EfSearch: 10, MaxBytes: logicalBytes - 1}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("fallback limit-1 error = %v", err)
	}
	if _, err := db.VectorSearch(make([]float32, 16), VectorSearchOptions{K: 10, EfSearch: 10, MaxWork: 1_600}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("explicit ANN+exact work limit error = %v", err)
	}
	results, err := db.VectorSearch(make([]float32, 16), VectorSearchOptions{K: 10, EfSearch: 10, MaxBytes: logicalBytes})
	if err != nil {
		t.Fatalf("fallback exact limit: %v", err)
	}
	if len(results) != 10 {
		t.Fatalf("fallback returned %d results", len(results))
	}
}

type cancelAfterChecks struct {
	checks atomic.Int32
	limit  int32
}

func (*cancelAfterChecks) Deadline() (time.Time, bool) { return time.Time{}, false }
func (*cancelAfterChecks) Done() <-chan struct{}       { return nil }
func (ctx *cancelAfterChecks) Err() error {
	limit := ctx.limit
	if limit == 0 {
		limit = 3
	}
	if ctx.checks.Add(1) >= limit {
		return context.Canceled
	}
	return nil
}
func (*cancelAfterChecks) Value(any) any { return nil }

func TestVectorExactOnlyOpenSkipsIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "exact-only.ltdb")
	db, err := Open(path, OpenOptions{Create: true, EnableVector: true, VectorDimensions: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"vector": []float32{1, 2}}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, OpenOptions{EnableVector: true, VectorDimensions: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if db.graph.VectorIndex.Nodes.Len() != 0 {
		t.Fatal("exact-only Open built a vector index")
	}
	stats, err := db.VectorIndexStats()
	if err != nil || stats.LiveEntries != 1 {
		t.Fatalf("exact-only live vectors after reopen = %d, %v; want 1", stats.LiveEntries, err)
	}
	results, err := db.VectorSearch([]float32{1, 2}, VectorSearchOptions{K: 1})
	if err != nil || len(results) != 1 {
		t.Fatalf("exact-only search = %#v, %v", results, err)
	}
	if _, err := db.Query("MATCH (n) WHERE id(n) = 1 REMOVE n.vector", nil); err != nil {
		t.Fatal(err)
	}
	stats, err = db.VectorIndexStats()
	if err != nil || stats.LiveEntries != 0 {
		t.Fatalf("exact-only live vectors after removal = %d, %v; want 0", stats.LiveEntries, err)
	}
	if err := db.Update(func(tx *Tx) error { return tx.SetVector(1, "vector", []float32{2, 3}) }); err != nil {
		t.Fatal(err)
	}
	stats, err = db.VectorIndexStats()
	if err != nil || stats.LiveEntries != 1 {
		t.Fatalf("exact-only live vectors after re-add = %d, %v; want 1", stats.LiveEntries, err)
	}
	if err := db.Update(func(tx *Tx) error { return tx.DeleteNode(1) }); err != nil {
		t.Fatal(err)
	}
	stats, err = db.VectorIndexStats()
	if err != nil || stats.LiveEntries != 0 {
		t.Fatalf("exact-only live vectors after delete = %d, %v; want 0", stats.LiveEntries, err)
	}
}

func TestVectorIndexChurnInvariantsAndDeterministicRebuild(t *testing.T) {
	graph := store.NewGraphState()
	graph.VectorDimensions = 16
	var random uint64 = 7
	for id := uint64(1); id <= 1_000; id++ {
		vector := make([]float32, 16)
		for index := range vector {
			random ^= random << 13
			random ^= random >> 7
			random ^= random << 17
			vector[index] = float32(random&0xffff) / 0xffff
		}
		graph.Nodes.Set(id, &store.NodeRecord{ID: id, Properties: map[string]any{"vector": vector}})
	}
	rebuildVectorIndex(graph)
	fingerprint := vectorIndexFingerprint(graph)
	for range 3 {
		rebuildVectorIndex(graph)
		if got := vectorIndexFingerprint(graph); got != fingerprint {
			t.Fatal("vector rebuild is not deterministic")
		}
	}
	for id := uint64(1); id <= 1_000; id++ {
		node := graph.Nodes.Get(id)
		if id%4 == 0 {
			vector := slices.Clone(node.Properties["vector"].([]float32))
			graph.Nodes.Delete(id)
			tombstoneVectorIndex(graph, id, vector)
		} else if id%5 == 0 {
			vector := slices.Clone(node.Properties["vector"].([]float32))
			vector[0] += 0.01
			node.Properties["vector"] = vector
			insertVectorIndex(graph, id)
		}
	}
	if err := validateVectorIndex(graph); err != nil {
		t.Fatal(err)
	}
	refreshVectorLiveCount(graph)
	db := &DB{graph: graph, enableVector: true, vectorDimensions: 16, queryCache: map[string]*queryPlan{}}
	matches := 0
	total := 0
	for id := uint64(1); id <= 100; id += 7 {
		node := graph.Nodes.Get(id)
		if node == nil {
			continue
		}
		query := node.Properties["vector"].([]float32)
		exact, err := db.VectorSearch(query, VectorSearchOptions{K: 10, Exact: true})
		if err != nil {
			t.Fatal(err)
		}
		approximate, err := db.VectorSearch(query, VectorSearchOptions{K: 10})
		if err != nil {
			t.Fatal(err)
		}
		expected := map[uint64]struct{}{}
		for _, result := range exact {
			expected[result.NodeID] = struct{}{}
		}
		for _, result := range approximate {
			if _, ok := expected[result.NodeID]; ok {
				matches++
			}
		}
		total += len(exact)
	}
	if matches*100 < total*80 {
		t.Fatalf("post-churn recall = %d/%d, want at least 80%%", matches, total)
	}
}

func TestOpenRejectsPersistedMalformedVector(t *testing.T) {
	path := filepath.Join(t.TempDir(), "malformed-vector.ltdb")
	db, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"vector": []float32{1}}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path, OpenOptions{EnableVector: true, VectorDimensions: 2}); err == nil {
		t.Fatal("Open accepted a persisted malformed vector")
	}
	reopened, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("failed Open leaked the path lock: %v", err)
	}
	_ = reopened.Close()
}

func TestVectorMutationDebtRequiresExplicitMaintenance(t *testing.T) {
	graph := store.NewGraphState()
	graph.VectorDimensions = 2
	graph.Nodes.Set(1, &store.NodeRecord{ID: 1, Properties: map[string]any{"vector": []float32{1, 2}}})
	rebuildVectorIndex(graph)
	base := graph
	graph = store.CloneGraphStateShallow(base)
	graph.Nodes.CloneShardOnce(1)
	graph.Nodes.Set(1, &store.NodeRecord{ID: 1, Properties: map[string]any{"vector": []float32{2, 3}}})
	graph.VectorMutations = 4097
	tx := &Tx{graph: graph, base: base, changes: &txChanges{upsertNodes: map[uint64]struct{}{1: {}}}}
	if err := tx.applyVectorIndexChanges(); !errors.Is(err, ErrVectorIndexMaintenanceRequired) {
		t.Fatalf("maintenance error = %v", err)
	}
	if err := rebuildVectorIndexContext(context.Background(), graph); err != nil {
		t.Fatal(err)
	}
	if graph.VectorMutations != 0 || graph.VectorTombstones.Len() != 0 {
		t.Fatalf("explicit rebuild left debt: mutations=%d tombstones=%d", graph.VectorMutations, graph.VectorTombstones.Len())
	}
}

func TestVectorBuildBudgetAndCancellation(t *testing.T) {
	graph := store.NewGraphState()
	graph.VectorDimensions = 4096
	graph.Nodes.Set(1, &store.NodeRecord{ID: 1, Properties: map[string]any{"vector": make([]float32, 4096)}})
	graph.Nodes.Set(2, &store.NodeRecord{ID: 2, Properties: map[string]any{"vector": make([]float32, 4096)}})
	if err := rebuildVectorIndexBudget(context.Background(), graph, ^uint64(0), 1); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("build byte budget error = %v", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := rebuildVectorIndexBudget(cancelled, graph, ^uint64(0), ^uint64(0)); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled build error = %v", err)
	}
	if err := rebuildVectorIndexBudget(context.Background(), graph, 1, ^uint64(0)); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("build work budget error = %v", err)
	}
	if err := rebuildVectorIndexBudget(context.Background(), graph, ^uint64(0), ^uint64(0)); err != nil {
		t.Fatal(err)
	}
	newOnly := estimateVectorIndexBytes(2, graph.VectorDimensions) + 128<<10
	if err := rebuildVectorIndexBudget(context.Background(), graph, ^uint64(0), newOnly); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("old+new logical byte budget error = %v", err)
	}
}

func TestHNSWLevelHasHardUpperBound(t *testing.T) {
	for id := uint64(1); id <= 1_000_000; id++ {
		if level := vectorLevel(id); level < 0 || level > vectorIndexMaxLevel {
			t.Fatalf("node %d level=%d", id, level)
		}
	}
	graph := store.NewGraphState()
	graph.VectorDimensions = 2
	graph.Nodes.Set(1, &store.NodeRecord{ID: 1, Properties: map[string]any{"vector": []float32{1, 2}}})
	graph.VectorIndex.EntryID = 1
	graph.VectorIndex.MaxLevel = vectorIndexMaxLevel + 1
	graph.VectorIndex.Nodes.Set(1, &store.VectorIndexNode{Level: vectorIndexMaxLevel + 1, Neighbors: make([][]uint64, vectorIndexMaxLevel+2)})
	if err := validateVectorIndex(graph); err == nil {
		t.Fatal("validator accepted a level above the hard cap")
	}
}

func TestSearchBudgetsHighDimensionAndFuzzyScratch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "resource-budgets.ltdb")
	db, err := Open(path, OpenOptions{Create: true, EnableVector: true, VectorDimensions: 4096})
	if err != nil {
		t.Fatal(err)
	}
	longToken := strings.Repeat("가", 20_000)
	if err := db.Update(func(tx *Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"vector": make([]float32, 4096)}})
		if err != nil {
			return err
		}
		return tx.FTSIndex(node.ID, longToken)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.VectorSearch(make([]float32, 4096), VectorSearchOptions{K: 1, Exact: true, MaxWork: 4095}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("dimension work budget error = %v", err)
	}
	if _, err := db.FTSSearch("가나", FTSSearchOptions{MaxDistance: 1, MinTermLength: 1, MaxWork: ^uint64(0), MaxBytes: 1024}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("fuzzy scratch budget error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestVectorDebtUsesVectorPopulationNotAllNodes(t *testing.T) {
	graph := store.NewGraphState()
	graph.VectorDimensions = 2
	for id := uint64(1); id <= 10_000; id++ {
		graph.Nodes.Set(id, &store.NodeRecord{ID: id, Properties: map[string]any{}})
	}
	graph.Nodes.Set(1, &store.NodeRecord{ID: 1, Properties: map[string]any{"vector": []float32{1, 2}}})
	rebuildVectorIndex(graph)
	base := graph
	graph = store.CloneGraphStateShallow(base)
	graph.Nodes.CloneShardOnce(1)
	graph.Nodes.Set(1, &store.NodeRecord{ID: 1, Properties: map[string]any{"vector": []float32{2, 3}}})
	graph.VectorMutations = 4097
	tx := &Tx{graph: graph, base: base, changes: &txChanges{upsertNodes: map[uint64]struct{}{1: {}}}}
	if err := tx.applyVectorIndexChanges(); !errors.Is(err, ErrVectorIndexMaintenanceRequired) {
		t.Fatalf("maintenance error = %v", err)
	}
	if graph.VectorMutations != 4098 {
		t.Fatalf("maintenance path changed private debt: %d", graph.VectorMutations)
	}
}

func TestVectorIndexStats(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "stats.ltdb"), OpenOptions{Create: true, EnableVector: true, VectorDimensions: 2, VectorIndexMode: VectorIndexHNSWSynchronous})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"vector": []float32{1, 2}}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	stats, err := db.VectorIndexStats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.LiveEntries != 1 || stats.IndexEntries != 1 || stats.EstimatedBuildLogicalBytes == 0 {
		t.Fatalf("stats = %#v", stats)
	}
	db.mu.Lock()
	db.graph.VectorMutations = 4097
	db.mu.Unlock()
	if err := db.Update(func(tx *Tx) error { return tx.SetProperty(1, "name", "allowed") }); err != nil {
		t.Fatalf("non-vector write was blocked by vector maintenance debt: %v", err)
	}
	if err := db.Update(func(tx *Tx) error { return tx.SetVector(1, "vector", []float32{2, 3}) }); !errors.Is(err, ErrVectorIndexMaintenanceRequired) {
		t.Fatalf("vector maintenance error = %v", err)
	}
	if err := db.RebuildVectorIndexContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	stats, err = db.VectorIndexStats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.MutationDebt != 0 || stats.Rebuilds != 1 || stats.DebtUntilRebuild == 0 {
		t.Fatalf("post-rebuild stats = %#v", stats)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestVectorSearchDoesNotFallbackWhenAllLiveVectorsAreFound(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "sparse.ltdb"), OpenOptions{
		Create: true, EnableVector: true, VectorDimensions: 2, VectorIndexMode: VectorIndexHNSWSynchronous,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		if _, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"vector": []float32{1, 0}}}); err != nil {
			return err
		}
		for range 9 {
			if _, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"name": "non-vector"}}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	before, err := db.VectorIndexStats()
	if err != nil {
		t.Fatal(err)
	}
	if before.LiveEntries != 1 {
		t.Fatalf("live vectors after create = %d, want 1", before.LiveEntries)
	}
	results, err := db.VectorSearch([]float32{1, 0}, VectorSearchOptions{K: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].NodeID != 1 {
		t.Fatalf("sparse vector results = %#v, want node 1 only", results)
	}
	after, err := db.VectorIndexStats()
	if err != nil {
		t.Fatal(err)
	}
	if after.ExactFallbacks != before.ExactFallbacks {
		t.Fatalf("exact fallbacks changed from %d to %d", before.ExactFallbacks, after.ExactFallbacks)
	}
	if _, err := db.Query("MATCH (n) WHERE id(n) = 1 REMOVE n.vector", nil); err != nil {
		t.Fatal(err)
	}
	stats, err := db.VectorIndexStats()
	if err != nil || stats.LiveEntries != 0 {
		t.Fatalf("live vectors after removal = %d, %v; want 0", stats.LiveEntries, err)
	}
	if err := db.Update(func(tx *Tx) error { return tx.SetVector(1, "vector", []float32{2, 3}) }); err != nil {
		t.Fatal(err)
	}
	stats, err = db.VectorIndexStats()
	if err != nil || stats.LiveEntries != 1 {
		t.Fatalf("live vectors after re-add = %d, %v; want 1", stats.LiveEntries, err)
	}
	if err := db.Update(func(tx *Tx) error { return tx.DeleteNode(1) }); err != nil {
		t.Fatal(err)
	}
	stats, err = db.VectorIndexStats()
	if err != nil || stats.LiveEntries != 0 {
		t.Fatalf("live vectors after delete = %d, %v; want 0", stats.LiveEntries, err)
	}
}

func TestExplicitCommitMaintenanceErrorIsTerminal(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "terminal-maintenance.ltdb"), OpenOptions{Create: true, EnableVector: true, VectorDimensions: 2, VectorIndexMode: VectorIndexHNSWSynchronous})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"vector": []float32{1, 2}}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	db.mu.Lock()
	db.graph.VectorMutations = 4097
	db.mu.Unlock()
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.SetVector(1, "vector", []float32{2, 3}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrVectorIndexMaintenanceRequired) {
		t.Fatalf("maintenance commit error = %v", err)
	}
	if err := tx.Rollback(); !errors.Is(err, ErrInactiveTx) {
		t.Fatalf("rollback after terminal commit = %v", err)
	}
	if err := db.RebuildVectorIndexContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrInactiveTx) {
		t.Fatalf("stale transaction commit = %v", err)
	}
	if err := db.Update(func(tx *Tx) error { return tx.SetVector(1, "vector", []float32{2, 3}) }); err != nil {
		t.Fatalf("fresh transaction after rebuild: %v", err)
	}
}

func TestConcurrentVectorReadersRebuildAndCancellation(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "concurrent-rebuild.ltdb"), OpenOptions{Create: true, EnableVector: true, VectorDimensions: 16, VectorIndexMode: VectorIndexHNSWSynchronous})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		for id := 0; id < 200; id++ {
			vector := make([]float32, 16)
			vector[0] = float32(id)
			if _, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"vector": vector}}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	reader, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	oldFingerprint := vectorIndexFingerprint(reader.graph)
	ctx, cancel := context.WithCancel(context.Background())
	errorsOut := make(chan error, 4)
	var wait sync.WaitGroup
	for range 4 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for {
				_, err := db.VectorSearchContext(ctx, make([]float32, 16), VectorSearchOptions{K: 10})
				if errors.Is(err, context.Canceled) {
					return
				}
				if err != nil {
					errorsOut <- err
					return
				}
			}
		}()
	}
	if err := db.RebuildVectorIndexContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	updated := make([]float32, 16)
	updated[0] = 999
	if err := db.Update(func(tx *Tx) error { return tx.SetVector(1, "vector", updated) }); err != nil {
		t.Fatal(err)
	}
	cancel()
	wait.Wait()
	close(errorsOut)
	for err := range errorsOut {
		t.Fatal(err)
	}
	if got := vectorIndexFingerprint(reader.graph); got != oldFingerprint {
		t.Fatal("long reader's published index changed during rebuild")
	}
	if err := validateVectorIndex(db.graph); err != nil {
		t.Fatal(err)
	}
	if err := reader.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBackgroundVectorRebuildReplaysDeltasAndCoalesces(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "background-rebuild.ltdb"), OpenOptions{Create: true, EnableVector: true, VectorDimensions: 2, VectorIndexMode: VectorIndexHNSWSynchronous})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var first, second uint64
	if err := db.Update(func(tx *Tx) error {
		if node, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"vector": []float32{1, 0}}}); err != nil {
			return err
		} else {
			first = node.ID
		}
		if node, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"vector": []float32{0, 1}}}); err != nil {
			return err
		} else {
			second = node.ID
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	db.vectorRebuildBeforeBuild = func() { close(started); <-release }
	firstDone := make(chan error, 1)
	go func() { firstDone <- db.RebuildVectorIndexContext(context.Background()) }()
	<-started
	if err := db.Update(func(tx *Tx) error { return tx.SetVector(first, "vector", []float32{3, 0}) }); err != nil {
		t.Fatalf("write during rebuild: %v", err)
	}
	if err := db.Update(func(tx *Tx) error { return tx.DeleteNode(second) }); err != nil {
		t.Fatalf("delete during rebuild: %v", err)
	}
	secondDone := make(chan error, 1)
	go func() { secondDone <- db.RebuildVectorIndexContext(context.Background()) }()
	select {
	case err := <-secondDone:
		t.Fatalf("coalesced rebuild returned early: %v", err)
	default:
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	if err := validateVectorIndex(db.graph); err != nil {
		t.Fatal(err)
	}
	results, err := db.VectorSearch([]float32{3, 0}, VectorSearchOptions{K: 2})
	if err != nil || len(results) != 1 || results[0].NodeID != first {
		t.Fatalf("replayed search = %#v, %v", results, err)
	}
}

func TestVectorRebuildDeltaReservationsRespectLimits(t *testing.T) {
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	state := &vectorRebuildState{dimensions: 2, maxBytes: estimateVectorIndexBytes(1, 2) - 1, maxWork: ^uint64(0), cancel: cancel}
	db := &DB{vectorRebuild: state}
	db.appendVectorRebuildDeltasLocked([]vectorRebuildDelta{{id: 1, after: []float32{1, 2}}})
	if !errors.Is(state.err, ErrResourceLimit) {
		t.Fatalf("delta log error = %v", state.err)
	}

	budget := &directSearchBudget{ctx: context.Background(), maxWork: ^uint64(0), maxBytes: estimateVectorIndexBytes(1, 2), annVisitedLimit: ^uint64(0)}
	if err := reserveVectorRebuildDelta(budget, 2); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("replay reservation error = %v", err)
	}
}

func TestVectorSearchScratchReset(t *testing.T) {
	scratch := &vectorSearchScratch{
		frontier: []vectorCandidate{{id: 1}},
		best:     []vectorCandidate{{id: 2}},
		visited:  map[uint64]struct{}{1: {}, 2: {}},
	}
	releaseVectorSearchScratch(scratch)
	if len(scratch.frontier) != 0 || len(scratch.best) != 0 || len(scratch.visited) != 0 {
		t.Fatalf("pooled scratch retained state: %#v", scratch)
	}
}

func TestVectorSearchLayerScratchAllocations(t *testing.T) {
	db := benchmarkSearchDB(1_000, true)
	query := db.graph.Nodes.Get(500).Properties["embedding"].([]float32)
	scratch := &vectorSearchScratch{visited: make(map[uint64]struct{})}
	if results := vectorSearchLayerScratch(db.graph, query, db.graph.VectorIndex.EntryID, 0, vectorIndexSearchEF, 0, scratch); len(results) == 0 {
		t.Fatal("warmup vector search returned no results")
	}
	allocations := testing.AllocsPerRun(20, func() {
		if results := vectorSearchLayerScratch(db.graph, query, db.graph.VectorIndex.EntryID, 0, vectorIndexSearchEF, 0, scratch); len(results) == 0 {
			panic("vector search returned no results")
		}
	})
	if allocations > 1 {
		t.Fatalf("reused vector search allocations = %.0f, want <= 1", allocations)
	}
}

func TestVectorSearchRejectsNonFiniteQueryWithoutMutation(t *testing.T) {
	db := benchmarkSearchDB(10, true)
	query := make([]float32, 16)
	query[0] = float32(math.Inf(1))
	if _, err := db.VectorSearch(query, VectorSearchOptions{K: 1}); err == nil {
		t.Fatal("expected non-finite query to fail")
	}
	if !math.IsInf(float64(query[0]), 1) {
		t.Fatal("vector search mutated caller query")
	}
}

func TestZeroVectorDimensionsRejectActiveVectors(t *testing.T) {
	scalar := &store.NodeRecord{ID: 1, Properties: map[string]any{"name": "ok"}}
	if err := validateNodeVectors(0, scalar); err != nil {
		t.Fatalf("scalar node rejected: %v", err)
	}
	vector := &store.NodeRecord{ID: 2, Properties: map[string]any{"embedding": []float32{1, 2}}}
	if err := validateNodeVectors(0, vector); err == nil {
		t.Fatal("zero-dimension validator accepted vector node")
	}

	graph := store.NewGraphState()
	graph.VectorDimensions = 2
	graph.Nodes.Set(vector.ID, vector)
	insertVectorIndex(graph, vector.ID)
	graph.VectorDimensions = 0
	fingerprint := vectorIndexFingerprint(graph)
	if err := rebuildVectorIndexContext(context.Background(), graph); err == nil {
		t.Fatal("zero-dimension rebuild accepted vector node")
	}
	if got := vectorIndexFingerprint(graph); got != fingerprint {
		t.Fatal("failed rebuild mutated vector index")
	}

	db := &DB{graph: graph, enableVector: true, vectorDimensions: 0, queryCache: map[string]*queryPlan{}}
	if _, err := db.VectorSearch([]float32{1, 2}, VectorSearchOptions{Exact: true}); err == nil {
		t.Fatal("zero-dimension exact search succeeded")
	}
	if _, err := db.Query("MATCH (n) WHERE n.embedding <=> $vector RETURN n", map[string]any{"vector": []float32{1, 2}}); err == nil {
		t.Fatal("zero-dimension vector query succeeded")
	}
}

func TestZeroVectorDimensionsCommitDoesNotPublish(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "commit-zero-dim.ltdb"), OpenOptions{Create: true, EnableVector: true, VectorDimensions: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	node, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"embedding": []float32{1, 2}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.SetVector(node.ID, "embedding", []float32{3, 4}); err != nil {
		t.Fatal(err)
	}
	tx.graph.VectorDimensions = 0
	if err := tx.Commit(); err == nil {
		t.Fatal("commit accepted active zero dimensions")
	}
	if db.graph.Nodes.Get(node.ID) != nil {
		t.Fatal("failed commit published node")
	}
}

func TestVectorSearchScratchRejectsOversizedPooledCapacity(t *testing.T) {
	scratch := &vectorSearchScratch{visited: make(map[uint64]struct{}), visitedCapacity: 100, frontier: make([]vectorCandidate, 0, 100), best: make([]vectorCandidate, 0, 100)}
	if vectorSearchScratchFits(scratch, 16, 16, 16) {
		t.Fatal("oversized pooled scratch fit a small request")
	}
}
