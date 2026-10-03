package engine

import (
	"context"
	"errors"
	"math"
	"path/filepath"
	"slices"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/pagestore"
	"github.com/mrchypark/latticedb-go/internal/store"
)

func pageVectorFixture(t *testing.T, count int) (*pagestore.DB, *store.PageGraph, *store.GraphState) {
	t.Helper()
	db, err := pagestore.Open(filepath.Join(t.TempDir(), "pages.db"), pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	page := &store.PageGraph{Tx: tx}
	graph := store.NewGraphState()
	graph.VectorDimensions = 2
	graph.VectorIndexM = 4
	for id := 1; id <= count; id++ {
		var p store.Properties
		p.Set("embedding", []float32{float32(id), float32((id*37)%101) / 100})
		if err := page.PutNode(&store.NodeRecord{ID: uint64(id), Properties: p}); err != nil {
			t.Fatal(err)
		}
	}
	graph.PageBase = page
	return db, page, graph
}

func TestPageVectorHNSWRecallAndBoundedWork(t *testing.T) {
	db, page, _ := pageVectorFixture(t, 300)
	budget := &directSearchBudget{ctx: context.Background(), maxWork: 20_000_000, maxBytes: 256 << 20, annVisitedLimit: ^uint64(0)}
	if err := rebuildPageVectorIndex(context.Background(), page, "default", func(n *store.NodeRecord) ([]float32, bool) { return n.Properties.FirstVector() }, 2, 4, 128<<20, budget); err != nil {
		t.Fatal(err)
	}
	if err := page.Tx.Commit(); err != nil {
		t.Fatal(err)
	}
	read, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	idx := store.PageVectorIndex{Tx: read, Namespace: "default"}
	queries := [][]float32{{17.25, .2}, {111.5, .8}, {284.1, .3}}
	for _, q := range queries {
		work := &directSearchBudget{ctx: context.Background(), maxWork: 250, maxBytes: 1 << 20}
		results, err := pageVectorSearch(context.Background(), idx, q, 5, 24, 4, work)
		if err != nil {
			t.Fatal(err)
		}
		if work.work >= 300*2 {
			t.Fatalf("ANN evaluated %d dimensions, full scan requires %d", work.work, 300*2)
		}
		bestID := uint64(1)
		best := math.Inf(1)
		for id := 1; id <= 300; id++ {
			x := float64(id) - float64(q[0])
			y := float64((id*37)%101)/100 - float64(q[1])
			d := x*x + y*y
			if d < best {
				best, bestID = d, uint64(id)
			}
		}
		found := false
		for _, r := range results {
			if r.NodeID == bestID {
				found = true
			}
		}
		if !found {
			t.Fatalf("top-5 missed exact nearest node %d: %+v", bestID, results)
		}
	}
	// A write transaction carries changed nodes outside the persisted HNSW view.
	// Search must use exact ranking over that overlay to include inserts and deletes.
	view := store.NewGraphState()
	view.PageBase = &store.PageGraph{Tx: read}
	view.VectorDimensions = 2
	var inserted store.Properties
	inserted.Set("embedding", []float32{17.25, .2})
	view.Nodes.Set(301, &store.NodeRecord{ID: 301, Properties: inserted})
	overlayBudget := &directSearchBudget{ctx: context.Background(), maxWork: 10_000, maxBytes: 1 << 20}
	results, fallback, err := searchVectorGraph(view, []float32{17.25, .2}, VectorSearchOptions{K: 1}, overlayBudget, false)
	if err != nil || !fallback || len(results) != 1 || results[0].NodeID != 301 {
		t.Fatalf("overlay result=%+v fallback=%v err=%v", results, fallback, err)
	}
	_ = read.Rollback()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPageVectorBuildCancellationKeepsCommittedIndex(t *testing.T) {
	db, page, _ := pageVectorFixture(t, 20)
	budget := &directSearchBudget{ctx: context.Background(), maxWork: 2_000_000, maxBytes: 64 << 20, annVisitedLimit: ^uint64(0)}
	selectVector := func(n *store.NodeRecord) ([]float32, bool) { return n.Properties.FirstVector() }
	if err := rebuildPageVectorIndex(context.Background(), page, "default", selectVector, 2, 4, 64<<20, budget); err != nil {
		t.Fatal(err)
	}
	if err := page.Tx.Commit(); err != nil {
		t.Fatal(err)
	}
	write, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = rebuildPageVectorIndex(ctx, &store.PageGraph{Tx: write}, "default", selectVector, 2, 4, 64<<20, &directSearchBudget{ctx: ctx, maxWork: 2_000_000, maxBytes: 64 << 20})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled build error=%v", err)
	}
	if err := write.Rollback(); err != nil {
		t.Fatal(err)
	}
	read, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := (store.PageVectorIndex{Tx: read, Namespace: "default"}).Meta()
	if err != nil || meta.Count != uint64(20) {
		t.Fatalf("committed index changed: meta=%+v err=%v", meta, err)
	}
	_ = read.Rollback()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPageVectorAggregateBuildLimitRollsBack(t *testing.T) {
	db, page, _ := pageVectorFixture(t, 12)
	selector := func(n *store.NodeRecord) ([]float32, bool) { return n.Properties.FirstVector() }
	entry := estimateVectorIndexBytesForM(1, 2, 4)
	limit := entry * 20 // enough for one target's persistent estimate, not two.
	shared := &pageVectorBuildBudget{maxPersistentBytes: limit, maxStagedBytes: 8 << 20}
	budget := &directSearchBudget{ctx: context.Background(), maxWork: 2_000_000, maxBytes: 8 << 20}
	if err := rebuildPageVectorIndexWithBudget(context.Background(), page, "default", selector, 2, 4, shared, budget); err != nil {
		t.Fatal(err)
	}
	if err := rebuildPageVectorIndexWithBudget(context.Background(), page, "namespace-b", selector, 2, 4, shared, budget); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("second namespace error=%v, want aggregate limit", err)
	}
	if err := page.Tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	read, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	has, err := (store.PageVectorIndex{Tx: read, Namespace: "default"}).HasMeta()
	if err != nil || has {
		_ = read.Rollback()
		t.Fatalf("rolled-back index remains: has=%v err=%v", has, err)
	}
	if err = read.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPageVectorExplicitRebuildKeepsPinnedReader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "page-vector-rebuild")
	db, err := Open(path, OpenOptions{Create: true, PageStorage: true, EnableVector: true, VectorDimensions: 2, VectorIndexMode: VectorIndexHNSWSynchronous})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err = db.Update(func(tx *Tx) error {
		for i := 0; i < 24; i++ {
			if _, e := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"embedding": []float32{float32(i), float32(i % 3)}}}); e != nil {
				return e
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	before := db.graph
	if err = db.RebuildVectorIndexContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if db.graph == before || db.graph.PageBase.Tx == before.PageBase.Tx {
		t.Fatal("rebuild did not rebind a fresh reader")
	}
	pinned, lease, err := db.SnapshotGraph()
	if err != nil {
		t.Fatal(err)
	}
	oldTx := pinned.PageBase.Tx
	err = db.RebuildVectorIndexContext(context.Background())
	if !errors.Is(err, ErrResourceLimit) || !errors.Is(err, pagestore.ErrSnapshotGrowth) {
		lease.Release()
		t.Fatalf("pinned rebuild error=%v, want guarded growth refusal", err)
	}
	if db.graph != pinned {
		lease.Release()
		t.Fatal("rejected pinned rebuild changed current generation")
	}
	if _, err = (store.PageVectorIndex{Tx: oldTx, Namespace: "default"}).Meta(); err != nil {
		lease.Release()
		t.Fatalf("pinned generation lost its reader: %v", err)
	}
	lease.Release()
	if err = db.RebuildVectorIndexContext(context.Background()); err != nil {
		t.Fatalf("rebuild after releasing pin: %v", err)
	}
}

func TestPublicPageVectorMutationDeleteAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "page-vector-reopen")
	opts := OpenOptions{Create: true, PageStorage: true, EnableVector: true, VectorDimensions: 2, VectorIndexMode: VectorIndexHNSWSynchronous}
	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]uint64, 40)
	if err = db.Update(func(tx *Tx) error {
		for i := range ids {
			n, e := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"embedding": []float32{float32(100 + i), float32(i % 5)}}})
			if e != nil {
				return e
			}
			ids[i] = n.ID
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err = db.Update(func(tx *Tx) error {
		if e := tx.SetProperty(ids[14], "embedding", []float32{0, 0}); e != nil {
			return e
		}
		return tx.DeleteNode(ids[15])
	}); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	opts.Create = false
	db, err = Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ann, err := db.VectorSearch([]float32{0, 0}, VectorSearchOptions{K: 4, EfSearch: 32})
	if err != nil {
		t.Fatal(err)
	}
	exact, err := db.VectorSearch([]float32{0, 0}, VectorSearchOptions{K: 4, Exact: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(ann) == 0 || len(exact) == 0 || ann[0].NodeID != ids[14] || exact[0].NodeID != ids[14] {
		t.Fatalf("ANN=%+v exact=%+v want updated id %d", ann, exact, ids[14])
	}
	for _, result := range ann {
		if result.NodeID == ids[15] {
			t.Fatalf("deleted node returned by page HNSW: %+v", ann)
		}
	}
}

func TestPageVectorReinsertAfterAllDeletedResetsEntry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "page-vector-reinsert")
	db, err := pagestore.Open(path, pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	write, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	page := &store.PageGraph{Tx: write}
	idx := store.PageVectorIndex{Tx: page.Tx, Namespace: "default"}
	if vectorLevel(1) != 0 || vectorLevel(2) != 1 || vectorLevel(3) != 0 {
		t.Fatalf("unexpected deterministic levels: 1=%d 2=%d 3=%d", vectorLevel(1), vectorLevel(2), vectorLevel(3))
	}
	if err := idx.Put(1, &store.PageVectorNode{Level: 0, Neighbors: [][]uint64{{2}}, Vector: []float32{50, 50}}); err != nil {
		t.Fatal(err)
	}
	if err := idx.Put(2, &store.PageVectorNode{Level: 1, Neighbors: [][]uint64{{1}, {1}}, Vector: []float32{60, 60}}); err != nil {
		t.Fatal(err)
	}
	meta := store.PageVectorMeta{EntryID: 2, LastID: 2, MaxLevel: 1, Count: 2, M: 4, Dimensions: 2, Valid: true}
	if err := idx.PutMeta(meta); err != nil {
		t.Fatal(err)
	}
	budget := &directSearchBudget{ctx: context.Background(), maxWork: 100_000, maxBytes: 4 << 20}
	for _, id := range []uint64{1, 2} {
		if err := mutatePageVectorIndex(context.Background(), idx, &meta, id, nil, false, 4, budget); err != nil {
			t.Fatal(err)
		}
	}
	if meta.Count != 2 || meta.DeletedCount != 2 {
		t.Fatalf("deleted metadata=%+v", meta)
	}
	if err := mutatePageVectorIndex(context.Background(), idx, &meta, 3, []float32{0, 0}, true, 4, budget); err != nil {
		t.Fatal(err)
	}
	if meta.EntryID != 3 || meta.Count != 1 || meta.DeletedCount != 0 {
		t.Fatalf("reinitialized meta=%+v", meta)
	}
	if err := page.Tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = pagestore.Open(path, pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	read, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	got, err := pageVectorSearch(context.Background(), store.PageVectorIndex{Tx: read, Namespace: "default"}, []float32{0, 0}, 1, 8, 4, &directSearchBudget{ctx: context.Background(), maxWork: 100_000, maxBytes: 4 << 20})
	if err != nil || len(got) != 1 || got[0].NodeID != 3 {
		t.Fatalf("search=%+v err=%v want reinserted node 3", got, err)
	}
	if err := read.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPageVectorNamespaceKeySeparatesDelimiterContainingFields(t *testing.T) {
	a := VectorNamespace{Scope: "a\x00b", Property: "c", Dimensions: 2, Metric: VectorMetricL2}
	b := VectorNamespace{Scope: "a", Property: "b\x00c", Dimensions: 2, Metric: VectorMetricL2}
	if _, err := normalizeVectorNamespace(a, 2); err != nil {
		t.Fatalf("namespace A rejected: %v", err)
	}
	if _, err := normalizeVectorNamespace(b, 2); err != nil {
		t.Fatalf("namespace B rejected: %v", err)
	}
	legacyA := a.Scope + "\x00" + a.Property + "\x00" + "2\x00" + "0"
	legacyB := b.Scope + "\x00" + b.Property + "\x00" + "2\x00" + "0"
	if legacyA != legacyB {
		t.Fatal("test namespaces do not reproduce the former delimiter collision")
	}
	keyA, keyB := pageVectorNamespaceKey(&a), pageVectorNamespaceKey(&b)
	if keyA == keyB {
		t.Fatal("distinct namespaces encoded to the same page HNSW key")
	}
	path := filepath.Join(t.TempDir(), "namespace-keys.db")
	db, err := pagestore.Open(path, pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	w, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	for i, key := range []string{keyA, keyB} {
		idx := store.PageVectorIndex{Tx: w, Namespace: key}
		id := uint64(i + 1)
		if err := idx.PutMeta(store.PageVectorMeta{EntryID: id, LastID: id, MaxLevel: 0, Count: 1, M: 4, Dimensions: 2, Valid: true}); err != nil {
			t.Fatal(err)
		}
		vector := []float32{0, 0}
		if i == 1 {
			vector = []float32{100, 0}
		}
		if err := idx.Put(id, &store.PageVectorNode{Level: 0, Neighbors: [][]uint64{{}}, Vector: vector}); err != nil {
			t.Fatal(err)
		}
	}
	if err = w.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = pagestore.Open(path, pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	r, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	for i, key := range []string{keyA, keyB} {
		idx := store.PageVectorIndex{Tx: r, Namespace: key}
		meta, e := idx.Meta()
		if e != nil || meta.EntryID != uint64(i+1) {
			t.Fatalf("namespace %d meta=%+v err=%v", i, meta, e)
		}
		query := []float32{0, 0}
		want := uint64(1)
		if i == 1 {
			query = []float32{100, 0}
			want = 2
		}
		got, e := pageVectorSearch(context.Background(), idx, query, 1, 4, 4, &directSearchBudget{ctx: context.Background(), maxWork: 1000, maxBytes: 1 << 20})
		if e != nil || len(got) != 1 || got[0].NodeID != want {
			t.Fatalf("namespace %d search=%+v err=%v want=%d", i, got, e, want)
		}
	}
	if err = r.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPageVectorPublicNamespacesMutateAndReopen(t *testing.T) {
	a := VectorNamespace{Scope: "a\x00b", Property: "c", Dimensions: 2, Metric: VectorMetricL2}
	b := VectorNamespace{Scope: "a", Property: "b\x00c", Dimensions: 2, Metric: VectorMetricL2}
	path := filepath.Join(t.TempDir(), "public-namespace-collision")
	opts := OpenOptions{Create: true, PageStorage: true, EnableVector: true, VectorDimensions: 2, VectorIndexMode: VectorIndexHNSWSynchronous, VectorNamespaces: []VectorNamespace{a, b}}
	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	var ids [2]uint64
	err = db.Update(func(tx *Tx) error {
		node, e := tx.CreateNode(CreateNodeOptions{Labels: []string{a.Scope}, Properties: map[string]any{a.Property: []float32{0, 0}}})
		if e != nil {
			return e
		}
		ids[0] = node.ID
		node, e = tx.CreateNode(CreateNodeOptions{Labels: []string{b.Scope}, Properties: map[string]any{b.Property: []float32{100, 0}}})
		if e != nil {
			return e
		}
		ids[1] = node.ID
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		ns    VectorNamespace
		query []float32
		id    uint64
	}{{a, []float32{0, 0}, ids[0]}, {b, []float32{100, 0}, ids[1]}} {
		got, e := db.VectorSearch(tc.query, VectorSearchOptions{Namespace: &tc.ns, K: 1})
		if e != nil || len(got) != 1 || got[0].NodeID != tc.id {
			t.Fatalf("namespace=%+v got=%+v err=%v want=%d", tc.ns, got, e, tc.id)
		}
	}
	if err = db.Update(func(tx *Tx) error {
		if e := tx.DeleteNode(ids[0]); e != nil {
			return e
		}
		return tx.SetProperty(ids[1], b.Property, []float32{90, 0})
	}); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	opts.Create = false
	db, err = Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	got, err := db.VectorSearch([]float32{90, 0}, VectorSearchOptions{Namespace: &b, K: 1})
	if err != nil || len(got) != 1 || got[0].NodeID != ids[1] {
		t.Fatalf("reopened B=%+v err=%v want=%d", got, err, ids[1])
	}
	got, err = db.VectorSearch([]float32{0, 0}, VectorSearchOptions{Namespace: &a, K: 1})
	if err != nil || len(got) != 0 {
		t.Fatalf("reopened deleted A=%+v err=%v", got, err)
	}
}

func TestPageVectorZeroAndMixedZeroCodecRoundTrip(t *testing.T) {
	db, err := pagestore.Open(filepath.Join(t.TempDir(), "zero-vector.db"), pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	w, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	idx := store.PageVectorIndex{Tx: w, Namespace: "zero"}
	if err = idx.Put(1, &store.PageVectorNode{Level: 0, Neighbors: [][]uint64{{}}, Vector: []float32{0, 0, 0}}); err != nil {
		t.Fatal(err)
	}
	if err = idx.Put(2, &store.PageVectorNode{Level: 0, Neighbors: [][]uint64{{1}}, Vector: []float32{0, 1.25, 0, -2}}); err != nil {
		t.Fatal(err)
	}
	if err = w.Commit(); err != nil {
		t.Fatal(err)
	}
	r, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	for id, want := range map[uint64][]float32{1: {0, 0, 0}, 2: {0, 1.25, 0, -2}} {
		got, e := (store.PageVectorIndex{Tx: r, Namespace: "zero"}).Get(id)
		if e != nil || got == nil || !slices.Equal(got.Vector, want) {
			t.Fatalf("id=%d got=%v err=%v want=%v", id, got, e, want)
		}
	}
	if err = r.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPageVectorPublicReinsertAfterNamespaceEmptied(t *testing.T) {
	ns := VectorNamespace{Property: "embedding", Scope: "active", Dimensions: 2}
	path := filepath.Join(t.TempDir(), "public-reinsert")
	opts := OpenOptions{Create: true, PageStorage: true, EnableVector: true, VectorDimensions: 2, VectorIndexMode: VectorIndexHNSWSynchronous, VectorNamespaces: []VectorNamespace{ns}}
	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	var ids []uint64
	if err = db.Update(func(tx *Tx) error {
		for _, v := range [][]float32{{50, 50}, {60, 60}} {
			node, e := tx.CreateNode(CreateNodeOptions{Labels: []string{"active"}, Properties: map[string]any{"embedding": v}})
			if e != nil {
				return e
			}
			ids = append(ids, node.ID)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || vectorLevel(ids[0]) != 0 || vectorLevel(ids[1]) != 1 {
		t.Fatalf("fixture IDs/levels=%v", ids)
	}
	if err = db.Update(func(tx *Tx) error { return tx.DeleteNode(ids[0]) }); err != nil {
		t.Fatal(err)
	}
	if err = db.Update(func(tx *Tx) error { return tx.DeleteNode(ids[1]) }); err != nil {
		t.Fatal(err)
	}
	var inserted uint64
	if err = db.Update(func(tx *Tx) error {
		node, e := tx.CreateNode(CreateNodeOptions{Labels: []string{"active"}, Properties: map[string]any{"embedding": []float32{0, 0}}})
		if e == nil {
			inserted = node.ID
		}
		return e
	}); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	opts.Create = false
	db, err = Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	got, err := db.VectorSearch([]float32{0, 0}, VectorSearchOptions{Namespace: &ns, K: 1})
	if err != nil || len(got) != 1 || got[0].NodeID != inserted {
		t.Fatalf("reopened reinsertion=%+v err=%v want=%d", got, err, inserted)
	}
}

func TestPageVectorMutationBudgetRejectsOversizedFirstNode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "page-vector-write-budget")
	opts := OpenOptions{Create: true, PageStorage: true, EnableVector: true, VectorDimensions: 4096, VectorIndexMode: VectorIndexHNSWSynchronous, VectorIndexBuildMaxLogicalBytes: 1024}
	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Update(func(tx *Tx) error {
		_, e := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"embedding": make([]float32, 4096)}})
		return e
	})
	if !errors.Is(err, ErrResourceLimit) {
		_ = db.Close()
		t.Fatalf("oversized first vector write error=%v, want resource limit", err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	opts.Create = false
	db, err = Open(path, opts)
	if err != nil {
		t.Fatalf("reopen within the same bound after rejected write: %v", err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPageVectorInvalidationBudgetCountsMetadataOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "page-vector-invalidation")
	db, err := pagestore.Open(path, pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	w, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	idx := store.PageVectorIndex{Tx: w, Namespace: "default"}
	meta := store.PageVectorMeta{EntryID: 1, LastID: 256, Count: 256, M: 4, Dimensions: 4096, Valid: true}
	if err = idx.PutMeta(meta); err != nil {
		t.Fatal(err)
	}
	inactive := store.PageVectorIndex{Tx: w, Namespace: "inactive"}
	if err = inactive.PutMeta(store.PageVectorMeta{EntryID: 1, LastID: 1, MaxLevel: 0, Count: 1, M: 4, Dimensions: 4096, Valid: true}); err != nil {
		t.Fatal(err)
	}
	vector := make([]float32, 4096)
	for i := range vector {
		vector[i] = 1.25
	}
	for id := uint64(1); id <= 256; id++ {
		if err = idx.Put(id, &store.PageVectorNode{Level: 0, Neighbors: [][]uint64{{}}, Vector: vector}); err != nil {
			t.Fatal(err)
		}
	}
	if err = w.Commit(); err != nil {
		t.Fatal(err)
	}
	w, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	var work, reserved, staged uint64
	budget := &store.PageVectorInvalidationBudget{
		Work:         func(n uint64) error { work += n; return nil },
		ReserveBytes: func(n uint64) error { reserved += n; return nil },
		ReleaseBytes: func(n uint64) { reserved -= n },
		StageBytes:   func(n uint64) error { staged += n; return nil },
	}
	if err = store.InvalidatePageVectorIndexesExceptBudget(context.Background(), w, []string{"default"}, budget); err != nil {
		_ = w.Rollback()
		t.Fatal(err)
	}
	if work != 2 || reserved != 0 || staged != 128 {
		_ = w.Rollback()
		t.Fatalf("invalidation charges work=%d outstanding-temp=%d staged=%d, want 2, 0, 128", work, reserved, staged)
	}
	if err = w.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPageVectorMutationBudgetAggregatesNamespaces(t *testing.T) {
	path := filepath.Join(t.TempDir(), "page-vector-multi-budget")
	namespaces := []VectorNamespace{{Property: "embeddingA", Dimensions: 2}, {Property: "embeddingB", Dimensions: 2}}
	opts := OpenOptions{Create: true, PageStorage: true, EnableVector: true, VectorDimensions: 2, VectorIndexMode: VectorIndexHNSWSynchronous, VectorNamespaces: namespaces, VectorIndexBuildMaxLogicalBytes: 8_000}
	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Update(func(tx *Tx) error {
		if _, e := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"embeddingA": []float32{0, 0}}}); e != nil {
			return e
		}
		_, e := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"embeddingB": []float32{1, 1}}})
		return e
	})
	if !errors.Is(err, ErrResourceLimit) {
		_ = db.Close()
		t.Fatalf("two-namespace vector write error=%v, want aggregate resource limit", err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	opts.Create = false
	db, err = Open(path, opts)
	if err != nil {
		t.Fatalf("reopen after rejected aggregate mutation: %v", err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
}
