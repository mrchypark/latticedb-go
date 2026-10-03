package engine

import (
	"context"
	"errors"
	"math"
	"path/filepath"
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
	limit := entry * 160 // enough for one target's reciprocal staging, not two.
	shared := &pageVectorBuildBudget{maxPersistentBytes: limit, maxStagedBytes: limit}
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
