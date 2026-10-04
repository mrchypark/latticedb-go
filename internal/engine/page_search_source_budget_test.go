package engine

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/pagestore"
	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestPageVectorSearchAdmitsCanonicalSource(t *testing.T) {
	makeDB := func(count int, body string) *DB {
		t.Helper()
		db, err := Open(filepath.Join(t.TempDir(), "vectors"), OpenOptions{Create: true, PageStorage: true, EnableVector: true, VectorDimensions: 2, VectorM: 2, VectorIndexMode: VectorIndexHNSWSynchronous})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if db != nil {
				db.Close()
			}
		})
		if err := db.Update(func(tx *Tx) error {
			for i := 0; i < count; i++ {
				props := map[string]any{"embedding": []float32{1, 1}}
				if i == 0 {
					props["body"] = body
				}
				if _, err := tx.CreateNode(CreateNodeOptions{Properties: props}); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return db
	}
	db := makeDB(20, strings.Repeat("x", 1<<20))
	for _, exact := range []bool{false, true} {
		opts := VectorSearchOptions{K: 20, EfSearch: 20, Exact: exact, MaxBytes: 128 << 10}
		if _, err := db.VectorSearch([]float32{1, 1}, opts); !errors.Is(err, ErrResourceLimit) {
			t.Fatalf("exact=%v low budget=%v", exact, err)
		}
		opts.MaxBytes = 8 << 20
		got, err := db.VectorSearch([]float32{1, 1}, opts)
		if err != nil || len(got) != 20 {
			t.Fatalf("exact=%v admitted results=%d err=%v", exact, len(got), err)
		}
	}
	small := makeDB(256, "")
	got, err := small.VectorSearch([]float32{1, 1}, VectorSearchOptions{K: 1, Exact: true, MaxBytes: 4096})
	if err != nil || len(got) != 1 {
		t.Fatalf("record scratch not released: results=%d err=%v", len(got), err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := small.VectorSearchContext(ctx, []float32{1, 1}, VectorSearchOptions{K: 1, Exact: true}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation=%v", err)
	}
}

func staleManualFTSSourceFixture(t *testing.T, count int, text string) *DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "manual-source")
	db, err := Open(path, OpenOptions{Create: true, PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if db != nil {
			db.Close()
		}
	})
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < count; i++ {
			n, err := tx.CreateNode(CreateNodeOptions{})
			if err != nil {
				return err
			}
			if err := tx.FTSIndex(n.ID, text); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	got, err := db.FTSSearchContext(context.Background(), "absentneedle", FTSSearchOptions{Limit: 1, MaxWork: 32, MaxBytes: 1024})
	if err != nil || len(got) != 0 {
		t.Fatalf("ready index control=%v err=%v", got, err)
	}
	pagePath := db.files.State
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	pages, err := pagestore.Open(pagePath, pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer pages.Close()
	write, err := pages.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer write.Rollback()
	page := &store.PageGraph{Tx: write}
	for _, index := range []string{store.PageFTSManualStandard, store.PageFTSManualPorter} {
		if err := page.SetFTSIndexReady(index, false); err != nil {
			t.Fatal(err)
		}
	}
	if err := write.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := pages.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, OpenOptions{ReadOnly: true, PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestStaleFTSSearchAdmitsCanonicalSource(t *testing.T) {
	db := staleManualFTSSourceFixture(t, 1, strings.Repeat("x", 1<<20))
	if _, err := db.FTSSearchContext(context.Background(), "needle", FTSSearchOptions{Limit: 1, MaxWork: 32, MaxBytes: 1024}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("low stale scan=%v", err)
	}
	for _, analyzer := range []FTSAnalyzer{FTSAnalyzerStandard, FTSAnalyzerEnglishPorter} {
		for _, scoring := range []FTSScoring{FTSScoringFrequency, FTSScoringBM25} {
			got, err := db.FTSSearchContext(context.Background(), "needle", FTSSearchOptions{Limit: 1, MaxWork: 32 << 20, MaxBytes: 16 << 20, Analyzer: analyzer, Scoring: scoring})
			if err != nil || len(got) != 0 {
				t.Fatalf("admitted analyzer=%v scoring=%v results=%v err=%v", analyzer, scoring, got, err)
			}
		}
	}
}

func TestStaleFTSScanReleasesBytesButSharesBM25Work(t *testing.T) {
	db := staleManualFTSSourceFixture(t, 128, "running needle")
	opts := FTSSearchOptions{Limit: 1, MaxWork: 1 << 20, MaxBytes: 1024, Scoring: FTSScoringBM25}
	got, err := db.FTSSearchContext(context.Background(), "needle", opts)
	if err != nil || len(got) != 1 {
		t.Fatalf("per-record reuse=%v err=%v", got, err)
	}
	budget := &directSearchBudget{ctx: context.Background(), maxWork: 1 << 20, maxBytes: 1024}
	if err := db.View(func(tx *Tx) error {
		_, _, _, err := ftsBM25CorpusStats(context.Background(), tx.graph, []string{"needle"}, opts, budget)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	opts.MaxWork = budget.work + 64
	if _, err := db.FTSSearchContext(context.Background(), "needle", opts); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("BM25 passes reset cumulative work=%v", err)
	}
}

func TestPageFuzzyVocabularyAdmitsRawAndDecodedTerm(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "vocab"), OpenOptions{Create: true, PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Update(func(tx *Tx) error {
		n, err := tx.CreateNode(CreateNodeOptions{})
		if err != nil {
			return err
		}
		return tx.FTSIndex(n.ID, strings.Repeat("a", 1<<20))
	}); err != nil {
		t.Fatal(err)
	}
	opts := FTSSearchOptions{Limit: 1, MaxDistance: 1, MinTermLength: 2, MaxBytes: (1 << 20) + 1024, MaxWork: 32 << 20}
	if _, err = db.FTSSearchContext(context.Background(), "x", opts); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("raw plus decoded term admission=%v", err)
	}
	opts.MaxBytes = 8 << 20
	if got, err := db.FTSSearchContext(context.Background(), "x", opts); err != nil || len(got) != 0 {
		t.Fatalf("sufficient budget=%v err=%v", got, err)
	}
}

func TestPageStreamAdmitsPayloadBeforeDecode(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "stream"), OpenOptions{Create: true, PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Update(func(tx *Tx) error { return tx.PublishStream("events", "event", strings.Repeat("x", 1<<20)) }); err != nil {
		t.Fatal(err)
	}
	got, err := db.ReadStreamContext(context.Background(), "events", 0, StreamReadOptions{Limit: 1000000, MaxBytes: 1024})
	if err != nil || !got.ByteLimited || len(got.Records) != 0 || cap(got.Records) > 22 {
		t.Fatalf("small budget=%+v err=%v", got, err)
	}
	got, err = db.ReadStreamContext(context.Background(), "events", 0, StreamReadOptions{Limit: 1, MaxBytes: 16 << 20})
	if err != nil || got.ByteLimited || len(got.Records) != 1 {
		t.Fatalf("sufficient budget: count=%d limited=%v err=%v", len(got.Records), got.ByteLimited, err)
	}
}

func TestPageVectorMaintenanceAdmitsSource(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "maintenance"), OpenOptions{Create: true, PageStorage: true, EnableVector: true, VectorDimensions: 2, VectorM: 2, VectorIndexMode: VectorIndexHNSWSynchronous})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"embedding": []float32{1, 1}, "body": strings.Repeat("x", 1<<20)}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	db.vectorIndexBuildMaxLogicalBytes = 128 << 10
	if err = db.RebuildVectorIndexContext(context.Background()); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("rebuild admission=%v", err)
	}
	write, err := db.pages.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	budget := &directSearchBudget{ctx: context.Background(), maxWork: 32 << 20, maxBytes: 128 << 10}
	err = applyPageVectorChanges(context.Background(), &store.PageGraph{Tx: write}, db.graph, db.graph, []uint64{1}, budget)
	write.Rollback()
	if !errors.Is(err, ErrResourceLimit) || budget.bytes != 0 {
		t.Fatalf("maintenance admission=%v retained=%d", err, budget.bytes)
	}
	db.vectorIndexBuildMaxLogicalBytes = 32 << 20
	if err = db.RebuildVectorIndexContext(context.Background()); err != nil {
		t.Fatalf("sufficient rebuild=%v", err)
	}
	if err = db.Update(func(tx *Tx) error { return tx.SetProperty(1, "embedding", []float32{2, 2}) }); err != nil {
		t.Fatalf("sufficient maintenance=%v", err)
	}
}
