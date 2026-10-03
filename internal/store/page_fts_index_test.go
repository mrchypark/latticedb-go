package store

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/pagestore"
)

func TestPageFTSPostingsUseExactTermBoundariesAndSurviveReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pages")
	db, err := pagestore.Open(path, pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	write, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	page := &PageGraph{Tx: write}
	if err := page.ReplaceFTSDocument(context.Background(), "manual-standard", 1, []string{"go", "go"}); err != nil {
		t.Fatal(err)
	}
	if err := page.ReplaceFTSDocument(context.Background(), "manual-standard", 2, []string{"golang"}); err != nil {
		t.Fatal(err)
	}
	if err := page.SetFTSIndexReady("manual-standard", true); err != nil {
		t.Fatal(err)
	}
	if err := write.Commit(); err != nil {
		t.Fatal(err)
	}
	read, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	var ids []uint64
	if err := (&PageGraph{Tx: read}).VisitFTSPostings(context.Background(), "manual-standard", "go", func(posting PageFTSPosting) error {
		ids = append(ids, posting.DocumentID)
		if posting.Frequency != 2 {
			t.Fatalf("go frequency=%d", posting.Frequency)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("exact go postings=%v", ids)
	}
	if err := read.Rollback(); err != nil {
		t.Fatal(err)
	}
	write, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := (&PageGraph{Tx: write}).ReplaceFTSDocument(context.Background(), "manual-standard", 1, []string{"changed"}); err != nil {
		t.Fatal(err)
	}
	if err := write.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = pagestore.Open(path, pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	read, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Rollback()
	page = &PageGraph{Tx: read}
	ready, err := page.FTSIndexReady("manual-standard")
	if err != nil || !ready {
		t.Fatalf("ready=%t err=%v", ready, err)
	}
	ids = nil
	if err := page.VisitFTSPostings(context.Background(), "manual-standard", "go", func(p PageFTSPosting) error { ids = append(ids, p.DocumentID); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("rollback/reopen postings=%v", ids)
	}
}

func TestPageFTSVocabularyChecksTermSizeBeforeVisit(t *testing.T) {
	db, err := pagestore.Open(filepath.Join(t.TempDir(), "pages"), pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	page := &PageGraph{Tx: tx}
	if err := page.ReplaceFTSDocument(context.Background(), "declared:x", 1, []string{"largevocabularyterm"}); err != nil {
		t.Fatal(err)
	}
	visited := false
	err = page.VisitFTSVocabularyWithLimit(context.Background(), "declared:x", 2, func(string) error { visited = true; return nil })
	if !errors.Is(err, ErrLoadResourceLimit) || visited {
		t.Fatalf("visit=%t err=%v", visited, err)
	}
	_ = tx.Rollback()
}

func TestPageFTSRejectsCorruptAggregateLength(t *testing.T) {
	db, err := pagestore.Open(filepath.Join(t.TempDir(), "pages"), pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	write, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	page := &PageGraph{Tx: write}
	if err := page.ReplaceFTSDocument(context.Background(), "manual-standard", 1, []string{"one", "two"}); err != nil {
		t.Fatal(err)
	}
	bad, err := encodePageRecord(10, func(e *binaryEncoder) { e.u(1); e.u(0) })
	if err != nil {
		t.Fatal(err)
	}
	if err := write.Put(pageFTSStats, pageFTSIndexPrefix("manual-standard"), bad); err != nil {
		t.Fatal(err)
	}
	if err := page.ReplaceFTSDocument(context.Background(), "manual-standard", 1, []string{"three"}); err == nil {
		t.Fatal("corrupt aggregate underflow accepted")
	}
	_ = write.Rollback()
}

func TestPageFTSPostingsSupportTokensAboveBboltKeyLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pages")
	db, err := pagestore.Open(path, pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	term := strings.Repeat("a", 33<<10)
	write, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	page := &PageGraph{Tx: write}
	if err := page.ReplaceFTSDocument(context.Background(), PageFTSManualStandard, 1, []string{term}); err != nil {
		t.Fatalf("write large token posting: %v", err)
	}
	if err := write.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = pagestore.Open(path, pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	read, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Rollback()
	var got []uint64
	page = &PageGraph{Tx: read}
	if err := page.VisitFTSPostings(context.Background(), PageFTSManualStandard, term, func(p PageFTSPosting) error { got = append(got, p.DocumentID); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != 1 {
		t.Fatalf("large token postings=%v", got)
	}
	var vocabulary int
	if err := page.VisitFTSVocabulary(context.Background(), PageFTSManualStandard, func(v string) error {
		if v == term {
			vocabulary++
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if vocabulary != 1 {
		t.Fatalf("large token vocabulary count=%d", vocabulary)
	}
}

func TestDropFTSIndexAdmitsKeysAndDeletesWithoutSkipping(t *testing.T) {
	db, err := pagestore.Open(filepath.Join(t.TempDir(), "drop-keys"), pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	write, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer write.Rollback()
	page := &PageGraph{Tx: write}
	buckets := []string{pageFTSPostings, pageFTSDocuments, pageFTSTerms, pageFTSStats, pageFTSReady}
	payload := []byte(strings.Repeat("x", 33<<10))
	for _, bucket := range buckets {
		for id := uint64(1); id <= 160; id++ {
			if err := write.Put(bucket, pageFTSDocumentKey("docs", id), payload); err != nil {
				t.Fatal(err)
			}
		}
		if err := write.Put(bucket, pageFTSDocumentKey("other", 1), payload); err != nil {
			t.Fatal(err)
		}
	}
	low := &sourceFTSReadBudget{maxWork: 1000, maxBytes: 32}
	ctx := WithFTSMaintenanceBudget(context.Background(), low)
	if err := page.DropFTSIndexWithCharge(ctx, "docs", nil); !errors.Is(err, errSourceFTSReadBudget) {
		t.Fatalf("low-budget drop=%v", err)
	}
	for _, bucket := range buckets {
		var count int
		if err := write.ScanKeys(context.Background(), bucket, nil, nil, func([]byte) error { count++; return nil }); err != nil {
			t.Fatal(err)
		}
		if count != 161 {
			t.Fatalf("%s changed before key admission: count=%d", bucket, count)
		}
	}
	enough := &sourceFTSReadBudget{maxWork: 1000, maxBytes: 128 << 10}
	ctx = WithFTSMaintenanceBudget(context.Background(), enough)
	if err := page.DropFTSIndexWithCharge(ctx, "docs", nil); err != nil {
		t.Fatal(err)
	}
	if enough.work != 800 {
		t.Fatalf("work=%d, want each of 800 records charged once", enough.work)
	}
	for _, bucket := range buckets {
		var count int
		if err := write.ScanKeys(context.Background(), bucket, nil, nil, func(key []byte) error {
			count++
			if string(key) != string(pageFTSDocumentKey("other", 1)) {
				t.Fatalf("%s retained target key", bucket)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("%s remaining=%d, want other index only", bucket, count)
		}
	}
}
