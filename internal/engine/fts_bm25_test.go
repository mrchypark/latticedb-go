package engine

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestFTSSearchBM25RanksShortRareDocumentFirst(t *testing.T) {
	db, err := Open(t.TempDir(), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var long, short uint64
	if err := db.Update(func(tx *Tx) error {
		first, err := tx.CreateNode(CreateNodeOptions{})
		if err != nil {
			return err
		}
		long = first.ID
		second, err := tx.CreateNode(CreateNodeOptions{})
		if err != nil {
			return err
		}
		short = second.ID
		if err := tx.FTSIndex(long, "rare "+repeatFTSToken("common", 100)); err != nil {
			return err
		}
		return tx.FTSIndex(short, "rare")
	}); err != nil {
		t.Fatal(err)
	}
	frequency, err := db.FTSSearch("rare", FTSSearchOptions{Limit: 2})
	if err != nil || len(frequency) != 2 || frequency[0].NodeID != long {
		t.Fatalf("frequency results=%v error=%v", frequency, err)
	}
	bm25, err := db.FTSSearch("rare", FTSSearchOptions{Limit: 2, Scoring: FTSScoringBM25})
	if err != nil || len(bm25) != 2 || bm25[0].NodeID != short || bm25[0].Score <= bm25[1].Score {
		t.Fatalf("BM25 results=%v error=%v", bm25, err)
	}
}

func TestFTSSearchEnglishPorterAndFuzzy(t *testing.T) {
	db, err := Open(t.TempDir(), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var id uint64
	if err := db.Update(func(tx *Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{})
		if err != nil {
			return err
		}
		id = node.ID
		return tx.FTSIndex(id, "running cats")
	}); err != nil {
		t.Fatal(err)
	}
	if got, err := db.FTSSearch("run", FTSSearchOptions{Limit: 1}); err != nil || len(got) != 0 {
		t.Fatalf("standard analyzer results=%v error=%v", got, err)
	}
	porter := FTSSearchOptions{Limit: 1, Analyzer: FTSAnalyzerEnglishPorter}
	if got, err := db.FTSSearch("run", porter); err != nil || len(got) != 1 || got[0].NodeID != id || got[0].Score != 1 {
		t.Fatalf("Porter results=%v error=%v", got, err)
	}
	if got, err := db.FTSSearch("cat", FTSSearchOptions{Limit: 1, Analyzer: FTSAnalyzerEnglishPorter, Scoring: FTSScoringBM25}); err != nil || len(got) != 1 || got[0].NodeID != id {
		t.Fatalf("Porter BM25 results=%v error=%v", got, err)
	}
	if got, err := db.FTSSearch("runnin", FTSSearchOptions{Limit: 1, MaxDistance: 1, MinTermLength: 1, Scoring: FTSScoringBM25}); err != nil || len(got) != 1 || got[0].NodeID != id {
		t.Fatalf("fuzzy BM25 results=%v error=%v", got, err)
	}
}

func TestFTSSearchEnglishPorterTracksUpdateAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fts")
	db, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	var id uint64
	if err := db.Update(func(tx *Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{})
		if err != nil {
			return err
		}
		id = node.ID
		return tx.FTSIndex(id, "running")
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error { return tx.FTSIndex(id, "connected") }); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, test := range []struct {
		query string
		want  bool
	}{{"run", false}, {"connect", true}} {
		got, err := db.FTSSearchContext(context.Background(), test.query, FTSSearchOptions{Limit: 1, Analyzer: FTSAnalyzerEnglishPorter, Scoring: FTSScoringBM25})
		if err != nil || (len(got) == 1) != test.want {
			t.Fatalf("query=%q results=%v error=%v", test.query, got, err)
		}
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := db.FTSSearchContext(canceled, "connect", FTSSearchOptions{Analyzer: FTSAnalyzerEnglishPorter, Scoring: FTSScoringBM25}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Porter BM25 = %v", err)
	}
}

func repeatFTSToken(token string, count int) string {
	result := ""
	for i := 0; i < count; i++ {
		result += token + " "
	}
	return result
}
