package latticedb

import "testing"

func TestPublicFuzzyFrequencyRanking(t *testing.T) {
	for _, mode := range []string{"memory", "disk"} {
		t.Run(mode, func(t *testing.T) {
			path := ":memory:"
			if mode == "disk" {
				path = t.TempDir()
			}
			db, err := Open(path, OpenOptions{Create: true})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if db != nil {
					_ = db.Close()
				}
			})
			var want uint64
			if err := db.Update(func(tx *Tx) error {
				for _, text := range []string{"book books", "book book"} {
					node, err := tx.CreateNode(CreateNodeOptions{})
					if err != nil {
						return err
					}
					want = node.ID
					if err := tx.FTSIndex(node.ID, text); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			check := func() {
				t.Helper()
				hits, err := db.FTSSearch("book", FTSSearchOptions{Limit: 1, MaxDistance: 1, MinTermLength: 1})
				if err != nil || len(hits) != 1 || hits[0].NodeID != want || hits[0].Score != 2 {
					t.Fatalf("winner=%v err=%v, want node %d score 2", hits, err, want)
				}
			}
			check()
			if mode == "disk" {
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				db, err = Open(path, OpenOptions{})
				if err != nil {
					t.Fatal(err)
				}
				check()
			}
		})
	}
}

func TestPublicFTSSearchBM25AndPorter(t *testing.T) {
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
		return tx.FTSIndex(id, "running")
	}); err != nil {
		t.Fatal(err)
	}
	results, err := db.FTSSearch("run", FTSSearchOptions{Limit: 1, Scoring: FTSScoringBM25, Analyzer: FTSAnalyzerEnglishPorter})
	if err != nil || len(results) != 1 || results[0].NodeID != id {
		t.Fatalf("results=%v err=%v", results, err)
	}
}
