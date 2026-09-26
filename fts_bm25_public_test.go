package latticedb

import "testing"

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
