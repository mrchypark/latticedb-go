package engine

import (
	"context"
	"math/rand/v2"
	"path/filepath"
	"reflect"
	"testing"
)

func TestFTSCandidateResultsMatchScanAcrossPropertyChanges(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "fts-differential"), OpenOptions{Create: true, FTSProperties: []string{"text"}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	texts := []string{"rare common", "common common", "한글 Rare", "unrelated", ""}
	var ids []uint64
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 40; i++ {
			node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Document"}, Properties: map[string]any{"text": texts[i%len(texts)], "kind": int64(i % 2)}})
			if err != nil {
				return err
			}
			ids = append(ids, node.ID)
			if err := tx.FTSIndex(node.ID, "manual mismatch"); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	random := rand.New(rand.NewPCG(50, 197))
	for step := 0; step < 20; step++ {
		if err := db.Update(func(tx *Tx) error {
			return tx.SetProperty(ids[random.IntN(len(ids))], "text", texts[random.IntN(len(texts))])
		}); err != nil {
			t.Fatal(err)
		}
		for _, query := range []string{
			"MATCH (n:Document) WHERE n.text @@ $q RETURN id(n) AS id",
			"MATCH (n:Document) WHERE n.text @@ $q RETURN id(n) AS id SKIP 1 LIMIT 3",
			"MATCH (n:Document) WHERE n.kind = 1 AND n.text @@ $q RETURN id(n) AS id LIMIT 3",
			"MATCH (n:Document) WHERE n.text @@ $q RETURN id(n) AS id ORDER BY id DESC LIMIT 3",
		} {
			for _, term := range []string{"rare", "한글", "common rare", "manual", ""} {
				tx, err := db.Begin(true)
				if err != nil {
					t.Fatal(err)
				}
				params := map[string]any{"q": term}
				indexed, indexErr := tx.QueryContext(context.Background(), query, params, QueryOptions{})
				scanGraph := *tx.graph
				scanGraph.FTSProperties = nil
				tx.graph = &scanGraph
				scanned, scanErr := tx.QueryContext(context.Background(), query, params, QueryOptions{})
				_ = tx.Rollback()
				if indexErr != nil || scanErr != nil {
					t.Fatalf("query=%s indexed error=%v scan error=%v", query, indexErr, scanErr)
				}
				if !reflect.DeepEqual(indexed.Rows, scanned.Rows) {
					t.Fatalf("step=%d query=%s term=%q indexed=%v scan=%v", step, query, term, indexed.Rows, scanned.Rows)
				}
			}
		}
	}
}
