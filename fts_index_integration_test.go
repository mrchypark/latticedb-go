package latticedb

import (
	"context"
	"path/filepath"
	"testing"
)

func TestScopedFTSSchemaSurvivesIncrementalRestoreAndMemory(t *testing.T) {
	archive := filepath.Join(t.TempDir(), "archive")
	db, err := Open(t.TempDir(), OpenOptions{Create: true, BackupDirectory: archive})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var nodeID, edgeID uint64
	if err = db.Update(func(tx *Tx) error {
		a, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Doc"}, Properties: map[string]any{"body": "running fast"}})
		if err != nil {
			return err
		}
		nodeID = a.ID
		b, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Other"}, Properties: map[string]any{"body": "running fast"}})
		if err != nil {
			return err
		}
		e, err := tx.CreateEdge(a.ID, b.ID, "LINK", CreateEdgeOptions{Properties: map[string]any{"body": "walked slowly"}})
		edgeID = e.ID
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err = db.Update(func(tx *Tx) error {
		if err := tx.CreateFTSIndex(FTSIndexDefinition{Name: "documents", Kind: FTSIndexNode, Scope: "Doc", Property: "body"}); err != nil {
			return err
		}
		return tx.CreateFTSIndex(FTSIndexDefinition{Name: "links", Kind: FTSIndexEdge, Scope: "LINK", Property: "body"})
	}); err != nil {
		t.Fatal(err)
	}
	check := func(t *testing.T, db *DB) {
		t.Helper()
		for _, item := range []struct {
			name, query string
			id          uint64
		}{{"documents", "run", nodeID}, {"links", "walk", edgeID}} {
			hits, err := db.FTSSearchIndex(item.name, item.query, FTSSearchOptions{Analyzer: FTSAnalyzerEnglishPorter, Scoring: FTSScoringBM25})
			if err != nil || len(hits) != 1 || hits[0].EntityID != item.id {
				t.Fatalf("%s: %+v %v", item.name, hits, err)
			}
		}
		hits, err := db.FTSSearchIndex("documents", "runing", FTSSearchOptions{MaxDistance: 1, MinTermLength: 1})
		if err != nil || len(hits) != 1 || hits[0].EntityID != nodeID {
			t.Fatalf("fuzzy: %+v %v", hits, err)
		}
	}
	check(t, db)
	data, err := db.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	memory, err := Deserialize(data, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	check(t, memory)
	if err = memory.Close(); err != nil {
		t.Fatal(err)
	}
	if err = db.Update(func(tx *Tx) error {
		if err := tx.DropFTSIndex("documents"); err != nil {
			return err
		}
		return tx.DropFTSIndex("links")
	}); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	commit := uint64(2)
	destination := filepath.Join(t.TempDir(), "restored")
	if _, err = RestoreBackup(context.Background(), archive, destination, BackupRestoreOptions{CommitID: &commit}); err != nil {
		t.Fatal(err)
	}
	restored, err := Open(destination, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	check(t, restored)
	if err = restored.Close(); err != nil {
		t.Fatal(err)
	}
	latestPath := filepath.Join(t.TempDir(), "latest")
	if _, err = RestoreBackup(context.Background(), archive, latestPath, BackupRestoreOptions{}); err != nil {
		t.Fatal(err)
	}
	latest, err := Open(latestPath, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer latest.Close()
	if _, err = latest.FTSSearchIndex("documents", "running", FTSSearchOptions{}); err == nil {
		t.Fatal("dropped definition returned after restore")
	}
}
