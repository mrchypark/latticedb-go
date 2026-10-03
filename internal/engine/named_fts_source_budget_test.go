package engine

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func TestNamedFTSPorterScanAdmitsCanonicalNodeAndEdge(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "named-source"), OpenOptions{Create: true, PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	body := strings.Repeat("x", 1<<20)
	if err := db.Update(func(tx *Tx) error {
		n, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"text": "running", "body": body}})
		if err != nil {
			return err
		}
		_, err = tx.CreateEdge(n.ID, n.ID, "LINK", CreateEdgeOptions{Properties: map[string]any{"text": "running", "body": body}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for _, def := range []FTSIndexDefinition{{Name: "nodes", Kind: FTSIndexNode, Scope: "Item", Property: "text"}, {Name: "edges", Kind: FTSIndexEdge, Scope: "LINK", Property: "text"}} {
		if err := db.CreateFTSIndex(def); err != nil {
			t.Fatal(err)
		}
		// Ready Standard postings avoid unrelated canonical properties.
		got, err := db.FTSSearchIndexContext(context.Background(), def.Name, "running", FTSSearchOptions{Limit: 1, MaxWork: 32, MaxBytes: 1024})
		if err != nil || len(got) != 1 {
			t.Fatalf("ready %s=%v err=%v", def.Name, got, err)
		}
		_, err = db.FTSSearchIndexContext(context.Background(), def.Name, "run", FTSSearchOptions{Analyzer: FTSAnalyzerEnglishPorter, Limit: 1, MaxBytes: 128 << 10})
		if !errors.Is(err, ErrResourceLimit) {
			t.Fatalf("low scan %s=%v", def.Name, err)
		}
		got, err = db.FTSSearchIndexContext(context.Background(), def.Name, "run", FTSSearchOptions{Analyzer: FTSAnalyzerEnglishPorter, Limit: 1, MaxBytes: 8 << 20})
		if err != nil || len(got) != 1 {
			t.Fatalf("admitted scan %s=%v err=%v", def.Name, got, err)
		}
	}
}
