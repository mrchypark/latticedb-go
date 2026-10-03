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

func TestFTSBuildAdmitsUnrelatedCanonicalProperties(t *testing.T) {
	for _, kind := range []string{"node", "edge", "configured"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "build-source")
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
				props := map[string]any{"body": strings.Repeat("x", 1<<20)}
				if kind == "edge" {
					n, err := tx.CreateNode(CreateNodeOptions{})
					if err != nil {
						return err
					}
					_, err = tx.CreateEdge(n.ID, n.ID, "OTHER", CreateEdgeOptions{Properties: props})
					return err
				}
				_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Other"}, Properties: props})
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			low := OpenOptions{PageStorage: true, DerivedIndexBuildMaxWork: 8 << 20, DerivedIndexBuildMaxLogicalBytes: 1024}
			def := FTSIndexDefinition{Name: "filtered", Kind: FTSIndexNode, Scope: "Item", Property: "text"}
			if kind == "edge" {
				def.Kind = FTSIndexEdge
				def.Scope = "LINK"
			}
			if kind == "configured" {
				low.FTSProperties = []string{"text"}
				db, err = Open(path, low)
			} else {
				db, err = Open(path, low)
				if err != nil {
					t.Fatal(err)
				}
				before := db.commitID
				err = db.CreateFTSIndex(def)
				if db.commitID != before {
					t.Fatal("rejected source read published index definition")
				}
			}
			if !errors.Is(err, ErrResourceLimit) {
				t.Fatalf("source read under 1KiB: %v", err)
			}
			if db != nil {
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				db = nil
			}
			high := low
			high.DerivedIndexBuildMaxLogicalBytes = 8 << 20
			db, err = Open(path, high)
			if err != nil {
				t.Fatal(err)
			}
			if kind != "configured" {
				if err := db.CreateFTSIndex(def); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
