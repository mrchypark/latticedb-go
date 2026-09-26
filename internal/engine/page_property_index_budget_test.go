package engine

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestPagePropertyIndexBuildBudgetAbortsNodeAndEdge(t *testing.T) {
	for _, test := range []struct {
		node   bool
		budget string
	}{{true, "work"}, {true, "logical-bytes"}, {false, "work"}, {false, "logical-bytes"}} {
		node := test.node
		kind, scope := "edge", "LINK"
		if node {
			kind, scope = "node", "Item"
		}
		t.Run(kind+"/"+test.budget, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db")
			db, err := Open(path, OpenOptions{Create: true, PageStorage: true})
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Update(func(tx *Tx) error {
				for i := 0; i < 64; i++ {
					if node {
						if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{scope}, Properties: map[string]any{"value": int64(7)}}); err != nil {
							return err
						}
						continue
					}
					a, err := tx.CreateNode(CreateNodeOptions{})
					if err != nil {
						return err
					}
					b, err := tx.CreateNode(CreateNodeOptions{})
					if err != nil {
						return err
					}
					if _, err := tx.CreateEdge(a.ID, b.ID, scope, CreateEdgeOptions{Properties: map[string]any{"value": int64(7)}}); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			limits := OpenOptions{PageStorage: true, DerivedIndexBuildMaxWork: 10000, DerivedIndexBuildMaxLogicalBytes: 100000}
			if test.budget == "work" {
				limits.DerivedIndexBuildMaxWork = 20
			} else {
				limits.DerivedIndexBuildMaxLogicalBytes = 700
			}
			db, err = Open(path, limits)
			if err != nil {
				t.Fatal(err)
			}
			if node {
				err = db.CreateNodePropertyIndex(scope, "value")
			} else {
				err = db.CreateEdgePropertyIndex(scope, "value")
			}
			if !errors.Is(err, ErrResourceLimit) {
				t.Fatalf("create index error = %v, want ErrResourceLimit", err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			// Reopen the page store and verify the failed physical transaction
			// published neither its definition nor postings written before the limit.
			db, err = Open(path, OpenOptions{PageStorage: true, DerivedIndexBuildMaxWork: 10000, DerivedIndexBuildMaxLogicalBytes: 100000})
			if err != nil {
				t.Fatal(err)
			}
			read, err := db.pages.Begin(false)
			if err != nil {
				t.Fatal(err)
			}
			indexes, err := (&store.PageGraph{Tx: read}).LoadPropertyIndexes(context.Background(), node)
			if err != nil {
				t.Fatal(err)
			}
			definition := store.PropertyIndexDefinition{Scope: scope, Property: "value"}
			if indexes.Has(definition) {
				t.Fatal("failed build left a property index definition")
			}
			bucket := "edge-property-postings"
			if node {
				bucket = "node-property-postings"
			}
			postings := 0
			if err := read.Scan(context.Background(), bucket, nil, nil, func(_, _ []byte) error {
				postings++
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if postings != 0 {
				t.Fatalf("failed build left %d postings", postings)
			}
			if err := read.Rollback(); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			db, err = Open(path, OpenOptions{PageStorage: true, DerivedIndexBuildMaxWork: 10000, DerivedIndexBuildMaxLogicalBytes: 100000})
			if err != nil {
				t.Fatal(err)
			}
			if node {
				err = db.CreateNodePropertyIndex(scope, "value")
			} else {
				err = db.CreateEdgePropertyIndex(scope, "value")
			}
			if err != nil {
				t.Fatalf("definition remained published after failed build: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
