package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/pagestore"
	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestConfiguredFTSLongPropertyStagingBudget(t *testing.T) {
	for _, property := range []string{"text", strings.Repeat("p", 4096)} {
		name := "short"
		if len(property) > 4 {
			name = "long"
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db")
			db, err := Open(path, OpenOptions{Create: true, PageStorage: true})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if db != nil {
					db.Close()
				}
			})
			tokens := make([]string, 64)
			for i := range tokens {
				tokens[i] = fmt.Sprintf("term%04d", i)
			}
			if err := db.Update(func(tx *Tx) error {
				_, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{property: strings.Join(tokens, " ")}})
				return err
			}); err != nil {
				t.Fatal(err)
			}
			pagePath := db.files.State
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db = nil
			inspect := func(wantReady bool) []byte {
				pages, err := pagestore.Open(pagePath, pagestore.Options{ReadOnly: true})
				if err != nil {
					t.Fatal(err)
				}
				defer pages.Close()
				tx, err := pages.Begin(false)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				page := &store.PageGraph{Tx: tx}
				index := configuredPropertyFTSPageIndexName(property)
				ready, err := page.FTSIndexReady(index)
				if err != nil || ready != wantReady {
					t.Fatalf("ready=%v err=%v", ready, err)
				}
				hits := 0
				err = page.VisitFTSPostings(context.Background(), index, tokens[0], func(store.PageFTSPosting) error { hits++; return nil })
				want := 0
				if wantReady {
					want = 1
				}
				if err != nil || hits != want {
					t.Fatalf("hits=%d want=%d err=%v", hits, want, err)
				}
				prefix := append([]byte{byte(len(index) >> 8), byte(len(index))}, index...)
				entries := 0
				for _, bucket := range []string{"fts-postings", "fts-terms", "fts-documents", "fts-stats", "fts-ready"} {
					if err := tx.Scan(context.Background(), bucket, nil, nil, func(key, value []byte) error {
						if bytes.HasPrefix(key, prefix) {
							entries++
						}
						return nil
					}); err != nil {
						t.Fatal(err)
					}
				}
				wantEntries := 0
				if wantReady {
					wantEntries = 131
				}
				if entries != wantEntries {
					t.Fatalf("derived records=%d want=%d", entries, wantEntries)
				}
				stamp, err := tx.Get("search-generation", []byte("history"))
				if err != nil {
					t.Fatal(err)
				}
				return stamp
			}
			before := inspect(false)
			opts := OpenOptions{PageStorage: true, FTSProperties: []string{property}, DerivedIndexBuildMaxWork: 2 << 20, DerivedIndexBuildMaxLogicalBytes: 128 << 10}
			db, err = Open(path, opts)
			if name == "long" {
				if !errors.Is(err, ErrResourceLimit) {
					t.Fatalf("long prefix admission=%v", err)
				}
				if after := inspect(false); !bytes.Equal(before, after) {
					t.Fatal("rejected build changed source history")
				}
				opts.DerivedIndexBuildMaxLogicalBytes = 4 << 20
				db, err = Open(path, opts)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db = nil
			inspect(true)
		})
	}
}
