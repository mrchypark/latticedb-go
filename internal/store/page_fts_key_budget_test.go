package store

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/pagestore"
)

func TestFTSFullKeyStagingBudget(t *testing.T) {
	tokens := make([]string, 64)
	for i := range tokens {
		tokens[i] = fmt.Sprintf("term%04d", i)
	}
	for _, name := range []string{"text", strings.Repeat("p", 4096)} {
		label := "short"
		if len(name) > 4 {
			label = "long"
		}
		t.Run(label, func(t *testing.T) {
			db, err := pagestore.Open(filepath.Join(t.TempDir(), "pages"), pagestore.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			run := func(remove bool, maxBytes uint64, wantLimit bool) {
				tx, err := db.Begin(true)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				budget := &sourceFTSReadBudget{maxWork: 2 << 20, maxBytes: maxBytes}
				ctx := WithFTSMaintenanceBudget(context.Background(), budget)
				page := &PageGraph{Tx: tx}
				if remove {
					err = page.DeleteFTSDocument(ctx, name, 1)
				} else {
					err = page.ReplaceFTSDocument(ctx, name, 1, tokens)
				}
				buffered, bufferErr := tx.BufferedBytes()
				if bufferErr != nil {
					t.Fatal(bufferErr)
				}
				if buffered > budget.bytes || buffered > maxBytes {
					t.Fatalf("staged=%d charged=%d max=%d", buffered, budget.bytes, maxBytes)
				}
				if wantLimit {
					if !errors.Is(err, errSourceFTSReadBudget) {
						t.Fatalf("admission=%v", err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if !remove {
					if err := page.SetFTSIndexReadyContext(ctx, name, true); err != nil {
						t.Fatal(err)
					}
				}
				buffered, err = tx.BufferedBytes()
				if err != nil || buffered > budget.bytes || buffered > maxBytes {
					t.Fatalf("final staged=%d charged=%d err=%v", buffered, budget.bytes, err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
			}
			if label == "long" {
				run(false, 128<<10, true)
				run(false, 4<<20, false)
				run(true, 128<<10, true)
			} else {
				run(false, 128<<10, false)
			}
			// A rejected deletion must leave the committed postings intact.
			read, err := db.Begin(false)
			if err != nil {
				t.Fatal(err)
			}
			hits := 0
			err = (&PageGraph{Tx: read}).VisitFTSPostings(context.Background(), name, tokens[0], func(PageFTSPosting) error { hits++; return nil })
			read.Rollback()
			if err != nil || hits != 1 {
				t.Fatalf("retained hits=%d err=%v", hits, err)
			}
			run(true, 4<<20, false)
			read, err = db.Begin(false)
			if err != nil {
				t.Fatal(err)
			}
			defer read.Rollback()
			hits = 0
			err = (&PageGraph{Tx: read}).VisitFTSPostings(context.Background(), name, tokens[0], func(PageFTSPosting) error { hits++; return nil })
			if err != nil || hits != 0 {
				t.Fatalf("deleted hits=%d err=%v", hits, err)
			}
		})
	}
}
