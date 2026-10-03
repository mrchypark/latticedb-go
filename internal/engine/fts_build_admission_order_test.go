package engine

import (
	"context"
	"encoding/binary"
	"errors"
	"path/filepath"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/pagestore"
	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestFTSMaintenanceAdmitsMalformedSourceBeforeDecode(t *testing.T) {
	for _, kind := range []string{"node", "edge", "configured"} {
		for _, incremental := range []bool{false, true} {
			name := kind + "/rebuild"
			if incremental {
				name = kind + "/incremental"
			}
			t.Run(name, func(t *testing.T) {
				pages, err := pagestore.Open(filepath.Join(t.TempDir(), "pages"), pagestore.Options{})
				if err != nil {
					t.Fatal(err)
				}
				defer pages.Close()
				tx, err := pages.Begin(true)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				page := &store.PageGraph{Tx: tx}
				key := make([]byte, 8)
				binary.BigEndian.PutUint64(key, 1)
				bucket := "nodes"
				def := FTSIndexDefinition{Name: "text", Kind: FTSIndexNode, Scope: "Item", Property: "text"}
				if kind == "edge" {
					bucket = "edges"
					def.Kind = FTSIndexEdge
					def.Scope = "LINK"
				}
				// Deliberately invalid framing. A raw-size rejection must win over decode.
				if err := tx.Put(bucket, key, make([]byte, 1<<20)); err != nil {
					t.Fatal(err)
				}
				run := func(maxBytes uint64) error {
					budget := &ftsIndexBudget{maxWork: 32 << 20, maxBytes: maxBytes}
					ctx := store.WithFTSMaintenanceBudget(context.Background(), budget)
					if incremental {
						switch kind {
						case "node":
							return indexDeclaredNode(ctx, page, def, "test", 1, budget)
						case "edge":
							return indexDeclaredEdge(ctx, page, def, "test", 1, budget)
						default:
							return indexConfiguredPropertyNode(ctx, page, "text", "test", 1, budget)
						}
					}
					if kind == "configured" {
						return rebuildConfiguredPropertyPageFTS(ctx, page, nil, "text", budget)
					}
					return rebuildDeclaredPageFTS(ctx, page, def, "test", budget)
				}
				if err := run(16 << 10); !errors.Is(err, ErrResourceLimit) {
					t.Fatalf("source admission: %v", err)
				}
				if err := run(32 << 20); err == nil || errors.Is(err, ErrResourceLimit) {
					t.Fatalf("admitted malformed source must fail decoding: %v", err)
				}
			})
		}
	}
}
