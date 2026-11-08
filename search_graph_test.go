package latticedb

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSearchGraphFTSRankingExpansionAndBudget(t *testing.T) {
	for _, backend := range []string{"memory", "page"} {
		t.Run(backend, func(t *testing.T) {
			path := ":memory:"
			if backend == "page" {
				path = filepath.Join(t.TempDir(), "db")
			}
			db, err := Open(path, OpenOptions{Create: true, PageSize: 1024})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var ids []uint64
			var edgeIDs []uint64
			if err := db.Update(func(tx *Tx) error {
				for _, label := range []string{"Doc", "Doc", "Keep", "Keep", "Skip"} {
					node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{label}})
					if err != nil {
						return err
					}
					ids = append(ids, node.ID)
				}
				if err := tx.FTSIndex(ids[0], "needle needle"); err != nil {
					return err
				}
				if err := tx.FTSIndex(ids[1], "needle"); err != nil {
					return err
				}
				for _, index := range []int{3, 4, 2, 2} {
					edge, err := tx.CreateEdge(ids[1], ids[index], "LINK", CreateEdgeOptions{})
					if err != nil {
						return err
					}
					edgeIDs = append(edgeIDs, edge.ID)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			ctx := t.Context()
			options := SearchGraphOptions{
				FTS:        &SearchGraphFTS{Text: "needle"},
				Expand:     &SearchGraphExpansion{EdgeType: "LINK", TargetLabel: "Keep"},
				CandidateK: 1, ResultK: 2,
			}
			result, err := db.SearchGraphContext(ctx, options)
			if err != nil || len(result.Rows) != 0 {
				t.Fatalf("candidate K must apply before expansion: %+v, %v", result, err)
			}
			options.CandidateK = 2
			result, err = db.SearchGraphContext(ctx, options)
			want := []SearchGraphRow{
				{NodeID: ids[1], TargetID: ids[2], EdgeID: edgeIDs[2], Value: 1},
				{NodeID: ids[1], TargetID: ids[2], EdgeID: edgeIDs[3], Value: 1},
			}
			if err != nil || !reflect.DeepEqual(result.Rows, want) || result.Metric != SearchGraphFrequency ||
				result.Direction != SearchGraphHigherFirst || !result.Exact || result.DatabaseID == "" {
				t.Fatalf("rank, parallel edges or metadata mismatch: %+v, %v", result, err)
			}
			options.MaxRows = 7 // Two candidates + four examined edges + two output rows need eight.
			if limited, err := db.SearchGraphContext(ctx, options); !errors.Is(err, ErrResourceLimit) || len(limited.Rows) != 0 {
				t.Fatalf("shared row budget: %+v, %v", limited, err)
			}
			options.MaxRows = 8
			if limited, err := db.SearchGraphContext(ctx, options); err != nil || !reflect.DeepEqual(limited.Rows, want) {
				t.Fatalf("exact shared row budget: %+v, %v", limited, err)
			}
			read, err := db.BeginRead()
			if err != nil {
				t.Fatal(err)
			}
			defer read.Rollback()
			fromTx, err := read.SearchGraphContext(ctx, options)
			if err != nil || !reflect.DeepEqual(fromTx, result) {
				t.Fatalf("DB and Tx differ: %+v, %v", fromTx, err)
			}
			if err := db.Update(func(tx *Tx) error {
				if err := tx.FTSIndex(ids[0], "needle needle needle"); err != nil {
					return err
				}
				_, err := tx.CreateEdge(ids[0], ids[3], "LINK", CreateEdgeOptions{})
				return err
			}); err != nil {
				t.Fatal(err)
			}
			options.MaxRows = 0
			pinned, err := read.SearchGraphContext(ctx, options)
			if err != nil || !reflect.DeepEqual(pinned, result) {
				t.Fatalf("old reader mixed generations: %+v, %v", pinned, err)
			}
			head, err := db.SearchGraphContext(ctx, options)
			if err != nil || head.CommitID != result.CommitID+1 || len(head.Rows) != 2 ||
				head.Rows[0].NodeID != ids[0] || head.Rows[0].TargetID != ids[3] || head.Rows[0].Value != 3 {
				t.Fatalf("current head did not see new posting and edge: %+v, %v", head, err)
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if got, err := read.SearchGraphContext(canceled, options); !errors.Is(err, context.Canceled) || len(got.Rows) != 0 {
				t.Fatalf("canceled: %+v, %v", got, err)
			}
		})
	}
}

func TestSearchGraphVectorNativeRanking(t *testing.T) {
	for _, backend := range []string{"memory", "page"} {
		t.Run(backend, func(t *testing.T) {
			path := ":memory:"
			if backend == "page" {
				path = filepath.Join(t.TempDir(), "db")
			}
			namespace := VectorNamespace{Property: "embedding", Scope: "Point", Dimensions: 2, Metric: VectorMetricL2}
			db, err := Open(path, OpenOptions{
				Create: true, EnableVector: true, VectorDimensions: 2, VectorIndexMode: VectorIndexHNSWSynchronous,
				VectorNamespaces: []VectorNamespace{namespace},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if err := db.Update(func(tx *Tx) error {
				for _, x := range []float32{2, 1, -1, 3} {
					_, err := tx.CreateNode(CreateNodeOptions{
						Labels: []string{"Point"}, Properties: map[string]Value{"embedding": []float32{x, 0}},
					})
					if err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			query := &SearchGraphVector{Embedding: []float32{0, 0}, Namespace: &namespace}
			options := SearchGraphOptions{Vector: query, CandidateK: 4, ResultK: 2}
			for _, approximate := range []bool{false, true} {
				query.Approximate = approximate
				result, err := db.SearchGraphContext(t.Context(), options)
				want := []SearchGraphRow{{NodeID: 2, Value: 1}, {NodeID: 3, Value: 1}}
				if err != nil || !reflect.DeepEqual(result.Rows, want) || result.Metric != SearchGraphL2 ||
					result.Direction != SearchGraphLowerFirst || result.Exact == approximate ||
					result.Namespace == nil || *result.Namespace != namespace {
					t.Fatalf("approximate=%v: %+v, %v", approximate, result, err)
				}
			}
		})
	}
}

func TestSearchGraphReportsExactVectorFallback(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "db"), OpenOptions{
		Create: true, EnableVector: true, VectorDimensions: 2, VectorM: 2, VectorIndexMode: VectorIndexHNSWSynchronous,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		for range 20 {
			if _, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]Value{"embedding": []float32{1, 1}}}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	options := SearchGraphOptions{
		Vector:     &SearchGraphVector{Embedding: []float32{1, 1}, Approximate: true, EfSearch: 20},
		CandidateK: 20, ResultK: 20,
	}
	before, err := db.VectorIndexStats()
	if err != nil {
		t.Fatal(err)
	}
	approx, err := db.SearchGraphContext(t.Context(), options)
	if err != nil || !approx.Exact || len(approx.Rows) != 20 {
		t.Fatalf("ANN exact fallback: %+v, %v", approx, err)
	}
	after, err := db.VectorIndexStats()
	if err != nil || after.ExactFallbacks != before.ExactFallbacks+1 {
		t.Fatalf("expected exact fallback counter to advance: before=%d after=%d err=%v", before.ExactFallbacks, after.ExactFallbacks, err)
	}
	options.Vector.Approximate = false
	exact, err := db.SearchGraphContext(t.Context(), options)
	if err != nil || !reflect.DeepEqual(approx, exact) {
		t.Fatalf("fallback differs from exact: %+v, %v", exact, err)
	}
}
