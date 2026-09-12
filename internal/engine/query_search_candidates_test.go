package engine

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"testing"
)

func TestFTSCandidatePathCanFitBudgetsThatRejectTheScan(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "fts-budget"), OpenOptions{Create: true, FTSProperties: []string{"text"}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 1_000; i++ {
			text := "ordinary document"
			if i == 777 {
				text = "needle"
			}
			if _, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"text": text}}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	const query = `MATCH (n) WHERE n.text @@ "needle" RETURN id(n) AS id`
	for _, test := range []struct {
		name string
		opts QueryOptions
	}{
		{name: "work", opts: QueryOptions{MaxWork: 32}},
		{name: "bytes", opts: QueryOptions{MaxBytes: 1 << 10}},
	} {
		t.Run(test.name, func(t *testing.T) {
			indexed, err := db.QueryContext(context.Background(), query, nil, test.opts)
			if err != nil || len(indexed.Rows) != 1 {
				t.Fatalf("indexed query rows=%d err=%v", len(indexed.Rows), err)
			}

			scanTx, err := db.Begin(true)
			if err != nil {
				t.Fatal(err)
			}
			scanGraph := *scanTx.graph
			scanGraph.FTSProperties = nil
			scanTx.graph = &scanGraph
			_, scanErr := scanTx.QueryContext(context.Background(), query, nil, test.opts)
			_ = scanTx.Rollback()
			if !errors.Is(scanErr, ErrResourceLimit) {
				t.Fatalf("unindexed query error=%v, want resource limit", scanErr)
			}
		})
	}
}

func TestApproximateVectorCandidatePathUsesBudgetAndMatchesDirectSearch(t *testing.T) {
	namespace := VectorNamespace{Property: "embedding", Scope: "Group", Dimensions: 2, Metric: VectorMetricL2}
	db, err := Open(filepath.Join(t.TempDir(), "vector-budget"), OpenOptions{
		Create: true, EnableVector: true, VectorDimensions: 2,
		VectorIndexMode: VectorIndexHNSWSynchronous, VectorNamespaces: []VectorNamespace{namespace},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 1_000; i++ {
			if _, err := tx.CreateNode(CreateNodeOptions{
				Labels:     []string{"Group"},
				Properties: map[string]any{"embedding": []float32{float32(i + 10), 0}},
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	query := `MATCH (n:Group) WHERE n.embedding <=> $vector RETURN n LIMIT 5`
	params := map[string]any{"vector": []float32{0, 0}}
	indexed, err := db.QueryContext(context.Background(), query, params, QueryOptions{
		MaxWork: 3_000, VectorNamespace: &namespace, ApproximateVector: true, VectorEfSearch: 5,
	})
	if err != nil {
		t.Fatalf("approximate query error=%v", err)
	}
	if len(indexed.Rows) != 5 {
		t.Fatalf("approximate rows=%d, want 5", len(indexed.Rows))
	}
	indexedIDs := queryResultNodeIDs(indexed)
	direct, err := db.VectorSearch([]float32{0, 0}, VectorSearchOptions{K: 5, Namespace: &namespace})
	if err != nil {
		t.Fatal(err)
	}
	directIDs := make([]uint64, len(direct))
	for i, result := range direct {
		directIDs[i] = result.NodeID
	}
	if !slices.Equal(indexedIDs, directIDs) {
		t.Fatalf("approximate query IDs=%v, direct IDs=%v", indexedIDs, directIDs)
	}

	_, err = db.QueryContext(context.Background(), query, params, QueryOptions{
		MaxWork: 3_000, VectorNamespace: &namespace,
	})
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("exact query error=%v, want resource limit", err)
	}
}

func TestApproximateVectorDirtyTransactionFallsBackToSnapshotScan(t *testing.T) {
	namespace := VectorNamespace{Property: "embedding", Scope: "Group", Dimensions: 2, Metric: VectorMetricL2}
	db, err := Open(filepath.Join(t.TempDir(), "vector-dirty"), OpenOptions{
		Create: true, EnableVector: true, VectorDimensions: 2,
		VectorIndexMode: VectorIndexHNSWSynchronous, VectorNamespaces: []VectorNamespace{namespace},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Group"}, Properties: map[string]any{"embedding": []float32{10, 0}}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	newNode, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Group"}, Properties: map[string]any{"embedding": []float32{0, 0}}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := tx.QueryContext(context.Background(), `MATCH (n:Group) WHERE n.embedding <=> $vector RETURN n LIMIT 1`, map[string]any{"vector": []float32{0, 0}}, QueryOptions{
		VectorNamespace: &namespace, ApproximateVector: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["n"].(Node).ID != newNode.ID {
		t.Fatalf("dirty query rows=%v, want node %d", result.Rows, newNode.ID)
	}
}

func TestApproximateVectorInvariantValidationKeepsEmptyScopeSemantics(t *testing.T) {
	namespace := VectorNamespace{Property: "embedding", Scope: "Present", Dimensions: 2, Metric: VectorMetricL2}
	db, err := Open(filepath.Join(t.TempDir(), "vector-empty"), OpenOptions{
		Create: true, EnableVector: true, VectorDimensions: 2,
		VectorIndexMode: VectorIndexHNSWSynchronous, VectorNamespaces: []VectorNamespace{namespace},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	queries := []struct {
		name   string
		params map[string]any
	}{
		{name: "missing parameter", params: nil},
		{name: "wrong parameter type", params: map[string]any{"vector": "not a vector"}},
	}
	query := `MATCH (n:Empty) WHERE n.embedding <=> $vector RETURN n`
	for _, test := range queries {
		t.Run(test.name, func(t *testing.T) {
			indexed, indexedErr := db.QueryContext(context.Background(), query, test.params, QueryOptions{
				VectorNamespace: &namespace, ApproximateVector: true,
			})
			exact, exactErr := db.QueryContext(context.Background(), query, test.params, QueryOptions{VectorNamespace: &namespace})
			if (indexedErr == nil) != (exactErr == nil) || !errors.Is(indexedErr, exactErr) {
				t.Fatalf("indexed error=%v exact error=%v", indexedErr, exactErr)
			}
			if indexedErr == nil && (len(indexed.Rows) != 0 || len(exact.Rows) != 0) {
				t.Fatalf("empty-scope rows indexed=%v exact=%v", indexed.Rows, exact.Rows)
			}
		})
	}
}

func TestFTSCandidateDoesNotHideEarlierScalarErrorWhenPostingsAreEmpty(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "fts-error-order"), OpenOptions{Create: true, FTSProperties: []string{"text"}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"kind": int64(1), "text": "present"}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	query := `MATCH (n) WHERE n.kind IN $invalid AND n.text @@ $q RETURN id(n) AS id`
	params := map[string]any{"invalid": "not a list", "q": "absent"}
	indexed, indexedErr := db.QueryContext(context.Background(), query, params, QueryOptions{})
	if indexedErr == nil {
		t.Fatalf("indexed query unexpectedly succeeded: %#v", indexed)
	}

	scanTx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	scanGraph := *scanTx.graph
	scanGraph.FTSProperties = nil
	scanTx.graph = &scanGraph
	scanned, scanErr := scanTx.QueryContext(context.Background(), query, params, QueryOptions{})
	_ = scanTx.Rollback()
	if scanErr == nil || indexedErr.Error() != scanErr.Error() {
		t.Fatalf("indexed error=%v scan result=%#v error=%v", indexedErr, scanned, scanErr)
	}
}

func TestFTSCandidateSkipsWidePostingsForNarrowLabel(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "fts-narrow-label"), OpenOptions{Create: true, FTSProperties: []string{"text"}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var rareID uint64
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 1_000; i++ {
			if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Common"}, Properties: map[string]any{"text": "needle"}}); err != nil {
				return err
			}
		}
		node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Rare"}, Properties: map[string]any{"text": "needle"}})
		if err == nil {
			rareID = node.ID
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	result, err := db.QueryContext(context.Background(), `MATCH (n:Rare) WHERE n.text @@ "needle" RETURN id(n) AS id`, nil, QueryOptions{MaxBytes: 1 << 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["id"] != int64(rareID) {
		t.Fatalf("rows=%v, want rare node %d", result.Rows, rareID)
	}
}

func queryResultNodeIDs(result QueryResult) []uint64 {
	ids := make([]uint64, len(result.Rows))
	for i, row := range result.Rows {
		ids[i] = row["n"].(Node).ID
	}
	return ids
}
