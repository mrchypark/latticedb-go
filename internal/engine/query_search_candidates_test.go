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

	// Aggregate LIMIT applies to output rows, not the ANN candidate count.
	for _, prefix := range []string{"", "WITH coalesce(0) AS seed "} {
		result, err := db.QueryContext(t.Context(), prefix+"MATCH (n:Group) WHERE n.embedding <=> $vector RETURN count(*) AS c, count(*) AS d LIMIT 1", params, QueryOptions{VectorNamespace: &namespace, ApproximateVector: true})
		if err != nil || len(result.Rows) != 1 || result.Rows[0]["c"] != int64(1000) {
			t.Fatalf("aggregate candidate truncation: %#v, %v", result.Rows, err)
		}
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

func TestPageFTSCandidateSkipsWidePostingsForNarrowLabels(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "page-fts-narrow-label"), OpenOptions{
		Create: true, PageStorage: true, FTSProperties: []string{"text"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 1_000; i++ {
			if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Common"}, Properties: map[string]any{"text": "needle"}}); err != nil {
				return err
			}
		}
		_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Common", "Rare"}, Properties: map[string]any{"text": "needle"}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateFTSIndex(FTSIndexDefinition{Name: "common-text", Kind: FTSIndexNode, Scope: "Common", Property: "text"}); err != nil {
		t.Fatal(err)
	}

	for i, query := range []string{
		`MATCH (n:Rare) WHERE n.text @@ "needle" RETURN id(n) AS id`,
		`MATCH (n:Common:Rare) WHERE n.text @@ "needle" RETURN id(n) AS id`,
	} {
		options := QueryOptions{MaxBytes: 4 << 10}
		if i == 0 {
			options.MaxWork = 32
		}
		result, err := db.QueryContext(context.Background(), query, nil, options)
		if err != nil {
			t.Fatalf("query %q: %v", query, err)
		}
		if len(result.Rows) != 1 {
			t.Fatalf("query %q rows=%v, want the single rare node", query, result.Rows)
		}
	}
}

func TestPageFTSCandidateKeepsSelectiveGlobalPostingUnderLowWork(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "page-fts-global-selective"), OpenOptions{
		Create: true, PageStorage: true, FTSProperties: []string{"text"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var matchID uint64
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 1_000; i++ {
			text := "ordinary"
			if i == 777 {
				text = "needle"
			}
			node, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"text": text}})
			if err != nil {
				return err
			}
			if i == 777 {
				matchID = node.ID
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	result, err := db.QueryContext(t.Context(), `MATCH (n) WHERE n.text @@ "needle" RETURN id(n) AS id`, nil, QueryOptions{
		MaxWork: 32, MaxBytes: 4 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["id"] != int64(matchID) {
		t.Fatalf("rows=%v, want only matching node %d", result.Rows, matchID)
	}
}

func TestPageFTSCandidateKeepsSelectivePostingWithLargeLabelUnderLowWork(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "page-fts-large-label-selective"), OpenOptions{
		Create: true, PageStorage: true, FTSProperties: []string{"text"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var matchID uint64
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 1_000; i++ {
			text := "ordinary"
			if i == 777 {
				text = "needle"
			}
			node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Common"}, Properties: map[string]any{"text": text}})
			if err != nil {
				return err
			}
			if i == 777 {
				matchID = node.ID
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	result, err := db.QueryContext(t.Context(), `MATCH (n:Common) WHERE n.text @@ "needle" RETURN id(n) AS id`, nil, QueryOptions{
		MaxWork: 32, MaxBytes: 4 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["id"] != int64(matchID) {
		t.Fatalf("rows=%v, want only matching node %d", result.Rows, matchID)
	}
}

func TestPageFTSCandidateKeepsTenMatchesWithLargeLabelUnderLowWork(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "page-fts-large-label-ten-matches"), OpenOptions{
		Create: true, PageStorage: true, FTSProperties: []string{"text"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	needle := "needlewithalongdescriptivesuffixover26characters"
	matchIDs := make(map[uint64]struct{})
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 1_000; i++ {
			text := "ordinary"
			if i < 10 {
				text = needle
			}
			node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Common"}, Properties: map[string]any{"text": text}})
			if err != nil {
				return err
			}
			if i < 10 {
				matchIDs[node.ID] = struct{}{}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	result, err := db.QueryContext(t.Context(), `MATCH (n:Common) WHERE n.text @@ "`+needle+`" RETURN id(n) AS id`, nil, QueryOptions{
		MaxWork: 768, MaxBytes: 32 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != len(matchIDs) {
		t.Fatalf("rows=%d, want %d matching nodes", len(result.Rows), len(matchIDs))
	}
	for _, row := range result.Rows {
		id, ok := row["id"].(int64)
		if !ok {
			t.Fatalf("row id=%#v, want int64", row["id"])
		}
		if _, ok := matchIDs[uint64(id)]; !ok {
			t.Fatalf("row id=%d not in matching set", id)
		}
	}
}

func TestPageFTSCandidateReleasesUnionBeforeMemoryBoundedFallback(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "page-fts-candidate-memory-fallback"), OpenOptions{
		Create: true, PageStorage: true, FTSProperties: []string{"text"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 100; i++ {
			text := "ordinary"
			if i < 80 {
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

	query := `MATCH (n) WHERE n.text @@ "needle" RETURN id(n) AS id LIMIT 1`
	plan, err := parseQuery(query)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	budget := newQueryBudget(t.Context(), QueryOptions{MaxBytes: 1 << 10})
	candidate, err := pageFTSSearchCandidate(tx, plan.matchPatterns[0].(nodePattern), plan.whereClauses[0], nil, budget)
	if err != nil {
		t.Fatal(err)
	}
	if candidate != nil || budget.bytes != 0 {
		t.Fatalf("memory-limited candidate=%v budget bytes=%d, want fallback with released scratch", candidate, budget.bytes)
	}
	releaseQueryBudget(budget)
	workBudget := newQueryBudget(t.Context(), QueryOptions{MaxWork: 45, MaxBytes: 1 << 10})
	_, err = pageFTSSearchCandidate(tx, plan.matchPatterns[0].(nodePattern), plan.whereClauses[0], nil, workBudget)
	if !errors.Is(err, ErrResourceLimit) || workBudget.bytes != 0 {
		t.Fatalf("work-limited candidate err=%v budget bytes=%d, want propagated limit and released scratch", err, workBudget.bytes)
	}
	releaseQueryBudget(workBudget)
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	result, err := db.QueryContext(t.Context(), query, nil, QueryOptions{})
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["id"] != int64(1) {
		t.Fatalf("unbounded fallback query rows=%v err=%v", result.Rows, err)
	}
}

func TestPageQueryUsesOnlyCurrentScopedFTSIndex(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "page-fts-candidate"), OpenOptions{
		Create: true, PageStorage: true, FTSProperties: []string{"text"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var matchID uint64
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 300; i++ {
			text := "ordinary"
			if i == 173 {
				text = "needle"
			}
			node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Person"}, Properties: map[string]any{"text": text}})
			if err != nil {
				return err
			}
			if i == 173 {
				matchID = node.ID
			}
		}
		return tx.CreateFTSIndex(FTSIndexDefinition{Name: "person-text", Kind: FTSIndexNode, Scope: "Person", Property: "text"})
	}); err != nil {
		t.Fatal(err)
	}
	query := `MATCH (n:Person) WHERE n.text @@ 'needle' RETURN id(n) AS id`
	result, err := db.QueryContext(t.Context(), query, nil, QueryOptions{MaxWork: 500, MaxBytes: 4096})
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["id"] != int64(matchID) {
		t.Fatalf("scoped page FTS rows=%v err=%v", result.Rows, err)
	}

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	tx.graph.PageBase.SearchIndexesCurrent = false
	_, err = tx.QueryContext(t.Context(), query, nil, QueryOptions{MaxWork: 500, MaxBytes: 4096})
	_ = tx.Rollback()
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("stale page FTS query error=%v, want exact scan resource limit", err)
	}
}

func TestPageApproximateVectorCandidateUsesPagedIndex(t *testing.T) {
	namespace := VectorNamespace{Property: "embedding", Scope: "Group", Dimensions: 2, Metric: VectorMetricL2}
	db, err := Open(filepath.Join(t.TempDir(), "page-vector-candidate"), OpenOptions{
		Create: true, PageStorage: true, EnableVector: true, VectorDimensions: 2,
		VectorIndexMode: VectorIndexHNSWSynchronous, VectorNamespaces: []VectorNamespace{namespace},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 30; i++ {
			if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Group"}, Properties: map[string]any{
				"embedding": []float32{float32(i + 1), 0},
			}}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	query := `MATCH (n:Group) WHERE n.embedding <=> $v RETURN n LIMIT 3`
	params := map[string]any{"v": []float32{0, 0}}
	result, err := db.QueryContext(t.Context(), query, params, QueryOptions{
		MaxWork: 256, VectorNamespace: &namespace, ApproximateVector: true, VectorEfSearch: 3,
	})
	if err != nil || len(result.Rows) != 3 {
		t.Fatalf("paged approximate query rows=%d err=%v", len(result.Rows), err)
	}
	if _, err := db.QueryContext(t.Context(), query, params, QueryOptions{
		MaxWork: 32, VectorNamespace: &namespace,
	}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("exact page vector query error=%v, want resource limit", err)
	}
}

func queryResultNodeIDs(result QueryResult) []uint64 {
	ids := make([]uint64, len(result.Rows))
	for i, row := range result.Rows {
		ids[i] = row["n"].(Node).ID
	}
	return ids
}
