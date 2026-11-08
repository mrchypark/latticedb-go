package engine

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
)

func TestQueryExplainOrderDirectionAndSnapshot(t *testing.T) {
	for _, page := range []bool{false, true} {
		name := "memory"
		if page {
			name = "page"
		}
		t.Run(name, func(t *testing.T) {
			db := openQueryExplainFixture(t, page)
			defer db.Close()

			query := "MATCH (b:Broad)-[e:LINK]->(r:Rare) WHERE r.key = \"seed\" RETURN b.id AS bid, r.key AS key ORDER BY id(b), id(e), id(r)"
			explanation, err := db.ExplainContext(t.Context(), query, nil, QueryOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if explanation.SchemaVersion != 1 || explanation.DatabaseID == "" || explanation.CommitID != db.commitID {
				t.Fatalf("snapshot identity = %#v", explanation)
			}
			operators := explanation.Root.Children
			if len(operators) < 2 || operators[0].Name != "NodeIndexSeek" || operators[1].Name != "EdgeExpand" {
				t.Fatalf("operator order = %#v", explainNames(explanation))
			}
			if operators[0].Estimate == nil || *operators[0].Estimate != 1 || operators[0].Access != "node_property:Rare.key" {
				t.Fatalf("seed operator = %#v", operators[0])
			}
			if operators[1].Direction != "incoming" || operators[1].Estimate == nil || *operators[1].Estimate != 32 {
				t.Fatalf("edge operator = %#v", operators[1])
			}

			tx, err := db.Begin(true)
			if err != nil {
				t.Fatal(err)
			}
			fromTx, err := tx.ExplainContext(t.Context(), query, nil, QueryOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if fromTx.CommitID != tx.commitID || !reflect.DeepEqual(fromTx.Root, explanation.Root) {
				t.Fatalf("Tx explanation = %#v, want %#v", fromTx, explanation)
			}

			if err := db.Update(func(tx *Tx) error {
				_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"AfterExplain"}})
				return err
			}); err != nil {
				t.Fatal(err)
			}
			old, err := tx.ExplainContext(t.Context(), query, nil, QueryOptions{})
			if err != nil {
				t.Fatal(err)
			}
			current, err := db.ExplainContext(t.Context(), query, nil, QueryOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if old.CommitID != explanation.CommitID || current.CommitID == old.CommitID {
				t.Fatalf("old commit = %d, current commit = %d", old.CommitID, current.CommitID)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestQueryExplainKnownZeroUnknownAndFallback(t *testing.T) {
	db := openQueryExplainFixture(t, false)
	defer db.Close()

	knownZero, err := db.ExplainContext(t.Context(), "MATCH (r:Rare) WHERE r.key = \"absent\" RETURN r", nil, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	zero := knownZero.Root.Children[0]
	if zero.Name != "NodeIndexSeek" || zero.Estimate == nil || *zero.Estimate != 0 {
		t.Fatalf("known-zero operator = %#v", zero)
	}

	unknown, err := db.ExplainContext(t.Context(), "MATCH (r:Rare) WHERE r.key = $missing RETURN r", nil, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dynamic := unknown.Root.Children[0]
	if dynamic.Name != "NodeIndexSeek" || dynamic.Estimate != nil || !dynamic.Conditional || dynamic.Reason == "" || dynamic.Fallback != "node_scan" {
		t.Fatalf("dynamic operator = %#v", dynamic)
	}

	if err := db.DropNodePropertyIndex("Rare", "key"); err != nil {
		t.Fatal(err)
	}
	fallback, err := db.ExplainContext(t.Context(), "MATCH (r:Rare) WHERE r.key = \"seed\" RETURN r", nil, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	scan := fallback.Root.Children[0]
	if scan.Name != "NodeScan" || scan.Access != "label:Rare" || scan.Estimate == nil || *scan.Estimate != 1 {
		t.Fatalf("fallback operator = %#v", scan)
	}

	search, err := db.ExplainContext(t.Context(), "MATCH (r:Rare) WHERE r.key @@ \"seed\" RETURN r", nil, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if scan := search.Root.Children[0]; scan.Name != "NodeScan" || scan.Conditional {
		t.Fatalf("unconfigured search must not claim an indexed seed: %#v", scan)
	}

	indexed, err := Open(":memory:", OpenOptions{FTSProperties: []string{"key"}})
	if err != nil {
		t.Fatal(err)
	}
	defer indexed.Close()
	search, err = indexed.ExplainContext(t.Context(), "MATCH (r:Rare) WHERE r.key @@ 'seed' RETURN r", nil, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	searchOperator := search.Root.Children[0]
	if searchOperator.Name != "SearchScan" || searchOperator.Estimate != nil || !searchOperator.Conditional || searchOperator.Fallback != "node_scan" {
		t.Fatalf("search operator = %#v", searchOperator)
	}
}

func TestQueryExplainTopKAndPagination(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "query-explain-topk.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	topK, err := db.ExplainContext(t.Context(), "MATCH (n:Item) RETURN id(n) AS id ORDER BY id(n) SKIP 1 LIMIT 2", nil, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if names := explainNames(topK); !reflect.DeepEqual(names, []string{"NodeScan", "TopK", "Project"}) {
		t.Fatalf("top-K operators = %#v", names)
	}

	paginated, err := db.ExplainContext(t.Context(), "MATCH (n:Item) RETURN DISTINCT id(n) AS id ORDER BY id(n) SKIP 1 LIMIT 2", nil, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if names := explainNames(paginated); !reflect.DeepEqual(names, []string{"NodeScan", "Sort", "Project", "Distinct", "Skip", "Limit"}) {
		t.Fatalf("paginated operators = %#v", names)
	}
	if got := paginated.Root.Children[1].Access; got != "id(n) ASC" {
		t.Fatalf("sort access = %q", got)
	}
}

func TestQueryExplainRejectsMutationAndUnsupportedShapes(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "query-explain-reject.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Build"}, Properties: map[string]any{"value": int64(1)}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	beforeCommit := db.commitID

	_, err = db.ExplainContext(t.Context(), "MATCH (n:Build) SET n.changed = true RETURN n", nil, QueryOptions{})
	if !isQueryErrorStage(err, QueryErrorStagePlan) || !errors.Is(err, ErrUnsupportedOption) {
		t.Fatalf("mutation error = %v", err)
	}
	result, err := db.Query("MATCH (n:Build) RETURN n.value AS value, n.changed AS changed", nil)
	if err != nil {
		t.Fatal(err)
	}
	if db.commitID != beforeCommit || len(result.Rows) != 1 || result.Rows[0]["value"] != int64(1) || result.Rows[0]["changed"] != nil {
		t.Fatalf("mutation rejection changed state: commit=%d rows=%#v", db.commitID, result.Rows)
	}

	for _, query := range []string{
		"MATCH (n) WITH n RETURN n",
		"MATCH (n) RETURN n UNION MATCH (m) RETURN m AS n",
		"OPTIONAL MATCH (n) RETURN n",
		"MATCH (a)-[:LINK*1..2]->(b) RETURN a",
	} {
		_, err := db.ExplainContext(t.Context(), query, nil, QueryOptions{})
		if !isQueryErrorStage(err, QueryErrorStagePlan) || !errors.Is(err, ErrUnsupportedOption) {
			t.Fatalf("%s error = %v", query, err)
		}
	}

	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExplainContext(t.Context(), "RETURN 1 AS value", nil, QueryOptions{}); !isQueryErrorStage(err, QueryErrorStagePlan) {
		t.Fatalf("write Tx error = %v", err)
	}
}

func TestQueryExplainBudgetAndCancellation(t *testing.T) {
	db := openQueryExplainFixture(t, false)
	defer db.Close()
	query := "MATCH (b:Broad)-[e:LINK]->(r:Rare) RETURN b, e, r ORDER BY id(b), id(e), id(r)"

	for _, opts := range []QueryOptions{{MaxWork: 1}, {MaxBytes: 1}} {
		_, err := db.ExplainContext(t.Context(), query, nil, opts)
		if !isQueryErrorStage(err, QueryErrorStagePlan) || !errors.Is(err, ErrResourceLimit) {
			t.Fatalf("opts %#v error = %v", opts, err)
		}
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := db.ExplainContext(ctx, query, nil, QueryOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled explain error = %v", err)
	}
}

func openQueryExplainFixture(t *testing.T, page bool) *DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "query-explain.ltdb"), OpenOptions{Create: true, PageStorage: page})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		rare, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Rare"}, Properties: map[string]any{"key": "seed"}})
		if err != nil {
			return err
		}
		for i := range 32 {
			broad, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Broad"}, Properties: map[string]any{"id": int64(i)}})
			if err != nil {
				return err
			}
			if _, err := tx.CreateEdge(broad.ID, rare.ID, "LINK", CreateEdgeOptions{}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("Rare", "key"); err != nil {
		db.Close()
		t.Fatal(err)
	}
	return db
}

func explainNames(explanation QueryExplanation) []string {
	names := make([]string, len(explanation.Root.Children))
	for index, operator := range explanation.Root.Children {
		names[index] = operator.Name
	}
	return names
}

func isQueryErrorStage(err error, stage QueryErrorStage) bool {
	var queryErr *QueryError
	return errors.As(err, &queryErr) && queryErr.Stage == stage
}

func TestQueryExplainCandidateSources(t *testing.T) {
	db := openQueryExplainFixture(t, false)
	defer db.Close()
	for _, tc := range []struct {
		query, name string
		count       uint64
		unknown     bool
	}{
		{"MATCH (n) RETURN n", "NodeScan", 33, false},
		{"MATCH (a)<-[e]-(b) RETURN a, b", "EdgeScan", 32, false},
		{"MATCH (n:Rare) WHERE n.key = $missing OR n.key = 'seed' RETURN n", "NodeScan", 0, true},
		{"MATCH (n:Rare), (m:Rare) WHERE n.key @@ 'seed' RETURN n", "NodeScan", 0, true},
	} {
		explanation, err := db.ExplainContext(t.Context(), tc.query, nil, QueryOptions{})
		if err != nil {
			t.Fatalf("%s: %v", tc.query, err)
		}
		op := explanation.Root.Children[0]
		if op.Name != tc.name || op.Direction != "" || (op.Estimate == nil) != tc.unknown ||
			(op.Estimate != nil && *op.Estimate != tc.count) {
			t.Fatalf("%s: unexpected operator %+v", tc.query, op)
		}
	}
}
