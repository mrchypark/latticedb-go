package engine

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestMutationBudgetFailureLeavesGraphUnchanged(t *testing.T) {
	db, center, edgeCount := mutationBudgetDatabase(t)
	defer db.Close()

	_, err := db.QueryContext(t.Context(), `MATCH (n:Center) DETACH DELETE n`, nil, QueryOptions{MaxWork: 3})
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("query error = %v, want resource limit", err)
	}
	assertMutationBudgetGraphUnchanged(t, db, center, edgeCount)
}

func TestMutationCancellationLeavesGraphUnchanged(t *testing.T) {
	db, center, edgeCount := mutationBudgetDatabase(t)
	defer db.Close()
	ctx := &cancelAfterQueryChecks{remaining: 14, done: make(chan struct{})}

	_, err := db.QueryContext(ctx, `MATCH (n:Center) DETACH DELETE n`, nil, QueryOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("query error = %v, want cancellation", err)
	}
	assertMutationBudgetGraphUnchanged(t, db, center, edgeCount)
}

func TestDeleteClauseCancellationStopsInsideEdgeBatch(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "direct-clause.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	_, center, _ := addMutationBudgetEntities(t, tx)
	plan, err := parseQuery(`MATCH (n:Center) DETACH DELETE n`)
	if err != nil {
		t.Fatal(err)
	}
	row := plan.newRow()
	row.set("n", boundValue{Node: tx.graph.Nodes.Get(center)})
	ctx := &cancelAfterQueryChecks{remaining: 13, done: make(chan struct{})}
	budget := newQueryBudget(ctx, QueryOptions{})
	defer releaseQueryBudget(budget)

	if err := plan.deleteClause.apply(tx, []queryRow{row}, budget); !errors.Is(err, context.Canceled) {
		t.Fatalf("delete clause error = %v, want cancellation", err)
	}
	if ctx.checks < 14 {
		t.Fatalf("cancellation checks = %d, want traversal into the edge batch", ctx.checks)
	}
}

func TestDeleteClauseCancellationStopsDuringFTSCleanup(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "direct-fts-cleanup.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Center"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.FTSIndex(node.ID, "one two three four five six"); err != nil {
		t.Fatal(err)
	}
	plan, err := parseQuery(`MATCH (n:Center) DETACH DELETE n`)
	if err != nil {
		t.Fatal(err)
	}
	row := plan.newRow()
	row.set("n", boundValue{Node: tx.graph.Nodes.Get(node.ID)})
	ctx := &cancelAfterQueryChecks{remaining: 6, done: make(chan struct{})}
	budget := newQueryBudget(ctx, QueryOptions{})
	defer releaseQueryBudget(budget)

	if err := plan.deleteClause.apply(tx, []queryRow{row}, budget); !errors.Is(err, context.Canceled) {
		t.Fatalf("delete clause error = %v, want cancellation", err)
	}
	if ctx.checks < 7 {
		t.Fatalf("cleanup checks = %d, want FTS token traversal", ctx.checks)
	}
}

func TestSetClauseCancellationStopsNestedValueNormalization(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "set-normalization.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	node, err := tx.CreateNode(CreateNodeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	row := queryRow{
		slots: []boundValue{{Node: tx.graph.Nodes.Get(node.ID), Bound: true}},
		index: map[string]int{"n": 0},
	}
	ctx := &cancelAfterQueryChecks{remaining: 2, done: make(chan struct{})}
	budget := newQueryBudget(ctx, QueryOptions{})
	defer releaseQueryBudget(budget)
	clause := &setClause{Kind: setProperty, Var: "n", Property: "value", Expr: literalExpr{Value: []any{[]any{[]any{"value"}}}}}

	if err := clause.apply(tx, []queryRow{row}, nil, budget); !errors.Is(err, context.Canceled) {
		t.Fatalf("set clause error = %v, want cancellation (checks=%d)", err, ctx.checks)
	}
	if _, ok, err := tx.GetProperty(node.ID, "value"); err != nil || ok {
		t.Fatalf("cancelled SET persisted value = ok:%v err:%v", ok, err)
	}
	if ctx.checks < 3 {
		t.Fatalf("normalization checks = %d, want nested value traversal", ctx.checks)
	}
}

func TestMutationNormalizationChargesNestedZeroByteVisits(t *testing.T) {
	budget := newQueryBudget(t.Context(), QueryOptions{MaxWork: 2})
	defer releaseQueryBudget(budget)

	if _, _, err := normalizeMutationValue([]any{[]any{}}, budget); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("normalization error = %v, want resource limit", err)
	}
}

func TestMutationNormalizationChecksLongVectorValidation(t *testing.T) {
	ctx := &cancelAfterQueryChecks{remaining: 3, done: make(chan struct{})}
	budget := newQueryBudget(ctx, QueryOptions{})
	defer releaseQueryBudget(budget)

	if _, _, err := normalizeMutationValue(make([]float32, 128), budget); !errors.Is(err, context.Canceled) {
		t.Fatalf("normalization error = %v, want cancellation", err)
	}
}

func TestTransactionMutationBudgetFailurePreservesStatementFork(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transaction-fork.ltdb")
	db, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	prior, center, edgeCount := addMutationBudgetEntities(t, tx)

	_, err = tx.QueryContext(t.Context(), `MATCH (n:Center) DETACH DELETE n`, nil, QueryOptions{MaxWork: 15})
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("query error = %v, want resource limit", err)
	}
	assertTransactionMutationBudgetState(t, tx, prior, center, edgeCount)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.View(func(reopened *Tx) error {
		assertTransactionMutationBudgetState(t, reopened, prior, center, edgeCount)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTransactionMutationCancellationPreservesStatementFork(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "transaction-cancel.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	prior, center, edgeCount := addMutationBudgetEntities(t, tx)
	ctx := &cancelAfterQueryChecks{remaining: 15, done: make(chan struct{})}

	_, err = tx.QueryContext(ctx, `MATCH (n:Center) DETACH DELETE n`, nil, QueryOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("query error = %v, want cancellation", err)
	}
	assertTransactionMutationBudgetState(t, tx, prior, center, edgeCount)
}

func TestTransactionMutationCancellationPreservesFTSCleanup(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "transaction-fts-cancel.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Center"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.FTSIndex(node.ID, "one two three four five six"); err != nil {
		t.Fatal(err)
	}
	before := len(tx.graph.FTS.Get(node.ID).Tokens)
	ctx := &cancelAfterQueryChecks{remaining: 8, done: make(chan struct{})}

	_, err = tx.QueryContext(ctx, `MATCH (n:Center) DETACH DELETE n`, nil, QueryOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("query error = %v, want cancellation", err)
	}
	exists, err := tx.NodeExists(node.ID)
	if err != nil || !exists {
		t.Fatalf("statement cancellation deleted node: exists=%v err=%v", exists, err)
	}
	if record := tx.graph.FTS.Get(node.ID); record == nil || len(record.Tokens) != before {
		t.Fatalf("statement cancellation changed FTS record: %#v", record)
	}
}

func mutationBudgetDatabase(t *testing.T) (*DB, uint64, int) {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "mutation-budget.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	var center uint64
	const edgeCount = 8
	if err := db.Update(func(tx *Tx) error {
		_, center, _ = addMutationBudgetEntities(t, tx)
		return nil
	}); err != nil {
		db.Close()
		t.Fatal(err)
	}
	return db, center, edgeCount
}

func addMutationBudgetEntities(t *testing.T, tx *Tx) (uint64, uint64, int) {
	t.Helper()
	prior, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Prior"}})
	if err != nil {
		t.Fatal(err)
	}
	center, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Center"}})
	if err != nil {
		t.Fatal(err)
	}
	const edgeCount = 8
	for range edgeCount {
		leaf, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Leaf"}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.CreateEdge(center.ID, leaf.ID, "LINK", CreateEdgeOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	return prior.ID, center.ID, edgeCount
}

func assertMutationBudgetGraphUnchanged(t *testing.T, db *DB, center uint64, edgeCount int) {
	t.Helper()
	if err := db.View(func(tx *Tx) error {
		exists, err := tx.NodeExists(center)
		if err != nil {
			return err
		}
		if !exists {
			t.Fatal("center node was deleted after failed mutation")
		}
		edges, err := tx.GetOutgoingEdges(center)
		if err != nil {
			return err
		}
		if len(edges) != edgeCount {
			t.Fatalf("outgoing edges = %d, want %d", len(edges), edgeCount)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func assertTransactionMutationBudgetState(t *testing.T, tx *Tx, prior, center uint64, edgeCount int) {
	t.Helper()
	for _, id := range []uint64{prior, center} {
		exists, err := tx.NodeExists(id)
		if err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Fatalf("node %d is missing after failed statement", id)
		}
	}
	edges, err := tx.GetOutgoingEdges(center)
	if err != nil {
		t.Fatal(err)
	}
	if len(edges) != edgeCount {
		t.Fatalf("outgoing edges = %d, want %d", len(edges), edgeCount)
	}
}

type cancelAfterQueryChecks struct {
	remaining int
	checks    int
	done      chan struct{}
}

func (ctx *cancelAfterQueryChecks) Deadline() (time.Time, bool) { return time.Time{}, false }
func (ctx *cancelAfterQueryChecks) Done() <-chan struct{}       { return ctx.done }
func (ctx *cancelAfterQueryChecks) Value(any) any               { return nil }

func (ctx *cancelAfterQueryChecks) Err() error {
	ctx.checks++
	ctx.remaining--
	if ctx.remaining >= 0 {
		return nil
	}
	select {
	case <-ctx.done:
	default:
		close(ctx.done)
	}
	return context.Canceled
}
