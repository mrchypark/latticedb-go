package engine

import (
	"context"
	"errors"
	"math"
	"testing"
)

func TestDistinctResultRowsCanonicalizesVectorSignedZero(t *testing.T) {
	negativeZero := float32(math.Copysign(0, -1))
	rows := []map[string]any{
		{"value": []float32{negativeZero, 1}},
		{"value": []float32{0, 1}},
	}
	budget := newQueryBudget(context.Background(), QueryOptions{})
	defer releaseQueryBudget(budget)

	distinct, err := distinctResultRows([]string{"value"}, rows, budget)
	if err != nil {
		t.Fatal(err)
	}
	if len(distinct) != 1 {
		t.Fatalf("DISTINCT rows = %d, want 1", len(distinct))
	}
}

func TestDistinctResultRowsMatchesNestedMapsListsAndNull(t *testing.T) {
	first := map[string]any{}
	first["outer"] = map[string]any{"items": []any{nil, int64(1)}}
	first["name"] = "value"
	second := map[string]any{}
	second["name"] = "value"
	second["outer"] = map[string]any{"items": []any{nil, int64(1)}}
	budget := newQueryBudget(context.Background(), QueryOptions{})
	defer releaseQueryBudget(budget)

	distinct, err := distinctResultRows([]string{"value"}, []map[string]any{{"value": first}, {"value": second}}, budget)
	if err != nil {
		t.Fatal(err)
	}
	if len(distinct) != 1 {
		t.Fatalf("DISTINCT rows = %d, want 1", len(distinct))
	}
}

func TestDistinctResultRowsStopsDuringVectorKeyWork(t *testing.T) {
	vector := make([]float32, 128)
	budget := newQueryBudget(context.Background(), QueryOptions{MaxWork: 3})
	defer releaseQueryBudget(budget)

	_, err := distinctResultRows([]string{"value"}, []map[string]any{{"value": vector}}, budget)
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("DISTINCT vector key error = %v, want resource limit", err)
	}
}

func TestDistinctResultRowsStopsDuringMapKeySortWork(t *testing.T) {
	value := make(map[string]any, 128)
	for index := 0; index < 128; index++ {
		value[string(rune('a'+index))] = int64(index)
	}
	budget := newQueryBudget(context.Background(), QueryOptions{MaxWork: 130})
	defer releaseQueryBudget(budget)

	_, err := distinctResultRows([]string{"value"}, []map[string]any{{"value": value}}, budget)
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("DISTINCT map key error = %v, want resource limit", err)
	}
}

func TestDistinctPublicEntityProjectionSupportsNestedProperties(t *testing.T) {
	db, err := Open(t.TempDir()+"/distinct-entity.ltdb", OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		left, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{
			"embedding": []float32{0, 1},
			"details":   map[string]any{"items": []any{nil, int64(1)}},
		}})
		if err != nil {
			return err
		}
		right, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}})
		if err != nil {
			return err
		}
		_, err = tx.CreateEdge(left.ID, right.ID, "REL", CreateEdgeOptions{Properties: map[string]any{"meta": map[string]any{"ok": true}}})
		return err
	}); err != nil {
		t.Fatal(err)
	}

	result, err := db.Query(`MATCH (a:Item)-[e:REL]->(b:Item) RETURN DISTINCT a, e`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("DISTINCT entity rows = %d, want 1", len(result.Rows))
	}
	if _, ok := result.Rows[0]["a"].(Node); !ok {
		t.Fatalf("node projection type = %T", result.Rows[0]["a"])
	}
	if _, ok := result.Rows[0]["e"].(Edge); !ok {
		t.Fatalf("edge projection type = %T", result.Rows[0]["e"])
	}
}

func TestDistinctNodeKeyReservesAndChecksNestedProperties(t *testing.T) {
	node := Node{ID: 1, Labels: []string{"Item"}, Properties: map[string]any{"vector": make([]float32, 128)}}
	budget := newQueryBudget(context.Background(), QueryOptions{MaxBytes: 95})
	_, err := distinctResultRows([]string{"value"}, []map[string]any{{"value": node}}, budget)
	releaseQueryBudget(budget)
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("DISTINCT node byte error = %v, want resource limit", err)
	}

	ctx := &cancelAfterChecks{limit: 10}
	budget = newQueryBudget(ctx, QueryOptions{})
	defer releaseQueryBudget(budget)
	_, err = distinctResultRows([]string{"value"}, []map[string]any{{"value": node}}, budget)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("DISTINCT node cancellation error = %v, want canceled", err)
	}
}
