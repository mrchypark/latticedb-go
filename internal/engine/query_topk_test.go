package engine

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
)

func TestQueryOrderLimitTopKMatchesFullOrder(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "query-topk.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		for _, value := range []int64{3, 1, 2, 1, 4} {
			if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"rank": value}}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name, query string
		want        []map[string]any
	}{
		{
			name:  "ascending skip preserves binding ties",
			query: `MATCH (n:Item) RETURN id(n) AS id ORDER BY n.rank SKIP 1 LIMIT 3`,
			want:  []map[string]any{{"id": int64(4)}, {"id": int64(3)}, {"id": int64(1)}},
		},
		{
			name:  "descending",
			query: `MATCH (n:Item) RETURN id(n) AS id ORDER BY n.rank DESC LIMIT 2`,
			want:  []map[string]any{{"id": int64(5)}, {"id": int64(1)}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := db.Query(test.query, nil)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(result.Rows, test.want) {
				t.Fatalf("rows = %#v, want %#v", result.Rows, test.want)
			}
		})
	}

	result, err := db.Query(`UNWIND $values AS value RETURN value ORDER BY value SKIP 1 LIMIT 3`, map[string]any{
		"values": []any{nil, "a", true, int64(2), []byte{1}, float64(1.5)},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []any{int64(2), true, "a"}
	got := make([]any, len(result.Rows))
	for index, row := range result.Rows {
		got[index] = row["value"]
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mixed rows = %#v, want %#v", got, want)
	}
}

func TestQueryOrderLimitTopKPreservesFallbacks(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "query-topk-fallbacks.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if result, err := db.QueryContext(context.Background(), `UNWIND $values AS value RETURN value ORDER BY value LIMIT 0`, map[string]any{"values": []any{int64(2), int64(1)}}, QueryOptions{MaxWork: 1}); err == nil || len(result.Rows) != 0 {
		t.Fatalf("LIMIT 0 result = %#v, %v; want resource limit", result.Rows, err)
	}
	if result, err := db.Query(`UNWIND $values AS value RETURN DISTINCT value ORDER BY value LIMIT 2`, map[string]any{"values": []any{int64(2), int64(1), int64(1), int64(3)}}); err != nil || !reflect.DeepEqual(result.Rows, []map[string]any{{"value": int64(1)}, {"value": int64(2)}}) {
		t.Fatalf("DISTINCT result = %#v, %v", result.Rows, err)
	}
	if result, err := db.Query(`UNWIND $values AS value RETURN count(*) AS count ORDER BY count LIMIT 1`, map[string]any{"values": []any{int64(1), int64(2)}}); err != nil || !reflect.DeepEqual(result.Rows, []map[string]any{{"count": int64(2)}}) {
		t.Fatalf("aggregate result = %#v, %v", result.Rows, err)
	}
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if result, err := db.Query(`MATCH (n:Item) SET n.active = true RETURN n.active AS active ORDER BY active LIMIT 1`, nil); err != nil || !reflect.DeepEqual(result.Rows, []map[string]any{{"active": true}}) {
		t.Fatalf("mutation result = %#v, %v", result.Rows, err)
	}

	maxInt := int64(^uint(0) >> 1)
	result, err := db.Query(`UNWIND $values AS value RETURN value ORDER BY value SKIP $skip LIMIT 1`, map[string]any{"values": []any{int64(1)}, "skip": maxInt})
	if err != nil || len(result.Rows) != 0 {
		t.Fatalf("overflow pagination result = %#v, %v", result.Rows, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := db.QueryContext(ctx, `UNWIND $values AS value RETURN value ORDER BY value LIMIT 1`, map[string]any{"values": []any{int64(1)}}, QueryOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ordered query = %v, want context cancellation", err)
	}
}

func TestCollectTopKRowsMatchesStableFullSort(t *testing.T) {
	plan := &queryPlan{
		slots: map[string]int{"rank": 0, "id": 1},
		orderClauses: []orderClause{{
			Kind: projectionValue,
			Var:  "rank",
		}},
	}
	rows := make([]queryRow, 10_000)
	state := uint32(1)
	for index := range rows {
		state = state*1664525 + 1013904223
		rows[index] = topKTestRow(plan, int64(state%17), int64(index))
	}
	full := append([]queryRow(nil), rows...)
	slices.SortStableFunc(full, plan.compareOrderedRows)

	const skip, limit = 7, 11
	budget := newQueryBudget(t.Context(), QueryOptions{MaxBytes: 1 << 20})
	defer releaseQueryBudget(budget)
	got, err := plan.collectTopKRows(&sliceQueryIterator{rows: rows}, skip, limit, budget)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) > skip+limit {
		t.Fatalf("retained rows = %d, want at most %d", len(got), skip+limit)
	}
	if !reflect.DeepEqual(got, full[:skip+limit]) {
		t.Fatal("top-K candidates differ from stable full sort")
	}
	budget.releaseTemporary(uint64(len(got)) * 128)
	if budget.bytes != 0 {
		t.Fatalf("candidate bytes after collection = %d", budget.bytes)
	}

	ties := make([]queryRow, 10)
	for index := range ties {
		ties[index] = topKTestRow(plan, 0, int64(index))
	}
	got, err = plan.collectTopKRows(&sliceQueryIterator{rows: ties}, 2, 3, budget)
	if err != nil {
		t.Fatal(err)
	}
	for index, row := range got[2:] {
		if id := row.slots[1].Value; id != int64(index+2) {
			t.Fatalf("tie %d has id %v, want %d", index, id, index+2)
		}
	}
	budget.releaseTemporary(uint64(len(got)) * 128)
}

func TestCollectTopKRowsHonorsBudgetAndCancellation(t *testing.T) {
	plan := &queryPlan{slots: map[string]int{"rank": 0}, orderClauses: []orderClause{{Kind: projectionValue, Var: "rank"}}}
	rows := []queryRow{topKTestRow(plan, 2, 0), topKTestRow(plan, 1, 1)}
	copyBudget := newQueryBudget(t.Context(), QueryOptions{MaxBytes: queryTopKCandidateBytes*2 - 1})
	if _, err := plan.collectTopKRows(&sliceQueryIterator{rows: rows[:1]}, 0, 1, copyBudget); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("candidate and returned-row bytes = %v, want resource limit", err)
	}
	releaseQueryBudget(copyBudget)

	budget := newQueryBudget(t.Context(), QueryOptions{MaxBytes: queryTopKCandidateBytes*2 - 1})
	if _, err := plan.collectTopKRows(&sliceQueryIterator{rows: rows}, 0, 2, budget); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("candidate byte limit = %v, want resource limit", err)
	}
	releaseQueryBudget(budget)

	ctx, cancel := context.WithCancel(t.Context())
	budget = newQueryBudget(ctx, QueryOptions{})
	defer releaseQueryBudget(budget)
	it := &cancelAfterQueryIterator{rows: rows, cancel: cancel, after: 1}
	if _, err := plan.collectTopKRows(it, 0, 2, budget); !errors.Is(err, context.Canceled) {
		t.Fatalf("mid-stream cancellation = %v, want context cancellation", err)
	}
	if it.index != 1 {
		t.Fatalf("iterator consumed %d rows after cancellation", it.index)
	}
}

func topKTestRow(plan *queryPlan, rank, id int64) queryRow {
	return queryRow{
		slots: []boundValue{{Value: rank, HasValue: true}, {Value: id, HasValue: true}},
		bound: []bool{true, true},
		index: plan.slots,
	}
}

type cancelAfterQueryIterator struct {
	rows   []queryRow
	index  int
	cancel context.CancelFunc
	after  int
}

func (it *cancelAfterQueryIterator) Next() (queryRow, bool, error) {
	if it.index == len(it.rows) {
		return queryRow{}, false, nil
	}
	row := it.rows[it.index]
	it.index++
	if it.index == it.after {
		it.cancel()
	}
	return row, true, nil
}
