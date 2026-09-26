package engine

import (
	"cmp"
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestSortQueryRowsBudgetAndCancellation(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		ctx, stop := context.WithCancel(t.Context())
		budget := newQueryBudget(ctx, QueryOptions{MaxWork: 5})
		rows := []int{9, 8, 7, 6, 5, 4, 3, 2, 1}
		comparisons := 0
		err := sortQueryRows(rows, func(a, b int) (int, error) {
			comparisons++
			if cancel && comparisons == 5 {
				stop()
			}
			return cmp.Compare(a, b), nil
		}, budget)
		want := ErrResourceLimit
		if cancel {
			want = context.Canceled
		}
		if !errors.Is(err, want) || comparisons != 5 || budget.work != 5 {
			t.Errorf("cancel=%v: error=%v comparisons=%d work=%d", cancel, err, comparisons, budget.work)
		}
		stop()
		releaseQueryBudget(budget)
	}
}

func TestSortQueryRowsPropagatesComparatorPanic(t *testing.T) {
	budget := newQueryBudget(t.Context(), QueryOptions{})
	defer releaseQueryBudget(budget)
	defer func() {
		if got := recover(); got != "comparator bug" {
			t.Errorf("panic = %v", got)
		}
	}()
	_ = sortQueryRows([]int{2, 1}, func(int, int) (int, error) { panic("comparator bug") }, budget)
}

func TestQuerySortPathsChargeWork(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "sort-work.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, query := range []string{
		`UNWIND $values AS x RETURN x ORDER BY x`,
		`UNWIND $values AS x WITH x ORDER BY x RETURN x`,
		`UNWIND $values AS x RETURN x, count(*) AS c ORDER BY x`,
	} {
		t.Run(query, func(t *testing.T) {
			plan, err := parseQuery(query)
			if err != nil {
				t.Fatal(err)
			}
			budget := newQueryBudget(t.Context(), QueryOptions{})
			_, err = plan.execute(tx, map[string]any{"values": []any{int64(3), int64(1), int64(2), int64(1)}}, budget)
			sortedWork := budget.work
			releaseQueryBudget(budget)
			if err != nil {
				t.Fatal(err)
			}
			plan.orderClauses, plan.withOrder = nil, nil
			budget = newQueryBudget(t.Context(), QueryOptions{})
			_, err = plan.execute(tx, map[string]any{"values": []any{int64(3), int64(1), int64(2), int64(1)}}, budget)
			unsortedWork := budget.work
			releaseQueryBudget(budget)
			if err != nil || sortedWork <= unsortedWork {
				t.Fatalf("sorted work=%d unsorted=%d error=%v", sortedWork, unsortedWork, err)
			}
			plan, err = parseQuery(query)
			if err != nil {
				t.Fatal(err)
			}
			budget = newQueryBudget(t.Context(), QueryOptions{MaxWork: uint64(unsortedWork)})
			_, err = plan.execute(tx, map[string]any{"values": []any{int64(3), int64(1), int64(2), int64(1)}}, budget)
			releaseQueryBudget(budget)
			if !errors.Is(err, ErrResourceLimit) {
				t.Fatalf("sort exceeding unsorted budget: %v", err)
			}
		})
	}
}
