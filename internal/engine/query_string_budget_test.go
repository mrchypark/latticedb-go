package engine

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestStringFunctionsChargeWorkAndCancel(t *testing.T) {
	text := strings.Repeat(" A", 1<<19)
	for _, expr := range []callExpr{
		fnCall("toLower", paramExpr{Name: "text"}), fnCall("toUpper", paramExpr{Name: "text"}),
		fnCall("trim", paramExpr{Name: "text"}), fnCall("split", paramExpr{Name: "text"}, fnLit("A")),
		fnCall("split", paramExpr{Name: "text"}, fnLit("")), fnCall("replace", paramExpr{Name: "text"}, fnLit("A"), fnLit("b")),
	} {
		t.Run(expr.Name, func(t *testing.T) {
			// trim must scan a long whitespace prefix, rather than stop at 'A'.
			value := text
			if expr.Name == "trim" {
				value = strings.Repeat(" ", len(text))
			}
			for _, tc := range []struct {
				ctx  context.Context
				work uint64
				want error
			}{
				{t.Context(), 200, ErrResourceLimit},
				{&cancelAfterQueryChecks{remaining: 12, done: make(chan struct{})}, 0, context.Canceled},
			} {
				budget := newQueryBudget(tc.ctx, QueryOptions{MaxWork: tc.work, MaxBytes: 64 << 20})
				_, err := expr.eval(queryRow{}, map[string]any{"text": value}, budget)
				releaseQueryBudget(budget)
				if !errors.Is(err, tc.want) {
					t.Fatalf("error = %v, want %v", err, tc.want)
				}
			}
		})
	}
}

func TestStringPredicatesChargeWorkAndCancel(t *testing.T) {
	text := strings.Repeat("a", 1<<20)
	row := callExprRow()
	row.slots[0].Node.Properties.Set("text", text)
	for _, kind := range []whereKind{whereContains, whereStartsWith, whereEndsWith, whereLess} {
		needle := text
		if kind == whereContains {
			needle = "z"
		}
		clause := whereClause{Kind: kind, Var: "n", Property: "text", Expr: paramExpr{Name: "needle"}}
		for _, tc := range []struct {
			ctx  context.Context
			work uint64
			want error
		}{
			{t.Context(), 200, ErrResourceLimit},
			{&cancelAfterQueryChecks{remaining: 3, done: make(chan struct{})}, 0, context.Canceled},
		} {
			budget := newQueryBudget(tc.ctx, QueryOptions{MaxWork: tc.work})
			_, err := clause.eval(row, map[string]any{"needle": needle}, budget)
			releaseQueryBudget(budget)
			if !errors.Is(err, tc.want) {
				t.Fatalf("%s error = %v, want %v", kind, err, tc.want)
			}
		}
	}
}

func TestBudgetedStringMatchesPreserveNativeResults(t *testing.T) {
	for _, tc := range []struct{ text, needle string }{
		{strings.Repeat("a", 4095) + "xyz" + strings.Repeat("a", 8192), "xyz"},
		{strings.Repeat("a", 20000) + "b", strings.Repeat("a", 5000) + "b"},
		{strings.Repeat("ab", 8000), "aba"},
		{strings.Repeat("가나", 3000), "나가"},
		{"x\x00x\x00", "\x00"},
	} {
		budget := newQueryBudget(t.Context(), QueryOptions{})
		var got []int
		err := visitQueryStringMatches(tc.text, tc.needle, budget, func(i int) (bool, error) { got = append(got, i); return true, nil })
		if budget.bytes != 0 {
			t.Fatal("retained string scan scratch")
		}
		releaseQueryBudget(budget)
		var want []int
		for start := 0; start <= len(tc.text)-len(tc.needle); {
			i := strings.Index(tc.text[start:], tc.needle)
			if i < 0 {
				break
			}
			i += start
			want = append(want, i)
			start = i + len(tc.needle)
		}
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("matches=%v, want %v; error=%v", got, want, err)
		}
	}
}

func TestStringAggregatesChargeComparisonWorkAndCancel(t *testing.T) {
	text := strings.Repeat("a", 1<<20)
	for _, kind := range []aggregateKind{aggregateMin, aggregateMax} {
		for _, tc := range []struct {
			ctx  context.Context
			work uint64
			want error
		}{
			{t.Context(), 200, ErrResourceLimit},
			{&cancelAfterQueryChecks{remaining: 3, done: make(chan struct{})}, 0, context.Canceled},
		} {
			budget := newQueryBudget(tc.ctx, QueryOptions{MaxWork: tc.work})
			a := newAggregateAccumulator(kind)
			a.extreme, a.seen = text, true
			err := a.addExpr(paramExpr{Name: "text"}, queryRow{}, map[string]any{"text": text + "b"}, budget)
			releaseQueryBudget(budget)
			if !errors.Is(err, tc.want) {
				t.Fatalf("%s: error=%v, want %v", kind, err, tc.want)
			}
		}
	}
}

func TestReplaceWorkBudgetDoesNotDependOnFragmentSize(t *testing.T) {
	text := strings.Repeat("a", 1<<20)
	for _, search := range []string{"a", strings.Repeat("a", 4096)} {
		budget := newQueryBudget(t.Context(), QueryOptions{MaxWork: 100_000, MaxBytes: 64 << 20})
		got, err := fnCall("replace", paramExpr{Name: "text"}, fnLit(search), fnLit(strings.Repeat("b", len(search)))).eval(queryRow{}, map[string]any{"text": text}, budget)
		releaseQueryBudget(budget)
		if err != nil || got != strings.Repeat("b", len(text)) {
			t.Fatalf("fragment size %d: error=%v", len(search), err)
		}
	}
}
