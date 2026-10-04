package engine

import (
	"context"
	"strings"
	"testing"
	"unsafe"
)

func TestAggregateDetachesRetainedExpressionViews(t *testing.T) {
	text := strings.Repeat("x", 8192)
	input := []any{make([]byte, 8192), text[:1]}
	params := map[string]any{"xs": input, "s": text}
	for _, kind := range []aggregateKind{aggregateCollect, aggregateMin, aggregateMax} {
		t.Run(string(kind), func(t *testing.T) {
			budget := newQueryBudget(context.Background(), QueryOptions{MaxBytes: 256 << 10})
			defer releaseQueryBudget(budget)
			a := newAggregateAccumulator(kind)
			expr := callExpr{Name: "tail", Args: []valueExpr{paramExpr{Name: "xs"}}}
			if err := a.addExpr(expr, queryRow{}, params, budget); err != nil {
				t.Fatal(err)
			}
			value := a.result()
			if kind == aggregateCollect {
				value = value.([]any)[0]
			}
			tail := value.([]any)
			if &tail[0] == &input[1] {
				t.Fatal("retained tail pins discarded prefix")
			}
			if unsafe.StringData(tail[0].(string)) == unsafe.StringData(text) {
				t.Fatal("nested string pins full backing buffer")
			}
		})
	}
}

func TestProjectionDetachesStringViews(t *testing.T) {
	text := strings.Repeat("x", 8192)
	expr := callExpr{Name: "substring", Args: []valueExpr{paramExpr{Name: "s"}, literalExpr{Value: int64(0)}, literalExpr{Value: int64(1)}}}
	projection := projection{Kind: projectionExpr, Expr: expr}
	for _, with := range []bool{false, true} {
		budget := newQueryBudget(context.Background(), QueryOptions{MaxBytes: 256 << 10})
		var value any
		var err error
		clause := returnClause{}
		if with {
			var binding boundValue
			binding, err = clause.withItemBinding(projection, queryRow{}, map[string]any{"s": text}, budget)
			value = binding.Value
		} else {
			value, err = clause.projectionValue(projection, queryRow{}, map[string]any{"s": text}, budget)
		}
		releaseQueryBudget(budget)
		if err != nil {
			t.Fatal(err)
		}
		if value != "x" || unsafe.StringData(value.(string)) == unsafe.StringData(text) {
			t.Fatalf("with=%v retained original string backing", with)
		}
	}
}

func TestMapFieldProjectionDetachesStringViews(t *testing.T) {
	text := strings.Repeat("x", 8192)
	row := queryRow{
		index: map[string]int{"m": 0},
		slots: []boundValue{{Bound: true, HasValue: true, Value: map[string]any{"text": text[:1]}}},
	}
	item := projection{Kind: projectionProperty, Var: "m", Property: "text", Alias: "v"}
	clause := returnClause{Projections: []projection{item}}
	for _, path := range []string{"rows", "row", "value"} {
		t.Run(path, func(t *testing.T) {
			budget := newQueryBudget(context.Background(), QueryOptions{MaxBytes: 256 << 10})
			defer releaseQueryBudget(budget)
			var value any
			var err error
			switch path {
			case "rows":
				var result QueryResult
				result, err = clause.render([]queryRow{row}, nil, budget)
				if err == nil {
					value = result.Rows[0]["v"]
				}
			case "row":
				var result map[string]any
				result, err = clause.renderRow(row, nil, budget)
				value = result["v"]
			case "value":
				value, err = clause.projectionValue(item, row, nil, budget)
			}
			if err != nil {
				t.Fatal(err)
			}
			if value != "x" || unsafe.StringData(value.(string)) == unsafe.StringData(text) {
				t.Fatal("map field retained original string backing")
			}
		})
	}
}
