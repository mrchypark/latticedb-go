package engine

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func TestArithmeticExpressions(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "arithmetic.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, test := range []struct {
		query  string
		params map[string]any
		want   any
	}{
		{`RETURN 1 + 2 * 3 AS value`, nil, int64(7)},
		{`RETURN 2 ^ 3 ^ 2 AS value`, nil, int64(512)},
		{`RETURN -2 ^ 2 AS value`, nil, int64(-4)},
		{`RETURN (-2) ^ 2 AS value`, nil, int64(4)},
		{`RETURN -9223372036854775808 + 1 AS value`, nil, int64(-9223372036854775807)},
		{`RETURN $value / 2 AS value`, map[string]any{"value": int64(3)}, float64(1.5)},
		{`UNWIND [2] AS value RETURN value+1 AS result`, nil, int64(3)},
		{`RETURN {sum: $value + 1, items: [2 * 3]} AS value`, map[string]any{"value": int64(4)}, map[string]any{"sum": int64(5), "items": []any{int64(6)}}},
		{`UNWIND [2] AS value RETURN value + 1 AS result`, nil, int64(3)},
	} {
		result, err := db.Query(test.query, test.params)
		if err != nil {
			t.Fatalf("query %q: %v", test.query, err)
		}
		column := "value"
		if test.query == `UNWIND [2] AS value RETURN value + 1 AS result` || test.query == `UNWIND [2] AS value RETURN value+1 AS result` {
			column = "result"
		}
		if len(result.Rows) != 1 || !queryValuesEqual(result.Rows[0][column], test.want) {
			t.Fatalf("query %q rows = %#v, want %v", test.query, result.Rows, test.want)
		}
	}
	if _, err := db.Query(`CREATE (:Item {value: 2})`, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query(`CREATE (:Number {age: 2})`, nil); err != nil {
		t.Fatal(err)
	}
	result, err := db.Query(`MATCH (n:Number) RETURN n.age-1 AS value, 1e-3+n.age AS scientific`, nil)
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["value"] != int64(1) || result.Rows[0]["scientific"] != float64(2.001) {
		t.Fatalf("identifier/exponent arithmetic = %#v, %v", result.Rows, err)
	}
	result, err = db.Query(`MATCH (n:Item) WHERE n.value = 1 + 1 RETURN sum(n.value + 1) AS value`, nil)
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["value"] != float64(3) {
		t.Fatalf("WHERE/aggregate arithmetic = %#v, %v", result.Rows, err)
	}
	result, err = db.Query(`MATCH (n:Item) SET n.value = n.value * 4 RETURN n.value AS value`, nil)
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["value"] != int64(8) {
		t.Fatalf("SET arithmetic = %#v, %v", result.Rows, err)
	}
}

func TestArithmeticErrorsAreBoundedAndAtomic(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "arithmetic-errors.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, test := range []struct{ query, want string }{
		{`RETURN "text" + 1`, "requires numbers"},
		{`RETURN 1 / 0`, "division by zero"},
		{`RETURN 9223372036854775807 + 1`, "overflow"},
		{`RETURN -9223372036854775808 - 1`, "overflow"},
		{`RETURN 10.0 ^ 400`, "not finite"},
	} {
		if _, err := db.Query(test.query, nil); err == nil || !strings.Contains(err.Error(), test.want) {
			t.Errorf("query %q error = %v, want %q", test.query, err, test.want)
		}
	}
	if _, err := db.Query(`CREATE (:Item {value: 1 / 0})`, nil); err == nil {
		t.Fatal("accepted invalid mutation")
	}
	result, err := db.Query(`MATCH (n:Item) RETURN count(*) AS count`, nil)
	if err != nil || result.Rows[0]["count"] != int64(0) {
		t.Fatalf("mutation after arithmetic error = %#v, %v", result.Rows, err)
	}
	expr, err := parseValueExpr(`2 ^ 10`)
	if err != nil {
		t.Fatal(err)
	}
	budget := newQueryBudget(context.Background(), QueryOptions{MaxWork: 4})
	_, err = expr.eval(queryRow{}, nil, budget)
	releaseQueryBudget(budget)
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("power budget error = %v", err)
	}
}
