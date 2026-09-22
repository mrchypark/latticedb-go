package engine

import (
	"fmt"
	"math"
	"reflect"
	"slices"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/store"
)

// callExprRow builds bindings directly, without a database: "n" is a node,
// "r" an edge, and "absent" is intentionally unbound.
func callExprRow() queryRow {
	row := queryRow{
		slots: make([]boundValue, 2),
		index: map[string]int{"n": 0, "r": 1},
	}
	row.set("n", boundValue{Node: &store.NodeRecord{
		ID:         7,
		Labels:     []string{"Person", "Admin"},
		Properties: store.PropertiesFromMap(map[string]any{"name": "Ada", "age": int64(36)}),
	}})
	row.set("r", boundValue{Edge: &store.EdgeRecord{
		ID:         3,
		SourceID:   1,
		TargetID:   2,
		Type:       "KNOWS",
		Properties: store.PropertiesFromMap(map[string]any{"since": int64(2020)}),
	}})
	return row
}

func fnCall(name string, args ...valueExpr) callExpr {
	return callExpr{Name: name, Args: args}
}

func fnLit(value any) valueExpr { return literalExpr{Value: value} }

func fnVar(name string) valueExpr { return variableExpr{Name: name} }

func fnList(items ...any) valueExpr { return literalExpr{Value: items} }

type callExprCase struct {
	expr callExpr
	// want is the expected value; wantType pins the exact Go type the render
	// layer depends on, and nil wantType means the result must be NULL.
	// wantErr, when set, is matched exactly against the returned error.
	want     any
	wantType reflect.Type
	wantErr  string
}

// callExprCases is the per-function table shared with the coverage check:
// every function gets a happy path, a NULL argument, a wrong-type argument,
// and an argument-count error.
func callExprCases() []callExprCase {
	return []callExprCase{
		// labels(): node labels, NULL for NULL, error for any other kind.
		{expr: fnCall("labels", fnVar("n")), want: []any{"Person", "Admin"}, wantType: reflect.TypeOf([]any(nil))},
		{expr: fnCall("labels", fnLit(nil))},
		{expr: fnCall("labels", fnLit(int64(1))), wantErr: "labels expects a node, got integer"},
		{expr: fnCall("labels", fnVar("r")), wantErr: "labels expects a node, got edge"},
		{expr: fnCall("labels"), wantErr: "labels expects 1 argument, got 0"},

		// type(): edge type, NULL for NULL, error for a node or a scalar.
		{expr: fnCall("type", fnVar("r")), want: "KNOWS", wantType: reflect.TypeOf("")},
		{expr: fnCall("type", fnLit(nil))},
		{expr: fnCall("type", fnLit(int64(1))), wantErr: "type expects an edge, got integer"},
		{expr: fnCall("type", fnVar("n")), wantErr: "type expects an edge, got node"},
		{expr: fnCall("type", fnVar("r"), fnVar("r")), wantErr: "type expects 1 argument, got 2"},

		// properties(): property map for nodes and edges, NULL for NULL.
		{expr: fnCall("properties", fnVar("n")), want: map[string]any{"name": "Ada", "age": int64(36)}, wantType: reflect.TypeOf(map[string]any(nil))},
		{expr: fnCall("properties", fnVar("r")), want: map[string]any{"since": int64(2020)}, wantType: reflect.TypeOf(map[string]any(nil))},
		{expr: fnCall("properties", fnLit(nil))},
		{expr: fnCall("properties", fnLit("text")), wantErr: "properties expects a node or edge, got string"},
		{expr: fnCall("properties"), wantErr: "properties expects 1 argument, got 0"},

		// size(): bytes for strings, entries for lists. NULL is an error.
		{expr: fnCall("size", fnLit("héllo")), want: int64(6), wantType: reflect.TypeOf(int64(0))},
		{expr: fnCall("size", fnList(int64(1), int64(2), int64(3))), want: int64(3), wantType: reflect.TypeOf(int64(0))},
		{expr: fnCall("size", fnLit([]byte("abc"))), want: int64(3), wantType: reflect.TypeOf(int64(0))},
		{expr: fnCall("size", fnLit(nil)), wantErr: "size expects a string, bytes, or list, got null"},
		{expr: fnCall("size", fnLit(int64(1))), wantErr: "size expects a string, bytes, or list, got integer"},
		{expr: fnCall("size"), wantErr: "size expects 1 argument, got 0"},
		// The arity check runs before any argument is evaluated.
		{expr: fnCall("size", fnVar("absent"), fnVar("absent")), wantErr: "size expects 1 argument, got 2"},

		// head() / last() / tail(): NULL in, NULL out; empty list gives NULL or [].
		{expr: fnCall("head", fnList(int64(1), int64(2))), want: int64(1), wantType: reflect.TypeOf(int64(0))},
		{expr: fnCall("head", fnList())},
		{expr: fnCall("head", fnLit(nil))},
		{expr: fnCall("head", fnLit("abc")), wantErr: "head expects a list, got string"},
		{expr: fnCall("head"), wantErr: "head expects 1 argument, got 0"},

		{expr: fnCall("last", fnList("a", "b")), want: "b", wantType: reflect.TypeOf("")},
		{expr: fnCall("last", fnList())},
		{expr: fnCall("last", fnLit(nil))},
		{expr: fnCall("last", fnLit(int64(2))), wantErr: "last expects a list, got integer"},
		{expr: fnCall("last"), wantErr: "last expects 1 argument, got 0"},

		{expr: fnCall("tail", fnList(int64(1), int64(2), int64(3))), want: []any{int64(2), int64(3)}, wantType: reflect.TypeOf([]any(nil))},
		{expr: fnCall("tail", fnList(int64(1))), want: []any{}, wantType: reflect.TypeOf([]any(nil))},
		{expr: fnCall("tail", fnList()), want: []any{}, wantType: reflect.TypeOf([]any(nil))},
		// NULL stays NULL: only a real list yields an empty list.
		{expr: fnCall("tail", fnLit(nil))},
		{expr: fnCall("tail", fnLit("abc")), wantErr: "tail expects a list, got string"},
		{expr: fnCall("tail"), wantErr: "tail expects 1 argument, got 0"},

		// range(): inclusive on both ends, integer and float arguments truncate.
		{expr: fnCall("range", fnLit(int64(1)), fnLit(int64(5))), want: []any{int64(1), int64(2), int64(3), int64(4), int64(5)}, wantType: reflect.TypeOf([]any(nil))},
		{expr: fnCall("range", fnLit(int64(5)), fnLit(int64(1)), fnLit(int64(-2))), want: []any{int64(5), int64(3), int64(1)}, wantType: reflect.TypeOf([]any(nil))},
		{expr: fnCall("range", fnLit(int64(3)), fnLit(int64(1))), want: []any{}, wantType: reflect.TypeOf([]any(nil))},
		{expr: fnCall("range", fnLit(1.9), fnLit(int64(3))), want: []any{int64(1), int64(2), int64(3)}, wantType: reflect.TypeOf([]any(nil))},
		{expr: fnCall("range", fnLit(nil), fnLit(int64(5))), wantErr: "range expects an integer start, got null"},
		{expr: fnCall("range", fnLit("1"), fnLit(int64(5))), wantErr: "range expects an integer start, got string"},
		{expr: fnCall("range", fnLit(int64(1)), fnLit(int64(5)), fnLit(int64(0))), wantErr: "range step must be non-zero"},
		{expr: fnCall("range", fnLit(int64(1))), wantErr: "range expects between 2 and 3 arguments, got 1"},

		// split(): all occurrences, empty delimiter splits per byte.
		{expr: fnCall("split", fnLit("a,b,c"), fnLit(",")), want: []any{"a", "b", "c"}, wantType: reflect.TypeOf([]any(nil))},
		{expr: fnCall("split", fnLit(""), fnLit(",")), want: []any{""}, wantType: reflect.TypeOf([]any(nil))},
		{expr: fnCall("split", fnLit("abc"), fnLit("")), want: []any{"a", "b", "c"}, wantType: reflect.TypeOf([]any(nil))},
		{expr: fnCall("split", fnLit(nil), fnLit(","))},
		{expr: fnCall("split", fnLit("a"), fnLit(nil)), wantErr: "split expects a string for the delimiter argument, got null"},
		{expr: fnCall("split", fnLit(int64(1)), fnLit(",")), wantErr: "split expects a string, got integer"},
		{expr: fnCall("split", fnLit("a")), wantErr: "split expects 2 arguments, got 1"},

		// replace(): every non-overlapping occurrence, empty search is a no-op.
		{expr: fnCall("replace", fnLit("a-b-c"), fnLit("-"), fnLit("+")), want: "a+b+c", wantType: reflect.TypeOf("")},
		{expr: fnCall("replace", fnLit("abc"), fnLit(""), fnLit("X")), want: "abc", wantType: reflect.TypeOf("")},
		{expr: fnCall("replace", fnLit(nil), fnLit("-"), fnLit("+"))},
		{expr: fnCall("replace", fnLit("a"), fnLit("-"), fnLit(nil)), wantErr: "replace expects a string for the replacement argument, got null"},
		{expr: fnCall("replace", fnLit("a"), fnLit(nil), fnLit("x")), wantErr: "replace expects a string for the search argument, got null"},
		{expr: fnCall("replace", fnLit("a"), fnLit("-")), wantErr: "replace expects 3 arguments, got 2"},

		// substring(): byte indexes, clamped rather than rejected.
		{expr: fnCall("substring", fnLit("hello"), fnLit(int64(1)), fnLit(int64(3))), want: "ell", wantType: reflect.TypeOf("")},
		{expr: fnCall("substring", fnLit("hello"), fnLit(int64(2))), want: "llo", wantType: reflect.TypeOf("")},
		{expr: fnCall("substring", fnLit("hello"), fnLit(1.9)), want: "ello", wantType: reflect.TypeOf("")},
		{expr: fnCall("substring", fnLit("hello"), fnLit(int64(-5))), want: "hello", wantType: reflect.TypeOf("")},
		{expr: fnCall("substring", fnLit("hello"), fnLit(int64(1)), fnLit(int64(-1))), want: "", wantType: reflect.TypeOf("")},
		{expr: fnCall("substring", fnLit("hello"), fnLit(int64(1)), fnLit(int64(99))), want: "ello", wantType: reflect.TypeOf("")},
		{expr: fnCall("substring", fnLit(nil), fnLit(int64(0)))},
		{expr: fnCall("substring", fnLit("hello"), fnLit("1")), wantErr: "substring expects an integer start, got string"},
		{expr: fnCall("substring", fnLit("hello"), fnLit(int64(1)), fnLit(nil)), wantErr: "substring expects an integer length, got null"},
		{expr: fnCall("substring", fnLit("hello")), wantErr: "substring expects between 2 and 3 arguments, got 1"},

		// trim(): only ASCII space, tab, newline and carriage return.
		{expr: fnCall("trim", fnLit("  hi\n")), want: "hi", wantType: reflect.TypeOf("")},
		{expr: fnCall("trim", fnLit("\u00a0x\u00a0")), want: "\u00a0x\u00a0", wantType: reflect.TypeOf("")},
		{expr: fnCall("trim", fnLit(nil))},
		{expr: fnCall("trim", fnLit(int64(1))), wantErr: "trim expects a string, got integer"},
		{expr: fnCall("trim"), wantErr: "trim expects 1 argument, got 0"},

		// toLower() / toUpper(): ASCII only, like upstream.
		{expr: fnCall("toLower", fnLit("AbÇ")), want: "abÇ", wantType: reflect.TypeOf("")},
		{expr: fnCall("toLower", fnLit(nil))},
		{expr: fnCall("toLower", fnLit(int64(1))), wantErr: "toLower expects a string, got integer"},
		{expr: fnCall("toLower"), wantErr: "toLower expects 1 argument, got 0"},
		{expr: fnCall("toUpper", fnLit("aBç")), want: "ABç", wantType: reflect.TypeOf("")},
		{expr: fnCall("toUpper", fnLit(nil))},
		{expr: fnCall("toUpper", fnLit(true)), wantErr: "toUpper expects a string, got boolean"},
		{expr: fnCall("toUpper"), wantErr: "toUpper expects 1 argument, got 0"},

		// toInteger() / toFloat(): unparsable input returns NULL, not an error.
		{expr: fnCall("toInteger", fnLit("42")), want: int64(42), wantType: reflect.TypeOf(int64(0))},
		{expr: fnCall("toInteger", fnLit(3.9)), want: int64(3), wantType: reflect.TypeOf(int64(0))},
		{expr: fnCall("toInteger", fnLit(-3.9)), want: int64(-3), wantType: reflect.TypeOf(int64(0))},
		{expr: fnCall("toInteger", fnLit("0x10"))},
		{expr: fnCall("toInteger", fnLit(nil))},
		{expr: fnCall("toInteger", fnLit(true))},
		{expr: fnCall("toInteger", fnList(int64(1)))},
		{expr: fnCall("toInteger"), wantErr: "toInteger expects 1 argument, got 0"},

		{expr: fnCall("toFloat", fnLit("2.5")), want: 2.5, wantType: reflect.TypeOf(float64(0))},
		{expr: fnCall("toFloat", fnLit(int64(2))), want: 2.0, wantType: reflect.TypeOf(float64(0))},
		{expr: fnCall("toFloat", fnLit("nope"))},
		{expr: fnCall("toFloat", fnLit(nil))},
		{expr: fnCall("toFloat", fnLit(true))},
		{expr: fnCall("toFloat"), wantErr: "toFloat expects 1 argument, got 0"},

		// toString(): NULL renders as the string "null", containers as NULL.
		{expr: fnCall("toString", fnLit(int64(12))), want: "12", wantType: reflect.TypeOf("")},
		{expr: fnCall("toString", fnLit(2.5)), want: "2.5", wantType: reflect.TypeOf("")},
		{expr: fnCall("toString", fnLit(1e21)), want: "1000000000000000000000", wantType: reflect.TypeOf("")},
		{expr: fnCall("toString", fnLit(math.NaN())), want: "nan", wantType: reflect.TypeOf("")},
		{expr: fnCall("toString", fnLit(true)), want: "true", wantType: reflect.TypeOf("")},
		{expr: fnCall("toString", fnLit(nil)), want: "null", wantType: reflect.TypeOf("")},
		{expr: fnCall("toString", fnList(int64(1)))},
		{expr: fnCall("toString"), wantErr: "toString expects 1 argument, got 0"},

		// abs(): integer and float only; NULL is an error.
		{expr: fnCall("abs", fnLit(int64(-4))), want: int64(4), wantType: reflect.TypeOf(int64(0))},
		{expr: fnCall("abs", fnLit(-2.5)), want: 2.5, wantType: reflect.TypeOf(float64(0))},
		{expr: fnCall("abs", fnLit(nil)), wantErr: "abs expects an integer or float, got null"},
		{expr: fnCall("abs", fnLit("x")), wantErr: "abs expects an integer or float, got string"},
		{expr: fnCall("abs"), wantErr: "abs expects 1 argument, got 0"},

		// coalesce(): no arity limit, first non-NULL wins, no NULL gives NULL.
		{expr: fnCall("coalesce", fnLit(nil), fnLit(nil), fnLit(int64(3))), want: int64(3), wantType: reflect.TypeOf(int64(0))},
		{expr: fnCall("coalesce", fnLit(nil), fnLit("x")), want: "x", wantType: reflect.TypeOf("")},
		{expr: fnCall("coalesce", fnLit(nil))},
		{expr: fnCall("coalesce")},

		{expr: fnCall("nope", fnLit(int64(1))), wantErr: `unknown function "nope"`},
		{expr: fnCall("abs", fnVar("absent")), wantErr: `unknown binding "absent"`},
	}
}

func TestCallExprFunctionCases(t *testing.T) {
	row := callExprRow()
	params := map[string]any{}
	for i, tc := range callExprCases() {
		t.Run(fmt.Sprintf("%d_%s", i, tc.expr.Name), func(t *testing.T) {
			got, err := tc.expr.eval(row, params, nil)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("eval() = %#v, want error %q", got, tc.wantErr)
				}
				if err.Error() != tc.wantErr {
					t.Fatalf("eval() error = %q, want %q", err.Error(), tc.wantErr)
				}
				if got != nil {
					t.Fatalf("eval() value = %#v with error, want nil", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("eval() unexpected error: %v", err)
			}
			if gotType := reflect.TypeOf(got); gotType != tc.wantType {
				t.Fatalf("eval() type = %v, want %v", gotType, tc.wantType)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("eval() = %#v, want %#v", got, tc.want)
			}
		})
	}
}

// callExprRequired is the function surface the parent parser dispatches to.
var callExprRequired = []string{
	"labels", "type", "properties", "size", "head", "last", "tail", "range",
	"split", "replace", "substring", "trim", "toLower", "toUpper",
	"toInteger", "toFloat", "toString", "abs", "coalesce",
}

// TestCallExprCoverage checks the table actually exercises all 19 required
// functions, each with at least one happy case and one error case. coalesce
// has no arity limit upstream, so it has no error case.
func TestCallExprCoverage(t *testing.T) {
	happy := map[string]int{}
	failing := map[string]int{}
	for _, tc := range callExprCases() {
		if tc.wantErr == "" {
			happy[tc.expr.Name]++
		} else {
			failing[tc.expr.Name]++
		}
	}
	for _, name := range callExprRequired {
		if happy[name] == 0 {
			t.Fatalf("%s: no happy-path case in the table", name)
		}
		if name != "coalesce" && failing[name] == 0 {
			t.Fatalf("%s: no error case in the table", name)
		}
	}
	for name := range happy {
		if !slices.Contains(callExprRequired, name) {
			t.Fatalf("%s: case table covers a function outside the required list", name)
		}
	}
}

// TestCallExprCoalesceShortCircuits pins the lazy argument evaluation: later
// arguments are never touched once a non-NULL value is found.
func TestCallExprCoalesceShortCircuits(t *testing.T) {
	row := callExprRow()
	got, err := fnCall("coalesce", fnLit(int64(1)), fnVar("absent")).eval(row, map[string]any{}, nil)
	if err != nil {
		t.Fatalf("eval() unexpected error: %v", err)
	}
	if got != int64(1) {
		t.Fatalf("eval() = %#v, want int64(1)", got)
	}
	// A node binding is not NULL, so coalesce passes the binding through.
	node, err := fnCall("coalesce", fnVar("n")).eval(row, map[string]any{}, nil)
	if err != nil {
		t.Fatalf("eval() unexpected error: %v", err)
	}
	binding, ok := node.(boundValue)
	if !ok || binding.Node == nil || binding.Node.ID != 7 {
		t.Fatalf("eval() = %#v, want the node binding", node)
	}
}

// TestCallExprErrorPropagatesFromArgument checks that an argument error
// surfaces unchanged from the enclosing call.
func TestCallExprErrorPropagatesFromArgument(t *testing.T) {
	row := callExprRow()
	_, err := fnCall("trim", fnVar("absent")).eval(row, map[string]any{}, nil)
	if err == nil || err.Error() != `unknown binding "absent"` {
		t.Fatalf("eval() error = %v, want unknown binding error", err)
	}
}

// TestCallExprMissingParameterPropagates covers the parameter path as well.
func TestCallExprMissingParameterPropagates(t *testing.T) {
	row := callExprRow()
	_, err := fnCall("size", paramExpr{Name: "p"}).eval(row, map[string]any{}, nil)
	if err == nil || err.Error() != `missing query parameter "p"` {
		t.Fatalf("eval() error = %v, want missing parameter error", err)
	}
}
