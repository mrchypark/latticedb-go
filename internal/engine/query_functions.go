package engine

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/mrchypark/latticedb-go/internal/store"
)

// callExpr evaluates a Cypher function call: name(arg, arg, ...).
//
// Semantics mirror the upstream Zig evaluator (src/query/expression.zig,
// evaluateFunction): argument counts are checked before any argument is
// evaluated, NULL arguments return NULL wherever the original does, and a
// value of the wrong type is an error rather than NULL.
type callExpr struct {
	Name string
	Args []valueExpr
}

func (expr callExpr) eval(row queryRow, params map[string]any, budgets ...*queryBudget) (any, error) {
	switch expr.Name {
	case "id":
		args, err := expr.evalArgs(1, 1, row, params, budgets...)
		if err != nil {
			return nil, err
		}
		binding, ok := args[0].(boundValue)
		if !ok {
			return nil, expr.typeError("a node or edge", args[0])
		}
		if value, ok := bindingID(binding); ok {
			return value, nil
		}
		return nil, nil

	case "coalesce":
		// Unrestricted arity upstream, and later arguments stay unevaluated.
		for _, arg := range expr.Args {
			value, err := arg.eval(row, params, budgets...)
			if err != nil {
				return nil, err
			}
			if value != nil {
				return value, nil
			}
		}
		return nil, nil

	case "labels":
		args, err := expr.evalArgs(1, 1, row, params, budgets...)
		if err != nil {
			return nil, err
		}
		if args[0] == nil {
			return nil, nil
		}
		node, ok := nodeBinding(args[0])
		if !ok {
			return nil, expr.typeError("a node", args[0])
		}
		if err := reserveExpression(budgets, uint64(len(node.Labels))*16); err != nil {
			return nil, err
		}
		labels := make([]any, 0, len(node.Labels))
		for _, label := range node.Labels {
			labels = append(labels, label)
		}
		return labels, nil

	case "type":
		args, err := expr.evalArgs(1, 1, row, params, budgets...)
		if err != nil {
			return nil, err
		}
		if args[0] == nil {
			return nil, nil
		}
		edge, ok := edgeBinding(args[0])
		if !ok {
			return nil, expr.typeError("an edge", args[0])
		}
		return edge.Type, nil

	case "properties":
		args, err := expr.evalArgs(1, 1, row, params, budgets...)
		if err != nil {
			return nil, err
		}
		if args[0] == nil {
			return nil, nil
		}
		if node, ok := nodeBinding(args[0]); ok {
			if err := reserveExpression(budgets, queryStoredPropertyBytes(node.Properties)); err != nil {
				return nil, err
			}
			return node.Properties.CloneMap(), nil
		}
		if edge, ok := edgeBinding(args[0]); ok {
			if err := reserveExpression(budgets, queryStoredPropertyBytes(edge.Properties)); err != nil {
				return nil, err
			}
			return edge.Properties.CloneMap(), nil
		}
		return nil, expr.typeError("a node or edge", args[0])

	case "size":
		args, err := expr.evalArgs(1, 1, row, params, budgets...)
		if err != nil {
			return nil, err
		}
		switch value := args[0].(type) {
		case string:
			return int64(len(value)), nil
		case []byte:
			return int64(len(value)), nil
		case []any:
			return int64(len(value)), nil
		default:
			// Upstream rejects NULL, maps, and vectors here.
			return nil, expr.typeError("a string, bytes, or list", args[0])
		}

	case "head":
		args, err := expr.evalArgs(1, 1, row, params, budgets...)
		if err != nil {
			return nil, err
		}
		list, ok, err := expr.listArg(args[0])
		if err != nil || !ok {
			return nil, err
		}
		if len(list) == 0 {
			return nil, nil
		}
		return list[0], nil

	case "last":
		args, err := expr.evalArgs(1, 1, row, params, budgets...)
		if err != nil {
			return nil, err
		}
		list, ok, err := expr.listArg(args[0])
		if err != nil || !ok {
			return nil, err
		}
		if len(list) == 0 {
			return nil, nil
		}
		return list[len(list)-1], nil

	case "tail":
		args, err := expr.evalArgs(1, 1, row, params, budgets...)
		if err != nil {
			return nil, err
		}
		list, ok, err := expr.listArg(args[0])
		if err != nil || !ok {
			return nil, err
		}
		if len(list) > 1 {
			return list[1:], nil
		}
		return []any{}, nil

	case "range":
		args, err := expr.evalArgs(2, 3, row, params, budgets...)
		if err != nil {
			return nil, err
		}
		start, ok := queryInt(args[0])
		if !ok {
			return nil, expr.typeError("an integer start", args[0])
		}
		end, ok := queryInt(args[1])
		if !ok {
			return nil, expr.typeError("an integer end", args[1])
		}
		step := int64(1)
		if len(args) == 3 {
			step, ok = queryInt(args[2])
			if !ok {
				return nil, expr.typeError("an integer step", args[2])
			}
			if step == 0 {
				return nil, fmt.Errorf("range step must be non-zero")
			}
		}
		// Both ends are inclusive upstream. The overflow guard keeps a huge
		// start or step from wrapping into an endless loop; upstream panics.
		items := make([]any, 0)
		if step > 0 {
			for i := start; i <= end; i += step {
				if len(budgets) > 0 && budgets[0] != nil {
					if err := budgets[0].check(1, 0); err != nil {
						return nil, err
					}
				}
				if err := reserveExpression(budgets, 16); err != nil {
					return nil, err
				}
				items = append(items, i)
				if i > math.MaxInt64-step {
					break
				}
			}
		} else {
			for i := start; i >= end; i += step {
				if len(budgets) > 0 && budgets[0] != nil {
					if err := budgets[0].check(1, 0); err != nil {
						return nil, err
					}
				}
				if err := reserveExpression(budgets, 16); err != nil {
					return nil, err
				}
				items = append(items, i)
				if i < math.MinInt64-step {
					break
				}
			}
		}
		return items, nil

	case "split":
		args, err := expr.evalArgs(2, 2, row, params, budgets...)
		if err != nil {
			return nil, err
		}
		text, ok := args[0].(string)
		if !ok {
			if args[0] == nil {
				return nil, nil
			}
			return nil, expr.typeError("a string", args[0])
		}
		delimiter, ok := args[1].(string)
		if !ok {
			return nil, expr.typeError("a string for the delimiter argument", args[1])
		}
		if delimiter == "" {
			// Upstream splits an empty delimiter into individual bytes.
			if err := reserveExpression(budgets, uint64(len(text))*16); err != nil {
				return nil, err
			}
			parts := make([]any, 0, len(text))
			for i := range len(text) {
				parts = append(parts, text[i:i+1])
			}
			return parts, nil
		}
		if err := reserveExpression(budgets, uint64(strings.Count(text, delimiter)+1)*32); err != nil {
			return nil, err
		}
		parts := strings.Split(text, delimiter)
		values := make([]any, 0, len(parts))
		for _, part := range parts {
			values = append(values, part)
		}
		return values, nil

	case "replace":
		args, err := expr.evalArgs(3, 3, row, params, budgets...)
		if err != nil {
			return nil, err
		}
		text, ok := args[0].(string)
		if !ok {
			if args[0] == nil {
				return nil, nil
			}
			return nil, expr.typeError("a string", args[0])
		}
		search, ok := args[1].(string)
		if !ok {
			return nil, expr.typeError("a string for the search argument", args[1])
		}
		replacement, ok := args[2].(string)
		if !ok {
			return nil, expr.typeError("a string for the replacement argument", args[2])
		}
		if search == "" {
			return text, nil
		}
		size := uint64(len(text))
		count := uint64(strings.Count(text, search))
		if len(replacement) > len(search) {
			size += count * uint64(len(replacement)-len(search))
		}
		if err := reserveExpression(budgets, size); err != nil {
			return nil, err
		}
		return strings.ReplaceAll(text, search, replacement), nil

	case "substring":
		args, err := expr.evalArgs(2, 3, row, params, budgets...)
		if err != nil {
			return nil, err
		}
		text, ok := args[0].(string)
		if !ok {
			if args[0] == nil {
				return nil, nil
			}
			return nil, expr.typeError("a string", args[0])
		}
		start, ok := queryInt(args[1])
		if !ok {
			return nil, expr.typeError("an integer start", args[1])
		}
		if start < 0 {
			start = 0
		}
		if start > int64(len(text)) {
			start = int64(len(text))
		}
		if len(args) == 2 {
			return text[start:], nil
		}
		length, ok := queryInt(args[2])
		if !ok {
			return nil, expr.typeError("an integer length", args[2])
		}
		if length < 0 {
			length = 0
		}
		if length > int64(len(text))-start {
			length = int64(len(text)) - start
		}
		return text[start : start+length], nil

	case "trim":
		args, err := expr.evalArgs(1, 1, row, params, budgets...)
		if err != nil {
			return nil, err
		}
		text, ok := args[0].(string)
		if !ok {
			if args[0] == nil {
				return nil, nil
			}
			return nil, expr.typeError("a string", args[0])
		}
		// Upstream trims exactly these four characters, not unicode space.
		return strings.Trim(text, " \t\n\r"), nil

	case "toLower":
		return expr.evalASCIIString(false, row, params, budgets...)

	case "toUpper":
		return expr.evalASCIIString(true, row, params, budgets...)

	case "toInteger":
		args, err := expr.evalArgs(1, 1, row, params, budgets...)
		if err != nil {
			return nil, err
		}
		switch value := args[0].(type) {
		case int64:
			return value, nil
		case int:
			return int64(value), nil
		case float64:
			truncated, ok := queryInt(value)
			if !ok {
				return nil, nil
			}
			return truncated, nil
		case string:
			parsed, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return nil, nil
			}
			return parsed, nil
		default:
			// Upstream returns NULL for anything else, including booleans.
			return nil, nil
		}

	case "toFloat":
		args, err := expr.evalArgs(1, 1, row, params, budgets...)
		if err != nil {
			return nil, err
		}
		switch value := args[0].(type) {
		case float64:
			return value, nil
		case int64:
			return float64(value), nil
		case int:
			return float64(value), nil
		case string:
			parsed, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return nil, nil
			}
			return parsed, nil
		default:
			return nil, nil
		}

	case "toString":
		args, err := expr.evalArgs(1, 1, row, params, budgets...)
		if err != nil {
			return nil, err
		}
		switch value := args[0].(type) {
		case string:
			return value, nil
		case int64:
			return strconv.FormatInt(value, 10), nil
		case int:
			return strconv.Itoa(value), nil
		case float64:
			return formatQueryFloat(value), nil
		case bool:
			if value {
				return "true", nil
			}
			return "false", nil
		case nil:
			// Upstream renders the NULL literal as the string "null".
			return "null", nil
		default:
			return nil, nil
		}

	case "abs":
		args, err := expr.evalArgs(1, 1, row, params, budgets...)
		if err != nil {
			return nil, err
		}
		switch value := args[0].(type) {
		case int64:
			if value < 0 {
				return -value, nil
			}
			return value, nil
		case int:
			if value < 0 {
				return int64(-value), nil
			}
			return int64(value), nil
		case float64:
			return math.Abs(value), nil
		default:
			return nil, expr.typeError("an integer or float", args[0])
		}
	}
	return nil, fmt.Errorf("unknown function %q", expr.Name)
}

// callArities is the accepted argument count for every built-in function. It
// mirrors the evalArgs bounds in eval so callers can reject bad calls while
// planning instead of after a row is produced.
var callArities = map[string][2]int{
	"id":         {1, 1},
	"labels":     {1, 1},
	"type":       {1, 1},
	"properties": {1, 1},
	"size":       {1, 1},
	"head":       {1, 1},
	"last":       {1, 1},
	"tail":       {1, 1},
	"range":      {2, 3},
	"split":      {2, 2},
	"replace":    {3, 3},
	"substring":  {2, 3},
	"trim":       {1, 1},
	"toLower":    {1, 1},
	"toUpper":    {1, 1},
	"toInteger":  {1, 1},
	"toFloat":    {1, 1},
	"toString":   {1, 1},
	"abs":        {1, 1},
	"coalesce":   {0, math.MaxInt},
}

// validateCallExpr rejects unknown functions and wrong argument counts while a
// query is parsed, and recurses into nested calls.
func validateCallExpr(call callExpr) error {
	arity, ok := callArities[call.Name]
	if !ok {
		return fmt.Errorf("unknown function %q", call.Name)
	}
	if len(call.Args) < arity[0] || len(call.Args) > arity[1] {
		if arity[0] == arity[1] {
			return fmt.Errorf("%s expects %d argument%s, got %d", call.Name, arity[0], plural(arity[0]), len(call.Args))
		}
		return fmt.Errorf("%s expects between %d and %d arguments, got %d", call.Name, arity[0], arity[1], len(call.Args))
	}
	for _, arg := range call.Args {
		nested, ok := arg.(callExpr)
		if !ok {
			continue
		}
		if err := validateCallExpr(nested); err != nil {
			return err
		}
	}
	return nil
}

// evalArgs checks that the call has between min and max arguments, then
// evaluates every argument in order and propagates the first error.
func (expr callExpr) evalArgs(min, max int, row queryRow, params map[string]any, budgets ...*queryBudget) ([]any, error) {
	if len(expr.Args) < min || len(expr.Args) > max {
		if min == max {
			return nil, fmt.Errorf("%s expects %d argument%s, got %d", expr.Name, min, plural(min), len(expr.Args))
		}
		return nil, fmt.Errorf("%s expects between %d and %d arguments, got %d", expr.Name, min, max, len(expr.Args))
	}
	args := make([]any, len(expr.Args))
	for i, arg := range expr.Args {
		value, err := arg.eval(row, params, budgets...)
		if err != nil {
			return nil, err
		}
		args[i] = value
	}
	return args, nil
}

// listArg interprets one argument as a list. The bool reports whether the
// argument was a list at all; NULL is neither a list nor an error, so callers
// return NULL for it.
func (expr callExpr) listArg(value any) ([]any, bool, error) {
	if value == nil {
		return nil, false, nil
	}
	list, ok := value.([]any)
	if !ok {
		return nil, false, expr.typeError("a list", value)
	}
	return list, true, nil
}

// evalASCIIString implements toLower/toUpper with byte-wise ASCII folding.
func (expr callExpr) evalASCIIString(upper bool, row queryRow, params map[string]any, budgets ...*queryBudget) (any, error) {
	args, err := expr.evalArgs(1, 1, row, params, budgets...)
	if err != nil {
		return nil, err
	}
	text, ok := args[0].(string)
	if !ok {
		if args[0] == nil {
			return nil, nil
		}
		return nil, expr.typeError("a string", args[0])
	}
	if err := reserveExpression(budgets, uint64(len(text))*2); err != nil {
		return nil, err
	}
	out := []byte(text)
	for i, char := range out {
		switch {
		case upper && char >= 'a' && char <= 'z':
			out[i] = char - ('a' - 'A')
		case !upper && char >= 'A' && char <= 'Z':
			out[i] = char + ('a' - 'A')
		}
	}
	return string(out), nil
}

func (expr callExpr) typeError(want string, value any) error {
	return fmt.Errorf("%s expects %s, got %s", expr.Name, want, valueKind(value))
}

func plural(count int) string {
	if count == 1 {
		return ""
	}
	return "s"
}

// nodeBinding reports the node record behind a node binding.
func nodeBinding(value any) (*store.NodeRecord, bool) {
	binding, ok := value.(boundValue)
	if !ok || binding.Node == nil {
		return nil, false
	}
	return binding.Node, true
}

// edgeBinding reports the edge record behind an edge binding.
func edgeBinding(value any) (*store.EdgeRecord, bool) {
	binding, ok := value.(boundValue)
	if !ok || binding.Edge == nil {
		return nil, false
	}
	return binding.Edge, true
}

// queryInt converts a value the way upstream EvalResult.toInt does: integers
// pass through and floats truncate toward zero. Everything else, including
// strings and NULL, fails.
func queryInt(value any) (int64, bool) {
	switch v := value.(type) {
	case int64:
		return v, true
	case int:
		return int64(v), true
	case float64:
		// Out-of-range and NaN floats fail instead of panicking.
		if math.IsNaN(v) || v < math.MinInt64 || v >= math.MaxInt64 {
			return 0, false
		}
		return int64(v), true
	default:
		return 0, false
	}
}

// formatQueryFloat matches the upstream "{d}" float formatting used by
// toString: shortest round-trip decimal form with lower-case nan and inf.
func formatQueryFloat(value float64) string {
	switch {
	case math.IsNaN(value):
		return "nan"
	case math.IsInf(value, 1):
		return "inf"
	case math.IsInf(value, -1):
		return "-inf"
	}
	return strconv.FormatFloat(value, 'f', -1, 64)
}

// valueKind names a value for error messages and follows the value model
// rather than Go type names.
func valueKind(value any) string {
	switch v := value.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case int64, int:
		return "integer"
	case float64:
		return "float"
	case string:
		return "string"
	case []byte:
		return "bytes"
	case []float32:
		return "vector"
	case []any:
		return "list"
	case map[string]any:
		return "map"
	case boundValue:
		switch {
		case v.Node != nil:
			return "node"
		case v.Edge != nil:
			return "edge"
		default:
			return "value"
		}
	default:
		return fmt.Sprintf("%T", value)
	}
}
