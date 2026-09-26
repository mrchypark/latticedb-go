package engine

import (
	"bytes"
	"fmt"
	"strings"
)

// Aggregate accumulators fold already-evaluated values into one Cypher
// aggregate result. Parsing, grouping, and rendering live in query.go; this
// file owns only the folding rules, which mirror the reference accumulator in
// upstream src/query/operators/aggregate.zig.

// aggregateKind names one aggregate function.
type aggregateKind string

const (
	aggregateCount   aggregateKind = "count"
	aggregateSum     aggregateKind = "sum"
	aggregateAvg     aggregateKind = "avg"
	aggregateMin     aggregateKind = "min"
	aggregateMax     aggregateKind = "max"
	aggregateCollect aggregateKind = "collect"
)

// aggregateKindFor reports the aggregate kind for a function name. Names match
// case-insensitively, like upstream aggregate.parseAggregateFunc. count(*) is
// not a separate kind: it is count with present=true for every row.
func aggregateKindFor(name string) (aggregateKind, bool) {
	switch strings.ToLower(name) {
	case "count":
		return aggregateCount, true
	case "sum":
		return aggregateSum, true
	case "avg":
		return aggregateAvg, true
	case "min":
		return aggregateMin, true
	case "max":
		return aggregateMax, true
	case "collect":
		return aggregateCollect, true
	default:
		return "", false
	}
}

// aggregateAccumulator folds values into a single aggregate result.
type aggregateAccumulator struct {
	kind         aggregateKind
	distinct     bool
	distinctSeen map[string]struct{}
	seenBytes    uint64
	// count is the number of counted rows for aggregateCount and the divisor
	// for aggregateAvg. Upstream shares one counter the same way.
	count int64
	sum   float64
	// extreme is the current min/max value; seen reports whether one exists.
	extreme any
	seen    bool
	items   []any
}

func newAggregateAccumulator(kind aggregateKind, distinct ...bool) *aggregateAccumulator {
	a := &aggregateAccumulator{kind: kind}
	if len(distinct) != 0 && distinct[0] {
		a.distinct = true
		a.distinctSeen = make(map[string]struct{})
	}
	return a
}

// accept reports whether value has not appeared in this aggregate. Its key uses
// the same canonical encoding as grouping and query DISTINCT.
func (a *aggregateAccumulator) accept(value any, budget *queryBudget) (bool, error) {
	if !a.distinct {
		return true, nil
	}
	keyValue := value
	if materialize, err := aggregateDistinctNeedsPublicValue(value, budget); err != nil {
		return false, err
	} else if materialize {
		bytes := queryValueBytes(value)
		if err := budget.chargeTemporary(bytes); err != nil {
			return false, err
		}
		defer budget.releaseTemporary(bytes)
		keyValue = publicProjectionValue(value)
	}
	before := budget.bytes
	var key strings.Builder
	if err := writeDistinctValueKey(&key, keyValue, budget); err != nil {
		return false, err
	}
	encoded := key.String()
	if _, ok := a.distinctSeen[encoded]; ok {
		budget.releaseTemporary(uint64(budget.bytes - before))
		return false, nil
	}
	const entryBytes = 16
	if err := budget.chargeResult(entryBytes); err != nil {
		return false, err
	}
	a.distinctSeen[encoded] = struct{}{}
	a.seenBytes += uint64(budget.bytes - before)
	return true, nil
}

// aggregateDistinctNeedsPublicValue avoids cloning ordinary aggregate values.
// The key writer already understands public values, but graph bindings need
// their public representation before it can encode them.
func aggregateDistinctNeedsPublicValue(value any, budget *queryBudget) (bool, error) {
	if err := budget.check(1, 0); err != nil {
		return false, err
	}
	switch value := value.(type) {
	case boundValue:
		return true, nil
	case []any:
		for _, item := range value {
			needsPublic, err := aggregateDistinctNeedsPublicValue(item, budget)
			if err != nil || needsPublic {
				return needsPublic, err
			}
		}
	case map[string]any:
		for _, item := range value {
			needsPublic, err := aggregateDistinctNeedsPublicValue(item, budget)
			if err != nil || needsPublic {
				return needsPublic, err
			}
		}
	}
	return false, nil
}

func (a *aggregateAccumulator) releaseDistinct(budget *queryBudget) {
	budget.releaseTemporary(a.seenBytes)
	a.seenBytes = 0
	a.distinctSeen = nil
}

// add folds one input value. present is false when the row has no value for the
// argument: a missing property or a NULL the parent did not turn into a value.
// count(*) has no argument, so its callers pass present=true for every row.
// add returns an error only for an unknown kind; wrong value types are handled
// per kind exactly like the reference, which skips them instead of failing.
func (a *aggregateAccumulator) add(value any, present bool) error {
	switch a.kind {
	case aggregateCount:
		// count(expr) skips rows without a value; count(*) arrives with
		// present=true, so the flag alone decides.
		if present {
			a.count++
		}
	case aggregateSum, aggregateAvg:
		// Only integers and floats fold in. Upstream still counts every input
		// in the avg divisor, including NULL and non-numeric values.
		switch number := value.(type) {
		case int64:
			a.sum += float64(number)
		case int:
			a.sum += float64(number)
		case float64:
			a.sum += number
		}
		a.count++
	case aggregateMin, aggregateMax:
		// Upstream skips NULL before comparing, so a NULL never becomes the
		// extreme even when the parent marks the row present.
		if !present || value == nil {
			return nil
		}
		if !a.seen {
			a.extreme, a.seen = value, true
			return nil
		}
		comparison, ordered := compareAggregateValues(value, a.extreme)
		if !ordered {
			return nil
		}
		if (a.kind == aggregateMin && comparison < 0) || (a.kind == aggregateMax && comparison > 0) {
			a.extreme = value
		}
	case aggregateCollect:
		// Upstream appends every value, NULL included, in input order.
		a.items = append(a.items, value)
	default:
		return fmt.Errorf("unknown aggregate %q", a.kind)
	}
	return nil
}

// result returns the aggregate for every value seen so far. An accumulator with
// no inputs yields int64 0 for count, float64 0 for sum, NULL for avg/min/max,
// and an empty list for collect.
func (a *aggregateAccumulator) result() any {
	switch a.kind {
	case aggregateCount:
		return a.count
	case aggregateSum:
		// Upstream returns a float for sum even when every input was an integer.
		return a.sum
	case aggregateAvg:
		if a.count == 0 {
			return nil
		}
		return a.sum / float64(a.count)
	case aggregateMin, aggregateMax:
		if !a.seen {
			return nil
		}
		return a.extreme
	case aggregateCollect:
		if a.items == nil {
			return []any{}
		}
		return a.items
	default:
		return nil
	}
}

// compareAggregateValues orders two values for min/max. Values the engine cannot
// order compare equal, which keeps the first value seen, matching the fallback
// in the reference compareValues.
func compareAggregateValues(left, right any) (int, bool) {
	if comparison, ok := compareQueryValues(left, right); ok {
		return comparison, true
	}
	// compareQueryValues covers integers, floats, and strings; the reference
	// also orders bytes lexicographically.
	if leftBytes, ok := left.([]byte); ok {
		if rightBytes, ok := right.([]byte); ok {
			return bytes.Compare(leftBytes, rightBytes), true
		}
	}
	return 0, false
}

// addExpr releases evaluated scratch after folding, retaining only collection
// elements or the current min/max value. Reservations precede materialization.
func (a *aggregateAccumulator) addExpr(expr valueExpr, row queryRow, params map[string]any, budget *queryBudget) error {
	before := budget.bytes
	value, err := evalQueryExpr(expr, row, params, budget)
	if err != nil {
		return err
	}
	defer budget.releaseTemporary(uint64(budget.bytes - before))
	accepted, err := a.accept(value, budget)
	if err != nil || !accepted {
		return err
	}
	// The defer above captures only expression scratch, before retained charges.
	switch a.kind {
	case aggregateCollect:
		if err := budget.chargeResult(queryValueBytes(value) + 16); err != nil {
			return err
		}
	case aggregateMin, aggregateMax:
		if value == nil {
			return nil
		}
		if a.seen {
			comparison, ordered := compareAggregateValues(value, a.extreme)
			if !ordered || a.kind == aggregateMin && comparison >= 0 || a.kind == aggregateMax && comparison <= 0 {
				return nil
			}
		}
		if err := budget.chargeResult(queryValueBytes(value)); err != nil {
			return err
		}
		if a.seen {
			budget.releaseTemporary(queryValueBytes(a.extreme))
		}
	}
	if a.kind == aggregateCollect || a.kind == aggregateMin || a.kind == aggregateMax {
		value = cloneRetainedQueryValue(value)
	}
	return a.add(value, value != nil)
}
