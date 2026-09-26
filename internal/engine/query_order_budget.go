package engine

import (
	"cmp"
	"sort"
	"strings"
)

const (
	queryOrderComparisonChunk = 4096
	queryOrderMapKeyBytes     = 16
)

// compareOrderValuesWithBudget preserves compareOrderValues ordering while
// accounting for deep comparison work and temporary map-key buffers.
func compareOrderValuesWithBudget(left, right any, budget *queryBudget) (int, error) {
	if err := budget.check(1, 0); err != nil {
		return 0, err
	}
	left, right = orderComparable(left), orderComparable(right)
	leftRank, rightRank := orderValueRank(left), orderValueRank(right)
	if leftRank != rightRank {
		return cmp.Compare(leftRank, rightRank), nil
	}
	switch leftRank {
	case orderNumber:
		return compareOrderNumbers(left, right), nil
	case orderBool:
		if left.(bool) == right.(bool) {
			return 0, nil
		}
		if !left.(bool) {
			return -1, nil
		}
		return 1, nil
	case orderString:
		return compareOrderStringWithBudget(left.(string), right.(string), budget)
	case orderBytes:
		return compareOrderBytesWithBudget(left.([]byte), right.([]byte), budget)
	case orderVector:
		return compareOrderVectorWithBudget(left.([]float32), right.([]float32), budget)
	case orderList:
		return compareOrderListWithBudget(left.([]any), right.([]any), budget)
	case orderMap:
		return compareOrderMapWithBudget(left.(map[string]any), right.(map[string]any), budget)
	default:
		return 0, nil
	}
}

func compareOrderStringWithBudget(left, right string, budget *queryBudget) (int, error) {
	for start := 0; start < min(len(left), len(right)); start += queryOrderComparisonChunk {
		end := min(start+queryOrderComparisonChunk, min(len(left), len(right)))
		if err := budget.check(uint64((end-start+63)/64), 0); err != nil {
			return 0, err
		}
		if comparison := strings.Compare(left[start:end], right[start:end]); comparison != 0 {
			return comparison, nil
		}
	}
	return cmp.Compare(len(left), len(right)), nil
}

func compareOrderBytesWithBudget(left, right []byte, budget *queryBudget) (int, error) {
	for start := 0; start < min(len(left), len(right)); start += queryOrderComparisonChunk {
		end := min(start+queryOrderComparisonChunk, min(len(left), len(right)))
		if err := budget.check(uint64((end-start+63)/64), 0); err != nil {
			return 0, err
		}
		for index := start; index < end; index++ {
			if comparison := cmp.Compare(left[index], right[index]); comparison != 0 {
				return comparison, nil
			}
		}
	}
	return cmp.Compare(len(left), len(right)), nil
}

func compareOrderVectorWithBudget(left, right []float32, budget *queryBudget) (int, error) {
	for start := 0; start < min(len(left), len(right)); start += queryOrderComparisonChunk {
		end := min(start+queryOrderComparisonChunk, min(len(left), len(right)))
		if err := budget.check(uint64((end-start+63)/64), 0); err != nil {
			return 0, err
		}
		for index := start; index < end; index++ {
			if comparison := cmp.Compare(left[index], right[index]); comparison != 0 {
				return comparison, nil
			}
		}
	}
	return cmp.Compare(len(left), len(right)), nil
}

func compareOrderListWithBudget(left, right []any, budget *queryBudget) (int, error) {
	for index := range min(len(left), len(right)) {
		comparison, err := compareOrderValuesWithBudget(left[index], right[index], budget)
		if err != nil || comparison != 0 {
			return comparison, err
		}
	}
	return cmp.Compare(len(left), len(right)), nil
}

func compareOrderMapWithBudget(left, right map[string]any, budget *queryBudget) (int, error) {
	keyBytes := uint64(len(left)+len(right)) * queryOrderMapKeyBytes
	if err := budget.chargeTemporary(keyBytes); err != nil {
		return 0, err
	}
	defer budget.releaseTemporary(keyBytes)
	leftKeys, err := sortedOrderMapKeys(left, budget)
	if err != nil {
		return 0, err
	}
	rightKeys, err := sortedOrderMapKeys(right, budget)
	if err != nil {
		return 0, err
	}
	for index := range min(len(leftKeys), len(rightKeys)) {
		comparison, err := compareOrderStringWithBudget(leftKeys[index], rightKeys[index], budget)
		if err != nil || comparison != 0 {
			return comparison, err
		}
		comparison, err = compareOrderValuesWithBudget(left[leftKeys[index]], right[rightKeys[index]], budget)
		if err != nil || comparison != 0 {
			return comparison, err
		}
	}
	return cmp.Compare(len(leftKeys), len(rightKeys)), nil
}

func sortedOrderMapKeys(values map[string]any, budget *queryBudget) (_ []string, err error) {
	keys := make([]string, 0, len(values))
	for key := range values {
		if len(keys)%64 == 0 {
			if err := budget.check(1, 0); err != nil {
				return nil, err
			}
		}
		keys = append(keys, key)
	}
	abort := &struct{ err error }{}
	defer func() {
		if recovered := recover(); recovered != nil {
			if recovered != abort {
				panic(recovered)
			}
			err = abort.err
		}
	}()
	sort.Slice(keys, func(left, right int) bool {
		comparison, comparisonErr := compareOrderStringWithBudget(keys[left], keys[right], budget)
		if comparisonErr != nil {
			abort.err = comparisonErr
			panic(abort)
		}
		return comparison < 0
	})
	return keys, nil
}

func (plan *queryPlan) compareOrderedRowsWithBudget(left, right queryRow, budget *queryBudget) (int, error) {
	for _, clause := range plan.orderClauses {
		comparison, err := compareOrderValuesWithBudget(clause.value(left), clause.value(right), budget)
		if err != nil {
			return 0, err
		}
		if clause.Desc {
			comparison = -comparison
		}
		if comparison != 0 {
			return comparison, nil
		}
	}
	return compareRowBindingsWithBudget(left, right, budget)
}

func (plan *queryPlan) compareOrderedQueryRowsWithBudget(left, right orderedQueryRow, budget *queryBudget) (int, error) {
	comparison, err := plan.compareOrderedRowsWithBudget(left.row, right.row, budget)
	if err != nil || comparison != 0 {
		return comparison, err
	}
	return cmp.Compare(left.sequence, right.sequence), nil
}

// compareRowBindingsWithBudget accounts for the binding-ID scan used to make
// query ordering deterministic after all ORDER BY values compare equal.
func compareRowBindingsWithBudget(left, right queryRow, budget *queryBudget) (int, error) {
	for _, slots := range []int{len(left.slots), len(right.slots)} {
		if slots == 0 {
			continue
		}
		if err := budget.check(uint64((slots+63)/64), 0); err != nil {
			return 0, err
		}
	}
	return compareRowBindings(left, right), nil
}
