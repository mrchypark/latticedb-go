package engine

import "slices"

// sortQueryRows checks work and cancellation before each comparison. The private
// sentinel exits the standard library sort immediately; unrelated panics propagate.
func sortQueryRows[E any](rows []E, compare func(E, E) int, budget *queryBudget) (err error) {
	abort := &struct{ err error }{}
	defer func() {
		if recovered := recover(); recovered != nil {
			if recovered != abort {
				panic(recovered)
			}
			err = abort.err
		}
	}()
	if err := budget.check(0, 0); err != nil {
		return err
	}
	slices.SortStableFunc(rows, func(left, right E) int {
		if err := budget.check(1, 0); err != nil {
			abort.err = err
			panic(abort)
		}
		return compare(left, right)
	})
	return budget.check(0, 0)
}
