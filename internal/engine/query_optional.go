package engine

import "fmt"

type optionalMatchPattern struct {
	plan     *queryPlan
	scope    matchWhereScope
	params   map[string]any
	limit    int
	skip     int
	produced []int
	bindings []string
}

func validateOptionalMatchPlan(plan *queryPlan) error {
	hasOptional := false
	for current := plan; current != nil; current = current.next {
		if current.hasOptionalMatch() {
			hasOptional = true
			break
		}
	}
	if !hasOptional {
		return nil
	}
	if plan.mutates() {
		return fmt.Errorf("OPTIONAL MATCH cannot be combined with mutation")
	}
	for current := plan; current != nil; current = current.next {
		for _, scope := range current.matchWhereScopes {
			if !scope.optional {
				continue
			}
			for _, pattern := range scope.patterns {
				edge, ok := pattern.(edgePattern)
				if ok && edge.VariableLength {
					return fmt.Errorf("OPTIONAL MATCH does not support variable-length relationships")
				}
			}
			if optionalScopeHasRankedSearch(scope) {
				return fmt.Errorf("OPTIONAL MATCH does not support vector or full-text search")
			}
		}
	}
	return nil
}

func (plan *queryPlan) hasOptionalMatch() bool {
	for _, scope := range plan.matchWhereScopes {
		if scope.optional {
			return true
		}
	}
	return false
}

func optionalScopeHasRankedSearch(scope matchWhereScope) bool {
	for _, clause := range scope.clauses {
		if clause.Kind == whereVector || clause.Kind == whereFTS {
			return true
		}
	}
	for _, clause := range wherePredicateClauses(scope.predicate) {
		if clause.Kind == whereVector || clause.Kind == whereFTS {
			return true
		}
	}
	return false
}

func newOptionalMatchPattern(plan *queryPlan, scope matchWhereScope, params map[string]any, limit, skip int) *optionalMatchPattern {
	bindings := make([]string, 0)
	seen := make(map[string]struct{})
	add := func(name string) {
		if name == "" {
			return
		}
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
		bindings = append(bindings, name)
	}
	for _, pattern := range scope.patterns {
		switch pattern := pattern.(type) {
		case nodePattern:
			add(pattern.Var)
		case edgePattern:
			add(pattern.Left.Var)
			add(pattern.EdgeVar)
			add(pattern.Right.Var)
		}
	}
	return &optionalMatchPattern{
		plan:     plan,
		scope:    scope,
		params:   params,
		limit:    limit,
		skip:     skip,
		produced: make([]int, len(scope.patterns)),
		bindings: bindings,
	}
}

func (pattern *optionalMatchPattern) apply(tx *Tx, rows []queryRow, budget *queryBudget) ([]queryRow, error) {
	output := make([]queryRow, 0, len(rows))
	unroot := budget.sourceRows(func() []queryRow { return output })
	defer unroot()
	for _, row := range rows {
		matches, err := pattern.applyRow(tx, row, budget)
		if err != nil {
			return nil, err
		}
		if len(matches) != 0 {
			output = append(output, matches...)
			continue
		}
		if err := budget.check(0, 1); err != nil {
			return nil, err
		}
		nullRow := row.clone()
		for _, name := range pattern.bindings {
			if _, bound := nullRow.get(name); !bound {
				nullRow.set(name, boundValue{})
			}
		}
		output = append(output, nullRow)
	}
	return output, nil
}

func (pattern *optionalMatchPattern) applyRow(tx *Tx, row queryRow, budget *queryBudget) ([]queryRow, error) {
	// The enclosing iterator still owns its input charge. Give the inner
	// pipeline its own charge because its stages consume the seed row.
	if err := budget.chargeRows(1); err != nil {
		return nil, err
	}
	var stream queryIterator = &sliceQueryIterator{rows: []queryRow{row}, budget: budget}
	root := budget.iteratorSources(stream)
	defer root.close(budget)
	defer func() { stream.Close() }()

	stages := make([]*patternQueryIterator, len(pattern.scope.patterns))
	for index, item := range pattern.scope.patterns {
		stage := newPatternQueryIterator(pattern.plan, tx, stream, item, pattern.params, pattern.limit, pattern.skip, budget, nil)
		stage.emitted = pattern.produced[index]
		stages[index] = stage
		stream = stage
		root.setIterator(stream)
	}

	next, err := applyWhereQueryStream(stream, tx, pattern.params, budget, pattern.scope.clauses, pattern.scope.predicate)
	if err != nil {
		return nil, err
	}
	stream = next
	root.setIterator(stream)
	matches, err := collectQueryRows(stream, budget)
	if err != nil {
		return nil, err
	}
	for index, stage := range stages {
		pattern.produced[index] = stage.emitted
	}
	budget.releaseRows(len(matches))
	return matches, nil
}
