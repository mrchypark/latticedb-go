package engine

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

var errExistsNeedsTransaction = errors.New("EXISTS requires transaction-scoped evaluation")

// existsPredicate is the parsed form of one correlated EXISTS expression. Its
// plan contains only immutable parser data so cached query plans remain safe to
// share between executions.
type existsPredicate struct {
	patterns       []matchPattern
	whereClauses   []*whereClause
	wherePredicate wherePredicate
	bindings       []string
}

// existsEvalState keeps one emitted-row counter per syntactic inner MATCH
// stage. The parent creates one state for a predicate stream so the counters
// stay cumulative across every correlated outer row.
type existsEvalState struct {
	stages map[*existsPredicate][]int
}

func newExistsEvalState() *existsEvalState {
	return &existsEvalState{stages: make(map[*existsPredicate][]int)}
}

func (state *existsEvalState) stageRows(predicate *existsPredicate) []int {
	if state.stages == nil {
		state.stages = make(map[*existsPredicate][]int)
	}
	rows, ok := state.stages[predicate]
	if !ok || len(rows) != len(predicate.patterns) {
		rows = make([]int, len(predicate.patterns))
		state.stages[predicate] = rows
	}
	return rows
}

func parseExistsPredicate(text string) (*existsPredicate, error) {
	text = normalizeQueryText(text)
	if !strings.HasPrefix(text, "EXISTS") || len(text) > len("EXISTS") && isQueryIdentifierChar(text[len("EXISTS")]) {
		return nil, fmt.Errorf("invalid EXISTS expression %q", text)
	}
	rest := strings.TrimSpace(text[len("EXISTS"):])
	if rest == "" || rest[0] != '{' {
		return nil, fmt.Errorf("EXISTS must contain a braced MATCH clause")
	}
	end := findMatchingBrace(rest, 0, '{', '}')
	if end < 0 {
		return nil, errors.New("unterminated EXISTS clause")
	}
	if strings.TrimSpace(rest[end+1:]) != "" {
		return nil, fmt.Errorf("unexpected text after EXISTS clause")
	}
	body := strings.TrimSpace(rest[1:end])
	if !strings.HasPrefix(body, "MATCH ") {
		return nil, errors.New("EXISTS must contain MATCH as its first clause")
	}
	matchText := strings.TrimSpace(strings.TrimPrefix(body, "MATCH "))
	if matchText == "" {
		return nil, errors.New("EXISTS MATCH pattern must be non-empty")
	}
	if containsExistsToken(matchText) {
		return nil, errors.New("nested EXISTS is not supported")
	}

	matchPart, keyword, tail := splitOnNextClause(matchText, append([]string{" WHERE "}, existsRejectedClauses...)...)
	if keyword != "" && keyword != " WHERE " {
		return nil, fmt.Errorf("unsupported clause %q in EXISTS", keyword)
	}
	if keyword == " WHERE " {
		if strings.TrimSpace(matchPart) == "" {
			return nil, errors.New("EXISTS MATCH pattern must be non-empty")
		}
		if strings.TrimSpace(tail) == "" {
			return nil, errors.New("EXISTS WHERE expression must be non-empty")
		}
		if clause := existsRejectedClause(tail); clause != "" {
			return nil, fmt.Errorf("unsupported clause %q in EXISTS", clause)
		}
	}

	parsed, err := parseMatchQuery("MATCH " + matchText)
	if err != nil {
		return nil, err
	}
	patterns := parsed.matchPatterns
	whereClauses := parsed.whereClauses
	renameExistsHiddenBindings(patterns, whereClauses)

	bindings := make([]string, 0)
	seen := make(map[string]struct{})
	addBinding := func(name string) {
		if name == "" {
			return
		}
		if _, ok := seen[name]; ok {
			return
		}
		seen[name] = struct{}{}
		bindings = append(bindings, name)
	}
	for _, item := range patterns {
		switch pattern := item.(type) {
		case nodePattern:
			addBinding(pattern.Var)
		case edgePattern:
			if pattern.VariableLength {
				return nil, errors.New("EXISTS does not support variable-length relationships")
			}
			addBinding(pattern.Left.Var)
			addBinding(pattern.EdgeVar)
			addBinding(pattern.Right.Var)
		}
	}
	for _, clause := range whereClauses {
		if clause.Kind == whereVector || clause.Kind == whereFTS {
			return nil, errors.New("EXISTS does not support vector or full-text search")
		}
	}
	for _, clause := range wherePredicateClauses(parsed.wherePredicate) {
		if clause.Kind == whereVector || clause.Kind == whereFTS {
			return nil, errors.New("EXISTS does not support vector or full-text search")
		}
	}
	if predicateHasExists(parsed.wherePredicate) {
		return nil, errors.New("nested EXISTS is not supported")
	}

	return &existsPredicate{
		patterns:       patterns,
		whereClauses:   whereClauses,
		wherePredicate: parsed.wherePredicate,
		bindings:       bindings,
	}, nil
}

var existsRejectedClauses = []string{
	" OPTIONAL MATCH ",
	" MATCH ",
	" WHERE ",
	" UNWIND ",
	" RETURN ",
	" CREATE ",
	" MERGE ",
	" SET ",
	" REMOVE ",
	" DETACH DELETE ",
	" DELETE ",
	" CALL ",
	" UNION ",
	" LOAD CSV ",
	" USE ",
}

func existsRejectedClause(text string) string {
	_, keyword, _ := splitOnNextClause(text, existsRejectedClauses...)
	if keyword != "" {
		return keyword
	}
	if findWithToken(text) >= 0 {
		return " WITH "
	}
	return ""
}

func containsExistsToken(text string) bool {
	var quote byte
	for index := 0; index < len(text); index++ {
		if scanQueryString(text, index, &quote) {
			continue
		}
		if !strings.HasPrefix(text[index:], "EXISTS") {
			continue
		}
		if index > 0 && isQueryIdentifierChar(text[index-1]) || index+len("EXISTS") < len(text) && isQueryIdentifierChar(text[index+len("EXISTS")]) {
			continue
		}
		next := index + len("EXISTS")
		for next < len(text) && isQuerySpace(text[next]) {
			next++
		}
		if next < len(text) && text[next] == '{' {
			return true
		}
	}
	return false
}

func predicateHasExists(predicate wherePredicate) bool {
	switch predicate := predicate.(type) {
	case nil, *whereClause:
		return false
	case booleanPredicate:
		return slices.ContainsFunc(predicate.Items, predicateHasExists)
	case notPredicate:
		return predicateHasExists(predicate.Item)
	case *existsPredicate:
		return true
	default:
		return false
	}
}

func renameExistsHiddenBindings(patterns []matchPattern, clauses []*whereClause) {
	renamed := make(map[string]string)
	rename := func(name string) string {
		if !strings.HasPrefix(name, "\x00") || strings.HasPrefix(name, "\x00exists:") {
			return name
		}
		if replacement, ok := renamed[name]; ok {
			return replacement
		}
		replacement := "\x00exists:" + strings.TrimPrefix(name, "\x00")
		renamed[name] = replacement
		return replacement
	}
	renameNode := func(node *nodePattern) {
		node.Var = rename(node.Var)
	}
	for index, item := range patterns {
		switch pattern := item.(type) {
		case nodePattern:
			renameNode(&pattern)
			patterns[index] = pattern
		case edgePattern:
			renameNode(&pattern.Left)
			renameNode(&pattern.Right)
			pattern.EdgeVar = rename(pattern.EdgeVar)
			patterns[index] = pattern
		}
	}
	for _, clause := range clauses {
		clause.Var = rename(clause.Var)
	}
}

// addMatchPatternBindings extends scope with the node and edge names declared
// by patterns. Reusing an existing name keeps its original role; a conflicting
// role is rejected instead of shadowed.
func addMatchPatternBindings(scope map[string]bindingRole, patterns []matchPattern) error {
	add := func(name string, role bindingRole) error {
		if name == "" {
			return nil
		}
		if existing, ok := scope[name]; ok && existing != role {
			return fmt.Errorf("binding %q has conflicting roles", name)
		}
		scope[name] = role
		return nil
	}
	for _, item := range patterns {
		switch pattern := item.(type) {
		case nodePattern:
			if err := add(pattern.Var, bindingNode); err != nil {
				return err
			}
		case edgePattern:
			edgeRole := bindingEdge
			if pattern.VariableLength {
				edgeRole = bindingValue
			}
			if err := add(pattern.Left.Var, bindingNode); err != nil {
				return err
			}
			if err := add(pattern.EdgeVar, edgeRole); err != nil {
				return err
			}
			if err := add(pattern.Right.Var, bindingNode); err != nil {
				return err
			}
		}
	}
	return nil
}

// validateExistsPredicateScope checks every EXISTS in predicate against the
// lexical scope at that predicate. Inner names remain local to the EXISTS.
func validateExistsPredicateScope(predicate wherePredicate, scope map[string]bindingRole) error {
	switch predicate := predicate.(type) {
	case nil:
		return nil
	case *whereClause:
		return validateExistsClauseScope(predicate, scope)
	case booleanPredicate:
		for _, item := range predicate.Items {
			if err := validateExistsPredicateScope(item, scope); err != nil {
				return err
			}
		}
		return nil
	case notPredicate:
		return validateExistsPredicateScope(predicate.Item, scope)
	case *existsPredicate:
		inner := make(map[string]bindingRole, len(scope)+len(predicate.bindings))
		for name, role := range scope {
			inner[name] = role
		}
		if err := addMatchPatternBindings(inner, predicate.patterns); err != nil {
			return err
		}
		for _, item := range predicate.patterns {
			switch pattern := item.(type) {
			case nodePattern:
				for _, expr := range pattern.PropertyExprs {
					if err := validateExistsExprScope(expr, inner); err != nil {
						return err
					}
				}
			case edgePattern:
				for _, expr := range pattern.Left.PropertyExprs {
					if err := validateExistsExprScope(expr, inner); err != nil {
						return err
					}
				}
				for _, expr := range pattern.Right.PropertyExprs {
					if err := validateExistsExprScope(expr, inner); err != nil {
						return err
					}
				}
				for _, expr := range pattern.PropertyExprs {
					if err := validateExistsExprScope(expr, inner); err != nil {
						return err
					}
				}
			}
		}
		for _, clause := range predicate.whereClauses {
			if err := validateExistsClauseScope(clause, inner); err != nil {
				return err
			}
		}
		return validateExistsPredicateScope(predicate.wherePredicate, inner)
	default:
		return fmt.Errorf("unsupported predicate in EXISTS scope")
	}
}

func validateExistsClauseScope(clause *whereClause, scope map[string]bindingRole) error {
	if clause.Var != "" {
		if _, ok := scope[clause.Var]; !ok {
			return fmt.Errorf("unknown binding %q", clause.Var)
		}
	}
	if err := validateExistsExprScope(clause.Expr, scope); err != nil {
		return err
	}
	return validateExistsExprScope(clause.LeftExpr, scope)
}

func validateExistsExprScope(expr valueExpr, scope map[string]bindingRole) error {
	for _, name := range valueExprBindings(expr) {
		if _, ok := scope[name]; !ok {
			return fmt.Errorf("unknown binding %q", name)
		}
	}
	return nil
}

func (predicate *existsPredicate) eval(row queryRow, params map[string]any, budget *queryBudget) (predicateTruth, error) {
	if budget == nil || budget.activeTx == nil {
		return predicateUnknown, errExistsNeedsTransaction
	}
	if budget.existsStages == nil {
		budget.existsStages = newExistsEvalState()
	}
	return predicate.evalWithTx(budget.activeTx, row, params, budget, budget.existsStages)
}

var errExistsMatch = errors.New("EXISTS matched")

func (predicate *existsPredicate) evalWithTx(tx *Tx, outer queryRow, params map[string]any, budget *queryBudget, state *existsEvalState) (predicateTruth, error) {
	if err := budget.check(1, 0); err != nil {
		return predicateUnknown, err
	}
	if err := budget.chargeRows(1); err != nil {
		return predicateUnknown, err
	}
	defer budget.releaseRows(1)
	correlated := predicate.correlatedRow(outer)
	root := budget.rowSources([]queryRow{correlated})
	defer root.close(budget)
	counts := state.stageRows(predicate)
	var match func(int, queryRow) error
	match = func(stage int, row queryRow) error {
		if stage == len(predicate.patterns) {
			for _, clause := range predicate.whereClauses {
				if err := budget.check(1, 0); err != nil {
					return err
				}
				truth, err := clause.eval(row, params, budget)
				if err != nil {
					return err
				}
				if truth != predicateTrue {
					return nil
				}
			}
			if predicate.wherePredicate != nil {
				truth, err := predicate.wherePredicate.eval(row, params, budget)
				if err != nil {
					return err
				}
				if truth != predicateTrue {
					return nil
				}
			}
			return errExistsMatch
		}
		oldSink := budget.matchRowSink
		defer func() { budget.matchRowSink = oldSink }()
		budget.matchRowSink = func(candidate queryRow) error {
			if err := budget.checkRows(counts[stage] + 1); err != nil {
				return err
			}
			counts[stage]++
			if err := budget.chargeRows(1); err != nil {
				return err
			}
			defer budget.releaseRows(1)
			candidateRoot := budget.rowSources([]queryRow{candidate})
			defer candidateRoot.close(budget)
			return match(stage+1, candidate)
		}
		pattern := predicate.patterns[stage]
		if node, ok := pattern.(nodePattern); ok {
			if existing, bound := row.get(node.Var); bound {
				if existing.Node == nil {
					return budget.check(1, 0)
				}
				_, err := node.applyID(tx, []queryRow{row}, existing.Node.ID, nil, budget)
				return err
			}
		}
		_, err := pattern.apply(tx, []queryRow{row}, budget)
		return err
	}
	err := match(0, correlated)
	if errors.Is(err, errExistsMatch) {
		return predicateTrue, nil
	}
	if err != nil {
		return predicateUnknown, err
	}
	return predicateFalse, nil
}

func (predicate *existsPredicate) correlatedRow(outer queryRow) queryRow {
	index := make(map[string]int, len(outer.index)+len(predicate.bindings))
	for name, slot := range outer.index {
		index[name] = slot
	}
	slots := slices.Clone(outer.slots)
	for _, name := range predicate.bindings {
		if _, ok := index[name]; ok {
			continue
		}
		index[name] = len(slots)
		slots = append(slots, boundValue{})
	}
	return queryRow{slots: slots, index: index, Order: outer.Order}
}
