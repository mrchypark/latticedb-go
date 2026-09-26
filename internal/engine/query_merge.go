package engine

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/mrchypark/latticedb-go/internal/store"
)

// MERGE matches the whole pattern, or creates its unbound elements. Execution
// uses the normal statement fork, so failures also roll back conditional SETs.
type mergeClause struct {
	Patterns []matchPattern
	OnCreate []*setClause
	OnMatch  []*setClause
}

func parseMergeTail(plan *queryPlan, text string) error {
	patternText, keyword, tail := splitOnNextClause(text, " ON CREATE SET ", " ON MATCH SET ", " SET ", " RETURN ")
	if len(splitTopLevel(patternText, ',')) != 1 {
		return errors.New("MERGE requires one connected pattern")
	}
	patterns, err := parseMatchPatterns(patternText)
	if err != nil {
		return err
	}
	privateName := func(name string, fallback string) string {
		if name == "" {
			return "\x00merge:" + fallback
		}
		if name[0] == 0 {
			return "\x00merge:" + name[1:]
		}
		return name
	}
	edges := map[string]bool{}
	for index, item := range patterns {
		switch pattern := item.(type) {
		case nodePattern:
			pattern.Var = privateName(pattern.Var, "node")
			patterns[index] = pattern
		case edgePattern:
			if pattern.VariableLength {
				return errors.New("MERGE does not allow variable-length relationships")
			}
			if pattern.EdgeType == "" {
				return errors.New("MERGE relationships require a type")
			}
			pattern.Left.Var = privateName(pattern.Left.Var, "left")
			pattern.Right.Var = privateName(pattern.Right.Var, "right")
			pattern.EdgeVar = privateName(pattern.EdgeVar, fmt.Sprintf("edge%d", index))
			if edges[pattern.EdgeVar] {
				return errors.New("MERGE cannot repeat a relationship binding")
			}
			edges[pattern.EdgeVar] = true
			patterns[index] = pattern
		}
	}
	clause := &mergeClause{Patterns: patterns}
	plan.mergeClause = clause
	seen := map[string]bool{}
	for keyword != "" {
		if seen[keyword] {
			return fmt.Errorf("duplicate MERGE clause %s", strings.TrimSpace(keyword))
		}
		seen[keyword] = true
		if keyword == " RETURN " {
			return parsePlanReturn(plan, tail)
		}
		assignments, next, remaining := splitOnNextClause(tail, " ON CREATE SET ", " ON MATCH SET ", " SET ", " RETURN ")
		sets, err := parseSetClauses(assignments)
		if err != nil {
			return err
		}
		switch keyword {
		case " ON CREATE SET ":
			clause.OnCreate = sets
		case " ON MATCH SET ":
			clause.OnMatch = sets
		case " SET ":
			plan.setClauses = sets
			if next != "" && next != " RETURN " {
				return errors.New("MERGE SET must follow conditional actions")
			}
		}
		keyword, tail = next, remaining
	}
	return nil
}

func (clause *mergeClause) validate(bind func(string, bindingRole) error, requireExpr func(valueExpr) error) error {
	// Only incoming names may be used to compute the matching properties.
	for _, predicate := range patternPropertyClauses(clause.Patterns) {
		if err := requireExpr(predicate.Expr); err != nil {
			return err
		}
	}
	for _, item := range clause.Patterns {
		switch pattern := item.(type) {
		case nodePattern:
			if err := bind(pattern.Var, bindingNode); err != nil {
				return err
			}
		case edgePattern:
			for _, node := range []nodePattern{pattern.Left, pattern.Right} {
				if err := bind(node.Var, bindingNode); err != nil {
					return err
				}
			}
			if err := bind(pattern.EdgeVar, bindingEdge); err != nil {
				return err
			}
		}
	}
	return nil
}

func (clause *mergeClause) apply(tx *Tx, input []queryRow, params map[string]any, budget *queryBudget) (output []queryRow, err error) {
	defer func() {
		if err != nil {
			budget.releaseRows(len(output))
		}
	}()
	for _, incoming := range input {
		row := incoming.clone()
		if err = refreshRowBindings(tx, &row, budget); err != nil {
			return output, err
		}
		patterns, temporary, resolveErr := clause.resolve(row, params, budget)
		if resolveErr != nil {
			budget.releaseTemporary(temporary)
			return output, resolveErr
		}
		matches, matchErr := mergeMatchRows(tx, row, patterns, params, budget)
		if matchErr != nil {
			budget.releaseTemporary(temporary)
			return output, matchErr
		}
		sets := clause.OnMatch
		if len(matches) == 0 {
			if err = budget.check(1, len(output)+1); err == nil {
				err = budget.chargeRows(1)
			}
			if err != nil {
				budget.releaseTemporary(temporary)
				return output, err
			}
			matches = []queryRow{row}
			err = createMergePattern(tx, &matches[0], patterns, budget)
			sets = clause.OnCreate
		}
		budget.releaseTemporary(temporary)
		if err == nil {
			err = budget.checkRows(len(output) + len(matches))
		}
		for _, set := range sets {
			if err != nil {
				break
			}
			err = set.apply(tx, matches, params, budget)
		}
		if err != nil {
			budget.releaseRows(len(matches))
			return output, err
		}
		output = append(output, matches...)
	}
	return output, nil
}

// Evaluate once per incoming row, before either matching or creating. A NULL
// identity property is ambiguous and is rejected rather than creating repeats.
func (clause *mergeClause) resolve(row queryRow, params map[string]any, budget *queryBudget) ([]matchPattern, uint64, error) {
	var temporary uint64
	properties := func(exprs map[string]valueExpr) (map[string]any, error) {
		props := make(map[string]any, len(exprs))
		for name, expr := range exprs {
			value, err := evalQueryExpr(expr, row, params, budget)
			if err != nil {
				return nil, err
			}
			if value == nil {
				return nil, fmt.Errorf("MERGE property %q cannot be null", name)
			}
			normalized, bytes, err := normalizeMutationValue(value, budget)
			temporary = saturatingAdd(temporary, bytes)
			if err != nil {
				return nil, err
			}
			props[name] = normalized
		}
		return props, nil
	}
	node := func(pattern nodePattern) (nodePattern, error) {
		props, err := properties(pattern.PropertyExprs)
		pattern.Properties, pattern.PropertyExprs = props, nil
		return pattern, err
	}
	patterns := make([]matchPattern, 0, len(clause.Patterns))
	for _, item := range clause.Patterns {
		switch pattern := item.(type) {
		case nodePattern:
			resolved, err := node(pattern)
			if err != nil {
				return nil, temporary, err
			}
			patterns = append(patterns, resolved)
		case edgePattern:
			var err error
			if pattern.Left, err = node(pattern.Left); err != nil {
				return nil, temporary, err
			}
			if pattern.Right, err = node(pattern.Right); err != nil {
				return nil, temporary, err
			}
			if pattern.Properties, err = properties(pattern.PropertyExprs); err != nil {
				return nil, temporary, err
			}
			pattern.PropertyExprs = nil
			patterns = append(patterns, pattern)
		}
	}
	return patterns, temporary, nil
}

func mergeMatchRows(tx *Tx, row queryRow, patterns []matchPattern, params map[string]any, budget *queryBudget) ([]queryRow, error) {
	if err := budget.chargeRows(1); err != nil {
		return nil, err
	}
	plan := &queryPlan{matchPatterns: patterns}
	var iterator queryIterator = &sliceQueryIterator{rows: []queryRow{row}, budget: budget}
	for _, pattern := range patterns {
		iterator = &patternQueryIterator{plan: plan, tx: tx, input: iterator, pattern: pattern, params: params, limit: ^uint(0), budget: budget}
	}
	defer iterator.Close()
	return collectQueryRows(iterator)
}

func createMergePattern(tx *Tx, row *queryRow, patterns []matchPattern, budget *queryBudget) error {
	// Collect repeated node declarations before creating anything, so a cycle's
	// shared endpoint receives all its labels and properties.
	nodes := map[string]nodePattern{}
	var order []string
	addNode := func(pattern nodePattern) error {
		previous, exists := nodes[pattern.Var]
		if !exists {
			order = append(order, pattern.Var)
			nodes[pattern.Var] = pattern
			return nil
		}
		previous.Labels = slices.Clone(previous.Labels)
		for _, label := range pattern.Labels {
			if !slices.Contains(previous.Labels, label) {
				previous.Labels = append(previous.Labels, label)
			}
		}
		for name, value := range pattern.Properties {
			if old, exists := previous.Properties[name]; exists {
				equal, err := queryValuesEqualWithBudget(old, value, budget)
				if err != nil {
					return err
				}
				if !equal {
					return fmt.Errorf("conflicting MERGE property %q", name)
				}
			}
			previous.Properties[name] = value
		}
		nodes[pattern.Var] = previous
		return nil
	}
	for _, item := range patterns {
		switch pattern := item.(type) {
		case nodePattern:
			if err := addNode(pattern); err != nil {
				return err
			}
		case edgePattern:
			if err := addNode(pattern.Left); err != nil {
				return err
			}
			if err := addNode(pattern.Right); err != nil {
				return err
			}
		}
	}
	for _, name := range order {
		if err := budget.check(1, 0); err != nil {
			return err
		}
		pattern := nodes[name]
		if binding, exists := row.get(name); exists {
			if binding.Node == nil || !store.LabelsMatch(binding.Node, pattern.Labels) {
				return fmt.Errorf("bound node %q does not satisfy MERGE", name)
			}
			matches, err := queryPropertiesMatchWithBudget(binding.Node.Properties, pattern.Properties, budget)
			if err != nil {
				return err
			}
			if !matches {
				return fmt.Errorf("bound node %q does not satisfy MERGE", name)
			}
			continue
		}
		node, err := tx.CreateNode(CreateNodeOptions{Labels: pattern.Labels, Properties: pattern.Properties})
		if err != nil {
			return err
		}
		record, err := tx.graph.ReadNode(node.ID)
		if err != nil {
			return err
		}
		row.set(name, boundValue{Node: record})
	}
	for _, item := range patterns {
		pattern, ok := item.(edgePattern)
		if !ok {
			continue
		}
		if err := budget.check(1, 0); err != nil {
			return err
		}
		left, _ := row.get(pattern.Left.Var)
		right, _ := row.get(pattern.Right.Var)
		if binding, exists := row.get(pattern.EdgeVar); exists {
			edge := binding.Edge
			if edge == nil || edge.Type != pattern.EdgeType || !(edge.SourceID == left.Node.ID && edge.TargetID == right.Node.ID || pattern.Undirected && edge.SourceID == right.Node.ID && edge.TargetID == left.Node.ID) {
				return fmt.Errorf("bound relationship %q does not satisfy MERGE", pattern.EdgeVar)
			}
			matches, err := queryPropertiesMatchWithBudget(edge.Properties, pattern.Properties, budget)
			if err != nil {
				return err
			}
			if !matches {
				return fmt.Errorf("bound relationship %q does not satisfy MERGE", pattern.EdgeVar)
			}
			continue
		}
		edge, err := tx.CreateEdge(left.Node.ID, right.Node.ID, pattern.EdgeType, CreateEdgeOptions{Properties: pattern.Properties})
		if err != nil {
			return err
		}
		record, err := tx.graph.ReadEdge(edge.ID)
		if err != nil {
			return err
		}
		row.set(pattern.EdgeVar, boundValue{Edge: record})
	}
	return nil
}
