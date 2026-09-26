package engine

import (
	"fmt"

	"github.com/mrchypark/latticedb-go/internal/store"
)

const variablePathStateBytes = 64

type variablePathState struct {
	node  *store.NodeRecord
	edges []uint64
	bytes uint64
}

// applyVariable forbids edge reuse within this one variable-length expansion.
// Nodes may repeat. Fixed connected patterns retain their existing semantics.
func (pattern edgePattern) applyVariable(tx *Tx, row queryRow, params map[string]any, budget *queryBudget) ([]queryRow, error) {
	var retained uint64
	success := false
	defer func() {
		if !success {
			budget.releaseTemporary(retained)
		}
	}()
	maxHops := pattern.MaxHops
	edgeCount, err := tx.graph.EdgeCount()
	if err != nil {
		return nil, err
	}
	if maxHops < 0 || uint64(maxHops) > edgeCount {
		maxHops = int(min(edgeCount, uint64(^uint(0)>>1)))
	}
	left, leftBound := boundNode(row, pattern.Left.Var)
	right, rightBound := boundNode(row, pattern.Right.Var)
	if (leftBound && left == nil) || (rightBound && right == nil) {
		success = true
		return nil, nil
	}
	boundEdges, boundBytes, edgeBound, err := pattern.variableEdgeBinding(tx, row, budget)
	if err != nil {
		return nil, err
	}
	defer budget.releaseTemporary(boundBytes)

	var rows []queryRow
	start := func(node *store.NodeRecord, reverse bool) error {
		if err := budget.check(1, len(rows)); err != nil {
			return err
		}
		startPattern, endPattern := pattern.Left, pattern.Right
		if reverse {
			startPattern, endPattern = endPattern, startPattern
		}
		matches, err := pattern.variableNodeMatches(row, startPattern, node, budget)
		if err != nil {
			return err
		}
		if !matches {
			return nil
		}
		if err := budget.chargeTemporary(variablePathStateBytes); err != nil {
			return err
		}
		stack := []variablePathState{{node: node, bytes: variablePathStateBytes}}
		defer func() {
			for _, state := range stack {
				budget.releaseTemporary(state.bytes)
			}
		}()
		for len(stack) != 0 {
			state := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			matches, err := pattern.variableNodeMatches(row, endPattern, state.node, budget)
			if err != nil {
				budget.releaseTemporary(state.bytes)
				return err
			}
			if len(state.edges) >= pattern.MinHops && matches && (!edgeBound || len(state.edges) == len(boundEdges)) {
				source, target := node, state.node
				if reverse {
					source, target = state.node, node
				}
				rows, err = pattern.appendVariableRow(tx, row, params, source, target, state.edges, rows, &retained, budget)
				if err != nil {
					budget.releaseTemporary(state.bytes)
					return err
				}
			}
			if len(state.edges) == maxHops || edgeBound && len(state.edges) == len(boundEdges) {
				budget.releaseTemporary(state.bytes)
				continue
			}
			push := func(edgeID uint64, next *store.NodeRecord) error {
				if err := budget.check(1, len(rows)); err != nil {
					return err
				}
				edge, err := tx.graph.ReadEdge(edgeID)
				if err != nil {
					return err
				}
				if edge == nil || (pattern.EdgeType != "" && edge.Type != pattern.EdgeType) {
					return nil
				}
				if edgeBound {
					if len(state.edges) == len(boundEdges) {
						return nil
					}
					expected := len(state.edges)
					if reverse {
						expected = len(boundEdges) - 1 - expected
					}
					if boundEdges[expected] != edgeID {
						return nil
					}
				}
				contains, err := variablePathContains(state.edges, edgeID, budget, len(rows))
				if err != nil || contains {
					return err
				}
				matches, err := queryPropertiesMatchWithBudget(edge.Properties, pattern.Properties, budget)
				if err != nil || !matches {
					return err
				}
				if err := variablePathWork(budget, uint64(len(state.edges)), len(rows)); err != nil {
					return err
				}
				bytes := variablePathStateBytes + uint64(len(state.edges)+1)*8
				if err := budget.chargeTemporary(bytes); err != nil {
					return err
				}
				nextEdges := make([]uint64, len(state.edges)+1)
				if reverse {
					copy(nextEdges[1:], state.edges)
					nextEdges[0] = edgeID
				} else {
					copy(nextEdges, state.edges)
					nextEdges[len(state.edges)] = edgeID
				}
				stack = append(stack, variablePathState{node: next, edges: nextEdges, bytes: bytes})
				return nil
			}
			if reverse {
				if err := variablePathEdges(tx, state.node, false, false, push, budget); err != nil {
					budget.releaseTemporary(state.bytes)
					return err
				}
			} else {
				if err := variablePathEdges(tx, state.node, true, false, push, budget); err != nil {
					budget.releaseTemporary(state.bytes)
					return err
				}
			}
			if pattern.Undirected {
				if reverse {
					if err := variablePathEdges(tx, state.node, true, true, push, budget); err != nil {
						budget.releaseTemporary(state.bytes)
						return err
					}
				} else if err := variablePathEdges(tx, state.node, false, true, push, budget); err != nil {
					budget.releaseTemporary(state.bytes)
					return err
				}
			}
			budget.releaseTemporary(state.bytes)
		}
		return nil
	}
	if left != nil {
		if err := start(left, false); err != nil {
			return nil, err
		}
		success = true
		return rows, nil
	}
	if right != nil && !pattern.Undirected {
		if err := start(right, true); err != nil {
			return nil, err
		}
		success = true
		return rows, nil
	}
	if err := tx.graph.VisitNodes(budget.ctx, func(node *store.NodeRecord) error {
		return start(node, false)
	}); err != nil {
		return nil, err
	}
	success = true
	return rows, nil
}

func (pattern edgePattern) variablePropertiesMatch(row queryRow, params map[string]any, edgeIDs []uint64, tx *Tx, budget *queryBudget) (bool, error) {
	for _, edgeID := range edgeIDs {
		if err := budget.check(1, 0); err != nil {
			return false, err
		}
		edge, err := tx.graph.ReadEdge(edgeID)
		if err != nil {
			return false, err
		}
		if edge == nil {
			return false, nil
		}
		for key, expr := range pattern.PropertyExprs {
			before := budget.bytes
			expected, err := expr.eval(row, params, budget)
			budget.releaseTemporary(uint64(budget.bytes - before))
			if err != nil {
				return false, err
			}
			actual, ok := edge.Properties.Lookup(key)
			if !ok {
				return false, nil
			}
			match, err := queryValuesEqualWithBudget(actual, expected, budget)
			if err != nil || !match {
				return match, err
			}
		}
	}
	return true, nil
}

func (pattern edgePattern) variableEdgeBinding(tx *Tx, row queryRow, budget *queryBudget) ([]uint64, uint64, bool, error) {
	if pattern.EdgeVar == "" {
		return nil, 0, false, nil
	}
	binding, ok := row.get(pattern.EdgeVar)
	if !ok {
		return nil, 0, false, nil
	}
	values, ok := binding.Value.([]any)
	if !binding.HasValue || !ok {
		return nil, 0, false, fmt.Errorf("binding %q must be a variable-path relationship list", pattern.EdgeVar)
	}
	bytes := uint64(len(values)) * 8
	if err := budget.chargeTemporary(bytes); err != nil {
		return nil, 0, false, err
	}
	ids := make([]uint64, len(values))
	for index, value := range values {
		if err := budget.check(1, 0); err != nil {
			budget.releaseTemporary(bytes)
			return nil, 0, false, err
		}
		edgeValue, ok := value.(boundValue)
		if !ok || edgeValue.Edge == nil {
			budget.releaseTemporary(bytes)
			return nil, 0, false, fmt.Errorf("binding %q must contain edges", pattern.EdgeVar)
		}
		edge, err := tx.graph.ReadEdge(edgeValue.Edge.ID)
		if err != nil {
			budget.releaseTemporary(bytes)
			return nil, 0, false, err
		}
		if edge == nil || edge.Type != edgeValue.Edge.Type || pattern.EdgeType != "" && edge.Type != pattern.EdgeType {
			budget.releaseTemporary(bytes)
			return nil, 0, false, fmt.Errorf("binding %q contains an invalid edge", pattern.EdgeVar)
		}
		contains, err := variablePathContains(ids[:index], edge.ID, budget, 0)
		if err != nil {
			budget.releaseTemporary(bytes)
			return nil, 0, false, err
		}
		if contains {
			budget.releaseTemporary(bytes)
			return nil, 0, false, fmt.Errorf("binding %q reuses edge %d", pattern.EdgeVar, edge.ID)
		}
		ids[index] = edge.ID
	}
	return ids, bytes, true, nil
}

func variablePathEdges(tx *Tx, node *store.NodeRecord, outgoing, skipSelf bool, visit func(uint64, *store.NodeRecord) error, budget *queryBudget) error {
	visitEdge := func(edgeID uint64) error {
		if err := budget.check(1, 0); err != nil {
			return err
		}
		edge, err := tx.graph.ReadEdge(edgeID)
		if err != nil {
			return err
		}
		if edge == nil {
			return nil
		}
		if skipSelf && edge.SourceID == edge.TargetID {
			return nil
		}
		var nextID uint64
		if outgoing {
			if edge.SourceID != node.ID {
				return nil
			}
			nextID = edge.TargetID
		} else {
			if edge.TargetID != node.ID {
				return nil
			}
			nextID = edge.SourceID
		}
		next, err := tx.graph.ReadNode(nextID)
		if err != nil {
			return err
		}
		if next != nil {
			return visit(edgeID, next)
		}
		return nil
	}
	if outgoing {
		return tx.graph.VisitOutgoing(budget.ctx, node.ID, visitEdge)
	}
	return tx.graph.VisitIncoming(budget.ctx, node.ID, visitEdge)
}

func variablePathContains(edges []uint64, edgeID uint64, budget *queryBudget, rows int) (bool, error) {
	for start := 0; start < len(edges); start += 64 {
		end := min(start+64, len(edges))
		if err := budget.check(uint64(end-start), rows); err != nil {
			return false, err
		}
		for _, used := range edges[start:end] {
			if used == edgeID {
				return true, nil
			}
		}
	}
	return false, nil
}

func variablePathWork(budget *queryBudget, work uint64, rows int) error {
	for work != 0 {
		chunk := min(work, uint64(64))
		if err := budget.check(chunk, rows); err != nil {
			return err
		}
		work -= chunk
	}
	return nil
}

func (pattern edgePattern) variableNodeMatches(row queryRow, nodePattern nodePattern, node *store.NodeRecord, budget *queryBudget) (bool, error) {
	if node == nil || !store.LabelsMatch(node, nodePattern.Labels) || !bindingMatchesNode(row, nodePattern.Var, node) {
		return false, nil
	}
	matches, err := queryPropertiesMatchWithBudget(node.Properties, nodePattern.Properties, budget)
	return matches, err
}

func (pattern edgePattern) appendVariableRow(tx *Tx, row queryRow, params map[string]any, source, target *store.NodeRecord, edgeIDs []uint64, rows []queryRow, retained *uint64, budget *queryBudget) ([]queryRow, error) {
	if pattern.Left.Var != "" && pattern.Left.Var == pattern.Right.Var && source.ID != target.ID {
		return rows, nil
	}
	if err := budget.check(0, len(rows)+1); err != nil {
		return nil, err
	}
	nextRow := row.clone()
	if pattern.Left.Var != "" {
		nextRow.set(pattern.Left.Var, boundValue{Node: source})
	}
	if pattern.Right.Var != "" {
		nextRow.set(pattern.Right.Var, boundValue{Node: target})
	}
	var bytes uint64
	if pattern.EdgeVar != "" {
		if existing, ok := row.get(pattern.EdgeVar); ok {
			if !existing.HasValue {
				return nil, fmt.Errorf("binding %q must be a variable-path relationship list", pattern.EdgeVar)
			}
		} else {
			bytes = uint64(len(edgeIDs)) * 16
			if err := budget.chargeTemporary(bytes); err != nil {
				return nil, err
			}
			values := make([]any, len(edgeIDs))
			for index, edgeID := range edgeIDs {
				if err := budget.check(1, 0); err != nil {
					budget.releaseTemporary(bytes)
					return nil, err
				}
				edge, err := tx.graph.ReadEdge(edgeID)
				if err != nil {
					budget.releaseTemporary(bytes)
					return nil, err
				}
				if edge == nil {
					budget.releaseTemporary(bytes)
					return nil, fmt.Errorf("variable path edge %d disappeared during query", edgeID)
				}
				values[index] = boundValue{Edge: edge}
			}
			nextRow.set(pattern.EdgeVar, boundValue{Value: values, HasValue: true})
		}
	}
	match, err := pattern.variablePropertiesMatch(nextRow, params, edgeIDs, tx, budget)
	if err != nil || !match {
		budget.releaseTemporary(bytes)
		return rows, err
	}
	*retained += bytes
	return append(rows, nextRow), nil
}
