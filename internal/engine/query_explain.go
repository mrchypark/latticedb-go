package engine

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/mrchypark/latticedb-go/internal/store"
)

const queryExplanationSchemaVersion = 1

// QueryExplanation is a deterministic description of a prepared read query.
type QueryExplanation struct {
	SchemaVersion int
	DatabaseID    string
	CommitID      uint64
	Root          PlanOperator
}

// PlanOperator describes one stable step in an explained query plan.
type PlanOperator struct {
	Name        string
	Access      string
	Direction   string
	Estimate    *uint64
	Conditional bool
	Reason      string
	Fallback    string
	Children    []PlanOperator
}

// ExplainContext explains a read query from one database snapshot.
func (db *DB) ExplainContext(ctx context.Context, query string, params map[string]any, opts QueryOptions) (QueryExplanation, error) {
	if db == nil {
		return QueryExplanation{}, ErrDatabaseClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return QueryExplanation{}, err
	}
	tx, err := db.Begin(true)
	if err != nil {
		return QueryExplanation{}, err
	}
	defer tx.rollbackInternal()
	return tx.explainContext(ctx, query, params, opts)
}

// ExplainContext explains a read query from this transaction's snapshot.
func (tx *Tx) ExplainContext(ctx context.Context, query string, params map[string]any, opts QueryOptions) (QueryExplanation, error) {
	if tx == nil || tx.closed || tx.db == nil {
		return QueryExplanation{}, ErrInactiveTx
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return QueryExplanation{}, err
	}
	if !tx.readOnly {
		return QueryExplanation{}, &QueryError{
			Stage: QueryErrorStagePlan,
			Err:   fmt.Errorf("%w: EXPLAIN requires a read-only transaction", ErrUnsupportedOption),
		}
	}
	return tx.explainContext(ctx, query, params, opts)
}

func (tx *Tx) explainContext(ctx context.Context, query string, params map[string]any, opts QueryOptions) (QueryExplanation, error) {
	if len(query) > maxQueryBytes {
		return QueryExplanation{}, fmt.Errorf("%w: query exceeds %d bytes", ErrResourceLimit, maxQueryBytes)
	}
	plan, err := tx.db.cachedQueryPlan(query)
	if err != nil {
		return QueryExplanation{}, &QueryError{Stage: QueryErrorStageParse, Err: err}
	}
	if err := explainValidateShape(plan); err != nil {
		return QueryExplanation{}, &QueryError{Stage: QueryErrorStagePlan, Err: err}
	}

	budget := newQueryBudget(ctx, opts)
	defer releaseQueryBudget(budget)
	budget.activeTx = tx
	budget.residentSources = tx.graph.PageBase == nil
	previousBudget := tx.queryBudget
	tx.queryBudget = budget
	defer func() {
		tx.queryBudget = previousBudget
		budget.activeTx = nil
	}()

	if budget.vectorNamespace != nil {
		namespace, err := resolveVectorNamespace(tx.graph, budget.vectorNamespace)
		if err != nil {
			return QueryExplanation{}, &QueryError{Stage: QueryErrorStagePlan, Err: err}
		}
		budget.vectorNamespace = namespace
		if err := plan.validateQueryVectorNamespace(*namespace); err != nil {
			return QueryExplanation{}, &QueryError{Stage: QueryErrorStagePlan, Err: err}
		}
	}

	normalized, paramBytes, err := normalizeQueryParamsWithBudget(params, budget)
	if err != nil {
		return QueryExplanation{}, &QueryError{Stage: QueryErrorStagePlan, Err: err}
	}
	defer budget.releaseTemporary(paramBytes)

	skip, err := paginationValue("SKIP", plan.skipExpr, normalized)
	if err != nil {
		return QueryExplanation{}, &QueryError{Stage: QueryErrorStagePlan, Err: err}
	}
	limit, err := paginationValue("LIMIT", plan.limitExpr, normalized)
	if err != nil {
		return QueryExplanation{}, &QueryError{Stage: QueryErrorStagePlan, Err: err}
	}
	ordered, err := plan.orderedMatchPatterns(tx, normalized, budget)
	if err != nil {
		return QueryExplanation{}, &QueryError{Stage: QueryErrorStagePlan, Err: err}
	}
	operators, err := explainOperators(plan, tx, ordered, normalized, skip, limit, budget)
	if err != nil {
		return QueryExplanation{}, &QueryError{Stage: QueryErrorStagePlan, Err: err}
	}
	if err := budget.check(1, len(operators)+1); err != nil {
		return QueryExplanation{}, &QueryError{Stage: QueryErrorStagePlan, Err: err}
	}
	if err := budget.chargeResult(128 + uint64(len("Query"))); err != nil {
		return QueryExplanation{}, &QueryError{Stage: QueryErrorStagePlan, Err: err}
	}
	if err := ctx.Err(); err != nil {
		return QueryExplanation{}, &QueryError{Stage: QueryErrorStagePlan, Err: err}
	}
	return QueryExplanation{
		SchemaVersion: queryExplanationSchemaVersion,
		DatabaseID:    tx.graph.DatabaseID,
		CommitID:      tx.commitID,
		Root: PlanOperator{
			Name:     "Query",
			Children: operators,
		},
	}, nil
}

func explainValidateShape(plan *queryPlan) error {
	if plan == nil {
		return fmt.Errorf("%w: empty query plan", ErrUnsupportedOption)
	}
	if plan.mutates() {
		return fmt.Errorf("%w: EXPLAIN does not support mutation", ErrUnsupportedOption)
	}
	if plan.union != nil {
		return fmt.Errorf("%w: EXPLAIN does not support UNION", ErrUnsupportedOption)
	}
	if plan.next != nil || plan.withClause != nil {
		return fmt.Errorf("%w: EXPLAIN does not support WITH chains", ErrUnsupportedOption)
	}
	if plan.hasOptionalMatch() || len(plan.matchWhereScopes) > 1 {
		return fmt.Errorf("%w: EXPLAIN does not support optional or scoped MATCH", ErrUnsupportedOption)
	}
	if predicateHasExists(plan.wherePredicate) || predicateHasExists(plan.withWhere) {
		return fmt.Errorf("%w: EXPLAIN does not support EXISTS", ErrUnsupportedOption)
	}
	for _, pattern := range plan.matchPatterns {
		if edge, ok := pattern.(edgePattern); ok && edge.VariableLength {
			return fmt.Errorf("%w: EXPLAIN does not support variable-length relationships", ErrUnsupportedOption)
		}
	}
	return nil
}

func explainOperators(plan *queryPlan, tx *Tx, patterns []matchPattern, params map[string]any, skip, limit int, budget *queryBudget) ([]PlanOperator, error) {
	var operators []PlanOperator
	add := func(operator PlanOperator) error {
		if err := budget.check(1, len(operators)+1); err != nil {
			return err
		}
		bytes := uint64(128 + len(operator.Name) + len(operator.Access) + len(operator.Direction) + len(operator.Reason) + len(operator.Fallback))
		if err := budget.chargeResult(bytes); err != nil {
			return err
		}
		operators = append(operators, operator)
		return nil
	}

	if plan.unwindClause != nil {
		if err := add(PlanOperator{Name: "Unwind", Access: "binding:" + plan.unwindClause.Var}); err != nil {
			return nil, err
		}
	}

	_, search := plan.searchCandidateShape(tx, patterns, limit, skip, budget)
	bound := make(map[string]bool)
	for _, pattern := range patterns {
		switch pattern := pattern.(type) {
		case nodePattern:
			operator, err := explainNodeOperator(plan, tx, pattern, search, params, budget)
			if err != nil {
				return nil, err
			}
			if err := add(operator); err != nil {
				return nil, err
			}
			if pattern.Var != "" {
				bound[pattern.Var] = true
			}
		case edgePattern:
			operator, err := explainEdgeOperator(plan, tx, pattern, params, bound, budget)
			if err != nil {
				return nil, err
			}
			if err := add(operator); err != nil {
				return nil, err
			}
			if pattern.Left.Var != "" {
				bound[pattern.Left.Var] = true
			}
			if pattern.Right.Var != "" {
				bound[pattern.Right.Var] = true
			}
		default:
			return nil, fmt.Errorf("%w: EXPLAIN does not support pattern type", ErrUnsupportedOption)
		}
	}

	if len(plan.whereClauses) != 0 || plan.wherePredicate != nil {
		if err := add(PlanOperator{Name: "Filter", Access: "predicate"}); err != nil {
			return nil, err
		}
	}

	topK := plan.usesTopK(skip, limit)
	if len(plan.orderClauses) != 0 {
		operator := PlanOperator{Name: "Sort", Access: explainOrderAccess(plan.orderClauses)}
		if topK {
			operator.Name = "TopK"
			operator.Access = fmt.Sprintf("skip=%d limit=%d", skip, limit)
		}
		if err := add(operator); err != nil {
			return nil, err
		}
	}
	if plan.returnClause != nil {
		access := "scalar"
		if plan.returnClause.hasAggregates() {
			access = "aggregate"
		}
		if err := add(PlanOperator{Name: "Project", Access: access}); err != nil {
			return nil, err
		}
		if plan.returnClause.Distinct {
			if err := add(PlanOperator{Name: "Distinct"}); err != nil {
				return nil, err
			}
		}
	}
	if !topK {
		if plan.skipExpr != nil {
			if err := add(PlanOperator{Name: "Skip", Access: fmt.Sprintf("skip=%d", skip)}); err != nil {
				return nil, err
			}
		}
		if plan.limitExpr != nil {
			if err := add(PlanOperator{Name: "Limit", Access: fmt.Sprintf("limit=%d", limit)}); err != nil {
				return nil, err
			}
		}
	}
	return operators, nil
}

func explainNodeOperator(plan *queryPlan, tx *Tx, node nodePattern, search *whereClause, params map[string]any, budget *queryBudget) (PlanOperator, error) {
	estimate, err := explainPatternEstimate(plan, tx, node, params, budget)
	if err != nil {
		return PlanOperator{}, err
	}
	operator := PlanOperator{
		Name:     "NodeScan",
		Access:   explainNodeAccess(node),
		Estimate: estimate,
	}
	if explainHasBindingID(plan, node.Var) {
		operator.Name = "NodeIDLookup"
		operator.Access = "id:" + node.Var
		operator.Conditional = true
		operator.Reason = "node identity is resolved during execution"
		operator.Fallback = "node_scan"
		return operator, nil
	}
	if search != nil {
		access, err := explainSearchAccess(tx, node, search, budget)
		if err != nil {
			return PlanOperator{}, err
		}
		if access != "" {
			operator.Name = "SearchScan"
			operator.Access = access
			operator.Estimate = nil
			operator.Conditional = true
			operator.Reason = "search parameters and selectivity are checked during execution"
			operator.Fallback = "node_scan"
			return operator, nil
		}
	}
	if access, ok := explainNodeIndexAccess(plan, tx, node); ok {
		operator.Name = "NodeIndexSeek"
		operator.Access = access
		operator.Conditional = true
		operator.Reason = "index candidates are checked against parameters and postings during execution"
		operator.Fallback = "node_scan"
	}
	return operator, nil
}

func explainEdgeOperator(plan *queryPlan, tx *Tx, edge edgePattern, params map[string]any, bound map[string]bool, budget *queryBudget) (PlanOperator, error) {
	estimate, err := explainPatternEstimate(plan, tx, edge, params, budget)
	if err != nil {
		return PlanOperator{}, err
	}
	direction := ""
	name := "EdgeScan"
	fallback := "edge_scan"
	if bound[edge.Left.Var] || bound[edge.Right.Var] {
		name = "EdgeExpand"
		fallback = "edge_expand"
		if bound[edge.Left.Var] {
			direction = "outgoing"
		} else {
			direction = "incoming"
		}
	}
	if edge.Undirected {
		direction = "undirected"
	}
	operator := PlanOperator{
		Name:      name,
		Access:    explainEdgeAccess(edge),
		Direction: direction,
		Estimate:  estimate,
	}
	if access, ok := explainEdgeIndexAccess(plan, tx, edge); ok {
		operator.Name = "EdgeIndexSeek"
		operator.Access = access
		operator.Conditional = true
		operator.Reason = "index candidates are checked against parameters and postings during execution"
		operator.Fallback = fallback
	}
	return operator, nil
}

func explainPatternEstimate(plan *queryPlan, tx *Tx, pattern matchPattern, params map[string]any, budget *queryBudget) (*uint64, error) {
	count, err := plan.patternCardinalityWithBudget(tx, pattern, params, budget)
	if err != nil {
		return nil, err
	}
	estimate := uint64(count)
	ctx := queryScanContext(budget)
	switch pattern := pattern.(type) {
	case nodePattern:
		if len(pattern.Labels) == 0 {
			nodes, err := tx.graph.NodeCountContext(ctx)
			if err != nil {
				return nil, err
			}
			estimate = min(estimate, nodes)
		}
	case edgePattern:
		if pattern.EdgeType == "" {
			edges, err := tx.graph.EdgeCountContext(ctx)
			if err != nil {
				return nil, err
			}
			estimate = min(estimate, edges)
		}
	}
	if estimate == 0 || !explainPatternDynamic(plan, pattern, params) {
		return &estimate, nil
	}
	return nil, nil
}

func explainPatternDynamic(plan *queryPlan, pattern matchPattern, params map[string]any) bool {
	if plan.wherePredicate != nil {
		return true
	}
	static := func(expr valueExpr) bool {
		switch expr := expr.(type) {
		case literalExpr:
			return true
		case paramExpr:
			_, ok := params[expr.Name]
			return ok
		default:
			return false
		}
	}
	checkClauses := func(names ...string) bool {
		for _, clause := range plan.whereClauses {
			if !slices.Contains(names, clause.Var) {
				continue
			}
			if clause.Kind == whereVector || clause.Kind == whereFTS {
				return true
			}
			if clause.Expr != nil && !static(clause.Expr) {
				return true
			}
		}
		return false
	}

	switch pattern := pattern.(type) {
	case nodePattern:
		for _, expr := range pattern.PropertyExprs {
			if !static(expr) {
				return true
			}
		}
		return checkClauses(pattern.Var)
	case edgePattern:
		if pattern.VariableLength {
			return true
		}
		for _, properties := range []map[string]valueExpr{pattern.Left.PropertyExprs, pattern.PropertyExprs, pattern.Right.PropertyExprs} {
			for _, expr := range properties {
				if !static(expr) {
					return true
				}
			}
		}
		return checkClauses(pattern.Left.Var, pattern.EdgeVar, pattern.Right.Var)
	default:
		return true
	}
}

func explainHasBindingID(plan *queryPlan, name string) bool {
	for _, clause := range plan.whereClauses {
		if clause.Var != name || clause.Kind != whereBindingID {
			continue
		}
		switch clause.Expr.(type) {
		case literalExpr, paramExpr:
			return true
		}
	}
	return false
}

func explainSearchAccess(tx *Tx, node nodePattern, clause *whereClause, budget *queryBudget) (string, error) {
	if clause.Kind == whereVector {
		if tx.db.enableVector && !tx.db.disableVectorIndex && tx.graph.VectorDimensions != 0 {
			return "vector:" + clause.Property, nil
		}
		return "", nil
	}
	if _, configured := tx.graph.FTSProperties[clause.Property]; configured {
		return "fts:" + clause.Property, nil
	}
	if tx.graph.PageBase == nil || len(node.Labels) == 0 {
		return "", nil
	}
	for key, value := range tx.graph.AppMetadata.All() {
		if err := budget.check(1, 0); err != nil {
			return "", err
		}
		if !strings.HasPrefix(key, ftsDefinitionPrefix) {
			continue
		}
		bytes := uint64(len(key)) + uint64(len(value))
		if err := budget.chargeTemporary(bytes); err != nil {
			return "", err
		}
		definition, err := decodeFTSIndexDefinition([]byte(key), value)
		budget.releaseTemporary(bytes)
		if err != nil {
			return "", err
		}
		if definition.Kind == FTSIndexNode && definition.Property == clause.Property && slices.Contains(node.Labels, definition.Scope) {
			return "fts:" + clause.Property, nil
		}
	}
	return "", nil
}

func explainNodeIndexAccess(plan *queryPlan, tx *Tx, node nodePattern) (string, bool) {
	for _, label := range node.Labels {
		for _, clause := range plan.whereClauses {
			if clause.Var != node.Var || clause.Kind != whereEquals {
				continue
			}
			switch clause.Expr.(type) {
			case literalExpr, paramExpr:
			default:
				continue
			}
			if tx.graph.NodeProperties.Has(propertyIndexDefinition(label, clause.Property)) {
				return "node_property:" + label + "." + clause.Property, true
			}
		}
	}
	return "", false
}

func explainEdgeIndexAccess(plan *queryPlan, tx *Tx, edge edgePattern) (string, bool) {
	if edge.EdgeVar == "" || edge.EdgeType == "" {
		return "", false
	}
	for _, clause := range plan.whereClauses {
		if clause.Var != edge.EdgeVar || clause.Kind != whereEquals {
			continue
		}
		switch clause.Expr.(type) {
		case literalExpr, paramExpr:
		default:
			continue
		}
		if tx.graph.EdgeProperties.Has(propertyIndexDefinition(edge.EdgeType, clause.Property)) {
			return "edge_property:" + edge.EdgeType + "." + clause.Property, true
		}
	}
	return "", false
}

func propertyIndexDefinition(scope, property string) store.PropertyIndexDefinition {
	return store.PropertyIndexDefinition{Scope: scope, Property: property}
}

func explainNodeAccess(node nodePattern) string {
	switch len(node.Labels) {
	case 0:
		return "all_nodes"
	case 1:
		return "label:" + node.Labels[0]
	default:
		return "labels:" + strings.Join(node.Labels, ",")
	}
}

func explainEdgeAccess(edge edgePattern) string {
	if edge.EdgeType == "" {
		return "all_edges"
	}
	return "edge_type:" + edge.EdgeType
}

func explainOrderAccess(clauses []orderClause) string {
	keys := make([]string, 0, len(clauses))
	for _, clause := range clauses {
		key := clause.Alias
		switch clause.Kind {
		case projectionProperty:
			key = clause.Var + "." + clause.Property
		case projectionBindingID:
			key = "id(" + clause.Var + ")"
		case projectionValue:
			if key == "" {
				key = clause.Var
			}
		}
		if key == "" {
			key = "expression"
		}
		if clause.Desc {
			key += " DESC"
		} else {
			key += " ASC"
		}
		keys = append(keys, key)
	}
	return strings.Join(keys, ", ")
}
