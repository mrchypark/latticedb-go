package engine

import (
	"fmt"
	"math"
	"slices"
)

type querySearchCandidate struct {
	variable      string
	nodeIDs       []uint64
	vectorResults []VectorSearchResult
	bytes         uint64
}

// searchCandidate returns complete FTS candidates, or explicitly approximate
// vector candidates, only for a single-node read plan. All candidate rows
// continue through the normal pattern and WHERE evaluation.
func (plan *queryPlan) searchCandidate(tx *Tx, patterns []matchPattern, params map[string]any, limit, skip int, budget *queryBudget) (*querySearchCandidate, error) {
	if tx == nil || !querySearchSnapshotClean(tx) || plan.unwindClause != nil || plan.mutates() || len(patterns) != 1 || plan.wherePredicate != nil {
		return nil, nil
	}
	node, ok := patterns[0].(nodePattern)
	if !ok || node.Var == "" || len(node.Properties) != 0 || len(node.PropertyExprs) != 0 {
		return nil, nil
	}
	var searchClause *whereClause
	searchClauseIndex := -1
	for index, clause := range plan.whereClauses {
		if clause.Var != node.Var {
			return nil, nil
		}
		if clause.Kind != whereVector && clause.Kind != whereFTS {
			continue
		}
		if searchClause != nil {
			return nil, nil
		}
		searchClause = clause
		searchClauseIndex = index
	}
	if searchClause == nil || !queryInvariantExpr(searchClause.Expr) {
		return nil, nil
	}
	if searchClause.Kind == whereFTS {
		// Preserve left-to-right error visibility: an empty FTS candidate set
		// must not hide an earlier scalar clause that would have failed.
		if searchClauseIndex != 0 {
			return nil, nil
		}
		return plan.ftsSearchCandidate(tx, node, searchClause, params, budget)
	}
	if !budget.approximateVector || budget.vectorNamespace == nil || plan.skipExpr != nil || skip != 0 || plan.limitExpr == nil || limit <= 0 || len(plan.orderClauses) != 0 || plan.returnClause == nil || plan.returnClause.Distinct || plan.returnClause.CountAlias != "" {
		return nil, nil
	}
	if len(plan.whereClauses) != 1 {
		return nil, nil
	}
	if len(node.Labels) != 0 && (budget.vectorNamespace.Scope == "" || len(node.Labels) != 1 || node.Labels[0] != budget.vectorNamespace.Scope) {
		return nil, nil
	}
	if limit > math.MaxUint32 {
		return nil, nil
	}
	expected, err := searchClause.Expr.eval(queryRow{}, params)
	if err != nil {
		return nil, nil
	}
	queryVector, ok := expected.([]float32)
	if !ok {
		return nil, nil
	}
	if tx.graph.VectorDimensions == 0 || len(queryVector) != int(tx.graph.VectorDimensions) {
		return nil, nil
	}
	for _, value := range queryVector {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return nil, nil
		}
	}
	if tx.db == nil || !tx.db.enableVector || tx.graph.VectorDimensions == 0 || tx.db.disableVectorIndex {
		return nil, nil
	}
	graph := vectorNamespaceFacade(tx.graph, *budget.vectorNamespace)
	if graph.VectorIndex.Nodes.Len() == 0 {
		return nil, nil
	}
	if limit > maxSearchResults {
		return nil, nil
	}
	remainingWork := uint64(budget.maxWork - budget.work)
	remainingBytes := uint64(budget.maxBytes - budget.bytes)
	if remainingWork == 0 || remainingBytes == 0 {
		return nil, fmt.Errorf("%w: query search budget exhausted", ErrResourceLimit)
	}
	resultCapacity := uint32(limit)
	searchBudget, err := newDirectSearchBudget(budget.ctx, remainingWork, remainingBytes, resultCapacity)
	if err != nil {
		return nil, err
	}
	results, _, err := searchVectorGraph(graph, queryVector, VectorSearchOptions{
		K:        resultCapacity,
		EfSearch: budget.vectorEfSearch,
	}, searchBudget, false)
	if err != nil {
		return nil, err
	}
	if err := budget.check(searchBudget.work, 0); err != nil {
		return nil, err
	}
	if searchBudget.bytes > remainingBytes {
		return nil, fmt.Errorf("%w: query search memory exceeds budget", ErrResourceLimit)
	}
	if err := budget.chargeTemporary(searchBudget.bytes); err != nil {
		return nil, err
	}
	return &querySearchCandidate{variable: node.Var, vectorResults: results, bytes: searchBudget.bytes}, nil
}

func (plan *queryPlan) ftsSearchCandidate(tx *Tx, node nodePattern, clause *whereClause, params map[string]any, budget *queryBudget) (*querySearchCandidate, error) {
	postings, configured := tx.graph.FTSProperties[clause.Property]
	if !configured {
		return nil, nil
	}
	expected, err := clause.Expr.eval(queryRow{}, params)
	if err != nil {
		return nil, nil
	}
	queryText, ok := expected.(string)
	if !ok {
		return nil, nil
	}
	terms, termBytes, err := tokenizeQueryText(queryText, budget)
	if err != nil {
		return nil, err
	}
	defer budget.releaseTemporary(termBytes)
	if len(terms) == 0 {
		return &querySearchCandidate{variable: node.Var}, nil
	}
	// The scorer retains duplicate query terms, but the candidate union does
	// not need to visit the same posting list more than once.
	slices.Sort(terms)
	terms = slices.Compact(terms)
	var candidateCount uint64
	for _, term := range terms {
		candidateCount = saturatingAdd(candidateCount, uint64(postings.Len(term)))
	}
	patternPool := uint64(tx.graph.Nodes.Len())
	for _, label := range node.Labels {
		patternPool = min(patternPool, uint64(tx.graph.Labels.Len(label)))
	}
	// If the postings cannot narrow the pattern, keep the existing pattern
	// scan. This avoids allocating a global high-frequency union for a narrow
	// label (and also handles an empty label without touching the postings).
	if candidateCount >= patternPool {
		return nil, nil
	}
	if candidateCount > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("%w: FTS candidate set is too large", ErrResourceLimit)
	}
	candidateBytes := saturatingMul(candidateCount, 8)
	if err := budget.chargeTemporary(candidateBytes); err != nil {
		return nil, err
	}
	ids := make([]uint64, 0, int(min(candidateCount, uint64(^uint(0)>>1))))
	for _, term := range terms {
		for nodeID := range postings.All(term) {
			if err := budget.check(1, 0); err != nil {
				budget.releaseTemporary(candidateBytes)
				return nil, err
			}
			ids = append(ids, nodeID)
		}
	}
	slices.Sort(ids)
	ids = slices.Compact(ids)
	return &querySearchCandidate{variable: node.Var, nodeIDs: ids, bytes: candidateBytes}, nil
}

func queryInvariantExpr(expr valueExpr) bool {
	switch expr.(type) {
	case literalExpr, paramExpr:
		return true
	default:
		return false
	}
}

func querySearchSnapshotClean(tx *Tx) bool {
	if tx.readOnly {
		return !tx.queryIndexesDisabled
	}
	if tx.changes == nil || tx.queryIndexesDisabled {
		return false
	}
	changes := tx.changes
	return len(changes.upsertNodes) == 0 && len(changes.deleteNodes) == 0 &&
		len(changes.upsertEdges) == 0 && len(changes.deleteEdges) == 0 &&
		len(changes.upsertFTS) == 0 && len(changes.deleteFTS) == 0 &&
		len(changes.nodePropertyKeys) == 0 && len(changes.edgePropertyKeys) == 0 &&
		len(changes.appMetadata) == 0 && len(changes.createNodeIndexes) == 0 &&
		len(changes.dropNodeIndexes) == 0 && len(changes.createEdgeIndexes) == 0 &&
		len(changes.dropEdgeIndexes) == 0 && !changes.streamsChanged &&
		len(changes.streamOperations) == 0
}
