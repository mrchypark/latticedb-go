package engine

import (
	"fmt"
	"io"
	"math"
	"slices"

	"github.com/mrchypark/latticedb-go/internal/store"
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
	if tx.graph.PageBase != nil && !tx.graph.PageBase.SearchIndexesCurrent {
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
	if !budget.approximateVector || budget.vectorNamespace == nil || plan.skipExpr != nil || skip != 0 || plan.limitExpr == nil || limit <= 0 || len(plan.orderClauses) != 0 || plan.returnClause == nil || plan.returnClause.Distinct || plan.returnClause.CountAlias != "" || plan.returnClause.hasAggregates() {
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
	expected, err := searchClause.Expr.eval(queryRow{}, params, nil)
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
	if graph.PageBase == nil && graph.VectorIndex.Nodes.Len() == 0 {
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
	if page := tx.graph.PageBase; page != nil {
		return pageFTSSearchCandidate(tx, node, clause, params, budget)
	}
	postings, configured := tx.graph.FTSProperties[clause.Property]
	if !configured {
		return nil, nil
	}
	expected, err := clause.Expr.eval(queryRow{}, params, nil)
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

func pageFTSSearchCandidate(tx *Tx, node nodePattern, clause *whereClause, params map[string]any, budget *queryBudget) (*querySearchCandidate, error) {
	page := tx.graph.PageBase
	if page == nil || !page.SearchIndexesCurrent {
		return nil, nil
	}
	expected, err := clause.Expr.eval(queryRow{}, params, nil)
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
	slices.Sort(terms)
	terms = slices.Compact(terms)

	var selected string
	if len(node.Labels) != 0 {
		definitions, err := ftsIndexDefinitions(tx.graph)
		if err != nil {
			return nil, err
		}
		for _, def := range definitions {
			if def.Kind != FTSIndexNode || def.Property != clause.Property || !slices.Contains(node.Labels, def.Scope) {
				continue
			}
			index := declaredFTSPageIndexName(def.Name)
			ready, err := page.FTSIndexReady(index)
			if err != nil {
				return nil, err
			}
			if ready {
				selected = index
				break
			}
		}
	}
	if selected == "" {
		if _, configured := tx.graph.FTSProperties[clause.Property]; configured {
			index := configuredPropertyFTSPageIndexName(clause.Property)
			ready, err := page.FTSIndexReady(index)
			if err != nil {
				return nil, err
			}
			if ready {
				selected = index
			}
		}
	}
	if selected == "" {
		return nil, nil
	}
	if err := budget.check(0, 0); err != nil {
		return nil, err
	}
	var population uint64
	if len(node.Labels) == 0 {
		population, err = tx.graph.NodeCount()
	} else {
		// Summed document frequencies bound the posting union. Probe labels
		// only far enough to show that this union can narrow them.
		var candidateBound uint64
		for _, term := range terms {
			scratch := saturatingAdd(saturatingMul(saturatingAdd(26, uint64(len(term))), 3), saturatingMul(saturatingAdd(34, uint64(len(selected))), 2))
			if err := budget.check(1, 0); err != nil {
				return nil, err
			}
			if err := budget.chargeTemporary(scratch); err != nil {
				return nil, err
			}
			count, countErr := page.FTSTermDocumentCount(selected, term)
			budget.releaseTemporary(scratch)
			if countErr != nil {
				return nil, countErr
			}
			candidateBound = saturatingAdd(candidateBound, count)
		}
		if candidateBound == 0 {
			return &querySearchCandidate{variable: node.Var}, nil
		}
		remainingWork := uint64(budget.maxWork - budget.work)
		probeWork := remainingWork / 4
		if probeWork < uint64(len(node.Labels)) {
			return nil, nil
		}
		perLabelLimit := min(saturatingAdd(candidateBound, 1), probeWork/uint64(len(node.Labels)))
		population = ^uint64(0)
		for _, label := range node.Labels {
			var count uint64
			countErr := tx.graph.VisitLabel(budget.ctx, label, func(uint64) error {
				if err := budget.check(1, 0); err != nil {
					return err
				}
				count++
				if count >= perLabelLimit {
					return io.EOF
				}
				return nil
			})
			if countErr != nil {
				return nil, countErr
			}
			population = min(population, count)
		}
	}
	if err != nil {
		return nil, err
	}
	// Label populations are probed only as far as the posting bound and work
	// allowance require. The minimum count is a lower bound: reaching it cannot prove
	// selectivity, so release the partial union before the normal label scan.
	if population == 0 {
		return nil, nil
	}

	ids := make([]uint64, 0)
	seen := make(map[uint64]struct{})
	var candidateBytes uint64
	keep := false
	defer func() {
		if !keep && candidateBytes != 0 {
			budget.releaseTemporary(candidateBytes)
		}
	}()
	for _, term := range terms {
		saturated := false
		memoryLimited := false
		err := page.VisitFTSPostings(budget.ctx, selected, term, func(posting store.PageFTSPosting) error {
			if err := budget.check(1, 0); err != nil {
				return err
			}
			if _, exists := seen[posting.DocumentID]; exists {
				return nil
			}
			if uint64(len(ids)) == population-1 {
				saturated = true
				return io.EOF
			}
			// Account for the ID slice and its deduplication entry while the
			// union is being built. The map portion is released below once
			// the final sorted candidate slice is retained.
			if uint64(budget.maxBytes-budget.bytes) < 24 {
				memoryLimited = true
				return io.EOF
			}
			if err := budget.chargeTemporary(24); err != nil {
				return err
			}
			candidateBytes += 24
			seen[posting.DocumentID] = struct{}{}
			ids = append(ids, posting.DocumentID)
			return nil
		})
		if err != nil {
			return nil, err
		}
		if memoryLimited {
			if err := budget.check(0, 0); err != nil {
				return nil, err
			}
			return nil, nil
		}
		if saturated || uint64(len(ids)) >= population {
			return nil, nil
		}
	}
	slices.Sort(ids)
	retainedBytes := uint64(len(ids)) * 8
	if candidateBytes > retainedBytes {
		budget.releaseTemporary(candidateBytes - retainedBytes)
		candidateBytes = retainedBytes
	}
	keep = true
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
