package engine

import (
	"context"
	"fmt"
	"io"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func (db *DB) GetNodesByLabel(label string) ([]uint64, error) {
	var ids []uint64
	err := db.View(func(tx *Tx) error {
		return tx.graph.VisitLabel(context.Background(), label, func(id uint64) error {
			ids = append(ids, id)
			return nil
		})
	})
	return ids, err
}

func (tx *Tx) IsReadOnly() bool { return tx != nil && tx.readOnly }

func (tx *Tx) IsActive() bool { return tx != nil && !tx.closed }

func (tx *Tx) DeleteEdge(sourceID, targetID uint64, edgeType string) error {
	if err := tx.ensureWritable(); err != nil {
		return err
	}
	if _, err := tx.requireNode(sourceID); err != nil {
		return err
	}
	if _, err := tx.requireNode(targetID); err != nil {
		return err
	}
	var match uint64
	if err := tx.graph.VisitOutgoing(context.Background(), sourceID, func(edgeID uint64) error {
		edge, err := tx.graph.ReadEdge(edgeID)
		if err != nil {
			return err
		}
		if edge != nil && edge.TargetID == targetID && edge.Type == edgeType {
			match = edgeID
			return io.EOF
		}
		return nil
	}); err != nil {
		return err
	}
	if match != 0 {
		return tx.deleteEdge(match)
	}
	return fmt.Errorf("edge %d-[%s]->%d not found", sourceID, edgeType, targetID)
}

func (tx *Tx) Query(query string, params map[string]any) (QueryResult, error) {
	return tx.QueryContext(context.Background(), query, params, QueryOptions{})
}

func (tx *Tx) QueryContext(ctx context.Context, query string, params map[string]any, opts QueryOptions) (QueryResult, error) {
	if tx == nil || tx.closed {
		return QueryResult{}, ErrInactiveTx
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return QueryResult{}, err
	}
	if len(query) > maxQueryBytes {
		return QueryResult{}, fmt.Errorf("%w: query exceeds %d bytes", ErrResourceLimit, maxQueryBytes)
	}
	plan, err := tx.db.cachedQueryPlan(query)
	if err != nil {
		return QueryResult{}, &QueryError{Stage: QueryErrorStageParse, Err: err}
	}
	budget := newQueryBudget(ctx, opts)
	defer releaseQueryBudget(budget)
	executionTx := tx
	if plan.mutates() {
		if err := tx.ensureWritable(); err != nil {
			return QueryResult{}, &QueryError{Stage: QueryErrorStageExecution, Err: err}
		}
		fork := *tx
		fork.graph = store.CloneGraphStateShallow(tx.graph)
		fork.base = tx.graph
		fork.changes = &txChanges{baseCommitID: tx.changes.baseCommitID}
		fork.queryIndexesDisabled = tx.queryIndexesDisabled
		executionTx = &fork
	}
	result, err := plan.execute(executionTx, params, budget)
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		return QueryResult{}, &QueryError{Stage: QueryErrorStageExecution, Err: err}
	}
	if executionTx != tx {
		if err := mergeStatementChanges(ctx, tx, executionTx.graph, executionTx.changes); err != nil {
			return QueryResult{}, &QueryError{Stage: QueryErrorStageExecution, Err: err}
		}
		tx.graph = executionTx.graph
	}
	return result, nil
}

func hasGraphChanges(changes *txChanges) bool {
	return changes != nil && (len(changes.upsertNodes) != 0 || len(changes.deleteNodes) != 0 || len(changes.upsertEdges) != 0 || len(changes.deleteEdges) != 0 || len(changes.upsertFTS) != 0 || len(changes.deleteFTS) != 0)
}

func mergeStatementChanges(ctx context.Context, tx *Tx, final *store.GraphState, changes *txChanges) error {
	type existence struct {
		final    bool
		original bool
	}
	collect := func(upserts, deletes map[uint64]struct{}, readFinal, readOriginal func(uint64) (bool, error)) (map[uint64]existence, error) {
		states := make(map[uint64]existence, len(upserts)+len(deletes))
		for id := range upserts {
			states[id] = existence{}
		}
		for id := range deletes {
			states[id] = existence{}
		}
		for id := range states {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			finalExists, err := readFinal(id)
			if err != nil {
				return nil, err
			}
			originalExists, err := readOriginal(id)
			if err != nil {
				return nil, err
			}
			states[id] = existence{final: finalExists, original: originalExists}
		}
		return states, nil
	}
	readNode := func(graph *store.GraphState, id uint64) (bool, error) {
		if graph == nil {
			return false, nil
		}
		node, err := graph.ReadNode(id)
		return node != nil, err
	}
	readEdge := func(graph *store.GraphState, id uint64) (bool, error) {
		if graph == nil {
			return false, nil
		}
		edge, err := graph.ReadEdge(id)
		return edge != nil, err
	}
	readFTS := func(graph *store.GraphState, id uint64) (bool, error) {
		if graph == nil {
			return false, nil
		}
		record, err := graph.ReadFTS(id)
		return record != nil, err
	}

	// Read every affected base and final record before changing parent state.
	nodes, err := collect(changes.upsertNodes, changes.deleteNodes,
		func(id uint64) (bool, error) { return readNode(final, id) },
		func(id uint64) (bool, error) { return readNode(tx.base, id) })
	if err != nil {
		return err
	}
	edges, err := collect(changes.upsertEdges, changes.deleteEdges,
		func(id uint64) (bool, error) { return readEdge(final, id) },
		func(id uint64) (bool, error) { return readEdge(tx.base, id) })
	if err != nil {
		return err
	}
	fts, err := collect(changes.upsertFTS, changes.deleteFTS,
		func(id uint64) (bool, error) { return readFTS(final, id) },
		func(id uint64) (bool, error) { return readFTS(tx.base, id) })
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	merge := func(states map[uint64]existence, parentUpserts, parentDeletes *map[uint64]struct{}) {
		for id, state := range states {
			if state.final {
				tx.markUpsert(parentUpserts, parentDeletes, id)
			} else {
				tx.markDelete(parentUpserts, parentDeletes, state.original, id)
			}
		}
	}
	merge(nodes, &tx.changes.upsertNodes, &tx.changes.deleteNodes)
	merge(edges, &tx.changes.upsertEdges, &tx.changes.deleteEdges)
	merge(fts, &tx.changes.upsertFTS, &tx.changes.deleteFTS)
	nodeOriginal := make(map[uint64]bool, len(nodes))
	for id, state := range nodes {
		nodeOriginal[id] = state.original
	}
	edgeOriginal := make(map[uint64]bool, len(edges))
	for id, state := range edges {
		edgeOriginal[id] = state.original
	}
	tx.mergePropertyTracking(changes, changes.upsertNodes, true, nodeOriginal)
	tx.mergePropertyTracking(changes, changes.upsertEdges, false, edgeOriginal)
	return nil
}
