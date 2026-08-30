package engine

import (
	"context"
	"fmt"
	"slices"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func (db *DB) GetNodesByLabel(label string) ([]uint64, error) {
	var ids []uint64
	err := db.View(func(tx *Tx) error {
		ids = slices.Clone(tx.graph.Labels.Get(label))
		return nil
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
	for _, edgeID := range tx.graph.Outgoing.Get(sourceID) {
		edge := tx.graph.Edges.Get(edgeID)
		if edge != nil && edge.TargetID == targetID && edge.Type == edgeType {
			tx.deleteEdge(edgeID)
			return nil
		}
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
		fork.changes = newTxChanges(tx.changes.baseCommitID)
		fork.queryIndexesDisabled = hasGraphChanges(tx.changes)
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
		tx.graph = executionTx.graph
		mergeStatementChanges(tx, executionTx.changes)
	}
	return result, nil
}

func hasGraphChanges(changes *txChanges) bool {
	return changes != nil && (len(changes.upsertNodes) != 0 || len(changes.deleteNodes) != 0 || len(changes.upsertEdges) != 0 || len(changes.deleteEdges) != 0 || len(changes.upsertFTS) != 0 || len(changes.deleteFTS) != 0)
}

func mergeStatementChanges(tx *Tx, changes *txChanges) {
	merge := func(upserts, deletes map[uint64]struct{}, parentUpserts, parentDeletes *map[uint64]struct{}, finalExists, originalExists func(uint64) bool) {
		apply := func(id uint64) {
			if finalExists(id) {
				tx.markUpsert(parentUpserts, parentDeletes, id)
			} else {
				tx.markDelete(parentUpserts, parentDeletes, originalExists(id), id)
			}
		}
		for id := range upserts {
			apply(id)
		}
		for id := range deletes {
			apply(id)
		}
	}
	merge(changes.upsertNodes, changes.deleteNodes, &tx.changes.upsertNodes, &tx.changes.deleteNodes,
		func(id uint64) bool { return tx.graph.Nodes.Get(id) != nil }, func(id uint64) bool { return tx.base.Nodes.Get(id) != nil })
	merge(changes.upsertEdges, changes.deleteEdges, &tx.changes.upsertEdges, &tx.changes.deleteEdges,
		func(id uint64) bool { return tx.graph.Edges.Get(id) != nil }, func(id uint64) bool { return tx.base.Edges.Get(id) != nil })
	merge(changes.upsertFTS, changes.deleteFTS, &tx.changes.upsertFTS, &tx.changes.deleteFTS,
		func(id uint64) bool { return tx.graph.FTS.Get(id) != nil }, func(id uint64) bool { return tx.base.FTS.Get(id) != nil })
}
