package engine

import (
	"fmt"
	"slices"
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
	if tx == nil || tx.closed {
		return QueryResult{}, ErrInactiveTx
	}
	if len(query) > maxQueryBytes {
		return QueryResult{}, fmt.Errorf("%w: query exceeds %d bytes", ErrResourceLimit, maxQueryBytes)
	}
	plan, err := tx.db.cachedQueryPlan(query)
	if err != nil {
		return QueryResult{}, &QueryError{Stage: QueryErrorStageParse, Err: err}
	}
	budget := newQueryBudget(nil, QueryOptions{})
	defer releaseQueryBudget(budget)
	result, err := plan.execute(tx, params, budget)
	if err != nil {
		return QueryResult{}, &QueryError{Stage: QueryErrorStageExecution, Err: err}
	}
	return result, nil
}
