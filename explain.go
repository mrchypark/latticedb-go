package latticedb

import (
	"context"

	"github.com/mrchypark/latticedb-go/internal/engine"
)

// QueryExplanation describes a read query's plan for one database snapshot.
type QueryExplanation = engine.QueryExplanation

// PlanOperator is an operator in a structured query plan.
type PlanOperator = engine.PlanOperator

// ExplainContext plans a read query in a new read transaction without executing it.
func (db *DB) ExplainContext(ctx context.Context, query string, params map[string]Value, opts QueryOptions) (QueryExplanation, error) {
	inner, err := db.requireOpen()
	if err != nil {
		return QueryExplanation{}, wrapError(err)
	}
	explanation, err := inner.ExplainContext(ctx, query, params, engineQueryOptions(opts))
	return explanation, wrapError(err)
}

// ExplainContext plans a read query using the active read transaction's snapshot.
func (tx *Tx) ExplainContext(ctx context.Context, query string, params map[string]Value, opts QueryOptions) (QueryExplanation, error) {
	if err := tx.requireInner(); err != nil {
		return QueryExplanation{}, err
	}
	explanation, err := tx.inner.ExplainContext(ctx, query, params, engineQueryOptions(opts))
	return explanation, wrapError(err)
}

func engineQueryOptions(opts QueryOptions) engine.QueryOptions {
	return engine.QueryOptions{
		MaxRows:           opts.MaxRows,
		MaxWork:           opts.MaxWork,
		MaxBytes:          opts.MaxBytes,
		VectorNamespace:   opts.VectorNamespace,
		ApproximateVector: opts.ApproximateVector,
		VectorEfSearch:    opts.VectorEfSearch,
	}
}
