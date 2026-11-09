package latticedb

import (
	"context"

	"github.com/mrchypark/latticedb-go/internal/engine"
)

// SearchGraphOptions selects one search source, result bounds, and optional one-hop expansion.
type SearchGraphOptions = engine.SearchGraphOptions

// SearchGraphFTS selects a query for the database's manually indexed text.
type SearchGraphFTS = engine.SearchGraphFTS

// SearchGraphVector selects an L2 query. Approximate explicitly opts into HNSW.
type SearchGraphVector = engine.SearchGraphVector

// SearchGraphExpansion selects one outgoing edge type and an optional target label.
type SearchGraphExpansion = engine.SearchGraphExpansion

// SearchGraphRow preserves a native score or distance for one candidate or edge.
type SearchGraphRow = engine.SearchGraphRow

// SearchGraphResult contains a pinned snapshot, ranking details, and ordered rows.
type SearchGraphResult = engine.SearchGraphResult

// SearchGraphMetric identifies the metric used for the result's native values.
type SearchGraphMetric = engine.SearchGraphMetric

// SearchGraphRankDirection specifies whether higher or lower values rank first.
type SearchGraphRankDirection = engine.SearchGraphRankDirection

const (
	SearchGraphFrequency = engine.SearchGraphFrequency
	SearchGraphBM25      = engine.SearchGraphBM25
	SearchGraphL2        = engine.SearchGraphL2

	SearchGraphHigherFirst = engine.SearchGraphHigherFirst
	SearchGraphLowerFirst  = engine.SearchGraphLowerFirst
)

// SearchGraphContext searches and optionally expands candidates in one read snapshot.
func (db *DB) SearchGraphContext(ctx context.Context, opts SearchGraphOptions) (SearchGraphResult, error) {
	inner, err := db.requireOpen()
	if err != nil {
		return SearchGraphResult{}, wrapError(err)
	}
	result, err := inner.SearchGraphContext(ctx, opts)
	return result, wrapError(err)
}

// SearchGraphContext uses the active read transaction's snapshot for search and expansion.
func (tx *Tx) SearchGraphContext(ctx context.Context, opts SearchGraphOptions) (SearchGraphResult, error) {
	if err := tx.requireInner(); err != nil {
		return SearchGraphResult{}, err
	}
	result, err := tx.inner.SearchGraphContext(ctx, opts)
	return result, wrapError(err)
}
