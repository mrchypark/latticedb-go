package latticedb

import (
	"context"

	"github.com/mrchypark/latticedb-go/internal/engine"
)

type FTSIndexKind = engine.FTSIndexKind
type FTSIndexDefinition = engine.FTSIndexDefinition
type FTSIndexSearchResult = engine.FTSIndexSearchResult

const (
	FTSIndexNode = engine.FTSIndexNode
	FTSIndexEdge = engine.FTSIndexEdge
)

// FTSSearchIndex searches the named node-label or edge-type property index.
func (db *DB) FTSSearchIndex(name, query string, opts FTSSearchOptions) ([]FTSIndexSearchResult, error) {
	inner, err := db.requireOpen()
	if err != nil {
		return nil, wrapError(err)
	}
	results, err := inner.FTSSearchIndex(name, query, engine.FTSSearchOptions{Limit: opts.Limit, MaxDistance: opts.MaxDistance, MinTermLength: opts.MinTermLength, MaxWork: opts.MaxWork, MaxBytes: opts.MaxBytes, Scoring: engine.FTSScoring(opts.Scoring), Analyzer: engine.FTSAnalyzer(opts.Analyzer)})
	return results, wrapError(err)
}

// FTSSearchIndexContext searches the named index with context cancellation and budgets.
func (db *DB) FTSSearchIndexContext(ctx context.Context, name, query string, opts FTSSearchOptions) ([]FTSIndexSearchResult, error) {
	inner, err := db.requireOpen()
	if err != nil {
		return nil, wrapError(err)
	}
	results, err := inner.FTSSearchIndexContext(ctx, name, query, engine.FTSSearchOptions{Limit: opts.Limit, MaxDistance: opts.MaxDistance, MinTermLength: opts.MinTermLength, MaxWork: opts.MaxWork, MaxBytes: opts.MaxBytes, Scoring: engine.FTSScoring(opts.Scoring), Analyzer: engine.FTSAnalyzer(opts.Analyzer)})
	return results, wrapError(err)
}

// CreateFTSIndex stores a versioned, persistent FTS index definition.
func (db *DB) CreateFTSIndex(definition FTSIndexDefinition) error {
	inner, err := db.requireOpen()
	if err != nil {
		return wrapError(err)
	}
	return wrapError(inner.CreateFTSIndex(definition))
}

func (db *DB) CreateFTSIndexContext(ctx context.Context, definition FTSIndexDefinition) error {
	inner, err := db.requireOpen()
	if err != nil {
		return wrapError(err)
	}
	return wrapError(inner.CreateFTSIndexContext(ctx, definition))
}

func (db *DB) DropFTSIndex(name string) error {
	inner, err := db.requireOpen()
	if err != nil {
		return wrapError(err)
	}
	return wrapError(inner.DropFTSIndex(name))
}

func (tx *Tx) CreateFTSIndex(definition FTSIndexDefinition) error {
	if err := tx.requireInner(); err != nil {
		return err
	}
	return wrapError(tx.inner.CreateFTSIndex(definition))
}

func (tx *Tx) DropFTSIndex(name string) error {
	if err := tx.requireInner(); err != nil {
		return err
	}
	return wrapError(tx.inner.DropFTSIndex(name))
}
