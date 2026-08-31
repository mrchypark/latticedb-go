package latticedb

import (
	"context"

	"github.com/mrchypark/latticedb-go/internal/engine"
)

func (db *DB) BeginRead() (*Tx, error)  { return db.Begin(true) }
func (db *DB) BeginWrite() (*Tx, error) { return db.Begin(false) }

func (db *DB) GetNodesByLabel(label string) ([]NodeID, error) {
	if db == nil || db.inner == nil {
		return nil, wrapError(ErrDatabaseClosed)
	}
	ids, err := db.inner.GetNodesByLabel(label)
	return ids, wrapError(err)
}

func (db *DB) FTSSearchFuzzy(query string, opts FTSSearchOptions) ([]FTSSearchResult, error) {
	return db.FTSSearch(query, opts)
}

func (tx *Tx) IsReadOnly() bool { return tx != nil && tx.inner != nil && tx.inner.IsReadOnly() }
func (tx *Tx) IsActive() bool   { return tx != nil && tx.inner != nil && tx.inner.IsActive() }

func (tx *Tx) DeleteEdge(sourceID, targetID NodeID, edgeType string) error {
	if tx == nil || tx.inner == nil {
		return wrapError(ErrInactiveTx)
	}
	return wrapError(tx.inner.DeleteEdge(sourceID, targetID, edgeType))
}

func (tx *Tx) Query(query string, params map[string]Value) (QueryResult, error) {
	return tx.QueryContext(context.Background(), query, params, QueryOptions{})
}

func (tx *Tx) QueryContext(ctx context.Context, query string, params map[string]Value, opts QueryOptions) (QueryResult, error) {
	if tx == nil || tx.inner == nil {
		return QueryResult{}, ErrInactiveTx
	}
	result, err := tx.inner.QueryContext(ctx, query, params, engine.QueryOptions{MaxRows: opts.MaxRows, MaxWork: opts.MaxWork, MaxBytes: opts.MaxBytes})
	if err != nil {
		return QueryResult{}, wrapError(err)
	}
	return convertQueryResult(result), nil
}

func (tx *Tx) GetAppMetadata(key []byte) ([]byte, bool, error) {
	if tx == nil || tx.inner == nil {
		return nil, false, ErrInactiveTx
	}
	value, ok, err := tx.inner.GetAppMetadata(key)
	return value, ok, wrapError(err)
}

func (tx *Tx) PutAppMetadata(key, value []byte) error {
	if tx == nil || tx.inner == nil {
		return ErrInactiveTx
	}
	return wrapError(tx.inner.PutAppMetadata(key, value))
}

func (tx *Tx) DeleteAppMetadata(key []byte) error {
	if tx == nil || tx.inner == nil {
		return ErrInactiveTx
	}
	return wrapError(tx.inner.DeleteAppMetadata(key))
}
