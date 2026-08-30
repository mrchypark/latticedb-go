package latticedb

func (db *DB) BeginRead() (*Tx, error)  { return db.Begin(true) }
func (db *DB) BeginWrite() (*Tx, error) { return db.Begin(false) }

func (db *DB) GetNodesByLabel(label string) ([]NodeID, error) {
	return db.inner.GetNodesByLabel(label)
}

func (db *DB) FTSSearchFuzzy(query string, opts FTSSearchOptions) ([]FTSSearchResult, error) {
	return db.FTSSearch(query, opts)
}

func (tx *Tx) IsReadOnly() bool { return tx != nil && tx.inner != nil && tx.inner.IsReadOnly() }
func (tx *Tx) IsActive() bool   { return tx != nil && tx.inner != nil && tx.inner.IsActive() }

func (tx *Tx) DeleteEdge(sourceID, targetID NodeID, edgeType string) error {
	return tx.inner.DeleteEdge(sourceID, targetID, edgeType)
}

func (tx *Tx) Query(query string, params map[string]Value) (QueryResult, error) {
	result, err := tx.inner.Query(query, params)
	if err != nil {
		return QueryResult{}, err
	}
	return convertQueryResult(result), nil
}
