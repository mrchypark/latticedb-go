package conformance

import latticedb "github.com/mrchypark/latticedb-go"

func init() {
	testDriver = driverAdapter{}
	testExporter = exporterAdapter{}
	testRecoveryHarness = recoveryAdapter{}
}

type driverAdapter struct{}

func (driverAdapter) Open(path string, opts OpenOptions) (Database, error) {
	db, err := latticedb.Open(path, latticedb.OpenOptions{
		Create:           opts.Create,
		ReadOnly:         opts.ReadOnly,
		CacheSizeMB:      opts.CacheSizeMB,
		PageSize:         opts.PageSize,
		EnableVector:     opts.EnableVector,
		VectorDimensions: opts.VectorDimensions,
	})
	if err != nil {
		return nil, err
	}
	return &databaseAdapter{db: db}, nil
}

type databaseAdapter struct {
	db *latticedb.DB
}

func (adapter *databaseAdapter) Close() error {
	return adapter.db.Close()
}

func (adapter *databaseAdapter) Begin(readOnly bool) (Tx, error) {
	tx, err := adapter.db.Begin(readOnly)
	if err != nil {
		return nil, err
	}
	return &txAdapter{tx: tx}, nil
}

func (adapter *databaseAdapter) View(fn func(Tx) error) error {
	return adapter.db.View(func(tx *latticedb.Tx) error {
		return fn(&txAdapter{tx: tx})
	})
}

func (adapter *databaseAdapter) Update(fn func(Tx) error) error {
	return adapter.db.Update(func(tx *latticedb.Tx) error {
		return fn(&txAdapter{tx: tx})
	})
}

func (adapter *databaseAdapter) Query(cypher string, params map[string]Value) (QueryResult, error) {
	result, err := adapter.db.Query(cypher, convertValueMap(params))
	if err != nil {
		return QueryResult{}, err
	}
	return convertQueryResult(result), nil
}

func (adapter *databaseAdapter) VectorSearch(vector []float32, opts VectorSearchOptions) ([]VectorSearchResult, error) {
	results, err := adapter.db.VectorSearch(vector, latticedb.VectorSearchOptions{
		K:        opts.K,
		EfSearch: opts.EfSearch,
	})
	if err != nil {
		return nil, err
	}
	out := make([]VectorSearchResult, len(results))
	for i, result := range results {
		out[i] = VectorSearchResult{
			NodeID:   result.NodeID,
			Distance: result.Distance,
		}
	}
	return out, nil
}

func (adapter *databaseAdapter) FTSSearch(query string, opts FTSSearchOptions) ([]FTSSearchResult, error) {
	results, err := adapter.db.FTSSearch(query, latticedb.FTSSearchOptions{
		Limit:         opts.Limit,
		MaxDistance:   opts.MaxDistance,
		MinTermLength: opts.MinTermLength,
	})
	if err != nil {
		return nil, err
	}
	out := make([]FTSSearchResult, len(results))
	for i, result := range results {
		out[i] = FTSSearchResult{
			NodeID: result.NodeID,
			Score:  result.Score,
		}
	}
	return out, nil
}

func (adapter *databaseAdapter) CacheClear() error {
	return adapter.db.CacheClear()
}

func (adapter *databaseAdapter) CacheStats() (QueryCacheStats, error) {
	stats, err := adapter.db.CacheStats()
	if err != nil {
		return QueryCacheStats{}, err
	}
	return QueryCacheStats{
		Entries: stats.Entries,
		Hits:    stats.Hits,
		Misses:  stats.Misses,
	}, nil
}

type txAdapter struct {
	tx *latticedb.Tx
}

func (adapter *txAdapter) Commit() error {
	return adapter.tx.Commit()
}

func (adapter *txAdapter) Rollback() error {
	return adapter.tx.Rollback()
}

func (adapter *txAdapter) CreateNode(opts CreateNodeOptions) (Node, error) {
	node, err := adapter.tx.CreateNode(latticedb.CreateNodeOptions{
		Labels:     append([]string(nil), opts.Labels...),
		Properties: convertValueMap(opts.Properties),
	})
	if err != nil {
		return Node{}, err
	}
	return convertNode(node), nil
}

func (adapter *txAdapter) DeleteNode(nodeID uint64) error {
	return adapter.tx.DeleteNode(nodeID)
}

func (adapter *txAdapter) NodeExists(nodeID uint64) (bool, error) {
	return adapter.tx.NodeExists(nodeID)
}

func (adapter *txAdapter) GetNode(nodeID uint64) (*Node, error) {
	node, err := adapter.tx.GetNode(nodeID)
	if err != nil || node == nil {
		return nil, err
	}
	converted := convertNode(*node)
	return &converted, nil
}

func (adapter *txAdapter) SetProperty(nodeID uint64, key string, value Value) error {
	return adapter.tx.SetProperty(nodeID, key, value)
}

func (adapter *txAdapter) GetProperty(nodeID uint64, key string) (Value, bool, error) {
	return adapter.tx.GetProperty(nodeID, key)
}

func (adapter *txAdapter) SetVector(nodeID uint64, key string, vector []float32) error {
	return adapter.tx.SetVector(nodeID, key, vector)
}

func (adapter *txAdapter) FTSIndex(nodeID uint64, text string) error {
	return adapter.tx.FTSIndex(nodeID, text)
}

func (adapter *txAdapter) CreateEdge(sourceID, targetID uint64, edgeType string, opts CreateEdgeOptions) (Edge, error) {
	edge, err := adapter.tx.CreateEdge(sourceID, targetID, edgeType, latticedb.CreateEdgeOptions{
		Properties: convertValueMap(opts.Properties),
	})
	if err != nil {
		return Edge{}, err
	}
	return convertEdge(edge), nil
}

func (adapter *txAdapter) GetEdgeProperty(edgeID uint64, key string) (Value, bool, error) {
	return adapter.tx.GetEdgeProperty(edgeID, key)
}

func (adapter *txAdapter) SetEdgeProperty(edgeID uint64, key string, value Value) error {
	return adapter.tx.SetEdgeProperty(edgeID, key, value)
}

func (adapter *txAdapter) RemoveEdgeProperty(edgeID uint64, key string) error {
	return adapter.tx.RemoveEdgeProperty(edgeID, key)
}

func (adapter *txAdapter) GetOutgoingEdges(nodeID uint64) ([]Edge, error) {
	edges, err := adapter.tx.GetOutgoingEdges(nodeID)
	if err != nil {
		return nil, err
	}
	out := make([]Edge, len(edges))
	for i, edge := range edges {
		out[i] = convertEdge(edge)
	}
	return out, nil
}

type exporterAdapter struct{}

func (exporterAdapter) Export(dbPath string, format ExportFormat, outputPath string) ([]byte, error) {
	return latticedb.Export(dbPath, latticedb.ExportFormat(format), outputPath)
}

func (exporterAdapter) Dump(dbPath string) ([]byte, error) {
	return latticedb.Dump(dbPath)
}

type recoveryAdapter struct{}

func (recoveryAdapter) SimulateCrash(dbPath string) error {
	return latticedb.SimulateCrash(dbPath)
}

func convertValueMap(in map[string]Value) map[string]latticedb.Value {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]latticedb.Value, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func convertNode(node latticedb.Node) Node {
	return Node{
		ID:         node.ID,
		Labels:     append([]string(nil), node.Labels...),
		Properties: convertBackValueMap(node.Properties),
	}
}

func convertEdge(edge latticedb.Edge) Edge {
	return Edge{
		ID:         edge.ID,
		SourceID:   edge.SourceID,
		TargetID:   edge.TargetID,
		Type:       edge.Type,
		Properties: convertBackValueMap(edge.Properties),
	}
}

func convertQueryResult(result latticedb.QueryResult) QueryResult {
	rows := make([]map[string]Value, len(result.Rows))
	for i, row := range result.Rows {
		rows[i] = convertBackValueMap(row)
	}
	return QueryResult{
		Columns: append([]string(nil), result.Columns...),
		Rows:    rows,
	}
}

func convertBackValueMap(in map[string]latticedb.Value) map[string]Value {
	if len(in) == 0 {
		return map[string]Value{}
	}
	out := make(map[string]Value, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
