package latticedb_test

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	latticedb "github.com/mrchypark/latticedb-go"
)

func TestPublicConfiguredFTSPropertyPreservesQueryResults(t *testing.T) {
	query := "MATCH (n:Document) WHERE n.body @@ $term RETURN id(n) AS id LIMIT 10"
	params := map[string]latticedb.Value{"term": "needle"}

	open := func(t *testing.T, name string, indexed bool) *latticedb.DB {
		t.Helper()
		opts := latticedb.OpenOptions{Create: true}
		if indexed {
			opts.FTSProperties = []string{"body"}
		}
		db, err := latticedb.Open(filepath.Join(t.TempDir(), name), opts)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		if err := db.Update(func(tx *latticedb.Tx) error {
			for _, body := range []string{"needle first", "ordinary text", "needle second"} {
				if _, err := tx.CreateNode(latticedb.CreateNodeOptions{
					Labels:     []string{"Document"},
					Properties: map[string]latticedb.Value{"body": body},
				}); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return db
	}

	scan, indexed := open(t, "scan", false), open(t, "indexed", true)
	scanResult, err := scan.QueryContext(context.Background(), query, params, latticedb.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	indexedResult, err := indexed.QueryContext(context.Background(), query, params, latticedb.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(scanResult.Rows, indexedResult.Rows) {
		t.Fatalf("configured FTS rows = %#v, scan rows = %#v", indexedResult.Rows, scanResult.Rows)
	}
	data, err := indexed.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := latticedb.Deserialize(data, latticedb.OpenOptions{FTSProperties: []string{"body"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restored.Close() })
	restoredResult, err := restored.QueryContext(context.Background(), query, params, latticedb.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(indexedResult.Rows, restoredResult.Rows) {
		t.Fatalf("deserialized configured FTS rows = %#v, indexed rows = %#v", restoredResult.Rows, indexedResult.Rows)
	}
}

func TestPublicQueryANNMatchesDirectNamespaceSearchAndExactDefault(t *testing.T) {
	namespace := latticedb.VectorNamespace{Property: "embedding", Scope: "Article", Dimensions: 2, Metric: latticedb.VectorMetricL2}
	db, err := latticedb.Open(t.TempDir(), latticedb.OpenOptions{
		Create:           true,
		EnableVector:     true,
		VectorDimensions: 2,
		VectorIndexMode:  latticedb.VectorIndexHNSWSynchronous,
		VectorNamespaces: []latticedb.VectorNamespace{namespace},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *latticedb.Tx) error {
		for _, vector := range [][]float32{{1, 0}, {0.8, 0}, {0, 1}, {-1, 0}} {
			if _, err := tx.CreateNode(latticedb.CreateNodeOptions{
				Labels:     []string{"Article"},
				Properties: map[string]latticedb.Value{"embedding": vector},
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	vector := []float32{1, 0}
	direct, err := db.VectorSearch(vector, latticedb.VectorSearchOptions{K: 2, EfSearch: 64, Namespace: &namespace})
	if err != nil {
		t.Fatal(err)
	}
	query := "MATCH (n:Article) WHERE n.embedding <=> $vector RETURN id(n) AS id LIMIT 2"
	params := map[string]latticedb.Value{"vector": vector}
	exact, err := db.QueryContext(context.Background(), query, params, latticedb.QueryOptions{VectorNamespace: &namespace})
	if err != nil {
		t.Fatal(err)
	}
	defaultIDs := queryIDs(t, exact.Rows)
	if len(direct) != 2 {
		t.Fatalf("direct namespace search returned %d results, want 2: %#v", len(direct), direct)
	}
	if !reflect.DeepEqual(defaultIDs, []int64{int64(direct[0].NodeID), int64(direct[1].NodeID)}) {
		t.Fatalf("exact query IDs = %v, direct IDs = %v", defaultIDs, direct)
	}
	exactOff, err := db.QueryContext(context.Background(), query, params, latticedb.QueryOptions{
		VectorNamespace:   &namespace,
		ApproximateVector: false,
		VectorEfSearch:    64,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := queryIDs(t, exactOff.Rows); !reflect.DeepEqual(got, defaultIDs) {
		t.Fatalf("explicit exact query IDs = %v, default IDs = %v", got, defaultIDs)
	}
	approx, err := db.QueryContext(context.Background(), query, params, latticedb.QueryOptions{
		VectorNamespace:   &namespace,
		ApproximateVector: true,
		VectorEfSearch:    64,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := queryIDs(t, approx.Rows); !reflect.DeepEqual(got, defaultIDs) {
		t.Fatalf("approximate query IDs = %v, exact IDs = %v", got, defaultIDs)
	}
	var txApprox latticedb.QueryResult
	if err := db.View(func(tx *latticedb.Tx) error {
		var err error
		txApprox, err = tx.QueryContext(context.Background(), query, params, latticedb.QueryOptions{
			VectorNamespace:   &namespace,
			ApproximateVector: true,
			VectorEfSearch:    64,
		})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if got := queryIDs(t, txApprox.Rows); !reflect.DeepEqual(got, queryIDs(t, approx.Rows)) {
		t.Fatalf("transaction approximate query IDs = %v, DB IDs = %v", got, queryIDs(t, approx.Rows))
	}
}

func queryIDs(t *testing.T, rows []map[string]latticedb.Value) []int64 {
	t.Helper()
	ids := make([]int64, len(rows))
	for index, row := range rows {
		id, ok := row["id"].(int64)
		if !ok {
			t.Fatalf("row %d id = %#v (%T), want int64", index, row["id"], row["id"])
		}
		ids[index] = id
	}
	return ids
}
