package latticedb_test

import (
	"context"
	"errors"
	"testing"

	latticedb "github.com/mrchypark/latticedb-go"
)

func TestPublicVectorNamespaceSearchQueryAndDeserialize(t *testing.T) {
	namespace := latticedb.VectorNamespace{Property: "embedding", Scope: "Article", Dimensions: 2, Metric: latticedb.VectorMetricL2}
	db, err := latticedb.Open(t.TempDir(), latticedb.OpenOptions{
		Create:           true,
		EnableVector:     true,
		VectorDimensions: 2,
		VectorNamespaces: []latticedb.VectorNamespace{namespace},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var articleID, otherID uint64
	if err := db.Update(func(tx *latticedb.Tx) error {
		article, err := tx.CreateNode(latticedb.CreateNodeOptions{Labels: []string{"Article"}, Properties: map[string]any{"embedding": []float32{1, 0}, "kind": "match"}})
		if err != nil {
			return err
		}
		articleID = article.ID
		other, err := tx.CreateNode(latticedb.CreateNodeOptions{Labels: []string{"Other"}, Properties: map[string]any{"embedding": []float32{1, 0}, "kind": "match"}})
		if err != nil {
			return err
		}
		otherID = other.ID
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	results, err := db.VectorSearch([]float32{1, 0}, latticedb.VectorSearchOptions{Exact: true, K: 10, Namespace: &namespace})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].NodeID != articleID || results[0].NodeID == otherID {
		t.Fatalf("namespace search = %#v, want article %d only", results, articleID)
	}
	query, err := db.QueryContext(context.Background(), "MATCH (n) WHERE n.embedding <=> $q RETURN id(n) AS id", map[string]latticedb.Value{"q": []float32{1, 0}}, latticedb.QueryOptions{VectorNamespace: &namespace})
	if err != nil {
		t.Fatal(err)
	}
	if len(query.Rows) != 1 || query.Rows[0]["id"] != int64(articleID) {
		t.Fatalf("namespace query = %#v, want article %d only", query.Rows, articleID)
	}
	query, err = db.QueryContext(context.Background(), "MATCH (n) WHERE n.kind = $kind AND n.embedding <=> $q RETURN id(n) AS id", map[string]latticedb.Value{"kind": "match", "q": []float32{1, 0}}, latticedb.QueryOptions{VectorNamespace: &namespace})
	if err != nil {
		t.Fatal(err)
	}
	if len(query.Rows) != 1 || query.Rows[0]["id"] != int64(articleID) {
		t.Fatalf("compound namespace query = %#v, want article %d only", query.Rows, articleID)
	}

	_, err = db.QueryContext(context.Background(), "MATCH (n:NoRows) WHERE n.other <=> $q RETURN id(n) AS id", map[string]latticedb.Value{"q": []float32{1, 0}}, latticedb.QueryOptions{VectorNamespace: &namespace})
	if !errors.Is(err, latticedb.ErrInvalidArgument) {
		t.Fatalf("wrong vector property on empty result error = %v, want ErrInvalidArgument", err)
	}
	_, err = db.QueryContext(context.Background(), "MATCH (a)-[e:LINK]->(b) WHERE e.embedding <=> $q RETURN id(e) AS id", map[string]latticedb.Value{"q": []float32{1, 0}}, latticedb.QueryOptions{VectorNamespace: &namespace})
	if !errors.Is(err, latticedb.ErrInvalidArgument) {
		t.Fatalf("edge vector binding on empty result error = %v, want ErrInvalidArgument", err)
	}

	data, err := db.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := latticedb.Deserialize(data, latticedb.OpenOptions{EnableVector: true, VectorDimensions: 2, VectorNamespaces: []latticedb.VectorNamespace{namespace}})
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	results, err = restored.VectorSearch([]float32{1, 0}, latticedb.VectorSearchOptions{Exact: true, K: 10, Namespace: &namespace})
	if err != nil || len(results) != 1 || results[0].NodeID != articleID {
		t.Fatalf("deserialized namespace search = %#v, error = %v", results, err)
	}
}

func TestPublicVectorNamespaceOpenValidation(t *testing.T) {
	base := latticedb.OpenOptions{Create: true, EnableVector: true, VectorDimensions: 2}
	for _, test := range []struct {
		name      string
		namespace latticedb.VectorNamespace
		want      error
	}{
		{name: "property", namespace: latticedb.VectorNamespace{Dimensions: 2}, want: latticedb.ErrInvalidArgument},
		{name: "scope", namespace: latticedb.VectorNamespace{Property: "embedding", Scope: string([]byte{0xff}), Dimensions: 2}},
		{name: "metric", namespace: latticedb.VectorNamespace{Property: "embedding", Dimensions: 2, Metric: latticedb.VectorMetric(1)}, want: latticedb.ErrUnsupportedOption},
		{name: "dimensions", namespace: latticedb.VectorNamespace{Property: "embedding", Dimensions: 3}},
		{name: "normalized duplicate", namespace: latticedb.VectorNamespace{Property: "embedding", Dimensions: 0}, want: latticedb.ErrInvalidArgument},
	} {
		t.Run(test.name, func(t *testing.T) {
			opts := base
			opts.VectorNamespaces = []latticedb.VectorNamespace{test.namespace}
			if test.name == "normalized duplicate" {
				opts.VectorNamespaces = append(opts.VectorNamespaces, latticedb.VectorNamespace{Property: "embedding", Dimensions: 2})
			}
			_, err := latticedb.Open(t.TempDir(), opts)
			if err == nil {
				t.Fatal("Open accepted invalid namespace configuration")
			}
			if test.want != nil && !errors.Is(err, test.want) {
				t.Fatalf("Open error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestPublicVectorNamespacePreservesSingleVectorPropertyRule(t *testing.T) {
	namespace := latticedb.VectorNamespace{Property: "embedding", Dimensions: 2, Metric: latticedb.VectorMetricL2}
	db, err := latticedb.Open(t.TempDir(), latticedb.OpenOptions{Create: true, EnableVector: true, VectorDimensions: 2, VectorNamespaces: []latticedb.VectorNamespace{namespace}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.Update(func(tx *latticedb.Tx) error {
		_, err := tx.CreateNode(latticedb.CreateNodeOptions{Properties: map[string]any{
			"embedding": []float32{1, 0},
			"other":     []float32{0, 1},
		}})
		return err
	})
	if err == nil {
		t.Fatal("configured namespace accepted two vector properties on one node")
	}
}

func TestPublicVectorNamespaceReopenInfersExistingVectorSupport(t *testing.T) {
	path := t.TempDir()
	namespace := latticedb.VectorNamespace{Property: "embedding", Scope: "Article", Dimensions: 2, Metric: latticedb.VectorMetricL2}
	db, err := latticedb.Open(path, latticedb.OpenOptions{Create: true, EnableVector: true, VectorDimensions: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *latticedb.Tx) error {
		_, err := tx.CreateNode(latticedb.CreateNodeOptions{Labels: []string{"Article"}, Properties: map[string]any{"embedding": []float32{1, 0}}})
		return err
	}); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := latticedb.Open(path, latticedb.OpenOptions{VectorNamespaces: []latticedb.VectorNamespace{namespace}})
	if err != nil {
		t.Fatalf("reopen with namespaces and EnableVector=false: %v", err)
	}
	defer reopened.Close()
	results, err := reopened.VectorSearch([]float32{1, 0}, latticedb.VectorSearchOptions{Exact: true, K: 1, Namespace: &namespace})
	if err != nil || len(results) != 1 {
		t.Fatalf("reopened namespace search = %#v, error = %v", results, err)
	}
}
