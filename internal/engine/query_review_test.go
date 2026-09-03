package engine

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"
)

func TestQueryBoundEndpointUsesAdjacency(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "query-adjacency.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var a, b, c uint64
	if err := db.Update(func(tx *Tx) error {
		var err error
		if node, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"name": "a"}}); err != nil {
			return err
		} else {
			a = node.ID
		}
		if node, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"name": "b"}}); err != nil {
			return err
		} else {
			b = node.ID
		}
		if node, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"name": "c"}}); err != nil {
			return err
		} else {
			c = node.ID
		}
		if _, err = tx.CreateEdge(a, b, "KEEP", CreateEdgeOptions{Properties: map[string]any{"active": true}}); err != nil {
			return err
		}
		if _, err = tx.CreateEdge(c, a, "KEEP", CreateEdgeOptions{Properties: map[string]any{"active": true}}); err != nil {
			return err
		}
		if _, err = tx.CreateEdge(a, a, "KEEP", CreateEdgeOptions{Properties: map[string]any{"active": true}}); err != nil {
			return err
		}
		for i := 0; i < 32; i++ {
			if _, err = tx.CreateEdge(b, c, "KEEP", CreateEdgeOptions{Properties: map[string]any{"active": false}}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	query := `MATCH (a), (a)-[:KEEP {active: true}]->(b) WHERE id(a) = $id RETURN b.name AS name`
	result, err := db.QueryContext(t.Context(), query, map[string]any{"id": int64(a)}, QueryOptions{MaxWork: 8})
	if err != nil || !reflect.DeepEqual(result.Rows, []map[string]any{{"name": "b"}, {"name": "a"}}) {
		t.Fatalf("source-bound traversal = %#v, %v", result.Rows, err)
	}

	query = `MATCH (b), (a)-[:KEEP {active: true}]->(b) WHERE id(b) = $id RETURN a.name AS name`
	result, err = db.QueryContext(t.Context(), query, map[string]any{"id": int64(b)}, QueryOptions{MaxWork: 7})
	if err != nil || !reflect.DeepEqual(result.Rows, []map[string]any{{"name": "a"}}) {
		t.Fatalf("target-bound traversal = %#v, %v", result.Rows, err)
	}

	query = `MATCH (a), (a)-[:KEEP {active: true}]-(b) WHERE id(a) = $id RETURN b.name AS name`
	result, err = db.QueryContext(t.Context(), query, map[string]any{"id": int64(a)}, QueryOptions{MaxWork: 12})
	if err != nil || !reflect.DeepEqual(result.Rows, []map[string]any{{"name": "b"}, {"name": "a"}, {"name": "c"}}) {
		t.Fatalf("undirected traversal = %#v, %v", result.Rows, err)
	}
}

func TestNormalizeQueryParamsReusesNormalizedValues(t *testing.T) {
	params, err := normalizeQueryParams(map[string]any{"items": []int{1, 2}})
	if err != nil {
		t.Fatal(err)
	}
	first, err := (paramExpr{Name: "items"}).eval(queryRow{}, params)
	if err != nil {
		t.Fatal(err)
	}
	second, err := (paramExpr{Name: "items"}).eval(queryRow{}, params)
	if err != nil {
		t.Fatal(err)
	}
	if reflect.ValueOf(first).Pointer() != reflect.ValueOf(second).Pointer() {
		t.Fatal("parameter evaluation copied a normalized value")
	}
}

func TestInlineLiteralPropertiesUsePropertyIndexes(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "query-inline-index.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		var nodes []Node
		for i := 0; i < 128; i++ {
			value := "other"
			if i == 127 {
				value = "wanted"
			}
			node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"key": value}})
			if err != nil {
				return err
			}
			nodes = append(nodes, node)
		}
		if _, err := tx.CreateEdge(nodes[0].ID, nodes[1].ID, "LINK", CreateEdgeOptions{Properties: map[string]any{"key": "wanted"}}); err != nil {
			return err
		}
		for i := 1; i < len(nodes); i++ {
			if _, err := tx.CreateEdge(nodes[0].ID, nodes[i].ID, "LINK", CreateEdgeOptions{Properties: map[string]any{"key": "other"}}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("Item", "key"); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateEdgePropertyIndex("LINK", "key"); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`MATCH (n:Item {key: "wanted"}) RETURN n.key AS key`,
		`MATCH (n:Item {key: $key}) RETURN n.key AS key`,
		`MATCH (n:Item) WHERE n.key = $key RETURN n.key AS key`,
	} {
		result, err := db.QueryContext(t.Context(), query, map[string]any{"key": "wanted"}, QueryOptions{MaxWork: 4})
		if err != nil || !reflect.DeepEqual(result.Rows, []map[string]any{{"key": "wanted"}}) {
			t.Fatalf("indexed node query %q = %#v, %v", query, result.Rows, err)
		}
	}
	for _, query := range []string{
		`MATCH ()-[r:LINK {key: "wanted"}]->() RETURN r.key AS key`,
		`MATCH ()-[r:LINK {key: $key}]->() RETURN r.key AS key`,
		`MATCH ()-[r:LINK]->() WHERE r.key = $key RETURN r.key AS key`,
	} {
		result, err := db.QueryContext(t.Context(), query, map[string]any{"key": "wanted"}, QueryOptions{MaxWork: 4})
		if err != nil || !reflect.DeepEqual(result.Rows, []map[string]any{{"key": "wanted"}}) {
			t.Fatalf("indexed edge query %q = %#v, %v", query, result.Rows, err)
		}
	}
	if err := db.DropNodePropertyIndex("Item", "key"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.QueryContext(t.Context(), `MATCH (n:Item {key: "wanted"}) RETURN n.key AS key`, nil, QueryOptions{MaxWork: 4}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("unindexed node query error = %v", err)
	}
	if err := db.DropEdgePropertyIndex("LINK", "key"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.QueryContext(t.Context(), `MATCH ()-[r:LINK {key: "wanted"}]->() RETURN r.key AS key`, nil, QueryOptions{MaxWork: 4}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("unindexed edge query error = %v", err)
	}
}
