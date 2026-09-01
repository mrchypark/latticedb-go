package engine

import (
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
	result, err := db.QueryContext(t.Context(), query, map[string]any{"id": int64(a)}, QueryOptions{MaxWork: 5})
	if err != nil || !reflect.DeepEqual(result.Rows, []map[string]any{{"name": "b"}, {"name": "a"}}) {
		t.Fatalf("source-bound traversal = %#v, %v", result.Rows, err)
	}

	query = `MATCH (b), (a)-[:KEEP {active: true}]->(b) WHERE id(b) = $id RETURN a.name AS name`
	result, err = db.QueryContext(t.Context(), query, map[string]any{"id": int64(b)}, QueryOptions{MaxWork: 4})
	if err != nil || !reflect.DeepEqual(result.Rows, []map[string]any{{"name": "a"}}) {
		t.Fatalf("target-bound traversal = %#v, %v", result.Rows, err)
	}

	query = `MATCH (a), (a)-[:KEEP {active: true}]-(b) WHERE id(a) = $id RETURN b.name AS name`
	result, err = db.QueryContext(t.Context(), query, map[string]any{"id": int64(a)}, QueryOptions{MaxWork: 9})
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
