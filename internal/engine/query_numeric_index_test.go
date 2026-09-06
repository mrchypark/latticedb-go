package engine

import (
	"errors"
	"math"
	"path/filepath"
	"reflect"
	"testing"
)

func TestQueryNestedNumericEqualityMatchesIndexedAndUnindexedGraphs(t *testing.T) {
	query := `MATCH (n:Item) WHERE n.value = $value RETURN n.name AS name`
	indexedPath := filepath.Join(t.TempDir(), "indexed.ltdb")

	openDB := func(path string, indexed bool) *DB {
		db, err := Open(path, OpenOptions{Create: true})
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Update(func(tx *Tx) error {
			for _, properties := range []map[string]any{
				{"name": "integer", "value": map[string]any{"scores": []any{int64(1)}}},
				{"name": "float", "value": map[string]any{"scores": []any{float64(2)}}},
				{"name": "zero", "value": map[string]any{"scores": []any{math.Copysign(0, -1)}}},
				{"name": "other", "value": map[string]any{"scores": []any{int64(3)}}},
			} {
				if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: properties}); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if indexed {
			if err := db.CreateNodePropertyIndex("Item", "value"); err != nil {
				t.Fatal(err)
			}
		}
		return db
	}

	indexed := openDB(indexedPath, true)
	unindexed := openDB(filepath.Join(t.TempDir(), "unindexed.ltdb"), false)
	defer unindexed.Close()
	assertRows := func(db *DB, value any, want []map[string]any) {
		result, err := db.Query(query, map[string]any{"value": value})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(result.Rows, want) {
			t.Fatalf("rows = %#v, want %#v", result.Rows, want)
		}
	}
	for _, test := range []struct {
		value any
		name  string
	}{
		{map[string]any{"scores": []any{float64(1)}}, "integer"},
		{map[string]any{"scores": []any{int64(2)}}, "float"},
		{map[string]any{"scores": []any{float64(0)}}, "zero"},
	} {
		want := []map[string]any{{"name": test.name}}
		assertRows(indexed, test.value, want)
		assertRows(unindexed, test.value, want)
	}
	if err := indexed.Close(); err != nil {
		t.Fatal(err)
	}
	indexed, err := Open(indexedPath, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer indexed.Close()
	assertRows(indexed, map[string]any{"scores": []any{float64(1)}}, []map[string]any{{"name": "integer"}})
}

func TestQueryNestedNumericEqualityMatchesIndexedAndUnindexedEdges(t *testing.T) {
	query := `MATCH ()-[r:LINK]->() WHERE r.value = $value RETURN r.name AS name`

	openDB := func(path string, indexed bool) *DB {
		db, err := Open(path, OpenOptions{Create: true})
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Update(func(tx *Tx) error {
			left, err := tx.CreateNode(CreateNodeOptions{})
			if err != nil {
				return err
			}
			right, err := tx.CreateNode(CreateNodeOptions{})
			if err != nil {
				return err
			}
			for _, properties := range []map[string]any{
				{"name": "integer", "value": []any{map[string]any{"score": int64(1)}}},
				{"name": "float", "value": []any{map[string]any{"score": float64(2)}}},
				{"name": "zero", "value": []any{map[string]any{"score": math.Copysign(0, -1)}}},
				{"name": "other", "value": []any{map[string]any{"score": int64(3)}}},
			} {
				if _, err := tx.CreateEdge(left.ID, right.ID, "LINK", CreateEdgeOptions{Properties: properties}); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if indexed {
			if err := db.CreateEdgePropertyIndex("LINK", "value"); err != nil {
				t.Fatal(err)
			}
		}
		return db
	}

	indexed := openDB(filepath.Join(t.TempDir(), "indexed.ltdb"), true)
	defer indexed.Close()
	unindexed := openDB(filepath.Join(t.TempDir(), "unindexed.ltdb"), false)
	defer unindexed.Close()
	for _, test := range []struct {
		value any
		name  string
	}{
		{[]any{map[string]any{"score": float64(1)}}, "integer"},
		{[]any{map[string]any{"score": int64(2)}}, "float"},
		{[]any{map[string]any{"score": float64(0)}}, "zero"},
	} {
		want := []map[string]any{{"name": test.name}}
		for _, db := range []*DB{indexed, unindexed} {
			result, err := db.Query(query, map[string]any{"value": test.value})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(result.Rows, want) {
				t.Fatalf("rows = %#v, want %#v", result.Rows, want)
			}
		}
	}
}

func TestQueryStringOnlyCompositeUsesInlinePropertyIndex(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "query-composite-index.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 128; i++ {
			value := "other"
			if i == 127 {
				value = "wanted"
			}
			if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"value": map[string]any{"name": value}}}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("Item", "value"); err != nil {
		t.Fatal(err)
	}
	query := `MATCH (n:Item {value: $value}) RETURN n.value AS value`
	params := map[string]any{"value": map[string]any{"name": "wanted"}}
	result, err := db.QueryContext(t.Context(), query, params, QueryOptions{MaxWork: 4})
	if err != nil || !reflect.DeepEqual(result.Rows, []map[string]any{{"value": params["value"]}}) {
		t.Fatalf("indexed query = %#v, %v", result.Rows, err)
	}
	if err := db.DropNodePropertyIndex("Item", "value"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.QueryContext(t.Context(), query, params, QueryOptions{MaxWork: 4}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("unindexed query error = %v", err)
	}
}
