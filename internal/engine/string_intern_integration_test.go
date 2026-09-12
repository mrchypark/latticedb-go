package engine

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestEngineIntern(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "intern.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		src, e := tx.CreateNode(CreateNodeOptions{
			Labels: []string{"A"}, Properties: map[string]any{"name": "x", "tags": []any{"a", "b"}},
		})
		if e != nil {
			return e
		}
		tgt, e := tx.CreateNode(CreateNodeOptions{
			Labels: []string{"B"}, Properties: map[string]any{"name": "y"},
		})
		if e != nil {
			return e
		}
		_, e = tx.CreateEdge(src.ID, tgt.ID, "REL", CreateEdgeOptions{
			Properties: map[string]any{"w": int64(9)},
		})
		return e
	}); err != nil {
		t.Fatal(err)
	}
	nodes, err := db.Query("MATCH (n:A) RETURN n.name AS name, n.tags AS tags", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes.Rows) != 1 || nodes.Rows[0]["name"] != "x" {
		t.Fatalf("node query: %v", nodes.Rows)
	}
	edges, err := db.Query("MATCH ()-[e:REL]->() RETURN e.w AS w", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(edges.Rows) != 1 || edges.Rows[0]["w"] != int64(9) {
		t.Fatalf("edge query: %v", edges.Rows)
	}
	data, err := db.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	db2, err := Deserialize(data, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	nodes2, err := db2.Query("MATCH (n:A) RETURN n.name AS name, n.tags AS tags", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(nodes.Rows, nodes2.Rows) {
		t.Fatalf("node roundtrip: %v vs %v", nodes.Rows, nodes2.Rows)
	}
	edges2, err := db2.Query("MATCH ()-[e:REL]->() RETURN e.w AS w", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(edges.Rows, edges2.Rows) {
		t.Fatalf("edge roundtrip: %v vs %v", edges.Rows, edges2.Rows)
	}
	b, err := db2.Query("MATCH (n:B) RETURN n.name AS name", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(b.Rows) != 1 || b.Rows[0]["name"] != "y" {
		t.Fatalf("label query: %v", b.Rows)
	}
}
