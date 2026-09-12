package engine

import (
	"reflect"
	"testing"
)

func TestCompactPropertyMutationPreservesReaderSnapshot(t *testing.T) {
	db, node, _, edge := openPropertyMutationDB(t)
	defer db.Close()
	reader, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Rollback()
	beforeNode := reader.graph.Nodes.Get(node.ID).Properties.CloneMap()
	beforeEdge := reader.graph.Edges.Get(edge.ID).Properties.CloneMap()
	if err := db.Update(func(tx *Tx) error {
		if err := tx.SetProperty(node.ID, "old", "changed"); err != nil {
			return err
		}
		if err := tx.SetProperty(node.ID, "new", nil); err != nil {
			return err
		}
		if _, err := tx.Query("MATCH (n) WHERE id(n) = $id REMOVE n.old", map[string]any{"id": int64(node.ID)}); err != nil {
			return err
		}
		if err := tx.SetEdgeProperty(edge.ID, "new", int64(1)); err != nil {
			return err
		}
		return tx.RemoveEdgeProperty(edge.ID, "old")
	}); err != nil {
		t.Fatal(err)
	}
	if got := reader.graph.Nodes.Get(node.ID).Properties.CloneMap(); !reflect.DeepEqual(got, beforeNode) {
		t.Fatalf("reader node changed: %#v != %#v", got, beforeNode)
	}
	if got := reader.graph.Edges.Get(edge.ID).Properties.CloneMap(); !reflect.DeepEqual(got, beforeEdge) {
		t.Fatalf("reader edge changed: %#v != %#v", got, beforeEdge)
	}
	if err := db.View(func(tx *Tx) error {
		if v, ok := tx.graph.Nodes.Get(node.ID).Properties.Lookup("new"); !ok || v != nil {
			t.Fatalf("new null property lost: %v %v", v, ok)
		}
		if _, ok := tx.graph.Edges.Get(edge.ID).Properties.Lookup("old"); ok {
			t.Fatal("edge removal lost")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
