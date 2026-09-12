package engine

import (
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
)

func TestPropertyMutationTrackingDirectQueryAndRollback(t *testing.T) {
	db, node, other, edge := openPropertyMutationDB(t)
	defer db.Close()

	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	if err := tx.SetProperty(node.ID, "direct", int64(1)); err != nil {
		t.Fatal(err)
	}
	if err := tx.SetEdgeProperty(edge.ID, "edgeDirect", int64(2)); err != nil {
		t.Fatal(err)
	}
	if err := tx.RemoveEdgeProperty(edge.ID, "old"); err != nil {
		t.Fatal(err)
	}
	if err := tx.SetProperty(node.ID, "revert", "after"); err != nil {
		t.Fatal(err)
	}
	if err := tx.SetProperty(node.ID, "revert", "before"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Query(`MATCH (n) WHERE id(n) = $id SET n.query = 3`, map[string]any{"id": int64(node.ID)}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Query(`MATCH (n) WHERE id(n) = $id SET n += $props`, map[string]any{
		"id":    int64(node.ID),
		"props": map[string]any{"merge": map[string]any{"nested": "value"}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Query(`MATCH (n) WHERE id(n) = $id REMOVE n.missing`, map[string]any{"id": int64(node.ID)}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Query(`MATCH ()-[e:LINK]->() WHERE id(e) = $id SET e.query = 4`, map[string]any{"id": int64(edge.ID)}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Query(`MATCH ()-[e:LINK]->() WHERE id(e) = $id REMOVE e.queryMissing`, map[string]any{"id": int64(edge.ID)}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Query(`MATCH (n) SET n.uncommitted = 1`, nil); err != nil {
		t.Fatal(err)
	}
	newNode, err := tx.CreateNode(CreateNodeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.SetProperty(newNode.ID, "new", int64(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Query(`MATCH (n) WHERE id(n) = $id SET n.queryNew = 1`, map[string]any{"id": int64(newNode.ID)}); err != nil {
		t.Fatal(err)
	}
	if err := tx.SetProperty(999999, "missing", int64(1)); err == nil {
		t.Fatal("missing node mutation succeeded")
	}

	wantNodes := map[uint64]map[string]struct{}{
		node.ID: {
			"direct": {}, "merge": {}, "missing": {}, "query": {}, "revert": {}, "uncommitted": {},
		},
		other.ID: {"uncommitted": {}},
	}
	wantEdges := map[uint64]map[string]struct{}{edge.ID: {"edgeDirect": {}, "old": {}, "query": {}, "queryMissing": {}}}
	if got := tx.changes.nodePropertyKeys; !reflect.DeepEqual(got, wantNodes) {
		t.Fatalf("node property keys = %#v, want %#v", got, wantNodes)
	}
	if got := tx.changes.edgePropertyKeys; !reflect.DeepEqual(got, wantEdges) {
		t.Fatalf("edge property keys = %#v, want %#v", got, wantEdges)
	}
	if _, exists := tx.changes.nodePropertyKeys[newNode.ID]; exists {
		t.Fatalf("new node %d was tracked as a property patch", newNode.ID)
	}
	if got := propertyKeyDeltas(tx.changes.upsertNodes, tx.changes.nodePropertyKeys)[node.ID]; !reflect.DeepEqual(got, []string{"direct", "merge", "missing", "query", "revert", "uncommitted"}) {
		t.Fatalf("sorted node property keys = %v", got)
	}
	_ = other
}

func TestPropertyMutationTrackingFullFallbackPersists(t *testing.T) {
	db, node, _, _ := openPropertyMutationDB(t)
	defer db.Close()

	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.SetProperty(node.ID, "before", int64(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Query(`MATCH (n) WHERE id(n) = $id SET n = {replacement: 1}`, map[string]any{"id": int64(node.ID)}); err != nil {
		t.Fatal(err)
	}
	if err := tx.SetProperty(node.ID, "after", int64(2)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Query(`MATCH (n) WHERE id(n) = $id SET n:Changed`, map[string]any{"id": int64(node.ID)}); err != nil {
		t.Fatal(err)
	}
	if err := tx.SetProperty(node.ID, "last", int64(3)); err != nil {
		t.Fatal(err)
	}
	if keys, exists := tx.changes.nodePropertyKeys[node.ID]; !exists || keys != nil {
		t.Fatalf("node fallback = %#v, exists=%v; want nil sentinel", keys, exists)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	if err := tx.SetProperty(node.ID, "kept", int64(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Query(`MATCH (n) SET n.failed = 1, n.bad = missing.name`, nil); err == nil {
		t.Fatal("failed query succeeded")
	}
	if got := tx.changes.nodePropertyKeys[node.ID]; !reflect.DeepEqual(got, map[string]struct{}{"kept": {}}) {
		t.Fatalf("failed query changed parent tracker = %#v", got)
	}
	if _, err := tx.Query(`MATCH (n) WHERE id(n) = $id SET n.afterFailure = 1`, map[string]any{"id": int64(node.ID)}); err != nil {
		t.Fatal(err)
	}
	if got := tx.changes.nodePropertyKeys[node.ID]; !reflect.DeepEqual(got, map[string]struct{}{"afterFailure": {}, "kept": {}}) {
		t.Fatalf("post-failure tracker = %#v", got)
	}
}

func TestPropertyMutationInputAndOutputNestedValuesAreIsolated(t *testing.T) {
	db, node, _, _ := openPropertyMutationDB(t)
	defer db.Close()

	input := map[string]any{"nested": map[string]any{"value": int64(1)}}
	if err := db.Update(func(tx *Tx) error { return tx.SetProperty(node.ID, "input", input) }); err != nil {
		t.Fatal(err)
	}
	input["nested"].(map[string]any)["value"] = int64(9)

	got := readPropertyMutationNode(t, db, node.ID)
	got.Properties["input"].(map[string]any)["nested"].(map[string]any)["value"] = int64(8)
	again := readPropertyMutationNode(t, db, node.ID)
	if value := again.Properties["input"].(map[string]any)["nested"].(map[string]any)["value"]; value != int64(1) {
		t.Fatalf("nested value = %v, want 1", value)
	}
}

func TestSetVectorTracksPropertyKey(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "vector"), OpenOptions{Create: true, EnableVector: true, VectorDimensions: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var node Node
	if err := db.Update(func(tx *Tx) error {
		var err error
		node, err = tx.CreateNode(CreateNodeOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := tx.SetVector(node.ID, "embedding", []float32{1, 2}); err != nil {
		t.Fatal(err)
	}
	if got := tx.changes.nodePropertyKeys[node.ID]; !reflect.DeepEqual(got, map[string]struct{}{"embedding": {}}) {
		t.Fatalf("vector property keys = %#v", got)
	}
}

func openPropertyMutationDB(t *testing.T) (*DB, Node, Node, Edge) {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "property-mutation"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	var node, other Node
	var edge Edge
	if err := db.Update(func(tx *Tx) error {
		node, err = tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"old": int64(1)}})
		if err != nil {
			return err
		}
		other, err = tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}})
		if err != nil {
			return err
		}
		edge, err = tx.CreateEdge(node.ID, other.ID, "LINK", CreateEdgeOptions{Properties: map[string]any{"old": int64(1)}})
		return err
	}); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	return db, node, other, edge
}

func readPropertyMutationNode(t *testing.T, db *DB, id uint64) Node {
	t.Helper()
	var node Node
	if err := db.View(func(tx *Tx) error {
		var ok bool
		var err error
		node, ok, err = tx.GetNodeValue(id)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("node %d not found", id)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return node
}
