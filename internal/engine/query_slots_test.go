package engine

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestQueryPlanUsesDenseBindingSlots(t *testing.T) {
	plan, err := parseQuery(`MATCH (a)-[r:KNOWS]->(b) WHERE a.missing = 1 RETURN b`)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.slots) != 3 || plan.slots["a"] == plan.slots["r"] || plan.slots["r"] == plan.slots["b"] {
		t.Fatalf("slots = %#v", plan.slots)
	}
	row := plan.newRow()
	if _, ok := row.get("a"); ok {
		t.Fatal("new slot must be unbound")
	}
}

func TestQueryUnboundVariablesAndEdgeOnlyScan(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "query-slots.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		a, err := tx.CreateNode(CreateNodeOptions{})
		if err != nil {
			return err
		}
		b, err := tx.CreateNode(CreateNodeOptions{})
		if err != nil {
			return err
		}
		_, err = tx.CreateEdge(a.ID, b.ID, "LINK", CreateEdgeOptions{Properties: map[string]any{"ok": true}})
		return err
	}); err != nil {
		t.Fatal(err)
	}

	result, err := db.Query(`MATCH ()-[r:LINK]->() RETURN id(r) AS id`, nil)
	if err != nil || len(result.Rows) != 1 {
		t.Fatalf("edge-only scan = %#v, %v", result.Rows, err)
	}
	result, err = db.Query(`MATCH (n) WHERE n.missing = 1 RETURN n`, nil)
	if err != nil || len(result.Rows) != 0 {
		t.Fatalf("unbound property semantics = %#v, %v", result.Rows, err)
	}
}

func TestQueryLimitStopsIndexedMatchWork(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "query-limit.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var id uint64
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 64; i++ {
			node, err := tx.CreateNode(CreateNodeOptions{})
			if err != nil {
				return err
			}
			if i == 0 {
				id = node.ID
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	result, err := db.QueryContext(t.Context(), `MATCH (n) WHERE id(n) = $id RETURN id(n) AS id LIMIT 1`, map[string]any{"id": int64(id)}, QueryOptions{MaxWork: 2})
	if err != nil || len(result.Rows) != 1 {
		t.Fatalf("limited match = %#v, %v", result.Rows, err)
	}
}

func TestQueryPatternIteratorEnforcesCumulativeMaxRows(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "query-max-rows.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 2; i++ {
			if _, err := tx.CreateNode(CreateNodeOptions{}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	_, err = db.QueryContext(t.Context(), `UNWIND $items AS x MATCH (n) RETURN n`, map[string]any{"items": []any{int64(1), int64(2)}}, QueryOptions{MaxRows: 3})
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("MaxRows error = %v", err)
	}
}
