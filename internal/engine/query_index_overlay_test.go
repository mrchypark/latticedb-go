package engine

import (
	"context"
	"errors"
	"testing"
)

func TestQueryPropertyIndexOverlayReadsNodeChanges(t *testing.T) {
	db := openQueryIndexOverlayDB(t)
	var keep, changed, removed, deleted Node
	if err := db.Update(func(tx *Tx) error {
		var err error
		keep, err = tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"k": "hit"}})
		if err != nil {
			return err
		}
		changed, err = tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"k": "old"}})
		if err != nil {
			return err
		}
		removed, err = tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"k": "hit"}})
		if err != nil {
			return err
		}
		deleted, err = tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"k": "hit"}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("Item", "k"); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	if err := tx.SetProperty(changed.ID, "k", "hit"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Query(`MATCH (n) WHERE id(n) = $id REMOVE n.k`, map[string]any{"id": int64(removed.ID)}); err != nil {
		t.Fatal(err)
	}
	if err := tx.DeleteNode(deleted.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"k": "hit"}}); err != nil {
		t.Fatal(err)
	}
	assertIndexedNodeQuery(t, tx, `MATCH (n:Item) WHERE n.k = "hit" RETURN count(n) AS count`, 3)
	_ = keep
}

func TestQueryPropertyIndexOverlayReadsEdgeChanges(t *testing.T) {
	db := openQueryIndexOverlayDB(t)
	var left, right Node
	var changed, removed Edge
	if err := db.Update(func(tx *Tx) error {
		var err error
		left, err = tx.CreateNode(CreateNodeOptions{})
		if err != nil {
			return err
		}
		right, err = tx.CreateNode(CreateNodeOptions{})
		if err != nil {
			return err
		}
		changed, err = tx.CreateEdge(left.ID, right.ID, "LINK", CreateEdgeOptions{Properties: map[string]any{"k": "old"}})
		if err != nil {
			return err
		}
		removed, err = tx.CreateEdge(left.ID, right.ID, "LINK", CreateEdgeOptions{Properties: map[string]any{"k": "hit"}})
		if err != nil {
			return err
		}
		_, err = tx.CreateEdge(left.ID, right.ID, "LINK", CreateEdgeOptions{Properties: map[string]any{"k": "hit"}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateEdgePropertyIndex("LINK", "k"); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	if err := tx.SetEdgeProperty(changed.ID, "k", "hit"); err != nil {
		t.Fatal(err)
	}
	if err := tx.RemoveEdgeProperty(removed.ID, "k"); err != nil {
		t.Fatal(err)
	}
	if err := tx.DeleteEdge(left.ID, right.ID, "LINK"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.CreateEdge(left.ID, right.ID, "LINK", CreateEdgeOptions{Properties: map[string]any{"k": "hit"}}); err != nil {
		t.Fatal(err)
	}
	result, err := tx.Query(`MATCH ()-[r:LINK]->() WHERE r.k = "hit" RETURN count(r) AS count`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := result.Rows[0]["count"]; got != int64(2) {
		t.Fatalf("edge count = %v, want 2", got)
	}
}

func TestQueryPropertyIndexOverlayRespectsLowByteBudget(t *testing.T) {
	db := openQueryIndexOverlayDB(t)
	var left, right Node
	if err := db.Update(func(tx *Tx) error {
		var err error
		left, err = tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"k": "hit"}})
		if err != nil {
			return err
		}
		right, err = tx.CreateNode(CreateNodeOptions{})
		if err != nil {
			return err
		}
		_, err = tx.CreateEdge(left.ID, right.ID, "LINK", CreateEdgeOptions{Properties: map[string]any{"k": "hit"}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("Item", "k"); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateEdgePropertyIndex("LINK", "k"); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"k": "hit"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.CreateEdge(left.ID, right.ID, "LINK", CreateEdgeOptions{Properties: map[string]any{"k": "hit"}}); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`MATCH (n:Item) WHERE n.k = "hit" RETURN n`,
		`MATCH ()-[r:LINK]->() WHERE r.k = "hit" RETURN r`,
	} {
		plan, err := parseQuery(query)
		if err != nil {
			t.Fatal(err)
		}
		budget := newQueryBudget(context.Background(), QueryOptions{MaxBytes: 8})
		var queryErr error
		switch pattern := plan.matchPatterns[0].(type) {
		case nodePattern:
			_, _, queryErr = plan.indexedNodeIDs(tx, pattern, nil, ^uint(0), budget)
		case edgePattern:
			_, _, queryErr = plan.indexedEdgeIDs(tx, pattern, nil, budget)
		}
		releaseQueryBudget(budget)
		if !errors.Is(queryErr, ErrResourceLimit) {
			t.Fatalf("%s low-byte budget error = %v", query, queryErr)
		}
	}
}

func TestQueryPropertyIndexOverlayManyUpsertsKeepsPublicResultsSorted(t *testing.T) {
	db := openQueryIndexOverlayDB(t)
	var base Node
	if err := db.Update(func(tx *Tx) error {
		var err error
		base, err = tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"k": "hit"}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("Item", "k"); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	want := []int64{int64(base.ID)}
	for range 3 {
		node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"k": "hit"}})
		if err != nil {
			t.Fatal(err)
		}
		want = append(want, int64(node.ID))
	}
	result, err := tx.Query(`MATCH (n:Item) WHERE n.k = "hit" RETURN id(n) AS id ORDER BY id`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != len(want) {
		t.Fatalf("rows = %#v, want %v ids", result.Rows, len(want))
	}
	for index, row := range result.Rows {
		if got := row["id"]; got != want[index] {
			t.Fatalf("row %d id = %v, want %d", index, got, want[index])
		}
	}
}

func openQueryIndexOverlayDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(t.TempDir()+"/overlay.ltdb", OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func assertIndexedNodeQuery(t *testing.T, tx *Tx, query string, want int64) {
	t.Helper()
	plan, err := parseQuery(query)
	if err != nil {
		t.Fatal(err)
	}
	pattern := plan.matchPatterns[0].(nodePattern)
	budget := newQueryBudget(context.Background(), QueryOptions{})
	_, found, err := plan.indexedNodeIDs(tx, pattern, nil, ^uint(0), budget)
	releaseQueryBudget(budget)
	if err != nil || !found {
		t.Fatalf("node index overlay = found %v, err %v", found, err)
	}
	result, err := tx.Query(query, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := result.Rows[0]["count"]; got != want {
		t.Fatalf("node count = %v, want %d", got, want)
	}
}
