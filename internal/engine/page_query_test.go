package engine

import (
	"context"
	"path/filepath"
	"slices"
	"testing"
)

func TestPageDBQueryUsesPropertyIndexesWithWriteOverlay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "query-db")
	db, err := Open(path, OpenOptions{Create: true, PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	var firstNode, secondNode, firstEdge, secondEdge uint64
	if err := db.Update(func(tx *Tx) error {
		first, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"key": "hit", "note": "old"}})
		if err != nil {
			return err
		}
		second, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"key": "hit", "note": "old"}})
		if err != nil {
			return err
		}
		firstNode, secondNode = first.ID, second.ID
		firstLink, err := tx.CreateEdge(first.ID, second.ID, "LINK", CreateEdgeOptions{Properties: map[string]any{"key": "hit", "note": "old"}})
		if err != nil {
			return err
		}
		secondLink, err := tx.CreateEdge(second.ID, first.ID, "LINK", CreateEdgeOptions{Properties: map[string]any{"key": "hit", "note": "old"}})
		if err != nil {
			return err
		}
		firstEdge, secondEdge = firstLink.ID, secondLink.ID
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
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, OpenOptions{PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	nodeQuery := `MATCH (n:Item) WHERE n.key = $key AND n.note = $note RETURN id(n) AS id ORDER BY id LIMIT 10`
	edgeQuery := `MATCH ()-[r:LINK]->() WHERE r.key = $key AND r.note = $note RETURN id(r) AS id ORDER BY id`
	check := func(tx *Tx) error {
		if err := tx.SetProperty(firstNode, "note", "fresh"); err != nil {
			return err
		}
		if err := tx.SetProperty(secondNode, "key", "changed"); err != nil {
			return err
		}
		if err := tx.SetEdgeProperty(firstEdge, "note", "fresh"); err != nil {
			return err
		}
		if err := tx.SetEdgeProperty(secondEdge, "key", "changed"); err != nil {
			return err
		}
		params := map[string]any{"key": "hit", "note": "fresh"}
		plan, err := parseQuery(nodeQuery)
		if err != nil {
			return err
		}
		budget := newQueryBudget(context.Background(), QueryOptions{})
		nodeIDs, found, err := plan.indexedNodeIDs(tx, plan.matchPatterns[0].(nodePattern), params, 10, budget)
		releaseQueryBudget(budget)
		if err != nil {
			return err
		}
		if !found || !slices.Equal(nodeIDs, []uint64{firstNode}) {
			t.Fatalf("indexed node candidates = %v, found %v", nodeIDs, found)
		}
		plan, err = parseQuery(edgeQuery)
		if err != nil {
			return err
		}
		budget = newQueryBudget(context.Background(), QueryOptions{})
		edgeIDs, found, err := plan.indexedEdgeIDs(tx, plan.matchPatterns[0].(edgePattern), params, budget)
		releaseQueryBudget(budget)
		if err != nil {
			return err
		}
		if !found || !slices.Equal(edgeIDs, []uint64{firstEdge}) {
			t.Fatalf("indexed edge candidates = %v, found %v", edgeIDs, found)
		}
		for _, item := range []struct {
			query string
			want  int64
		}{{nodeQuery, int64(firstNode)}, {edgeQuery, int64(firstEdge)}} {
			result, err := tx.Query(item.query, map[string]any{"key": "hit", "note": "fresh"})
			if err != nil {
				return err
			}
			if len(result.Rows) != 1 || result.Rows[0]["id"] != item.want {
				t.Fatalf("query %q rows = %#v, want id %d", item.query, result.Rows, item.want)
			}
		}
		return nil
	}
	if err := db.Update(check); err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		query string
		want  int64
	}{{nodeQuery, int64(firstNode)}, {edgeQuery, int64(firstEdge)}} {
		result, err := db.Query(item.query, map[string]any{"key": "hit", "note": "fresh"})
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Rows) != 1 || result.Rows[0]["id"] != item.want {
			t.Fatalf("query %q rows = %#v, want id %d", item.query, result.Rows, item.want)
		}
	}
}
