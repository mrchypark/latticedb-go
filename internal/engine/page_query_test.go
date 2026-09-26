package engine

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func openPagePostingDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "postings"), OpenOptions{Create: true, PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 4096; i++ {
			if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"key": "all", "broad": "all", "rare": "present"}}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("Item", "key"); err != nil {
		db.Close()
		t.Fatal(err)
	}
	for _, property := range []string{"broad", "rare"} {
		if err := db.CreateNodePropertyIndex("Item", property); err != nil {
			db.Close()
			t.Fatal(err)
		}
	}
	return db
}

func TestPageQueryChargesLargePostingScans(t *testing.T) {
	db := openPagePostingDB(t)
	defer db.Close()

	for _, query := range []string{
		`MATCH (n:Item) WHERE n.broad = 'all' AND n.rare = 'missing' RETURN id(n)`,
		`MATCH (n:Item) WHERE n.broad = 'all' AND n.rare = 'missing' RETURN id(n) LIMIT 1`,
	} {
		_, err := db.QueryContext(t.Context(), query, nil, QueryOptions{MaxWork: 32})
		if !errors.Is(err, ErrResourceLimit) {
			t.Fatalf("query %q error = %v, want ErrResourceLimit", query, err)
		}
	}

	err := db.View(func(tx *Tx) error {
		definition := store.PropertyIndexDefinition{Scope: "Item", Property: "key"}
		work := 0
		_, found, err := tx.graph.NodeProperties.CardinalityContext(t.Context(), definition, "all", func() error {
			work++
			if work > 32 {
				return ErrResourceLimit
			}
			return nil
		})
		if !found || !errors.Is(err, ErrResourceLimit) || work != 33 {
			t.Fatalf("cardinality result found=%v work=%d err=%v; want work 33 and ErrResourceLimit", found, work, err)
		}

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		work = 0
		_, found, err = tx.graph.NodeProperties.CardinalityContext(ctx, definition, "all", func() error {
			work++
			if work == 1 {
				cancel()
			}
			return nil
		})
		if !found || !errors.Is(err, context.Canceled) || work != 1 {
			t.Fatalf("canceled cardinality found=%v work=%d err=%v; want one posting and context.Canceled", found, work, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPageQueryMetersLabelTypeAndDegreeCounts(t *testing.T) {
	db := openPagePostingDB(t)
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 4096; i++ {
			if _, err := tx.CreateEdge(1, 2, "LINK", CreateEdgeOptions{}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.QueryContext(t.Context(), `MATCH (a)-[:LINK]->(b) WHERE id(a) = 1 RETURN id(b)`, nil, QueryOptions{MaxWork: 32}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("bound page-degree query error = %v, want ErrResourceLimit", err)
	}

	err := db.View(func(tx *Tx) error {
		check := func(name string, scan func(func() error) error) error {
			work := 0
			err := scan(func() error {
				work++
				if work > 32 {
					return ErrResourceLimit
				}
				return nil
			})
			if !errors.Is(err, ErrResourceLimit) || work != 33 {
				return errors.New(name + " did not charge each raw posting")
			}
			return nil
		}
		if err := check("label count", func(charge func() error) error {
			_, err := tx.graph.LabelCountContext(t.Context(), "Item", charge)
			return err
		}); err != nil {
			return err
		}
		if err := check("edge-type count", func(charge func() error) error {
			_, err := tx.graph.EdgeTypeCountContext(t.Context(), "LINK", charge)
			return err
		}); err != nil {
			return err
		}
		if err := check("outgoing degree", func(charge func() error) error {
			_, err := tx.graph.OutgoingCountContext(t.Context(), 1, charge)
			return err
		}); err != nil {
			return err
		}
		return check("incoming degree", func(charge func() error) error {
			_, err := tx.graph.IncomingCountContext(t.Context(), 2, charge)
			return err
		})
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestParseEdgeBodyIgnoresQuotedAsterisks(t *testing.T) {
	for _, test := range []struct {
		text         string
		variablePath bool
	}{
		{text: "`r*x`:`a*b`"},
		{text: "`r*x`:`a*b`*1..3", variablePath: true},
	} {
		pattern, err := parseEdgeBody(test.text)
		if err != nil {
			t.Fatalf("parseEdgeBody(%q): %v", test.text, err)
		}
		if pattern.EdgeVar != "r*x" || pattern.EdgeType != "a*b" || pattern.VariableLength != test.variablePath {
			t.Fatalf("parseEdgeBody(%q) = %#v", test.text, pattern)
		}
	}
	if _, err := parseEdgeBody("r:LINK*1..3*2"); err == nil {
		t.Fatal("duplicate unquoted variable-path delimiter accepted")
	}
}

func TestPageQueryAdjacentExpansionReturnsRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "adjacency")
	db, err := Open(path, OpenOptions{Create: true, PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	var sourceID, targetID uint64
	if err := db.Update(func(tx *Tx) error {
		source, err := tx.CreateNode(CreateNodeOptions{})
		if err != nil {
			return err
		}
		target, err := tx.CreateNode(CreateNodeOptions{})
		if err != nil {
			return err
		}
		sourceID, targetID = source.ID, target.ID
		_, err = tx.CreateEdge(source.ID, target.ID, "LINK", CreateEdgeOptions{})
		return err
	}); err != nil {
		db.Close()
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

	for _, test := range []struct {
		query string
		start uint64
		want  uint64
	}{
		{`MATCH (a)-[:LINK]->(b) WHERE id(a) = $id RETURN id(b) AS id`, sourceID, targetID},
		{`MATCH (a)<-[:LINK]-(b) WHERE id(a) = $id RETURN id(b) AS id`, targetID, sourceID},
	} {
		result, err := db.QueryContext(t.Context(), test.query, map[string]any{"id": int64(test.start)}, QueryOptions{})
		if err != nil || len(result.Rows) != 1 || result.Rows[0]["id"] != int64(test.want) {
			t.Fatalf("query %q rows=%#v err=%v; want id %d", test.query, result.Rows, err, test.want)
		}
	}
}

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
