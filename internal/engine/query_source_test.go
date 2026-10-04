package engine

import (
	"context"
	"errors"
	"github.com/mrchypark/latticedb-go/internal/store"
	"path/filepath"
	"strings"
	"testing"
)

func TestQueryDiskSourcesAdmittedBeforeDecode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "query-source")
	db, err := Open(path, OpenOptions{Create: true, PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Update(func(tx *Tx) error {
		for range 2 {
			if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Doc"}, Properties: map[string]any{"body": strings.Repeat("x", 1<<20), "key": int64(1)}}); err != nil {
				return err
			}
		}
		_, err := tx.CreateEdge(1, 2, "LINK", CreateEdgeOptions{Properties: map[string]any{"body": strings.Repeat("y", 1<<20), "key": int64(1)}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err = db.CreateNodePropertyIndex("Doc", "key"); err != nil {
		t.Fatal(err)
	}
	if err = db.CreateEdgePropertyIndex("LINK", "key"); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, OpenOptions{PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	queries := []string{
		"MATCH (n) RETURN id(n)",
		"MATCH (n:Doc) RETURN id(n)",
		"MATCH (n) WHERE id(n) = 1 RETURN id(n)",
		"MATCH (n) WHERE n.key = 99 RETURN id(n)",
		"MATCH (n:Doc {key:1}) RETURN id(n)",
		"MATCH (a)-[r:LINK]->(b) RETURN id(r)",
		"MATCH (a)-[r:LINK {key:1}]->(b) RETURN id(b)",
		"MATCH (a)-[r:LINK*1..2]->(b) RETURN id(b)",
		"MATCH (n) WITH n RETURN id(n)",
		"MATCH (n) WITH collect(n) AS ns UNWIND ns AS item RETURN item",
	}
	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			_, err := db.QueryContext(context.Background(), q, nil, QueryOptions{MaxBytes: 4096, MaxWork: 100 << 20})
			if !errors.Is(err, ErrResourceLimit) {
				t.Fatalf("source not admitted: %v", err)
			}
			if _, err := db.QueryContext(context.Background(), q, nil, QueryOptions{MaxBytes: 128 << 20, MaxWork: 100 << 20}); err != nil {
				t.Fatalf("sufficient budget: %v", err)
			}
		})
	}
}

func TestQuerySourceLedgerSurvivesScratchRelease(t *testing.T) {
	b := newQueryBudget(context.Background(), QueryOptions{MaxBytes: 4096})
	defer releaseQueryBudget(b)
	if err := b.ReservePageRead(1, 3072); err != nil {
		t.Fatal(err)
	}
	before := b.bytes
	if err := b.chargeTemporary(512); err != nil {
		t.Fatal(err)
	}
	b.releaseTemporary(uint64(b.bytes - before))
	if b.sourceBytes != 3072 || b.RemainingPageReadBytes() != 1024 {
		t.Fatalf("source ledger lost: %+v", b)
	}
	if err := b.chargeResult(1025); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	b.ReleasePageRead(3072)
	if err := b.chargeResult(4096); err != nil {
		t.Fatal(err)
	}
}

func querySourceFixture(t *testing.T, count, size int) *DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "sources")
	db, err := Open(path, OpenOptions{Create: true, PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Update(func(tx *Tx) error {
		for range count {
			if _, e := tx.CreateNode(CreateNodeOptions{Labels: []string{"Doc"}, Properties: map[string]any{"body": strings.Repeat("x", size), "key": int64(1)}}); e != nil {
				return e
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, OpenOptions{PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestQueryRejectedSourcesAreReleased(t *testing.T) {
	db := querySourceFixture(t, 64, 64<<10)
	// A pattern-internal rejection never escapes into an intermediate row.
	for _, labels := range [][]string{nil, {"Doc"}} {
		err := db.View(func(tx *Tx) error {
			b := newQueryBudget(context.Background(), QueryOptions{MaxBytes: 600 << 10, MaxWork: 100 << 20})
			defer releaseQueryBudget(b)
			pattern := nodePattern{Var: "n", Labels: labels, Properties: map[string]any{"key": int64(99)}}
			rows, err := pattern.apply(tx, []queryRow{{slots: make([]boundValue, 1), index: map[string]int{"n": 0}}}, b)
			if err != nil {
				return err
			}
			if len(rows) != 0 || b.sourceBytes != 0 {
				t.Fatalf("rows=%d bytes=%d", len(rows), b.sourceBytes)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}

}
func TestQueryWithDropsOnlyUnreferencedSources(t *testing.T) {
	db := querySourceFixture(t, 2, 64<<10)
	for _, tt := range []struct {
		projection string
		limited    bool
	}{
		{"id(n) AS previous", false}, {"n AS previous", true}, {"collect(n) AS previous", true},
	} {
		q := `MATCH (n) WHERE id(n) = 1 WITH ` + tt.projection + ` MATCH (m) WHERE id(m) = 2 RETURN previous, id(m)`
		_, err := db.QueryContext(context.Background(), q, nil, QueryOptions{MaxBytes: 700 << 10, MaxWork: 100 << 20})
		if tt.limited {
			if !errors.Is(err, ErrResourceLimit) {
				t.Fatalf("%s: expected retained-source limit, got %v", q, err)
			}
		} else if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
}
func TestQuerySourceAllocationIdentityAndCleanup(t *testing.T) {
	db := querySourceFixture(t, 1, 64<<10)
	err := db.View(func(tx *Tx) error {
		b := newQueryBudget(context.Background(), QueryOptions{MaxBytes: 4 << 20})
		defer releaseQueryBudget(b)
		a, err := b.readNode(tx.graph, 1)
		if err != nil {
			return err
		}
		one := b.sourceBytes
		c, err := b.readNode(tx.graph, 1)
		if err != nil {
			return err
		}
		if a == c || one == 0 || b.sourceBytes != 2*one {
			t.Fatalf("allocations not accounted independently: %d -> %d", one, b.sourceBytes)
		}
		nested := any(map[string]any{"rows": []any{boundValue{Node: a}, boundValue{Node: a}}})
		unroot := b.sourceRoot(func(visit func(any)) { visit(nested) })
		defer unroot()
		if err = b.sweepSources(); err != nil {
			return err
		}
		if b.sourceBytes != one {
			t.Fatalf("shared source charge=%d want %d", b.sourceBytes, one)
		}
		nested = nil
		if err = b.sweepSources(); err != nil {
			return err
		}
		if b.sourceBytes != 0 {
			t.Fatalf("dropped sources=%d", b.sourceBytes)
		}
		// The raw value fits, but admitting its decoded strings must fail cleanly.
		b.maxBytes = 128 << 10
		if _, err = b.readNode(tx.graph, 1); !errors.Is(err, ErrResourceLimit) {
			t.Fatalf("partial decode error=%v", err)
		}
		if b.sourceBytes != 0 {
			t.Fatalf("failed decoder retained %d", b.sourceBytes)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
func TestQueryOwnedReadSkipsShadowedAndDeletedBase(t *testing.T) {
	db := querySourceFixture(t, 2, 1<<20)
	err := db.Update(func(tx *Tx) error {
		// Prepare a resident overlay without reading the oversized base record.
		tx.graph.Nodes.Set(1, &store.NodeRecord{ID: 1})
		tx.graph.DeletedNodes.Set(2, true)
		b := newQueryBudget(context.Background(), QueryOptions{MaxBytes: 4096})
		defer releaseQueryBudget(b)
		count := 0
		err := b.visitNodes(tx.graph, func(n *store.NodeRecord) error {
			count++
			if n.ID != 1 {
				t.Fatalf("deleted ID=%d", n.ID)
			}
			return nil
		})
		if err != nil {
			return err
		}
		if count != 1 || b.sourceBytes != 0 {
			t.Fatalf("count=%d bytes=%d", count, b.sourceBytes)
		}
		return errors.New("rollback fixture")
	})
	if err == nil || err.Error() != "rollback fixture" {
		t.Fatal(err)
	}
}

// Emits one disk allocation at a time, so this tests the heap's lifetime rather
// than the current pattern iterator's deliberately materialized expansion.
type diskSourceTestIterator struct {
	graph       *store.GraphState
	budget      *queryBudget
	plan        *queryPlan
	next, total int
	descending  bool
	handoff     queryRow
}

func (it *diskSourceTestIterator) Next() (queryRow, bool, error) {
	it.handoff = queryRow{}
	if err := it.budget.sweepSources(); err != nil {
		return queryRow{}, false, err
	}
	if it.next == it.total {
		return queryRow{}, false, nil
	}
	id := it.next + 1
	if it.descending {
		id = it.total - it.next
	}
	it.next++
	node, err := it.budget.readNode(it.graph, uint64(id))
	if err != nil {
		return queryRow{}, false, err
	}
	if err = it.budget.chargeRows(1); err != nil {
		return queryRow{}, false, err
	}
	row := it.plan.newRow()
	row.set("n", boundValue{Node: node})
	it.handoff = row
	return row, true, nil
}
func (it *diskSourceTestIterator) Close() { it.handoff = queryRow{} }
func TestQueryTopKRetainsOnlyLiveSources(t *testing.T) {
	db := querySourceFixture(t, 24, 64<<10)
	for _, descending := range []bool{false, true} {
		err := db.View(func(tx *Tx) error {
			b := newQueryBudget(context.Background(), QueryOptions{MaxBytes: 1 << 20, MaxWork: 100 << 20})
			defer releaseQueryBudget(b)
			plan, err := parseQuery(`MATCH (n) RETURN id(n) ORDER BY id(n) LIMIT 1`)
			if err != nil {
				return err
			}
			it := &diskSourceTestIterator{graph: tx.graph, budget: b, plan: plan, total: 24, descending: descending}
			defer b.sourceRoot(func(visit func(any)) { visit(it.handoff) })()
			rows, err := plan.collectTopKRows(it, 0, 1, b)
			if err != nil {
				return err
			}
			if len(rows) != 1 {
				t.Fatalf("rows=%d", len(rows))
			}
			n, _ := rows[0].get("n")
			if n.Node == nil || n.Node.ID != 1 {
				t.Fatalf("best=%v", n.Node)
			}
			if len(b.source.records) != 1 {
				t.Fatalf("retained historical allocations=%d", len(b.source.records))
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
func TestQueryMutationKeepsSourceDependencies(t *testing.T) {
	db := querySourceFixture(t, 2, 64<<10)
	query := `MATCH (n) WHERE id(n) = 1 SET n.key = 2 WITH id(n) AS previous MATCH (m) WHERE id(m) = 2 RETURN previous, id(m)`
	if _, err := db.QueryContext(context.Background(), query, nil, QueryOptions{MaxBytes: 700 << 10, MaxWork: 100 << 20}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("COW source dropped early: %v", err)
	}
	if _, err := db.QueryContext(context.Background(), query, nil, QueryOptions{MaxBytes: 4 << 20, MaxWork: 100 << 20}); err != nil {
		t.Fatal(err)
	}
	result, err := db.Query(`MATCH (n) WHERE id(n) = 1 RETURN n.body AS body, n.key AS key`, nil)
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["body"] != strings.Repeat("x", 64<<10) || result.Rows[0]["key"] != int64(2) {
		t.Fatalf("mutation result=%v err=%v", result.Rows, err)
	}
}

func TestQueryCOWLeaseAndCancellationCleanup(t *testing.T) {
	db := querySourceFixture(t, 1, 64<<10)
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b := newQueryBudget(ctx, QueryOptions{MaxBytes: 2 << 20})
	defer releaseQueryBudget(b)
	tx.queryBudget = b
	defer func() { tx.queryBudget = nil }()
	scope := b.sourceScope()
	node, err := tx.writableNode(1, true)
	if err != nil {
		scope.close()
		t.Fatal(err)
	}
	scope.close()
	held := b.sourceBytes
	if held == 0 {
		t.Fatal("COW payload lost its source lease")
	}
	if err = b.sweepSources(); err != nil {
		t.Fatal(err)
	}
	if b.sourceBytes != held {
		t.Fatal("unbound COW payload was released")
	}
	cancel()
	if err = b.sweepSources(); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if b.sourceBytes != held {
		t.Fatal("canceled mark released live payload")
	}
	b.closeSources()
	if b.sourceBytes != 0 {
		t.Fatalf("statement cleanup retained %d", b.sourceBytes)
	}
	body, ok := node.Properties.Lookup("body")
	if !ok || body != strings.Repeat("x", 64<<10) {
		t.Fatal("transaction payload changed during query cleanup")
	}
}

func TestQueryDetachDeleteAdmitsIncidentSources(t *testing.T) {
	for _, source := range []string{"edge", "fts"} {
		t.Run(source, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "delete-source")
			db, err := Open(path, OpenOptions{Create: true, PageStorage: true})
			if err != nil {
				t.Fatal(err)
			}
			if err = db.Update(func(tx *Tx) error {
				for range 2 {
					if _, err := tx.CreateNode(CreateNodeOptions{}); err != nil {
						return err
					}
				}
				if source == "fts" {
					return tx.FTSIndex(1, strings.Repeat("x", 1<<20))
				}
				_, err := tx.CreateEdge(1, 2, "LINK", CreateEdgeOptions{Properties: map[string]any{"body": strings.Repeat("x", 1<<20)}})
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if err = db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = Open(path, OpenOptions{PageStorage: true})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			q := "MATCH (n) WHERE id(n) = 1 DETACH DELETE n"
			if _, err := db.QueryContext(context.Background(), q, nil, QueryOptions{MaxBytes: 16 << 10, MaxWork: 100 << 20}); !errors.Is(err, ErrResourceLimit) {
				t.Fatalf("unadmitted %s deletion: %v", source, err)
			}
			if err := db.View(func(tx *Tx) error {
				exists, err := tx.NodeExists(1)
				if err == nil && !exists {
					t.Fatal("failed query published deletion")
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := db.QueryContext(context.Background(), q, nil, QueryOptions{MaxBytes: 32 << 20, MaxWork: 100 << 20}); err != nil {
				t.Fatal(err)
			}
			if err := db.View(func(tx *Tx) error {
				exists, err := tx.NodeExists(1)
				if err == nil && exists {
					t.Fatal("successful query did not delete node")
				}
				edges, err := tx.graph.EdgeCount()
				if err == nil && edges != 0 {
					t.Fatal("incident edge remains")
				}
				fts, err := tx.graph.HasFTS(1)
				if err == nil && fts {
					t.Fatal("FTS document remains")
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
