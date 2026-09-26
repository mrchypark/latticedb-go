package engine

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestQueryVariablePaths(t *testing.T) {
	for _, test := range []struct {
		text    string
		minHops int
		maxHops int
	}{
		{"..3", 1, 3},
		{"0..3", 0, 3},
		{"", 1, -1},
	} {
		minHops, maxHops, err := parseVariableHops(test.text)
		if err != nil || minHops != test.minHops || maxHops != test.maxHops {
			t.Fatalf("parseVariableHops(%q) = (%d, %d, %v), want (%d, %d)", test.text, minHops, maxHops, err, test.minHops, test.maxHops)
		}
	}
	if _, err := parseQuery(`MATCH (a)-[r:LINK*1]->(b) RETURN id(r)`); err == nil {
		t.Fatal("id() accepted a variable-path relationship list")
	}
	db, err := Open(filepath.Join(t.TempDir(), "variable-path.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var a, b, c uint64
	if err := db.Update(func(tx *Tx) error {
		var err error
		for _, item := range []struct {
			name string
			id   *uint64
		}{{"a", &a}, {"b", &b}, {"c", &c}} {
			node, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"name": item.name}})
			if err != nil {
				return err
			}
			*item.id = node.ID
		}
		names := map[uint64]string{a: "a", b: "b", c: "c"}
		for _, edge := range [][2]uint64{{a, b}, {a, b}, {b, c}, {c, a}, {a, a}} {
			if _, err = tx.CreateEdge(edge[0], edge[1], "LINK", CreateEdgeOptions{Properties: map[string]any{"keep": true, "from": names[edge[0]]}}); err != nil {
				return err
			}
		}
		_, err = tx.CreateEdge(a, c, "LINK", CreateEdgeOptions{Properties: map[string]any{"keep": false, "from": "a"}})
		return err
	}); err != nil {
		t.Fatal(err)
	}

	result, err := db.QueryContext(t.Context(), `MATCH (a {name: "a"})-[r:LINK*0 {keep: true}]->(b) RETURN b.name AS name, r`, nil, QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Rows[0]["name"], "a") {
		t.Fatalf("zero-hop endpoint = %#v", result.Rows)
	}
	if edges, ok := result.Rows[0]["r"].([]any); !ok || len(edges) != 0 {
		t.Fatalf("zero-hop relationship binding = %#v", result.Rows[0]["r"])
	}
	result, err = db.QueryContext(t.Context(), `MATCH (a {name: "a"})-[r:LINK*1 {keep: true}]->(b) RETURN b.name AS name`, nil, QueryOptions{})
	if err != nil || len(result.Rows) != 3 {
		t.Fatalf("parallel and filtered paths = %#v, %v", result.Rows, err)
	}

	result, err = db.QueryContext(t.Context(), `MATCH (a {name: "a"})<-[r:LINK*1]-(b) RETURN b.name AS name`, nil, QueryOptions{})
	if err != nil || !reflect.DeepEqual(result.Rows, []map[string]any{{"name": "a"}, {"name": "c"}}) {
		t.Fatalf("incoming variable path = %#v, %v", result.Rows, err)
	}
	result, err = db.QueryContext(t.Context(), `MATCH (a {name: "a"})-[r:LINK*3 {keep: true}]->(b {name: "a"}) RETURN r`, nil, QueryOptions{})
	if err != nil || len(result.Rows) != 2 {
		t.Fatalf("cycle paths without edge reuse = %#v, %v", result.Rows, err)
	}
	result, err = db.QueryContext(t.Context(), `MATCH (a {name: "a"})-[r:LINK*1 {keep: true}]-(b) RETURN b.name AS name`, nil, QueryOptions{})
	if err != nil || len(result.Rows) != 4 {
		t.Fatalf("undirected variable path = %#v, %v", result.Rows, err)
	}
	result, err = db.QueryContext(t.Context(), `MATCH (a)-[r:LINK*1 {from: a.name}]->(b) RETURN b.name AS name`, nil, QueryOptions{})
	if err != nil || len(result.Rows) != 6 {
		t.Fatalf("endpoint-scoped edge property = %#v, %v", result.Rows, err)
	}
	result, err = db.QueryContext(t.Context(), `MATCH (a {name: "a"})-[r:LINK*2 {from: a.name, keep: true}]->(b {name: "c"}) RETURN b`, nil, QueryOptions{})
	if err != nil || len(result.Rows) != 0 {
		t.Fatalf("completed-path endpoint property = %#v, %v", result.Rows, err)
	}
	result, err = db.QueryContext(t.Context(), `MATCH (a)-[:LINK*1]->(a) RETURN a.name AS name`, nil, QueryOptions{})
	if err != nil || !reflect.DeepEqual(result.Rows, []map[string]any{{"name": "a"}}) {
		t.Fatalf("same endpoint variable path = %#v, %v", result.Rows, err)
	}
	result, err = db.QueryContext(t.Context(), `MATCH (a {name: "a"})-[r:LINK*1 {keep: true}]->(b) WITH r MATCH (x)-[r:LINK*1]->(y) RETURN y.name AS name`, nil, QueryOptions{})
	if err != nil || len(result.Rows) != 3 {
		t.Fatalf("reused relationship-list binding = %#v, %v", result.Rows, err)
	}
	result, err = db.QueryContext(t.Context(), `MATCH (a {name: "a"})-[r:LINK*2 {keep: true}]->(b) WITH b, r MATCH (a)-[r:LINK*2]->(b) RETURN a.name AS name`, nil, QueryOptions{})
	if err != nil || !reflect.DeepEqual(result.Rows, []map[string]any{{"name": "a"}, {"name": "a"}, {"name": "a"}, {"name": "a"}}) {
		t.Fatalf("reverse reused relationship-list binding = %#v, %v", result.Rows, err)
	}
	if _, err := db.QueryContext(t.Context(), `MATCH (a)-[:LINK*]->(b) RETURN b`, nil, QueryOptions{MaxWork: 1}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("unbounded path resource error = %v", err)
	}
}

func TestVariablePathDeepBudget(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "variable-path-budget.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var ids [12]uint64
	if err := db.Update(func(tx *Tx) error {
		for index := range ids {
			node, err := tx.CreateNode(CreateNodeOptions{})
			if err != nil {
				return err
			}
			ids[index] = node.ID
			if index != 0 {
				if _, err := tx.CreateEdge(ids[index-1], node.ID, "DEEP", CreateEdgeOptions{}); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	pattern := edgePattern{Left: nodePattern{Var: "a"}, EdgeType: "DEEP", Right: nodePattern{Var: "b"}, VariableLength: true, MinHops: 11, MaxHops: 11}
	row := queryRow{slots: []boundValue{{Node: &store.NodeRecord{ID: ids[0]}, Bound: true}, {}}, index: map[string]int{"a": 0, "b": 1}}
	for _, options := range []QueryOptions{{MaxWork: 32}, {MaxBytes: 128}} {
		if err := db.View(func(tx *Tx) error {
			row.slots[0].Node = tx.graph.Nodes.Get(ids[0])
			budget := newQueryBudget(t.Context(), options)
			defer releaseQueryBudget(budget)
			_, err := pattern.applyVariable(tx, row, nil, budget)
			if !errors.Is(err, ErrResourceLimit) {
				t.Fatalf("deep variable path options %#v error = %v", options, err)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestVariablePathBoundaries(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "variable-path-boundaries.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var a, b uint64
	if err := db.Update(func(tx *Tx) error {
		var err error
		if node, err := tx.CreateNode(CreateNodeOptions{}); err != nil {
			return err
		} else {
			a = node.ID
		}
		if node, err := tx.CreateNode(CreateNodeOptions{}); err != nil {
			return err
		} else {
			b = node.ID
		}
		if _, err = tx.CreateEdge(a, b, "LINK", CreateEdgeOptions{}); err != nil {
			return err
		}
		_, err = tx.CreateEdge(a, a, "LOOP", CreateEdgeOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}

	result, err := db.QueryContext(t.Context(), `MATCH (a)-[:LOOP]->(b)-[:LOOP]->(c) RETURN c`, nil, QueryOptions{})
	if err != nil || len(result.Rows) != 1 {
		t.Fatalf("fixed path edge reuse = %#v, %v", result.Rows, err)
	}
	pattern := edgePattern{Left: nodePattern{Var: "a"}, EdgeVar: "r", EdgeType: "LINK", Right: nodePattern{Var: "b"}, VariableLength: true, MinHops: 0, MaxHops: 1 << 30}
	if err := db.View(func(tx *Tx) error {
		row := queryRow{slots: make([]boundValue, 3), index: map[string]int{"a": 0, "b": 1, "r": 2}}
		row.set("a", boundValue{Node: tx.graph.Nodes.Get(a)})
		row.set("b", boundValue{Node: tx.graph.Nodes.Get(b)})
		budget := newQueryBudget(t.Context(), QueryOptions{})
		defer releaseQueryBudget(budget)
		rows, err := pattern.applyVariable(tx, row, nil, budget)
		if err != nil || len(rows) != 1 {
			t.Fatalf("bound endpoints = %#v, %v", rows, err)
		}

		row = queryRow{slots: make([]boundValue, 3), index: map[string]int{"a": 0, "b": 1, "r": 2}}
		row.set("a", boundValue{Node: tx.graph.Nodes.Get(a)})
		rows, err = pattern.applyVariable(tx, row, nil, budget)
		if err != nil || len(rows) != 2 {
			t.Fatalf("DFS output rows = %#v, %v", rows, err)
		}
		if first, ok := rows[0].get("r"); !ok || len(first.Value.([]any)) != 0 {
			t.Fatalf("zero-hop list changed after DFS = %#v", rows[0])
		}

		empty := edgePattern{Left: nodePattern{Var: "a"}, EdgeType: "MISSING", Right: nodePattern{Var: "b"}, VariableLength: true, MinHops: 1, MaxHops: 1}
		emptyBudget := newQueryBudget(t.Context(), QueryOptions{MaxBytes: 64})
		defer releaseQueryBudget(emptyBudget)
		rows, err = empty.applyVariable(tx, row, nil, emptyBudget)
		if err != nil || len(rows) != 0 || emptyBudget.bytes != 0 {
			t.Fatalf("empty path budget rows=%#v bytes=%d err=%v", rows, emptyBudget.bytes, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
