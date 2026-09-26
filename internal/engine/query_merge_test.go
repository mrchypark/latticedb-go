package engine

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
)

func TestMergeNodeActionsAndUnwind(t *testing.T) {
	db := openWithDB(t)
	query := "MERGE (n:Person {name: $name}) ON CREATE SET n.visits = 1 ON MATCH SET n.visits = n.visits + 1 RETURN n.visits AS visits"
	for want := 1; want <= 2; want++ {
		result, err := db.Query(query, map[string]any{"name": "Dee"})
		if err != nil || len(result.Rows) != 1 || fmt.Sprint(result.Rows[0]["visits"]) != fmt.Sprint(want) {
			t.Fatalf("MERGE %d: %+v, %v", want, result, err)
		}
	}
	rows := withRows(t, db, "UNWIND ['Eve', 'Eve', 'Fox'] AS name MERGE (n:Person {name: name}) RETURN count(DISTINCT n) AS total")
	if fmt.Sprint(rows[0]["total"]) != "2" {
		t.Fatalf("distinct merged nodes: %v", rows)
	}
	rows = withRows(t, db, "MATCH (n:Person) WHERE n.name = 'Eve' RETURN count(*) AS total")
	if fmt.Sprint(rows[0]["total"]) != "1" {
		t.Fatalf("duplicate node: %v", rows)
	}
}

func TestMergeRelationshipsAndWholePaths(t *testing.T) {
	db := openWithDB(t)
	query := "MATCH (a:Person {name: 'Ada'}), (b:Person {name: 'Bob'}) MERGE (a)-[r:KNOWS {since: 2026}]->(b) ON CREATE SET r.new = true ON MATCH SET r.new = false RETURN r.new AS fresh"
	for _, want := range []bool{true, false} {
		rows := withRows(t, db, query)
		if len(rows) != 1 || rows[0]["fresh"] != want {
			t.Fatalf("relationship MERGE: %v, want fresh=%t", rows, want)
		}
	}
	// An unbound full pattern is created as a whole, even when one endpoint exists.
	rows := withRows(t, db, "MERGE (a:Person {name: 'Ada'})-[r:NEW]->(b:Person {name: 'Zed'})-[s:BACK]->(a) RETURN id(a) AS a, id(b) AS b")
	if len(rows) != 1 || rows[0]["a"] == rows[0]["b"] {
		t.Fatalf("whole path create: %v", rows)
	}
	rows = withRows(t, db, "MATCH (n:Person {name: 'Ada'}) RETURN count(*) AS total")
	if fmt.Sprint(rows[0]["total"]) != "2" {
		t.Fatalf("whole pattern should create unbound Ada: %v", rows)
	}
	rows = withRows(t, db, "MERGE (a:Person {name: 'Ada'})-[r:NEW]->(b:Person {name: 'Zed'})-[s:BACK]->(a) RETURN count(*) AS total")
	if fmt.Sprint(rows[0]["total"]) != "1" {
		t.Fatalf("whole path match: %v", rows)
	}
	rows = withRows(t, db, "WITH 'inbound' AS name MERGE (a:Inbound {name: name})<-[r:TO]-(b:Origin) RETURN type(r) AS kind")
	if len(rows) != 1 || rows[0]["kind"] != "TO" {
		t.Fatalf("WITH/incoming: %v", rows)
	}
	rows = withRows(t, db, "MERGE (a:Inbound {name: 'inbound'})-[r:TO]-(b:Origin) RETURN count(*) AS total")
	if fmt.Sprint(rows[0]["total"]) != "1" {
		t.Fatalf("undirected reuses reverse edge: %v", rows)
	}
}

func TestMergeMultipleMatchesAndEmptyInput(t *testing.T) {
	db := openWithDB(t)
	rows := withRows(t, db, "MERGE (n:Person) ON MATCH SET n.seen = true RETURN count(*) AS total")
	if fmt.Sprint(rows[0]["total"]) != "3" {
		t.Fatalf("multiple matches: %v", rows)
	}
	withRows(t, db, "MATCH (n:Missing) MERGE (m:MustNotExist) RETURN m")
	rows = withRows(t, db, "MATCH (m:MustNotExist) RETURN count(*) AS total")
	if fmt.Sprint(rows[0]["total"]) != "0" {
		t.Fatalf("empty input created a node: %v", rows)
	}
}

func TestMergeActionsKeepMatchSetAndRefreshSharedTargets(t *testing.T) {
	db := openWithDB(t)
	withRows(t, db, "MERGE (a:Hub {key: 1})-[:R]->(:Leaf {key: 1}) RETURN a")
	withRows(t, db, "MATCH (a:Hub) MERGE (a)-[:R]->(:Leaf {key: 2}) RETURN a")
	rows := withRows(t, db, "MERGE (a:Hub {key: 1})-[:R]->(b:Leaf) ON MATCH SET a.visits = coalesce(a.visits, 0) + 1, a.key = 2 RETURN a.visits AS visits")
	if len(rows) != 2 || rows[0]["visits"] != int64(2) || rows[1]["visits"] != int64(2) {
		t.Fatalf("shared target actions: %v", rows)
	}
	for _, key := range []int{3, 3} {
		result, err := db.Query("MERGE (a:Hub {key: $key}) ON CREATE SET a.branch = 'create' ON MATCH SET a.branch = 'match' SET a.always = true RETURN a.always AS always", map[string]any{"key": key})
		if err != nil || len(result.Rows) != 1 || result.Rows[0]["always"] != true {
			t.Fatalf("unconditional SET: %v, %v", result.Rows, err)
		}
	}
	if err := db.Update(func(tx *Tx) error {
		if _, err := tx.Query("CREATE (:Earlier)", nil); err != nil {
			return err
		}
		if _, err := tx.Query("MERGE (a:LateFailure) ON CREATE SET a.changed = true RETURN 1 / 0 AS invalid", nil); err == nil {
			t.Fatal("late expression error accepted")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	rows = withRows(t, db, "MATCH (a:Earlier) RETURN count(*) AS count")
	if rows[0]["count"] != int64(1) {
		t.Fatalf("prior transaction statement lost: %v", rows)
	}
	rows = withRows(t, db, "MATCH (a:LateFailure) RETURN count(*) AS count")
	if rows[0]["count"] != int64(0) {
		t.Fatalf("late error persisted MERGE: %v", rows)
	}
}

func TestMergeRejectsAndRollsBack(t *testing.T) {
	db := openWithDB(t)
	for _, query := range []string{
		"MERGE (n:Bad {key: null}) RETURN n",
		"MERGE (n:Bad {key: n.key}) RETURN n",
		"MERGE (n:Bad)-[:R*1..2]->(m) RETURN n",
		"MERGE (n:Bad)-[]->(m) RETURN n",
		"MERGE (n:Bad), (m) RETURN n",
		"MERGE (n:Bad) ON CREATE SET n.bad = 1 / 0 RETURN n",
		"UNWIND [1, 0] AS divisor MERGE (n:Bad {key: divisor}) ON CREATE SET n.bad = 1 / divisor RETURN n",
		"MERGE (n:Bad {key: 1})-[:R]->(n:Bad {key: 2}) RETURN n",
	} {
		if _, err := db.Query(query, nil); err == nil {
			t.Errorf("query should fail: %s", query)
		}
	}
	rows := withRows(t, db, "MATCH (n:Bad) RETURN count(*) AS total")
	if fmt.Sprint(rows[0]["total"]) != "0" {
		t.Fatalf("failed MERGE left mutations: %v", rows)
	}
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Query("MERGE (n:Person)", nil); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("read-only MERGE: %v", err)
	}
}

func TestMergeLimitsAndDurability(t *testing.T) {
	path := filepath.Join(t.TempDir(), "merge.ltdb")
	db, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	_, err = db.QueryContext(context.Background(), "UNWIND [1, 2, 3] AS key MERGE (n:Limited {key: key}) RETURN n", nil, QueryOptions{MaxWork: 2})
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("work limit: %v", err)
	}
	rows := withRows(t, db, "MATCH (n:Limited) RETURN count(*) AS total")
	if fmt.Sprint(rows[0]["total"]) != "0" {
		t.Fatalf("limited query committed: %v", rows)
	}
	_, err = db.QueryContext(t.Context(), "MERGE (n:Limited) RETURN range(1, 10000) AS values", nil, QueryOptions{MaxBytes: 2048})
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("post-mutation result budget: %v", err)
	}
	rows = withRows(t, db, "MATCH (n:Limited) RETURN count(*) AS total")
	if rows[0]["total"] != int64(0) {
		t.Fatalf("result budget failure persisted MERGE: %v", rows)
	}
	withRows(t, db, "MERGE (a:Persist {key: 1})-[:R]->(b:Persist {key: 2}) RETURN a")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	rows = withRows(t, db, "MERGE (a:Persist {key: 1})-[:R]->(b:Persist {key: 2}) RETURN count(*) AS total")
	if fmt.Sprint(rows[0]["total"]) != "1" {
		t.Fatalf("reopened MERGE: %v", rows)
	}
}
