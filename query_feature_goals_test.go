package latticedb

import (
	"path/filepath"
	"testing"
)

// Exercise the feature goals through the public API, including nested edge
// conversion and the shared WITH/mutation pipeline.
func TestQueryFeatureGoalsTogether(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "goals.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Query("MERGE (a:Goal {key: 1})-[:NEXT]->(b:Goal {key: 2})", nil); err != nil {
		t.Fatal(err)
	}
	result, err := db.Query("UNWIND [2, 2] AS key MERGE (n:Goal {key: key}) ON MATCH SET n.visits = coalesce(n.visits, 0) + 1 RETURN sum(DISTINCT key ^ 2) AS total", nil)
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["total"] != float64(4) {
		t.Fatalf("MERGE/arithmetic/DISTINCT: %#v, %v", result.Rows, err)
	}
	result, err = db.Query("MATCH (a:Goal {key: 1})-[path:NEXT*0..1]->(b) WITH b, path, size(path) AS hops RETURN b.key AS key, path, hops ORDER BY hops", nil)
	if err != nil || len(result.Rows) != 2 {
		t.Fatalf("variable path/WITH: %#v, %v", result.Rows, err)
	}
	if result.Rows[0]["hops"] != int64(0) || result.Rows[1]["hops"] != int64(1) {
		t.Fatalf("path lengths: %#v", result.Rows)
	}
	path := result.Rows[1]["path"].([]any)
	if len(path) != 1 {
		t.Fatalf("path edges: %#v", path)
	}
	if edge, ok := path[0].(Edge); !ok || edge.Type != "NEXT" {
		t.Fatalf("public relationship conversion: %#v", path[0])
	}
	result, err = db.Query("RETURN (2 + 3) * 4 AS value", nil)
	if err != nil || result.Rows[0]["value"] != int64(20) {
		t.Fatalf("standalone RETURN: %#v, %v", result.Rows, err)
	}
}
