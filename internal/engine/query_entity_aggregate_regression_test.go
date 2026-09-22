package engine

import "testing"

func TestEntityAggregatePublicResultsAndOrdering(t *testing.T) {
	db := openWithDB(t)
	for _, query := range []string{
		"MATCH (n:Person) RETURN min(n) AS m",
		"MATCH (n:Person) WITH min(n) AS m RETURN m",
	} {
		result, err := db.Query(query, nil)
		if err != nil {
			t.Fatal(err)
		}
		node, ok := result.Rows[0]["m"].(Node)
		if !ok || node.ID == 0 || node.Properties["name"] != "Ada" {
			t.Fatalf("%s: %#v", query, result.Rows)
		}
		node.Properties["name"] = "changed"
	}
	for _, query := range []string{
		"MATCH (n:Person) RETURN collect(n) AS ns",
		"MATCH (n:Person) WITH collect(n) AS ns RETURN ns",
	} {
		result, err := db.Query(query, nil)
		if err != nil {
			t.Fatal(err)
		}
		list, ok := result.Rows[0]["ns"].([]any)
		if !ok || len(list) != 3 {
			t.Fatalf("%s: %#v", query, result.Rows)
		}
		for _, item := range list {
			if _, ok := item.(Node); !ok {
				t.Fatalf("internal entity leaked: %T", item)
			}
		}
	}
	for _, aggregate := range []string{"min", "collect"} {
		query := "MATCH (n:Person) WITH id(n) AS k, " + aggregate + "(n) AS m ORDER BY m DESC RETURN k"
		result, err := db.Query(query, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Rows) != 3 || result.Rows[0]["k"] != int64(3) {
			t.Fatalf("%s: %#v", query, result.Rows)
		}
	}
}

func TestEntityAggregateRetainsRoleAcrossWith(t *testing.T) {
	db := openWithDB(t)
	for _, query := range []string{
		"MATCH (n:Person) WITH min(n) AS m WITH m RETURN id(m) AS id",
		"MATCH (n:Person)-[e:KNOWS]->(m) WITH min(e) AS r WITH r RETURN id(r) AS id",
	} {
		result, err := db.Query(query, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Rows) != 1 || result.Rows[0]["id"] == nil {
			t.Fatalf("%s: %#v", query, result.Rows)
		}
	}
	for _, agg := range []string{"min", "collect"} {
		result, err := db.Query("MATCH (n:Person) WITH id(n) AS k, "+agg+"(n) AS m WITH k, m ORDER BY m DESC RETURN k", nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Rows) != 3 || result.Rows[0]["k"] != int64(3) {
			t.Fatal(result.Rows)
		}
	}
}
