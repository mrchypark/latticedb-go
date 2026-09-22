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

func TestWithComputedEntityProperties(t *testing.T) {
	db := openWithDB(t)
	result, err := db.Query("MATCH (n:Person) WITH coalesce(n) AS x RETURN x.name AS name ORDER BY name", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 3 || result.Rows[0]["name"] != "Ada" || result.Rows[2]["name"] != "Cy" {
		t.Fatalf("lost computed entity properties: %#v", result.Rows)
	}
}

func TestCollectedEntityPropertiesAfterUnwindAndMutation(t *testing.T) {
	db := openWithDB(t)
	for _, query := range []string{
		"MATCH (n:Person) WITH collect(n) AS ns UNWIND ns AS x RETURN x.name AS name ORDER BY name",
		"MATCH (n:Person) WITH collect(n) AS ns WITH coalesce(ns) AS ns UNWIND ns AS x RETURN x.name AS name ORDER BY name",
	} {
		result, err := db.Query(query, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Rows) != 3 || result.Rows[0]["name"] != "Ada" {
			t.Fatalf("%s: %#v", query, result.Rows)
		}
	}
	result, err := db.Query("MATCH (n:Person) WITH collect(n) AS ns MATCH (p:Person) SET p.flag = true RETURN ns", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range result.Rows {
		for _, item := range row["ns"].([]any) {
			if item.(Node).Properties["flag"] != true {
				t.Fatalf("stale entity: %#v", item)
			}
		}
	}
}

func TestCollectedEntityContinuationSnapshots(t *testing.T) {
	db := openWithDB(t)
	if _, err := db.Query("MATCH (n)-[e:KNOWS]->(m) SET e.name = 'Before' RETURN e", nil); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		"MATCH (n:Person) WITH collect(n) AS ns WITH head(ns) AS x RETURN x.name AS name",
		"MATCH (n)-[e:KNOWS]->(m) WITH collect(e) AS es UNWIND es AS x RETURN x.name AS name",
		"MATCH (n)-[e:KNOWS]->(m) WITH coalesce(e) AS x RETURN x.name AS name",
	} {
		result, err := db.Query(query, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Rows) == 0 || result.Rows[0]["name"] == nil {
			t.Fatalf("%s: %#v", query, result.Rows)
		}
	}
	for _, test := range []struct{ query, want string }{
		{"MATCH (n:Person) WITH n, collect(n) AS ns, collect(n.name) AS names MATCH (n) SET n.name = 'Changed' WITH ns, names WITH head(ns) AS x, names RETURN x.name AS current, names", "Ada"},
		{"MATCH (n)-[e:KNOWS]->(m) WITH e, collect(e) AS ns, collect(e.name) AS names MATCH (a)-[e:KNOWS]->(b) SET e.name = 'Changed' WITH ns, names WITH head(ns) AS x, names RETURN x.name AS current, names", "Before"},
	} {
		result, err := db.Query(test.query, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Rows) == 0 || result.Rows[0]["current"] != "Changed" || result.Rows[0]["names"].([]any)[0] != test.want {
			t.Fatalf("snapshot mismatch: %#v", result.Rows)
		}
	}
}
