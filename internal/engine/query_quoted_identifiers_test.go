package engine

import (
	"path/filepath"
	"testing"
)

func TestQuotedQueryIdentifiers(t *testing.T) {
	queries := []string{
		"CREATE (`node name`:`label:name` {`property key`: 1})",
		"MATCH (`node name`:`label:name`) WHERE `node name`.`property key` = 1 RETURN `node name` AS `column name`",
		"MATCH (`node name`:`label:name`) RETURN `node name` ORDER BY `node name` DESC",
		"UNWIND $`parameter name` AS `value name` RETURN `value name` AS `output name`",
		"MATCH (`node name`) WHERE id(`node name`) = 1 RETURN count(`node name`) AS `count name`",
		"MATCH (`node name`)-[`edge ]-> type`:`type: name`]->(`other node`) RETURN `edge ]-> type`.`property.key` AS `result name` ORDER BY `edge ]-> type`.`property.key` DESC",
	}
	for _, query := range queries {
		if _, err := parseQuery(query); err != nil {
			t.Fatalf("parseQuery(%q): %v", query, err)
		}
	}
}

func TestQuotedIdentifierEscapesAndValidation(t *testing.T) {
	got, err := parseQueryIdentifier("`a``b\\c`")
	if err != nil || got != "a`b\\c" {
		t.Fatalf("parseQueryIdentifier = %q, %v", got, err)
	}
	for _, query := range []string{
		"CREATE (n:``)",
		"CREATE (n:`unterminated)",
		"CREATE (n:`a`trailing`)",
		"CREATE (n:foo`)",
	} {
		if _, err := parseQuery(query); err == nil {
			t.Fatalf("parseQuery(%q) unexpectedly succeeded", query)
		}
	}
	for _, identifier := range []string{"`\x00`", string([]byte{'`', 0xff, '`'})} {
		if _, err := parseQueryIdentifier(identifier); err == nil {
			t.Fatalf("parseQueryIdentifier(%q) unexpectedly succeeded", identifier)
		}
	}
}

func TestQuotedIdentifiersEndToEnd(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "quoted-identifiers.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Query("CREATE (`node name`:`label:name` {`property key`: 'value'})", nil); err != nil {
		t.Fatal(err)
	}
	result, err := db.Query("MATCH (`node name`:`label:name`) WHERE `node name`.`property key` = 'value' RETURN count(`node name`) AS `count name`", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := result.Rows[0]["count name"]; got != int64(1) {
		t.Fatalf("count = %#v, want 1", got)
	}
}

func TestQuotedIdentifierPathAndDelimiterNames(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "quoted-path.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		left, err := tx.CreateNode(CreateNodeOptions{})
		if err != nil {
			return err
		}
		right, err := tx.CreateNode(CreateNodeOptions{})
		if err != nil {
			return err
		}
		_, err = tx.CreateEdge(left.ID, right.ID, "type: name", CreateEdgeOptions{Properties: map[string]any{"property.key": 1}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	query := "MATCH (a)-[r:`type: name`]->(b) WHERE r.`property.key` = 1 RETURN count(r) AS `count: name` ORDER BY `count: name` DESC"
	result, err := db.Query(query, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := result.Rows[0]["count: name"]; got != int64(1) {
		t.Fatalf("count = %#v, want 1", got)
	}
}

func TestQuotedIdentifierUnicodeRemainsDistinct(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "quoted-unicode.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	composed, decomposed := "café", "café"
	if composed == decomposed {
		t.Fatal("test identifiers unexpectedly equal")
	}
	for _, identifier := range []string{composed, decomposed} {
		query := "CREATE (n {`" + identifier + "`: 1})"
		if _, err := db.Query(query, nil); err != nil {
			t.Fatalf("create %q: %v", identifier, err)
		}
	}
	for _, identifier := range []string{composed, decomposed} {
		query := "MATCH (n {`" + identifier + "`: 1}) RETURN count(n) AS count"
		result, err := db.Query(query, nil)
		if err != nil || result.Rows[0]["count"] != int64(1) {
			t.Fatalf("match %q = %#v, %v", identifier, result.Rows, err)
		}
	}
}
