package engine

import "testing"

func TestQueryGrammarMatrix(t *testing.T) {
	accepted := map[string]string{
		"create anonymous node":         `CREATE ()`,
		"create labeled node":           `CREATE (n:Person:Employee)`,
		"create node properties":        `CREATE (n:Person {name: "Alice", age: -1, ratio: 1.5, active: true, disabled: false, note: null, copy: $name, nested: {team: 'graph'}}) RETURN id(n) AS id`,
		"match anonymous node":          `MATCH ()`,
		"match labeled node":            `MATCH (n:Person:Employee) RETURN count(n)`,
		"match literal properties":      `MATCH (n:Person {name: 'Alice', active: true}) RETURN n.name AS name`,
		"match parameter properties":    `MATCH ({name: $name}) RETURN count(*) AS count`,
		"match multiple patterns":       `MATCH (a:Person), (b:Person) WHERE a.name = $a AND b.name = $b RETURN a.name AS source, b.name AS target`,
		"match typed edge":              `MATCH (:Person)-[:KNOWS]->(:Person) RETURN count(*) AS count`,
		"match bound edge":              `MATCH (a)-[r:KNOWS]->(b) RETURN id(a) AS source, id(r) AS edge, id(b) AS target`,
		"match untyped edge":            `MATCH (a)-[r]->(b) RETURN count(r) AS count`,
		"match endpoint properties":     `MATCH (:Person {name: $from})-[:KNOWS]->(:Person {name: $to}) RETURN count(*) AS count`,
		"match incoming edge":           `MATCH (a)<-[r:KNOWS]-(b) RETURN a, r, b`,
		"match edge properties":         `MATCH (a)-[r:KNOWS {since: $since}]->(b) RETURN id(r)`,
		"match anonymous edge property": `MATCH (a)-[:KNOWS {since: 2024}]->(b) RETURN b`,
		"match chained path":            `MATCH (a)-[:KNOWS]->(b)-[:WORKS_WITH]->(c) RETURN c`,
		"where string equality":         `MATCH (n) WHERE n.name = "Alice" RETURN n.name`,
		"where single quoted equality":  `MATCH (n) WHERE n.name = 'Alice' RETURN n.name`,
		"where parameter equality":      `MATCH (n) WHERE n.name = $name RETURN n.name`,
		"where numeric equality":        `MATCH (n) WHERE n.score = -1.5 RETURN n.score`,
		"where boolean equality":        `MATCH (n) WHERE n.active = true RETURN n.active`,
		"where null equality":           `MATCH (n) WHERE n.note = null RETURN n.note`,
		"where binding id":              `MATCH (n) WHERE id(n) = $id RETURN id(n) AS id`,
		"where is null":                 `MATCH (n) WHERE n.note IS NULL RETURN count(n) AS count`,
		"where is not null":             `MATCH (n) WHERE n.name IS NOT NULL RETURN count(n) AS count`,
		"where vector search":           `MATCH (n) WHERE n.embedding <=> $vector RETURN n.name LIMIT 1`,
		"where full text search":        `MATCH (n) WHERE n.text @@ "graph" RETURN n.name LIMIT 1`,
		"where conjunction":             `MATCH (n) WHERE n.active = true AND n.name IS NOT NULL RETURN n.name`,
		"where comparisons":             `MATCH (n) WHERE n.age <> 1 AND n.age >= 2 AND n.age <= 3 AND n.age > 1 AND n.age < 4 RETURN n`,
		"where disjunction":             `MATCH (n) WHERE n.active = true OR n.admin = true RETURN n`,
		"where not and parentheses":     `MATCH (n) WHERE NOT (n.active = true OR n.age < 18) RETURN n`,
		"where in parameter":            `MATCH (n) WHERE n.kind IN $kinds RETURN n`,
		"where string predicates":       `MATCH (n) WHERE n.name STARTS WITH "A" OR n.name ENDS WITH $suffix OR n.name CONTAINS "mid" RETURN n`,
		"return count star":             `MATCH (n) RETURN count(*)`,
		"return count binding":          `MATCH (n) RETURN count(n) AS count`,
		"return binding":                `MATCH (n) RETURN n AS node`,
		"return property":               `MATCH (n) RETURN n.name`,
		"return binding id":             `MATCH (n) RETURN id(n) AS id`,
		"return multiple projections":   `MATCH (n) RETURN id(n) AS id, n.name AS name`,
		"order by property":             `MATCH (n) RETURN n.name ORDER BY n.name ASC`,
		"order by binding id":           `MATCH (n) RETURN id(n) ORDER BY id(n) DESC`,
		"order by multiple expressions": `MATCH (n) RETURN id(n), n.name ORDER BY n.name ASC, id(n) DESC LIMIT 2`,
		"order by return alias":         `MATCH (n) RETURN n.name AS name ORDER BY name`,
		"limit zero":                    `MATCH (n) RETURN n.name LIMIT 0`,
		"parameterized limit":           `MATCH (n) RETURN n.name LIMIT $limit`,
		"skip and limit":                `MATCH (n) RETURN n.name SKIP $skip LIMIT $limit`,
		"set property":                  `MATCH (n) SET n.name = $name`,
		"set property null":             `MATCH (n) SET n.name = null`,
		"replace properties":            `MATCH (n) SET n = {name: $name}`,
		"merge properties":              `MATCH (n) SET n += $properties`,
		"multiple set items":            `MATCH (n) SET n.a = 1, n.b = 2`,
		"set then return":               `MATCH (n) SET n.name = $name RETURN n.name AS name`,
		"set label":                     `MATCH (n) SET n:Active RETURN n`,
		"create edge":                   `MATCH (a), (b) CREATE (a)-[:KNOWS {since: 2024}]->(b)`,
		"create edge then return":       `MATCH (a), (b) CREATE (a)-[r:KNOWS]->(b) RETURN id(r) AS id`,
		"create incoming edge":          `MATCH (a), (b) CREATE (a)<-[r:KNOWS]-(b) RETURN id(r) AS id`,
		"remove property":               `MATCH (n) REMOVE n.name`,
		"remove then return":            `MATCH (n) REMOVE n.name RETURN n.name AS name`,
		"remove label":                  `MATCH (n) REMOVE n:Employee`,
		"remove multiple items":         `MATCH (n) REMOVE n.name, n:Employee`,
		"delete node":                   `MATCH (n) DELETE n`,
		"delete edge and nodes":         `MATCH (a)-[r]->(b) DELETE r, a, b`,
		"detach delete node":            `MATCH (n) DETACH DELETE n`,
		"unwind parameter":              `UNWIND $items AS item RETURN item AS value`,
		"unwind count":                  `UNWIND $items AS item RETURN count(item) AS count`,
		"unwind order and limit":        `UNWIND $items AS item RETURN item ORDER BY item DESC LIMIT 2`,
		"quoted structural characters":  `MATCH (n {text: 'a,b) RETURN value AND more'}) RETURN n.text`,
		"escaped double quoted string":  `MATCH (n {text: "a\"b"}) RETURN n.text`,
		"nested map value":              `CREATE (n {meta: {team: "graph", nested: {active: true}}})`,
		"trailing semicolon":            `MATCH (n) RETURN n;`,
		"multiline whitespace":          "MATCH\t(n)\nWHERE n.name = 'A  B'\r\nRETURN n.name;",
	}
	for name, query := range accepted {
		t.Run("accept/"+name, func(t *testing.T) {
			if _, err := parseQuery(query); err != nil {
				t.Fatalf("parseQuery(%q): %v", query, err)
			}
		})
	}

	rejected := map[string]string{
		"empty query":                 ``,
		"unsupported root":            `RETURN 1`,
		"lowercase keyword":           `match (n) RETURN n`,
		"optional match":              `OPTIONAL MATCH (n) RETURN n`,
		"merge":                       `MERGE (n:Person)`,
		"missing match pattern":       `MATCH`,
		"unterminated node":           `MATCH (n RETURN n`,
		"undirected edge":             `MATCH (a)-[:KNOWS]-(b) RETURN a`,
		"variable length edge":        `MATCH (a)-[:KNOWS*1..3]->(b) RETURN b`,
		"empty edge type":             `MATCH (a)-[:]->(b) RETURN b`,
		"compound top level create":   `CREATE (a)-[:KNOWS]->(b)`,
		"empty expression":            `MATCH (n) SET n.name =`,
		"empty parameter":             `MATCH (n) SET n.name = $`,
		"property expression value":   `MATCH (n) SET n.copy = n.name`,
		"create edge missing type":    `MATCH (a), (b) CREATE (a)-[]->(b)`,
		"create edge trailing text":   `MATCH (a), (b) CREATE (a)-[:KNOWS {since: 2024} trailing]->(b)`,
		"duplicate return alias":      `MATCH (n) RETURN id(n) AS value, n.name AS value`,
		"literal return":              `MATCH (n) RETURN 1`,
		"negative limit":              `MATCH (n) RETURN n LIMIT -1`,
		"union":                       `MATCH (n) RETURN n UNION MATCH (m) RETURN m`,
		"with":                        `MATCH (n) WITH n RETURN n`,
		"unwind without as":           `UNWIND $items RETURN item`,
		"unwind without return":       `UNWIND $items AS item`,
		"invalid binding identifier":  `MATCH (bad-name) RETURN bad-name`,
		"empty label":                 `CREATE (n:)`,
		"invalid label identifier":    `CREATE (n:Bad-Label)`,
		"invalid property identifier": `CREATE (n {bad-key: 1})`,
		"invalid return alias":        `MATCH (n) RETURN n.name AS bad-alias`,
		"invalid count alias":         `MATCH (n) RETURN count(n) AS bad-alias`,
		"invalid remove label":        `MATCH (n) REMOVE n:Bad-Label`,
		"empty property value":        `CREATE (n {name:})`,
		"duplicate property key":      `CREATE (n {name: "Alice", name: "Bob"})`,
		"multiple semicolons":         `MATCH (n) RETURN n;;`,
	}
	for name, query := range rejected {
		t.Run("reject/"+name, func(t *testing.T) {
			if _, err := parseQuery(query); err == nil {
				t.Fatalf("parseQuery(%q) unexpectedly succeeded", query)
			}
		})
	}
}
