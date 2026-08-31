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
		"return count star":             `MATCH (n) RETURN count(*)`,
		"return count binding":          `MATCH (n) RETURN count(n) AS count`,
		"return binding":                `MATCH (n) RETURN n AS node`,
		"return property":               `MATCH (n) RETURN n.name`,
		"return binding id":             `MATCH (n) RETURN id(n) AS id`,
		"return multiple projections":   `MATCH (n) RETURN id(n) AS id, n.name AS name`,
		"order by property":             `MATCH (n) RETURN n.name ORDER BY n.name ASC`,
		"order by binding id":           `MATCH (n) RETURN id(n) ORDER BY id(n) DESC`,
		"order by multiple expressions": `MATCH (n) RETURN id(n), n.name ORDER BY n.name ASC, id(n) DESC LIMIT 2`,
		"limit zero":                    `MATCH (n) RETURN n.name LIMIT 0`,
		"set property":                  `MATCH (n) SET n.name = $name`,
		"set property null":             `MATCH (n) SET n.name = null`,
		"replace properties":            `MATCH (n) SET n = {name: $name}`,
		"merge properties":              `MATCH (n) SET n += $properties`,
		"create edge":                   `MATCH (a), (b) CREATE (a)-[:KNOWS {since: 2024}]->(b)`,
		"remove property":               `MATCH (n) REMOVE n.name`,
		"remove label":                  `MATCH (n) REMOVE n:Employee`,
		"remove multiple items":         `MATCH (n) REMOVE n.name, n:Employee`,
		"delete node":                   `MATCH (n) DELETE n`,
		"delete edge and nodes":         `MATCH (a)-[r]->(b) DELETE r, a, b`,
		"unwind parameter":              `UNWIND $items AS item RETURN item AS value`,
		"unwind count":                  `UNWIND $items AS item RETURN count(item) AS count`,
		"unwind order and limit":        `UNWIND $items AS item RETURN item ORDER BY item DESC LIMIT 2`,
		"quoted structural characters":  `MATCH (n {text: 'a,b) RETURN value AND more'}) RETURN n.text`,
		"escaped double quoted string":  `MATCH (n {text: "a\"b"}) RETURN n.text`,
		"nested map value":              `CREATE (n {meta: {team: "graph", nested: {active: true}}})`,
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
		"incoming edge":               `MATCH (a)<-[:KNOWS]-(b) RETURN a`,
		"undirected edge":             `MATCH (a)-[:KNOWS]-(b) RETURN a`,
		"variable length edge":        `MATCH (a)-[:KNOWS*1..3]->(b) RETURN b`,
		"empty edge type":             `MATCH (a)-[:]->(b) RETURN b`,
		"compound top level create":   `CREATE (a)-[:KNOWS]->(b)`,
		"where disjunction":           `MATCH (n) WHERE n.a = 1 OR n.b = 2 RETURN n`,
		"where inequality":            `MATCH (n) WHERE n.age > 1 RETURN n`,
		"empty expression":            `MATCH (n) SET n.name =`,
		"empty parameter":             `MATCH (n) SET n.name = $`,
		"property expression value":   `MATCH (n) SET n.copy = n.name`,
		"multiple set clauses":        `MATCH (n) SET n.a = 1, n.b = 2`,
		"create edge missing type":    `MATCH (a), (b) CREATE (a)-[]->(b)`,
		"create edge trailing text":   `MATCH (a), (b) CREATE (a)-[:KNOWS {since: 2024} trailing]->(b)`,
		"duplicate return alias":      `MATCH (n) RETURN id(n) AS value, n.name AS value`,
		"literal return":              `MATCH (n) RETURN 1`,
		"order by return alias":       `MATCH (n) RETURN n.name AS name ORDER BY name`,
		"negative limit":              `MATCH (n) RETURN n LIMIT -1`,
		"non numeric limit":           `MATCH (n) RETURN n LIMIT $limit`,
		"skip":                        `MATCH (n) RETURN n SKIP 1`,
		"union":                       `MATCH (n) RETURN n UNION MATCH (m) RETURN m`,
		"with":                        `MATCH (n) WITH n RETURN n`,
		"detach delete":               `MATCH (n) DETACH DELETE n`,
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
	}
	for name, query := range rejected {
		t.Run("reject/"+name, func(t *testing.T) {
			if _, err := parseQuery(query); err == nil {
				t.Fatalf("parseQuery(%q) unexpectedly succeeded", query)
			}
		})
	}
}
