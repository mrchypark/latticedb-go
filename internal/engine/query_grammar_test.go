package engine

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

const auditedCypherParserDigest = "14b50baa6c3582cbb8e2a0e1f674785a94ccd007b24bf8726c0cc651e31e2f52"

func TestSupportedCypherGrammarContract(t *testing.T) {
	grammar, err := os.ReadFile(filepath.Join("testdata", "query_grammar.ebnf"))
	if err != nil {
		t.Fatal(err)
	}
	document, err := os.ReadFile(filepath.Join("..", "..", "docs", "engine_conformance.md"))
	if err != nil {
		t.Fatal(err)
	}
	const begin = "<!-- BEGIN supported-cypher-grammar -->"
	const end = "<!-- END supported-cypher-grammar -->"
	start := bytes.Index(document, []byte(begin))
	finish := bytes.Index(document, []byte(end))
	if bytes.Count(document, []byte(begin)) != 1 || bytes.Count(document, []byte(end)) != 1 || start < 0 || finish < 0 || finish <= start {
		t.Fatalf("docs/engine_conformance.md must contain one %s ... %s block", begin, end)
	}
	block := document[start+len(begin) : finish]
	if !bytes.Equal(block, markdownGrammarBlock(grammar)) {
		t.Fatal("documented Cypher grammar differs from the executable grammar contract")
	}

	digest := cypherParserDigest(t)
	if digest != auditedCypherParserDigest {
		t.Fatalf("Cypher parser changed (%s); audit the grammar, accepted/rejected matrix, and runtime conformance before updating auditedCypherParserDigest", digest)
	}
}

func markdownGrammarBlock(grammar []byte) []byte {
	newline := []byte("\n")
	if bytes.Contains(grammar, []byte("\r\n")) {
		newline = []byte("\r\n")
	}
	var want bytes.Buffer
	want.Write(newline)
	want.WriteString("```text")
	want.Write(newline)
	want.Write(grammar)
	want.WriteString("```")
	want.Write(newline)
	return want.Bytes()
}

func TestMarkdownGrammarBlockPreservesLineEndings(t *testing.T) {
	for _, grammar := range []string{"query = MATCH\n", "query = MATCH\r\n"} {
		newline := grammar[len(grammar)-1:]
		if strings.HasSuffix(grammar, "\r\n") {
			newline = "\r\n"
		}
		want := newline + "```text" + newline + grammar + "```" + newline
		if got := string(markdownGrammarBlock([]byte(grammar))); got != want {
			t.Fatalf("markdownGrammarBlock(%q) = %q, want %q", grammar, got, want)
		}
	}
}

func cypherParserDigest(t *testing.T) string {
	t.Helper()
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, "query.go", nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatal(err)
	}
	byName := make(map[string][]*ast.FuncDecl)
	for _, declaration := range file.Decls {
		if function, ok := declaration.(*ast.FuncDecl); ok {
			byName[function.Name.Name] = append(byName[function.Name.Name], function)
		}
	}
	reachable := make(map[*ast.FuncDecl]bool)
	queue := append([]*ast.FuncDecl(nil), byName["parseQuery"]...)
	for len(queue) != 0 {
		function := queue[0]
		queue = queue[1:]
		if reachable[function] {
			continue
		}
		reachable[function] = true
		ast.Inspect(function.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			var name string
			switch called := call.Fun.(type) {
			case *ast.Ident:
				name = called.Name
			case *ast.SelectorExpr:
				name = called.Sel.Name
			}
			queue = append(queue, byName[name]...)
			return true
		})
	}
	functions := make([]*ast.FuncDecl, 0, len(reachable))
	for function := range reachable {
		functions = append(functions, function)
	}
	sort.Slice(functions, func(i, j int) bool { return functions[i].Pos() < functions[j].Pos() })
	var source bytes.Buffer
	for _, function := range functions {
		if err := format.Node(&source, set, function); err != nil {
			t.Fatal(err)
		}
		source.WriteByte('\n')
	}
	return fmt.Sprintf("%x", sha256.Sum256(source.Bytes()))
}

func TestQueryGrammarASTShape(t *testing.T) {
	plan, err := parseQuery("MATCH (n) WHERE n.a = 1 OR n.b = 2 AND NOT (n.c = 3 OR n.d = 4) RETURN n")
	if err != nil {
		t.Fatal(err)
	}
	or, ok := plan.wherePredicate.(booleanPredicate)
	if !ok || or.Operator != "OR" || len(or.Items) != 2 {
		t.Fatalf("root predicate = %#v, want two-item OR", plan.wherePredicate)
	}
	and, ok := or.Items[1].(booleanPredicate)
	if !ok || and.Operator != "AND" || len(and.Items) != 2 {
		t.Fatalf("right predicate = %#v, want two-item AND", or.Items[1])
	}
	not, ok := and.Items[1].(notPredicate)
	if !ok {
		t.Fatalf("AND right predicate = %#v, want NOT", and.Items[1])
	}
	parenthesized, ok := not.Item.(booleanPredicate)
	if !ok || parenthesized.Operator != "OR" || len(parenthesized.Items) != 2 {
		t.Fatalf("NOT predicate = %#v, want parenthesized two-item OR", not.Item)
	}

	plan, err = parseQuery("UNWIND $ids AS wanted MATCH (n) WHERE n.id = wanted SET n.active = true RETURN DISTINCT n.id AS id ORDER BY id SKIP 1 LIMIT 2")
	if err != nil {
		t.Fatal(err)
	}
	if plan.unwindClause == nil || len(plan.matchPatterns) != 1 || len(plan.whereClauses) != 1 || len(plan.setClauses) != 1 || plan.returnClause == nil || !plan.returnClause.Distinct || len(plan.orderClauses) != 1 || plan.skipExpr == nil || plan.limitExpr == nil {
		t.Fatalf("clause attachment changed: %#v", plan)
	}
}

func TestQueryGrammarMatrix(t *testing.T) {
	accepted := map[string]string{
		"create anonymous node":            `CREATE ()`,
		"create labeled node":              `CREATE (n:Person:Employee)`,
		"create node properties":           `CREATE (n:Person {name: "Alice", age: -1, ratio: 1.5, active: true, disabled: false, note: null, copy: $name, nested: {team: 'graph'}}) RETURN id(n) AS id`,
		"match anonymous node":             `MATCH ()`,
		"match without terminal":           `MATCH (n)`,
		"match labeled node":               `MATCH (n:Person:Employee) RETURN count(n)`,
		"match literal properties":         `MATCH (n:Person {name: 'Alice', active: true}) RETURN n.name AS name`,
		"match parameter properties":       `MATCH ({name: $name}) RETURN count(*) AS count`,
		"match multiple patterns":          `MATCH (a:Person), (b:Person) WHERE a.name = $a AND b.name = $b RETURN a.name AS source, b.name AS target`,
		"match typed edge":                 `MATCH (:Person)-[:KNOWS]->(:Person) RETURN count(*) AS count`,
		"match bound edge":                 `MATCH (a)-[r:KNOWS]->(b) RETURN id(a) AS source, id(r) AS edge, id(b) AS target`,
		"match untyped edge":               `MATCH (a)-[r]->(b) RETURN count(r) AS count`,
		"match endpoint properties":        `MATCH (:Person {name: $from})-[:KNOWS]->(:Person {name: $to}) RETURN count(*) AS count`,
		"match incoming edge":              `MATCH (a)<-[r:KNOWS]-(b) RETURN a, r, b`,
		"match undirected edge":            `MATCH (a)-[r:KNOWS]-(b) RETURN a, r, b`,
		"match edge properties":            `MATCH (a)-[r:KNOWS {since: $since}]->(b) RETURN id(r)`,
		"match anonymous edge property":    `MATCH (a)-[:KNOWS {since: 2024}]->(b) RETURN b`,
		"match chained path":               `MATCH (a)-[:KNOWS]->(b)-[:WORKS_WITH]->(c) RETURN c`,
		"match chained directions":         `MATCH (a)<-[:KNOWS]-(b)-[:WORKS_WITH]-(c) RETURN a, b, c`,
		"match node property expression":   `MATCH (source), (copy {name: source.name}) RETURN copy`,
		"match edge property expression":   `MATCH (source), (a)-[r:KNOWS {since: source.since}]->(b) RETURN r`,
		"where string equality":            `MATCH (n) WHERE n.name = "Alice" RETURN n.name`,
		"where single quoted equality":     `MATCH (n) WHERE n.name = 'Alice' RETURN n.name`,
		"where parameter equality":         `MATCH (n) WHERE n.name = $name RETURN n.name`,
		"where numeric equality":           `MATCH (n) WHERE n.score = -1.5 RETURN n.score`,
		"where scientific float":           `MATCH (n) WHERE n.score = 1e3 RETURN n.score`,
		"where leading plus integer":       `MATCH (n) WHERE n.score = +1 RETURN n.score`,
		"where leading dot float":          `MATCH (n) WHERE n.score = .5 RETURN n.score`,
		"where boolean equality":           `MATCH (n) WHERE n.active = true RETURN n.active`,
		"where null equality":              `MATCH (n) WHERE n.note = null RETURN n.note`,
		"where binding id":                 `MATCH (n) WHERE id(n) = $id RETURN id(n) AS id`,
		"where is null":                    `MATCH (n) WHERE n.note IS NULL RETURN count(n) AS count`,
		"where is not null":                `MATCH (n) WHERE n.name IS NOT NULL RETURN count(n) AS count`,
		"where vector search":              `MATCH (n) WHERE n.embedding <=> $vector RETURN n.name LIMIT 1`,
		"where full text search":           `MATCH (n) WHERE n.text @@ "graph" RETURN n.name LIMIT 1`,
		"where conjunction":                `MATCH (n) WHERE n.active = true AND n.name IS NOT NULL RETURN n.name`,
		"where comparisons":                `MATCH (n) WHERE n.age <> 1 AND n.age >= 2 AND n.age <= 3 AND n.age > 1 AND n.age < 4 RETURN n`,
		"where disjunction":                `MATCH (n) WHERE n.active = true OR n.admin = true RETURN n`,
		"where not and parentheses":        `MATCH (n) WHERE NOT (n.active = true OR n.age < 18) RETURN n`,
		"where in parameter":               `MATCH (n) WHERE n.kind IN $kinds RETURN n`,
		"where string predicates":          `MATCH (n) WHERE n.name STARTS WITH "A" OR n.name ENDS WITH $suffix OR n.name CONTAINS "mid" RETURN n`,
		"where property comparison":        `MATCH (n) WHERE n.min <= n.value RETURN n`,
		"where without terminal":           `MATCH (n) WHERE n.active = true`,
		"where then set":                   `MATCH (n) WHERE n.active = true SET n.checked = true`,
		"where then create edge":           `MATCH (a), (b) WHERE a.active = true CREATE (a)-[:KNOWS]->(b)`,
		"where then remove":                `MATCH (n) WHERE n.active = true REMOVE n.active`,
		"where then delete":                `MATCH (n) WHERE n.active = true DELETE n`,
		"where then detach delete":         `MATCH (n) WHERE n.active = true DETACH DELETE n`,
		"where in property expression":     `MATCH (n), (m) WHERE n.kind IN m.allowed RETURN n`,
		"where string property expression": `MATCH (n), (m) WHERE n.name STARTS WITH m.prefix RETURN n`,
		"where id property expression":     `MATCH (n), (m) WHERE id(n) = m.nodeID RETURN n`,
		"return count star":                `MATCH (n) RETURN count(*)`,
		"return count binding":             `MATCH (n) RETURN count(n) AS count`,
		"return binding":                   `MATCH (n) RETURN n AS node`,
		"return property":                  `MATCH (n) RETURN n.name`,
		"return binding id":                `MATCH (n) RETURN id(n) AS id`,
		"return multiple projections":      `MATCH (n) RETURN id(n) AS id, n.name AS name`,
		"return distinct":                  `MATCH (n) RETURN DISTINCT n.name AS name ORDER BY name`,
		"return distinct count":            `MATCH (n) RETURN DISTINCT count(*) AS count`,
		"return distinct projections":      `MATCH (n) RETURN DISTINCT n.name AS name, n.kind AS kind ORDER BY name`,
		"order by property":                `MATCH (n) RETURN n.name ORDER BY n.name ASC`,
		"order by binding id":              `MATCH (n) RETURN id(n) ORDER BY id(n) DESC`,
		"order by multiple expressions":    `MATCH (n) RETURN id(n), n.name ORDER BY n.name ASC, id(n) DESC LIMIT 2`,
		"order by return alias":            `MATCH (n) RETURN n.name AS name ORDER BY name`,
		"order by binding":                 `MATCH (n) RETURN n ORDER BY n`,
		"limit zero":                       `MATCH (n) RETURN n.name LIMIT 0`,
		"parameterized limit":              `MATCH (n) RETURN n.name LIMIT $limit`,
		"skip and limit":                   `MATCH (n) RETURN n.name SKIP $skip LIMIT $limit`,
		"skip without limit":               `MATCH (n) RETURN n.name SKIP 1`,
		"set property":                     `MATCH (n) SET n.name = $name`,
		"set property from property":       `MATCH (n) SET n.copy = n.name`,
		"set property null":                `MATCH (n) SET n.name = null`,
		"replace properties":               `MATCH (n) SET n = {name: $name}`,
		"merge properties":                 `MATCH (n) SET n += $properties`,
		"replace properties from property": `MATCH (n), (source) SET n = source.properties`,
		"merge properties from property":   `MATCH (n), (source) SET n += source.properties`,
		"multiple set items":               `MATCH (n) SET n.a = 1, n.b = 2`,
		"set then return":                  `MATCH (n) SET n.name = $name RETURN n.name AS name`,
		"set label":                        `MATCH (n) SET n:Active RETURN n`,
		"create edge":                      `MATCH (a), (b) CREATE (a)-[:KNOWS {since: 2024}]->(b)`,
		"edge property expression":         `MATCH (a), (b) CREATE (a)-[:KNOWS {weight: b.weight}]->(b)`,
		"create edge then return":          `MATCH (a), (b) CREATE (a)-[r:KNOWS]->(b) RETURN id(r) AS id`,
		"create incoming edge":             `MATCH (a), (b) CREATE (a)<-[r:KNOWS]-(b) RETURN id(r) AS id`,
		"remove property":                  `MATCH (n) REMOVE n.name`,
		"remove then return":               `MATCH (n) REMOVE n.name RETURN n.name AS name`,
		"remove label":                     `MATCH (n) REMOVE n:Employee`,
		"remove multiple items":            `MATCH (n) REMOVE n.name, n:Employee`,
		"delete node":                      `MATCH (n) DELETE n`,
		"delete edge and nodes":            `MATCH (a)-[r]->(b) DELETE r, a, b`,
		"detach delete node":               `MATCH (n) DETACH DELETE n`,
		"unwind parameter":                 `UNWIND $items AS item RETURN item AS value`,
		"unwind count":                     `UNWIND $items AS item RETURN count(item) AS count`,
		"unwind order and limit":           `UNWIND $items AS item RETURN item ORDER BY item DESC LIMIT 2`,
		"unwind create":                    `UNWIND $items AS item CREATE (n:Item {id: item.id}) RETURN n.id`,
		"unwind create without return":     `UNWIND $items AS item CREATE (n:Item {id: item.id})`,
		"unwind match return":              `UNWIND $ids AS wanted MATCH (n:Item {id: wanted}) RETURN n.id`,
		"unwind match without terminal":    `UNWIND $ids AS wanted MATCH (n:Item {id: wanted})`,
		"unwind match mutation":            `UNWIND $ids AS wanted MATCH (n:Item) WHERE n.id = wanted SET n.active = true RETURN n.id`,
		"unwind match create edge":         `UNWIND $ids AS wanted MATCH (a:Item), (b:Item) WHERE a.id = wanted CREATE (a)-[:LINK]->(b)`,
		"unwind match remove":              `UNWIND $ids AS wanted MATCH (n:Item) WHERE n.id = wanted REMOVE n.active`,
		"unwind match delete":              `UNWIND $ids AS wanted MATCH (n:Item) WHERE n.id = wanted DELETE n`,
		"unwind match detach delete":       `UNWIND $ids AS wanted MATCH (n:Item) WHERE n.id = wanted DETACH DELETE n`,
		"quoted structural characters":     `MATCH (n {text: 'a,b) RETURN value AND more'}) RETURN n.text`,
		"escaped double quoted string":     `MATCH (n {text: "a\"b"}) RETURN n.text`,
		"nested map value":                 `CREATE (n {meta: {team: "graph", nested: {active: true}}})`,
		"trailing semicolon":               `MATCH (n) RETURN n;`,
		"multiline whitespace":             "MATCH\t(n)\nWHERE n.name = 'A  B'\r\nRETURN n.name;",
		"keyword identifier":               `MATCH (RETURN) RETURN RETURN`,
	}
	for name, query := range accepted {
		t.Run("accept/"+name, func(t *testing.T) {
			if _, err := parseQuery(query); err != nil {
				t.Fatalf("parseQuery(%q): %v", query, err)
			}
		})
	}

	rejected := map[string]string{
		"empty query":                  ``,
		"unsupported root":             `RETURN 1`,
		"lowercase keyword":            `match (n) RETURN n`,
		"optional match":               `OPTIONAL MATCH (n) RETURN n`,
		"merge":                        `MERGE (n:Person)`,
		"missing match pattern":        `MATCH`,
		"empty where":                  `MATCH (n) WHERE`,
		"dangling boolean predicate":   `MATCH (n) WHERE n.active = true AND RETURN n`,
		"unterminated node":            `MATCH (n RETURN n`,
		"variable length edge":         `MATCH (a)-[:KNOWS*1..3]->(b) RETURN b`,
		"empty edge type":              `MATCH (a)-[:]->(b) RETURN b`,
		"compound top level create":    `CREATE (a)-[:KNOWS]->(b)`,
		"empty expression":             `MATCH (n) SET n.name =`,
		"compact comparison":           `MATCH (n) WHERE n.age=1 RETURN n`,
		"compact assignment":           `MATCH (n) SET n.age=1`,
		"empty parameter":              `MATCH (n) SET n.name = $`,
		"unterminated string":          `MATCH (n {name: 'Alice}) RETURN n`,
		"line comment":                 "MATCH (n) // comment\nRETURN n",
		"block comment":                `MATCH (n) /* comment */ RETURN n`,
		"hex integer":                  `MATCH (n) WHERE n.value = 0x10 RETURN n`,
		"hex float":                    `MATCH (n) WHERE n.value = 0x1p2 RETURN n`,
		"non-finite float":             `MATCH (n) WHERE n.value = NaN RETURN n`,
		"infinite float":               `MATCH (n) WHERE n.value = +Inf RETURN n`,
		"malformed exponent":           `MATCH (n) WHERE n.value = 1e RETURN n`,
		"underscored number":           `MATCH (n) WHERE n.value = 1_000 RETURN n`,
		"unknown property expression":  `MATCH (n) SET n.copy = missing.name`,
		"self create expression":       `CREATE (n {copy: n.name})`,
		"self edge expression":         `MATCH (a), (b) CREATE (a)-[r:KNOWS {copy: r.value}]->(b)`,
		"create edge missing type":     `MATCH (a), (b) CREATE (a)-[]->(b)`,
		"create undirected edge":       `MATCH (a), (b) CREATE (a)-[:KNOWS]-(b)`,
		"create edge trailing text":    `MATCH (a), (b) CREATE (a)-[:KNOWS {since: 2024} trailing]->(b)`,
		"create then mutation":         `CREATE (n) SET n.active = true`,
		"multiple terminals":           `MATCH (n) SET n.active = true REMOVE n.name`,
		"delete then return":           `MATCH (n) DELETE n RETURN n`,
		"duplicate return alias":       `MATCH (n) RETURN id(n) AS value, n.name AS value`,
		"distinct hidden order":        `MATCH (n) RETURN DISTINCT n.name ORDER BY id(n)`,
		"empty distinct return":        `MATCH (n) RETURN DISTINCT`,
		"invalid count expression":     `MATCH (n) RETURN count(n.name)`,
		"count with projection":        `MATCH (n) RETURN count(n), n.name`,
		"literal return":               `MATCH (n) RETURN 1`,
		"negative limit":               `MATCH (n) RETURN n LIMIT -1`,
		"negative skip":                `MATCH (n) RETURN n SKIP -1`,
		"skip after limit":             `MATCH (n) RETURN n LIMIT 1 SKIP 1`,
		"unknown order binding":        `MATCH (n) RETURN n ORDER BY missing`,
		"union":                        `MATCH (n) RETURN n UNION MATCH (m) RETURN m`,
		"with":                         `MATCH (n) WITH n RETURN n`,
		"unwind without as":            `UNWIND $items RETURN item`,
		"unwind without return":        `UNWIND $items AS item`,
		"unwind twice":                 `UNWIND $items AS item UNWIND item AS nested RETURN nested`,
		"unwind root binding":          `UNWIND item.values AS item RETURN item`,
		"unwind binding role conflict": `UNWIND $items AS n MATCH (n) RETURN n`,
		"vector under or":              `MATCH (n) WHERE n.embedding <=> $vector OR n.active = true RETURN n`,
		"full text under not":          `MATCH (n) WHERE NOT n.text @@ $query RETURN n`,
		"list literal":                 `MATCH (n) WHERE n.kind IN ["a", "b"] RETURN n`,
		"not equals spelling":          `MATCH (n) WHERE n.age != 1 RETURN n`,
		"invalid binding identifier":   `MATCH (bad-name) RETURN bad-name`,
		"empty label":                  `CREATE (n:)`,
		"invalid label identifier":     `CREATE (n:Bad-Label)`,
		"invalid property identifier":  `CREATE (n {bad-key: 1})`,
		"invalid return alias":         `MATCH (n) RETURN n.name AS bad-alias`,
		"invalid count alias":          `MATCH (n) RETURN count(n) AS bad-alias`,
		"invalid remove label":         `MATCH (n) REMOVE n:Bad-Label`,
		"empty property value":         `CREATE (n {name:})`,
		"duplicate property key":       `CREATE (n {name: "Alice", name: "Bob"})`,
		"multiple semicolons":          `MATCH (n) RETURN n;;`,
	}
	for name, query := range rejected {
		t.Run("reject/"+name, func(t *testing.T) {
			if _, err := parseQuery(query); err == nil {
				t.Fatalf("parseQuery(%q) unexpectedly succeeded", query)
			}
		})
	}
}
