package engine

import (
	"strings"
	"testing"
)

func FuzzParseQuery(f *testing.F) {
	for _, query := range []string{
		"MATCH (n) RETURN n",
		"MATCH (n:Person {name: 'Ada'}) WHERE n.age >= 18 RETURN n.name AS name ORDER BY name LIMIT 10",
		"MATCH (a)-[:KNOWS]->(b) SET a.count = 1 RETURN a",
		"MATCH (n) DETACH DELETE n",
		"CREATE (n:Person {name: 'Ada'}) RETURN n",
		"UNWIND [1, 2, 3] AS value RETURN value ORDER BY value",
	} {
		f.Add(query)
	}
	f.Fuzz(func(t *testing.T, query string) {
		if len(query) > 4<<10 || strings.Count(query, "(")+strings.Count(query, "[")+strings.Count(query, "{") > 64 {
			return
		}
		_, _ = parseQuery(query)
	})
}
