package engine

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestCanonicalQueryCachePreservesSemantics(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "cache.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, pair := range [][2]string{
		{"MATCH (n) WHERE n.x = 1 RETURN n", " \nMATCH   (n) WHERE n.x = 1 RETURN n; "},
		{"MATCH (n) WHERE n.x = +01 RETURN n", "MATCH (n) WHERE n.x = 1 RETURN n"},
		{`MATCH (n) WHERE n.x = 'hello' RETURN n`, `MATCH (n) WHERE n.x = "hello" RETURN n`},
		{`MATCH (n {a: 1, b: 'x'}) RETURN n`, `MATCH (n {b: "x", a: +01}) RETURN n`},
	} {
		left, err := db.cachedQueryPlan(pair[0])
		if err != nil {
			t.Fatal(err)
		}
		before := db.cacheMisses.Load()
		right, err := db.cachedQueryPlan(pair[1])
		if err != nil {
			t.Fatal(err)
		}
		if left != right || db.cacheMisses.Load() != before {
			t.Fatalf("equivalent queries did not share plan: %q / %q", pair[0], pair[1])
		}
	}
	for _, pair := range [][2]string{
		{"MATCH (n) WHERE n.x = 1 RETURN n", "MATCH (n) WHERE n.x = 1.0 RETURN n"},
		{"MATCH (n) WHERE n.x = $a RETURN n", "MATCH (n) WHERE n.x = $b RETURN n"},
		{"MATCH (n) RETURN n AS a", "MATCH (n) RETURN n AS b"},
		{"MATCH (n) SET n.x = 0.0 RETURN n", "MATCH (n) SET n.x = -0.0 RETURN n"},
		{"MATCH (n) WHERE n.x = 'a b' RETURN n", "MATCH (n) WHERE n.x = 'a  b' RETURN n"},
		{"MATCH (n) WHERE n.x = 1 AND n.y = 2 RETURN n", "MATCH (n) WHERE n.x = 1 OR n.y = 2 RETURN n"},
		{"MATCH (n) RETURN n.x, n.y", "MATCH (n) RETURN n.y, n.x"},
	} {
		left, err := db.cachedQueryPlan(pair[0])
		if err != nil {
			t.Fatal(err)
		}
		right, err := db.cachedQueryPlan(pair[1])
		if err != nil {
			t.Fatal(err)
		}
		if left == right {
			t.Fatalf("different queries shared plan: %q / %q", pair[0], pair[1])
		}
	}
	for _, query := range []string{`MATCH (n) WHERE n.x = 'broken RETURN n`, `MATCH (n) RETURN missing`} {
		before := db.cacheMisses.Load()
		if _, err := db.cachedQueryPlan(query); err == nil || db.cacheMisses.Load() != before {
			t.Fatalf("invalid query entered cache: %q, %v", query, err)
		}
	}
}

func TestCanonicalQueryCacheBoundsSourceAliases(t *testing.T) {
	db := &DB{queryCache: make(map[string]*queryPlan)}
	for i := 1; i <= queryCacheEntries*3; i++ {
		if _, err := db.cachedQueryPlan(fmt.Sprintf("MATCH (n) WHERE n.x = %d RETURN n", i)); err != nil {
			t.Fatal(err)
		}
	}
	if len(db.queryCache) != queryCacheEntries || len(db.queryCacheSources) != queryCacheEntries {
		t.Fatalf("unbounded plans/aliases: %d/%d", len(db.queryCache), len(db.queryCacheSources))
	}
	for i := 0; i < queryCacheEntries*2; i++ {
		if _, err := db.cachedQueryPlan(fmt.Sprintf("MATCH (n) WHERE n.x = %s1 RETURN n", strings.Repeat("0", i))); err != nil {
			t.Fatal(err)
		}
	}
	if len(db.queryCacheSources) > queryCacheEntries {
		t.Fatal("equivalent literal aliases exceeded capacity")
	}
	if err := db.CacheClear(); err != nil {
		t.Fatal(err)
	}
	if len(db.queryCacheSources) != 0 || db.queryCacheSourceKeys != [queryCacheEntries]string{} || db.queryCacheSourceNext != 0 {
		t.Fatal("clear retained source aliases")
	}
	if _, err := db.cachedQueryPlan("MATCH (n) RETURN n"); err != nil || db.cacheMisses.Load() != 1 {
		t.Fatalf("query after clear: %v, misses %d", err, db.cacheMisses.Load())
	}
}

func BenchmarkCanonicalQueryCacheHit(b *testing.B) {
	db := &DB{queryCache: make(map[string]*queryPlan)}
	const query = "MATCH (n) WHERE n.x = $value RETURN n"
	if _, err := db.cachedQueryPlan(query); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := db.cachedQueryPlan(query); err != nil {
			b.Fatal(err)
		}
	}
}

func TestCanonicalQueryCacheConcurrentAliases(t *testing.T) {
	db := &DB{queryCache: make(map[string]*queryPlan)}
	plans := make(chan *queryPlan, 16)
	errors := make(chan error, 16)
	var workers sync.WaitGroup
	for i := range 16 {
		workers.Go(func() {
			plan, err := db.cachedQueryPlan("MATCH (n) WHERE n.x = " + strings.Repeat("0", i) + "1 RETURN n")
			plans <- plan
			errors <- err
		})
	}
	workers.Wait()
	close(plans)
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	var first *queryPlan
	for plan := range plans {
		if first == nil {
			first = plan
		} else if plan != first {
			t.Fatal("concurrent equivalent queries did not share the immutable plan")
		}
	}
	if db.cacheMisses.Load() != 1 || db.cacheHits.Load() != 15 {
		t.Fatalf("concurrent cache stats: misses=%d hits=%d", db.cacheMisses.Load(), db.cacheHits.Load())
	}
}
