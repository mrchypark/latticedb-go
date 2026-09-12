package engine

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// repeatedStringGraphFixture creates a temp on-disk database with numNodes
// nodes, 2 labels, 4 distinct 512-byte property values (a/b/c/d), repeated
// property keys, one edge per 2 nodes with a repeated type. Closes the DB
// so the fixture is pure disk state.
func repeatedStringGraphFixture(b *testing.B, numNodes int) string {
	b.Helper()
	dir := filepath.Join(b.TempDir(), "bench")
	db, err := Open(dir, OpenOptions{Create: true})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	a := strings.Repeat("a", 512)
	bstr := strings.Repeat("b", 512)
	c := strings.Repeat("c", 512)
	d := strings.Repeat("d", 512)
	err = db.Update(func(tx *Tx) error {
		for i := 0; i < numNodes; i++ {
			if _, err := tx.CreateNode(CreateNodeOptions{
				Labels:     []string{"Alpha", "Beta"},
				Properties: map[string]any{"name": a, "desc": bstr, "tag": c, "ref": d},
			}); err != nil {
				return err
			}
		}
		for i := uint64(2); i <= uint64(numNodes); i += 2 {
			if _, err := tx.CreateEdge(i-1, i, "LINK", CreateEdgeOptions{
				Properties: map[string]any{"note": a},
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	if err = db.Close(); err != nil {
		b.Fatal(err)
	}
	return dir
}

// BenchmarkRepeatedStringGraphOpen measures repeated ReadOnly Open+Close on
// a graph with 4 distinct 512-byte property values per node. Fixture is built
// once; only the Open+Close loop is timed.
func BenchmarkRepeatedStringGraphOpen(b *testing.B) {
	for _, numNodes := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("nodes_%d", numNodes), func(b *testing.B) {
			dir := repeatedStringGraphFixture(b, numNodes)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				db, err := Open(dir, OpenOptions{ReadOnly: true})
				if err != nil {
					b.Fatal(err)
				}
				if err := db.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
		})
	}
}

// BenchmarkRepeatedStringGraphRetained measures Go heap retained by one
// reopened read-only handle. Each iteration: GC baseline, open DB, GC with
// DB live, report signed HeapAlloc delta. KeepAlive after MemStats read
// ensures the handle is live through the measurement window.
func BenchmarkRepeatedStringGraphRetained(b *testing.B) {
	for _, numNodes := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("nodes_%d", numNodes), func(b *testing.B) {
			dir := repeatedStringGraphFixture(b, numNodes)
			b.ResetTimer()
			var db *DB
			var err error
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				runtime.GC()
				var before runtime.MemStats
				runtime.ReadMemStats(&before)
				b.StartTimer()

				db, err = Open(dir, OpenOptions{ReadOnly: true})
				if err != nil {
					b.Fatal(err)
				}

				b.StopTimer()
				runtime.GC()
				var after runtime.MemStats
				runtime.ReadMemStats(&after)
				runtime.KeepAlive(db)
				diff := int64(after.HeapAlloc) - int64(before.HeapAlloc)
				b.ReportMetric(float64(diff), "retained_heap_B")

				if err := db.Close(); err != nil {
					b.Fatal(err)
				}
				db = nil
			}
			b.StopTimer()
		})
	}
}
