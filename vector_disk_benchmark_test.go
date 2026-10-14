package latticedb

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"slices"
	"testing"
	"time"

	"github.com/mrchypark/latticedb-go/internal/benchvector"
)

// BenchmarkVectorSearchDisk128D includes durable public-API insertion and reopen.
// Run with -benchtime=1x. The resident index benchmark remains a separate workload.
func BenchmarkVectorSearchDisk128D(b *testing.B) {
	for _, scale := range []struct {
		name string
		n    int
	}{{"1K", 1000}, {"10K", 10000}, {"100K", 100000}} {
		b.Run(scale.name, func(b *testing.B) {
			vectors, queries := benchvector.Generate(scale.n)
			if dir := os.Getenv("LATTICEDB_VECTOR_FIXTURE_DIR"); dir != "" {
				if err := writeDiskVectorFixture(filepath.Join(dir, fmt.Sprintf("vectors-%d.bin", scale.n)), vectors, queries); err != nil {
					b.Fatal(err)
				}
			}
			db, build, reopen, bytes := diskVectorDB(b, vectors)
			opts := VectorSearchOptions{K: 10, EfSearch: 64, MaxWork: 1 << 40, MaxBytes: 256 << 20}
			for _, q := range queries[:10] {
				if _, err := db.VectorSearch(q, opts); err != nil {
					b.Fatal(err)
				}
			}
			before, err := db.VectorIndexStats()
			if err != nil {
				b.Fatal(err)
			}
			times := make([]time.Duration, len(queries))
			var total time.Duration
			for i, q := range queries {
				start := time.Now()
				hits, err := db.VectorSearch(q, opts)
				times[i] = time.Since(start)
				if err != nil || len(hits) != 10 {
					b.Fatalf("search: hits=%d err=%v", len(hits), err)
				}
				total += times[i]
			}
			after, err := db.VectorIndexStats()
			if err != nil {
				b.Fatal(err)
			}
			if after.ExactFallbacks != before.ExactFallbacks {
				b.Fatal("disk ANN comparison fell back to exact search")
			}
			slices.Sort(times)
			recall := diskVectorRecall(b, db, queries[:10], opts)
			b.ReportAllocs()
			b.ResetTimer()
			b.ReportMetric(1, "disk-api-contract")
			b.ReportMetric(float64(build.Nanoseconds())/1e6, "index-build-ms")
			b.ReportMetric(float64(reopen.Nanoseconds())/1e6, "reopen-ms")
			b.ReportMetric(float64(bytes), "file-bytes")
			b.ReportMetric(float64(total.Nanoseconds())/float64(len(times)), "mean-ns")
			b.ReportMetric(float64(times[(len(times)-1)*99/100].Nanoseconds()), "p99-ns")
			b.ReportMetric(recall*100, "recall@10")
			for i := 0; i < b.N; i++ {
				if _, err := db.VectorSearch(queries[i%len(queries)], opts); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func diskVectorDB(tb testing.TB, vectors [][]float32) (*DB, time.Duration, time.Duration, int64) {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), "vectors.ltdb")
	opts := OpenOptions{Create: true, EnableVectors: true, VectorDimensions: 128,
		VectorIndexMode: VectorIndexHNSWSynchronous, VectorM: 16, CacheSizeMB: 64,
		VectorIndexBuildMaxWork: 1 << 40, VectorIndexBuildMaxLogicalBytes: 1 << 30,
		DerivedIndexBuildMaxWork: 1 << 40, DerivedIndexBuildMaxLogicalBytes: 1 << 30}
	start := time.Now()
	db, err := Open(path, opts)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		if db != nil {
			if err := db.Close(); err != nil {
				tb.Error(err)
			}
		}
	})
	for offset := 0; offset < len(vectors); offset += 1000 {
		batchStart := time.Now()
		end := min(offset+1000, len(vectors))
		if err := db.Update(func(tx *Tx) error {
			ids, err := tx.BatchInsertVectors("Vector", vectors[offset:end])
			if err != nil {
				return err
			}
			for i, id := range ids {
				if id != uint64(offset+i+1) {
					return fmt.Errorf("unexpected node ID %d", id)
				}
			}
			return nil
		}); err != nil {
			tb.Fatal(err)
		}
		if os.Getenv("LATTICEDB_VECTOR_BUILD_PROGRESS") == "1" {
			fmt.Fprintf(os.Stderr, "disk-build-progress n=%d inserted=%d batch_ms=%.1f elapsed_ms=%.1f\n", len(vectors), end, float64(time.Since(batchStart).Microseconds())/1000, float64(time.Since(start).Microseconds())/1000)
		}
		if os.Getenv("LATTICEDB_VECTOR_MEMORY_STATS") == "1" {
			var mem runtime.MemStats
			runtime.ReadMemStats(&mem)
			fmt.Fprintf(os.Stderr, "disk-memory n=%d inserted=%d heap_alloc=%d heap_inuse=%d heap_sys=%d heap_objects=%d total_alloc=%d gc=%d pause_ns=%d\n", len(vectors), end, mem.HeapAlloc, mem.HeapInuse, mem.HeapSys, mem.HeapObjects, mem.TotalAlloc, mem.NumGC, mem.PauseTotalNs)
		}
	}
	if profilePath := os.Getenv("LATTICEDB_VECTOR_HEAP_PROFILE"); profilePath != "" {
		if os.Getenv("LATTICEDB_VECTOR_PROFILE_GC") == "1" {
			runtime.GC()
			var mem runtime.MemStats
			runtime.ReadMemStats(&mem)
			fmt.Fprintf(os.Stderr, "disk-memory-after-gc heap_alloc=%d heap_inuse=%d heap_sys=%d heap_idle=%d heap_released=%d\n", mem.HeapAlloc, mem.HeapInuse, mem.HeapSys, mem.HeapIdle, mem.HeapReleased)
		}
		f, err := os.Create(profilePath)
		if err != nil {
			tb.Fatal(err)
		}
		profileErr := pprof.WriteHeapProfile(f)
		closeErr := f.Close()
		if profileErr != nil || closeErr != nil {
			tb.Fatalf("heap profile: write=%v close=%v", profileErr, closeErr)
		}
	}
	if err := db.Close(); err != nil {
		tb.Fatal(err)
	}
	build := time.Since(start)
	info, err := os.Stat(filepath.Join(path, "state.json"))
	if err != nil || info.Size() == 0 {
		tb.Fatalf("missing disk database: %v", err)
	}
	opts.Create = false
	start = time.Now()
	db, err = Open(path, opts)
	if err != nil {
		tb.Fatal(err)
	}
	reopen := time.Since(start)
	stats, err := db.VectorIndexStats()
	if err != nil || stats.LiveEntries != uint64(len(vectors)) || stats.IndexEntries != uint64(len(vectors)) {
		tb.Fatalf("reopened index count: %+v err=%v", stats, err)
	}
	if err := db.View(func(tx *Tx) error {
		for _, id := range []uint64{1, uint64(len(vectors))} {
			node, err := tx.GetNode(id)
			if err != nil {
				return err
			}
			vector, ok := node.Properties["vector"].([]float32)
			if !ok || !slices.Equal(vector, vectors[id-1]) {
				return fmt.Errorf("reopen lost vector %d", id)
			}
		}
		return nil
	}); err != nil {
		tb.Fatal(err)
	}
	return db, build, reopen, info.Size()
}

func diskVectorRecall(tb testing.TB, db *DB, queries [][]float32, opts VectorSearchOptions) float64 {
	tb.Helper()
	hits := 0
	for _, q := range queries {
		approx, err := db.VectorSearch(q, opts)
		if err != nil {
			tb.Fatal(err)
		}
		exactOpts := opts
		exactOpts.Exact = true
		exact, err := db.VectorSearch(q, exactOpts)
		if err != nil || len(exact) != 10 || len(approx) != 10 {
			tb.Fatalf("recall lengths %d/%d: %v", len(approx), len(exact), err)
		}
		for _, a := range approx {
			for _, e := range exact {
				if a.NodeID == e.NodeID {
					hits++
					break
				}
			}
		}
	}
	return float64(hits) / float64(10*len(queries))
}

func writeDiskVectorFixture(path string, vectors, queries [][]float32) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	writeErr := binary.Write(f, binary.LittleEndian, [3]uint32{uint32(len(vectors)), 128, uint32(len(queries))})
	for _, rows := range [][][]float32{vectors, queries} {
		for _, row := range rows {
			if writeErr == nil {
				writeErr = binary.Write(f, binary.LittleEndian, row)
			}
		}
	}
	closeErr := f.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

func TestDiskVectorBenchmarkReopensPersistedIndex(t *testing.T) {
	vectors, queries := benchvector.Generate(32)
	db, _, _, _ := diskVectorDB(t, vectors)
	opts := VectorSearchOptions{K: 10, EfSearch: 64}
	if recall := diskVectorRecall(t, db, queries[:2], opts); recall < .9 {
		t.Fatalf("recall = %g", recall)
	}
	stats, err := db.VectorIndexStats()
	if err != nil || stats.ExactFallbacks != 0 {
		t.Fatalf("ANN fallback: %+v %v", stats, err)
	}
	path := filepath.Join(t.TempDir(), "vectors.bin")
	if err := writeDiskVectorFixture(path, vectors, queries); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 12+4*128*(len(vectors)+len(queries)) || binary.LittleEndian.Uint32(data[:4]) != 32 {
		t.Fatal("invalid fixture")
	}
	if err := writeDiskVectorFixture(path, vectors, queries); err == nil {
		t.Fatal("fixture overwritten")
	}
}
