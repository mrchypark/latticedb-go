package engine

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

func BenchmarkGroupCommit(b *testing.B) {
	for _, mode := range []string{"individual", "batch"} {
		b.Run(mode, func(b *testing.B) {
			var syncs atomic.Int64
			db, err := Open(filepath.Join(b.TempDir(), "batch"), OpenOptions{Create: true, walSync: func(f *os.File) error { syncs.Add(1); return f.Sync() }})
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			fn := func(tx *Tx) error {
				_, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"value": int64(1)}})
				return err
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if mode == "individual" {
					for range maxBatchRequests {
						if err := db.Update(fn); err != nil {
							b.Fatal(err)
						}
					}
				} else {
					var wg sync.WaitGroup
					errs := make(chan error, maxBatchRequests)
					for range maxBatchRequests {
						wg.Add(1)
						go func() { defer wg.Done(); errs <- db.Batch(fn) }()
					}
					wg.Wait()
					close(errs)
					for err := range errs {
						if err != nil {
							b.Fatal(err)
						}
					}
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*maxBatchRequests), "ns/request")
			b.ReportMetric(float64(syncs.Load())/float64(b.N*maxBatchRequests), "syncs/request")
		})
	}
}
