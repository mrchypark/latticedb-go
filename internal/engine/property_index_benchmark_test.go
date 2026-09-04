package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

func BenchmarkCreateNodePropertyIndexContext(b *testing.B) {
	for _, size := range []int{10_000, 100_000} {
		b.Run(fmt.Sprintf("nodes_%d", size), func(b *testing.B) {
			db, err := Open(filepath.Join(b.TempDir(), "db"), OpenOptions{Create: true, WALCheckpointThresholdBytes: ^uint64(0)})
			if err != nil {
				b.Fatal(err)
			}
			if err := db.Update(func(tx *Tx) error {
				for i := 0; i < size; i++ {
					if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"key": int64(i)}}); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := db.CreateNodePropertyIndexContext(context.Background(), "Item", "key"); err != nil {
					b.Fatal(err)
				}
				if err := db.DropNodePropertyIndex("Item", "key"); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if err := db.Close(); err != nil {
				b.Fatal(err)
			}
		})
	}
}
