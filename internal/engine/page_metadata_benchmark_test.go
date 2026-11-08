package engine

import (
	"fmt"
	"path/filepath"
	"testing"
)

func BenchmarkPageCommitWithGrowingMetadata(b *testing.B) {
	for _, metadataEntries := range []int{0, 256, 4096} {
		b.Run(fmt.Sprintf("metadata_%d", metadataEntries), func(b *testing.B) {
			db, err := Open(filepath.Join(b.TempDir(), "db"), OpenOptions{Create: true, PageStorage: true})
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = db.Close() })
			var nodeID uint64
			if err := db.Update(func(tx *Tx) error {
				for i := range metadataEntries {
					if err := tx.PutAppMetadata([]byte(fmt.Sprintf("metadata/%04d", i)), []byte(fmt.Sprintf("value/%0240d", i))); err != nil {
						return err
					}
				}
				node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"value": int64(0)}})
				if err == nil {
					nodeID = node.ID
				}
				return err
			}); err != nil {
				b.Fatal(err)
			}
			if got := db.graph.AppMetadata.Len(); got != metadataEntries {
				b.Fatalf("metadata entries = %d, want %d", got, metadataEntries)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				if err := db.Update(func(tx *Tx) error { return tx.SetProperty(nodeID, "value", int64(i)) }); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
