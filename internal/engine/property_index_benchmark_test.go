package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
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

func BenchmarkPropertyIndexMutation(b *testing.B) {
	for _, test := range []struct {
		name             string
		unrelatedIndexes int
		propertyCount    int
		labelCount       int
	}{
		{name: "unrelated_indexes", unrelatedIndexes: 512, propertyCount: 1},
		{name: "wide_record", propertyCount: 512},
		{name: "many_labels_few_properties", unrelatedIndexes: 16, propertyCount: 1, labelCount: 32},
	} {
		b.Run(test.name, func(b *testing.B) {
			db, err := Open(filepath.Join(b.TempDir(), "db"), OpenOptions{Create: true, WALCheckpointThresholdBytes: ^uint64(0)})
			if err != nil {
				b.Fatal(err)
			}
			properties := map[string]any{"key": int64(0)}
			for i := 1; i < test.propertyCount; i++ {
				properties["property"+strconv.Itoa(i)] = int64(i)
			}
			labels := []string{"Item"}
			for i := 1; i < test.labelCount; i++ {
				labels = append(labels, "Label"+strconv.Itoa(i))
			}
			if err := db.Update(func(tx *Tx) error {
				_, err := tx.CreateNode(CreateNodeOptions{Labels: labels, Properties: properties})
				return err
			}); err != nil {
				b.Fatal(err)
			}
			if err := db.CreateNodePropertyIndex("Item", "key"); err != nil {
				b.Fatal(err)
			}
			for i := 0; i < test.unrelatedIndexes; i++ {
				if err := db.CreateNodePropertyIndex("Other"+strconv.Itoa(i), "key"); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := db.Update(func(tx *Tx) error { return tx.SetProperty(1, "key", int64(i)) }); err != nil {
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
