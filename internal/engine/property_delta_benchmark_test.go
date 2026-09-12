package engine

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func BenchmarkPropertyDelta(b *testing.B) {
	for _, entity := range []string{"node", "edge"} {
		for _, width := range []int{10, 1000} {
			if entity == "edge" && width != 10 {
				continue
			}
			for _, nested := range []bool{false, true} {
				shape := "flat"
				if nested {
					shape = "nested"
				}
				b.Run(fmt.Sprintf("%s/%s/width_%d", entity, shape, width), func(b *testing.B) {
					db, entityID := openPropertyDeltaFixture(b, entity, width, nested)
					before, err := db.wal.TailSize()
					if err != nil {
						b.Fatal(err)
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						value := int64(i + 1)
						if err := db.Update(func(tx *Tx) error {
							if entity == "node" {
								return tx.SetProperty(entityID, "counter", value)
							}
							return tx.SetEdgeProperty(entityID, "counter", value)
						}); err != nil {
							b.Fatal(err)
						}
					}
					b.StopTimer()
					after, err := db.wal.TailSize()
					if err != nil {
						b.Fatal(err)
					}
					b.ReportMetric(float64(after-before)/float64(b.N), "wal-bytes/op")
					if err := db.Close(); err != nil {
						b.Fatal(err)
					}
				})
			}
		}
	}
}

func openPropertyDeltaFixture(b *testing.B, entity string, width int, nested bool) (*DB, uint64) {
	b.Helper()
	db, err := Open(filepath.Join(b.TempDir(), "db"), OpenOptions{
		Create:                      true,
		WALCheckpointThresholdBytes: ^uint64(0),
		// Durability is intentionally omitted: the Open default is DurabilityStandard (fullSync=false).
	})
	if err != nil {
		b.Fatal(err)
	}
	var entityID uint64
	if err := db.Update(func(tx *Tx) error {
		properties := propertyDeltaFixtureProperties(width, nested)
		node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: properties})
		if err != nil {
			return err
		}
		if entity == "node" {
			entityID = node.ID
			return nil
		}
		other, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}})
		if err != nil {
			return err
		}
		edge, err := tx.CreateEdge(node.ID, other.ID, "LINK", CreateEdgeOptions{Properties: properties})
		if err != nil {
			return err
		}
		entityID = edge.ID
		return nil
	}); err != nil {
		_ = db.Close()
		b.Fatal(err)
	}
	return db, entityID
}

func propertyDeltaFixtureProperties(width int, nested bool) map[string]any {
	properties := make(map[string]any, width)
	properties["counter"] = int64(0)
	for i := 1; i < width; i++ {
		key := fmt.Sprintf("property_%04d", i)
		payload := strings.Repeat("x", 128)
		if nested {
			properties[key] = map[string]any{
				"payload": map[string]any{"text": payload},
			}
			continue
		}
		properties[key] = payload
	}
	return properties
}
