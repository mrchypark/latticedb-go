package latticedb

import (
	"fmt"
	"path/filepath"
	"testing"
)

func benchmarkDB(b *testing.B) *DB {
	b.Helper()
	db, err := Open(filepath.Join(b.TempDir(), "bench.db"), OpenOptions{
		Create:           true,
		EnableVector:     true,
		VectorDimensions: 16,
		Durability:       DurabilityStandard,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })

	err = db.Update(func(tx *Tx) error {
		for i := 0; i < 1_000; i++ {
			node, err := tx.CreateNode(CreateNodeOptions{
				Labels:     []string{"Document"},
				Properties: map[string]Value{"name": fmt.Sprintf("document-%d", i)},
			})
			if err != nil {
				return err
			}
			if err := tx.SetVector(node.ID, "embedding", make([]float32, 16)); err != nil {
				return err
			}
			if err := tx.FTSIndex(node.ID, "lattice database benchmark"); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
	return db
}

func BenchmarkReadRequests(b *testing.B) {
	db := benchmarkDB(b)
	b.Run("node_lookup", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if err := db.View(func(tx *Tx) error {
				_, err := tx.GetNode(500)
				return err
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("query", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := db.Query("MATCH (n:Document) WHERE id(n) = 500 RETURN n.name", nil); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("vector_search", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := db.VectorSearch(make([]float32, 16), VectorSearchOptions{K: 10}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("fts_search", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := db.FTSSearch("lattice", FTSSearchOptions{Limit: 10}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("write_rollback", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			tx, err := db.Begin(false)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Document"}}); err != nil {
				b.Fatal(err)
			}
			if err := tx.Rollback(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("write_commit", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			tx, err := db.Begin(false)
			if err != nil {
				b.Fatal(err)
			}
			if err := tx.SetProperty(500, "revision", i); err != nil {
				b.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("query_mutation", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := db.Query("MATCH (n:Document) WHERE id(n) = 500 SET n.revision = $revision", map[string]Value{"revision": int64(i)}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkOutgoingEdges(b *testing.B) {
	db, err := Open(filepath.Join(b.TempDir(), "edges.db"), OpenOptions{Create: true})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	if err := db.Update(func(tx *Tx) error {
		for range 2_002 {
			if _, err := tx.CreateNode(CreateNodeOptions{}); err != nil {
				return err
			}
		}
		for i := uint64(3); i < 2_002; i++ {
			if _, err := tx.CreateEdge(i, i+1, "CHAIN", CreateEdgeOptions{}); err != nil {
				return err
			}
		}
		for range 4 {
			if _, err := tx.CreateEdge(2, 3, "FOUR", CreateEdgeOptions{}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}

	for _, benchmark := range []struct {
		name   string
		nodeID uint64
	}{{"degree_0", 1}, {"degree_4", 2}} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if err := db.View(func(tx *Tx) error {
					_, err := tx.GetOutgoingEdges(benchmark.nodeID)
					return err
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSingleRecordCommitScaling(b *testing.B) {
	for _, size := range []int{1_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("nodes_%d", size), func(b *testing.B) {
			db, target := benchmarkWriteScaleDB(b, size)
			b.Run("direct", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if err := db.Update(func(tx *Tx) error { return tx.SetProperty(target, "value", int64(i)) }); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("query", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if _, err := db.Query("MATCH (n) WHERE id(n) = $id SET n.value = $value", map[string]Value{"id": int64(target), "value": int64(i)}); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func BenchmarkCheckpoint(b *testing.B) {
	db, target := benchmarkWriteScaleDB(b, 10_000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Update(func(tx *Tx) error {
			return tx.SetProperty(target, "checkpoint_revision", int64(i))
		}); err != nil {
			b.Fatal(err)
		}
		if err := db.Checkpoint(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkColdOpen(b *testing.B) {
	db, _ := benchmarkWriteScaleDB(b, 10_000)
	path := db.Path()
	if err := db.Close(); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		db, err = Open(path, OpenOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if err := db.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReaderDuringCommit(b *testing.B) {
	db, target := benchmarkWriteScaleDB(b, 10_000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		readerReady := make(chan struct{})
		releaseReader := make(chan struct{})
		readerDone := make(chan error, 1)
		go func() {
			readerDone <- db.View(func(tx *Tx) error {
				if _, err := tx.GetNode(target); err != nil {
					return err
				}
				close(readerReady)
				<-releaseReader
				return nil
			})
		}()
		<-readerReady

		commitErr := db.Update(func(tx *Tx) error {
			return tx.SetProperty(target, "reader_overlap", int64(i))
		})
		close(releaseReader)

		if err := <-readerDone; err != nil {
			b.Fatal(err)
		}
		if commitErr != nil {
			b.Fatal(commitErr)
		}
	}
}

func BenchmarkMatchedScale10K(b *testing.B) {
	db, err := Open(filepath.Join(b.TempDir(), "matched.ltdb"), OpenOptions{Create: true, EnableVector: true, VectorDimensions: 16, VectorIndexMode: VectorIndexHNSWSynchronous})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	if err := db.Update(func(tx *Tx) error {
		for index := 0; index < 10_000; index++ {
			node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Document"}, Properties: map[string]Value{"name": fmt.Sprintf("document-%d", index)}})
			if err != nil {
				return err
			}
			vector := make([]float32, 16)
			vector[0] = float32(index%1000) / 1000
			if err := tx.SetVector(node.ID, "embedding", vector); err != nil {
				return err
			}
			text := "common token"
			if index == 0 {
				text += " rare"
			}
			if err := tx.FTSIndex(node.ID, text); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	b.Run("vector", func(b *testing.B) {
		query := make([]float32, 16)
		b.ReportAllocs()
		for range b.N {
			if _, err := db.VectorSearch(query, VectorSearchOptions{K: 10}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("fts_rare", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := db.FTSSearch("rare", FTSSearchOptions{Limit: 10}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("vector_mutation", func(b *testing.B) {
		b.ReportAllocs()
		for index := range b.N {
			vector := make([]float32, 16)
			vector[0] = float32(index & 1)
			if err := db.Update(func(tx *Tx) error { return tx.SetVector(1, "embedding", vector) }); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkPropertyEquality10K(b *testing.B) {
	db, err := Open(filepath.Join(b.TempDir(), "property-index.ltdb"), OpenOptions{Create: true})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	if err := db.Update(func(tx *Tx) error {
		for index := range 10_000 {
			if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]Value{"key": int64(index)}}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("Item", "key"); err != nil {
		b.Fatal(err)
	}
	b.Run("indexed", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if err := db.View(func(tx *Tx) error {
				_, err := tx.FindNodesByLabelProperty("Item", "key", int64(9_999), 1)
				return err
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("query_indexed", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := db.Query("MATCH (n:Item) WHERE n.key = $key RETURN n", map[string]Value{"key": int64(9_999)}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkPropertyIndexCommonValue10K(b *testing.B) {
	db, err := Open(filepath.Join(b.TempDir(), "property-index-common.ltdb"), OpenOptions{Create: true})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	if err := db.Update(func(tx *Tx) error {
		for range 10_000 {
			if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]Value{"key": "common"}}); err != nil {
				return err
			}
		}
		_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]Value{"key": "unique"}})
		return err
	}); err != nil {
		b.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("Item", "key"); err != nil {
		b.Fatal(err)
	}
	b.Run("limit_1", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if err := db.View(func(tx *Tx) error {
				_, err := tx.FindNodesByLabelProperty("Item", "key", "common", 1)
				return err
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("full_then_limit_1", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if err := db.View(func(tx *Tx) error {
				ids, err := tx.FindNodesByLabelProperty("Item", "key", "common", ^uint(0))
				if err == nil {
					_ = ids[:1]
				}
				return err
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("unique_limit_1", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if err := db.View(func(tx *Tx) error {
				_, err := tx.FindNodesByLabelProperty("Item", "key", "unique", 1)
				return err
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("unique_full_then_limit_1", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if err := db.View(func(tx *Tx) error {
				ids, err := tx.FindNodesByLabelProperty("Item", "key", "unique", ^uint(0))
				if err == nil {
					_ = ids[:1]
				}
				return err
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func benchmarkWriteScaleDB(b *testing.B, size int) (*DB, uint64) {
	b.Helper()
	db, err := Open(filepath.Join(b.TempDir(), "scale.db"), OpenOptions{Create: true, WALCheckpointThresholdBytes: ^uint64(0)})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	var target uint64
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < size; i++ {
			node, err := tx.CreateNode(CreateNodeOptions{})
			if err != nil {
				return err
			}
			if i == 0 {
				target = node.ID
			}
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	return db, target
}
