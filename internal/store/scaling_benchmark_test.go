package store

import (
	"fmt"
	"slices"
	"testing"
)

func benchmarkStreamStore(size int) StreamStore {
	store := NewStreamStore()
	for index := range size {
		store.Publish("events", "event", int64(index))
	}
	return store
}

func BenchmarkStreamScaling(b *testing.B) {
	for _, size := range []int{1_000, 10_000} {
		base := benchmarkStreamStore(size)
		b.Run(fmt.Sprintf("publish_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				fork := base.Fork()
				fork.Publish("events", "event", int64(size))
			}
		})
		b.Run(fmt.Sprintf("offset_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for index := range b.N {
				fork := base.Fork()
				fork.SetOffset("events", "worker", uint64(index))
			}
		})
		b.Run(fmt.Sprintf("read_tail_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				if records := base.Read("events", uint64(size-1), 1); len(records) != 1 {
					b.Fatal("tail record not found")
				}
			}
		})
	}
}

func BenchmarkStreamSnapshotAccountingScaling(b *testing.B) {
	for _, size := range []int{1_000, 10_000} {
		base := NewGraphState()
		base.Streams = benchmarkStreamStore(size)
		base.SnapshotBytes, _ = EstimateSnapshotBytes(base)
		updated := CloneGraphStateShallow(base)
		updated.Streams.Publish("events", "event", int64(size))
		b.Run(fmt.Sprintf("entries_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				if _, err := ApplyDeltaSnapshotBytes(base, updated, GraphDelta{StreamsChanged: true}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkStreamBulkReadScaling(b *testing.B) {
	for _, size := range []int{1_000, 10_000, 100_000} {
		store := NewStreamStore()
		for index := range size {
			store.Publish("events", "event", int64(index))
		}
		b.Run(fmt.Sprintf("entries_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				if records := store.Read("events", 0, uint(size)); len(records) != size {
					b.Fatal("short stream read")
				}
			}
		})
	}
}

func BenchmarkPagedMapSequentialSet(b *testing.B) {
	for _, size := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("entries_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				values := NewPagedMap[uint64]()
				for id := uint64(1); id <= uint64(size); id++ {
					values.Set(id, id)
				}
			}
		})
	}
}

func BenchmarkPagedMapAllScaling(b *testing.B) {
	for _, size := range []int{1, 1_000, 10_000} {
		values := NewPagedMap[uint64]()
		for id := 1; id <= size; id++ {
			values.Set(uint64(id), uint64(id))
		}
		b.Run(fmt.Sprintf("entries_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				var count int
				for range values.All() {
					count++
				}
				if count != size {
					b.Fatal("short paged map iteration")
				}
			}
		})
	}
}

func BenchmarkSequentialMapForkSet(b *testing.B) {
	for _, size := range []int{1_000, 10_000} {
		paged := NewPagedMap[uint64]()
		sharded := NewShardMap[uint64]()
		for id := uint64(1); id <= uint64(size); id++ {
			paged.Set(id, id)
			sharded.Set(id, id)
		}
		b.Run(fmt.Sprintf("paged_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				fork := paged.Fork()
				fork.CloneShardOnce(uint64(size))
				fork.Set(uint64(size), 0)
			}
		})
		b.Run(fmt.Sprintf("sharded_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				fork := sharded.Fork()
				fork.CloneShardOnce(uint64(size))
				fork.Set(uint64(size), 0)
			}
		})
	}
}

func BenchmarkShardMapDeleteOccupiedShards(b *testing.B) {
	for _, size := range []int{10_000, 50_000} {
		base := NewShardMap[uint64]()
		for id := 0; id < size; id++ {
			base.Set(uint64(id), uint64(id))
		}
		b.Run(fmt.Sprintf("entries_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				fork := base.Fork()
				for id := 0; id < size; id++ {
					fork.CloneShardOnce(uint64(id))
					fork.Delete(uint64(id))
				}
			}
		})
	}
}

func BenchmarkShardMapAllScaling(b *testing.B) {
	for _, size := range []int{1, 1_000, 10_000} {
		values := NewShardMap[uint64]()
		for id := 0; id < size; id++ {
			values.Set(uint64(id), uint64(id))
		}
		b.Run(fmt.Sprintf("entries_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				var count int
				for range values.All() {
					count++
				}
				if count != size {
					b.Fatal("short shard map iteration")
				}
			}
		})
	}
}

func BenchmarkAdjacencyAppendScaling(b *testing.B) {
	for _, size := range []int{1_000, 10_000} {
		var chunked *EdgeList
		flat := make([]uint64, size)
		for index := range size {
			id := uint64(index + 1)
			chunked = chunked.Append(id)
			flat[index] = id
		}
		b.Run(fmt.Sprintf("chunked_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				_ = chunked.Append(uint64(size + 1))
			}
		})
		b.Run(fmt.Sprintf("flat_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				_ = append(slices.Clone(flat), uint64(size+1))
			}
		})
	}
}

func BenchmarkGraphAdjacencyForkAppend(b *testing.B) {
	for _, size := range []int{1_000, 10_000} {
		var list *EdgeList
		flat := make([]uint64, size)
		for index := range size {
			id := uint64(index + 1)
			list = list.Append(id)
			flat[index] = id
		}
		paged := NewPagedMap[*EdgeList]()
		paged.Set(1, list)
		sharded := NewShardMap[[]uint64]()
		sharded.Set(1, flat)
		b.Run(fmt.Sprintf("paged_chunked_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				fork := paged.Fork()
				fork.CloneShardOnce(1)
				fork.Set(1, fork.Get(1).Append(uint64(size+1)))
			}
		})
		b.Run(fmt.Sprintf("sharded_flat_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				fork := sharded.Fork()
				fork.CloneShardOnce(1)
				fork.Set(1, append(slices.Clone(fork.Get(1)), uint64(size+1)))
			}
		})
	}
}

func BenchmarkAdjacencyReadScaling(b *testing.B) {
	for _, size := range []int{1_000, 10_000} {
		var chunked *EdgeList
		flat := make([]uint64, size)
		for index := range size {
			id := uint64(index + 1)
			chunked = chunked.Append(id)
			flat[index] = id
		}
		b.Run(fmt.Sprintf("chunked_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				var total uint64
				if chunked.IsInline() {
					for _, id := range chunked.InlineIDs() {
						total += id
					}
				} else {
					for chunk := range chunked.Chunks() {
						for _, id := range chunk {
							total += id
						}
					}
				}
				if total == 0 {
					b.Fatal("empty adjacency")
				}
			}
		})
		b.Run(fmt.Sprintf("flat_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				var total uint64
				for _, id := range flat {
					total += id
				}
				if total == 0 {
					b.Fatal("empty adjacency")
				}
			}
		})
	}
}

func BenchmarkAdjacencyLookupRequestScaling(b *testing.B) {
	for _, size := range []int{1_000, 10_000} {
		var chunked *EdgeList
		flat := make([]uint64, size)
		paged := NewPagedMap[*EdgeRecord]()
		sharded := NewShardMap[*EdgeRecord]()
		for index := range size {
			id := uint64(index + 1)
			edge := &EdgeRecord{ID: id, SourceID: 1, TargetID: id + 1}
			chunked = chunked.Append(id)
			flat[index] = id
			paged.Set(id, edge)
			sharded.Set(id, edge)
		}
		b.Run(fmt.Sprintf("paged_chunked_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				var total uint64
				if chunked.IsInline() {
					for _, id := range chunked.InlineIDs() {
						total += paged.Get(id).TargetID
					}
				} else {
					for chunk := range chunked.Chunks() {
						for _, id := range chunk {
							total += paged.Get(id).TargetID
						}
					}
				}
				if total == 0 {
					b.Fatal("empty adjacency")
				}
			}
		})
		b.Run(fmt.Sprintf("sharded_flat_%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				var total uint64
				for _, id := range flat {
					total += sharded.Get(id).TargetID
				}
				if total == 0 {
					b.Fatal("empty adjacency")
				}
			}
		})
	}
}
