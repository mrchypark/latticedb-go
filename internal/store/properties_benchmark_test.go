package store

import (
	"fmt"
	"maps"
	"runtime"
	"testing"
)

var (
	benchSinkAny   map[string]any
	benchSinkProps Properties
	benchSinkVal   any
	benchSinkBool  bool
)

func propertyBenchmarkInput(count int) map[string]any {
	m := make(map[string]any, count)
	for i := range count {
		switch i % 5 {
		case 0:
			m[fmt.Sprintf("int_%d", i)] = int64(i * 100)
		case 1:
			m[fmt.Sprintf("float_%d", i)] = float64(i) * 1.5
		case 2:
			m[fmt.Sprintf("str_%d", i)] = fmt.Sprintf("value-%d", i)
		case 3:
			m[fmt.Sprintf("bool_%d", i)] = i%2 == 0
		case 4:
			m[fmt.Sprintf("vec_%d", i)] = []float32{float32(i), float32(i + 1)}
		}
	}
	return m
}

func BenchmarkPropertyContainerConstruct(b *testing.B) {
	for _, count := range []int{1, 4, 8, 16, 64} {
		input := propertyBenchmarkInput(count)
		b.Run(fmt.Sprintf("map_%d", count), func(b *testing.B) {
			var sink map[string]any
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				sink = make(map[string]any, len(input))
				for k, v := range input {
					sink[k] = v
				}
			}
			benchSinkAny = sink
		})
		b.Run(fmt.Sprintf("properties_%d", count), func(b *testing.B) {
			var sink Properties
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				sink = PropertiesFromMap(input)
			}
			benchSinkProps = sink
		})
	}
}

func BenchmarkPropertyContainerLookup(b *testing.B) {
	for _, count := range []int{1, 4, 8, 16, 64} {
		input := propertyBenchmarkInput(count)
		m := make(map[string]any, len(input))
		for k, v := range input {
			m[k] = v
		}
		p := PropertiesFromMap(input)
		hitKey := "int_0"
		if _, ok := m[hitKey]; !ok {
			b.Fatalf("hitKey %q not in map for count %d", hitKey, count)
		}
		if _, ok := p.Lookup(hitKey); !ok {
			b.Fatalf("hitKey %q not in Properties for count %d", hitKey, count)
		}
		b.Run(fmt.Sprintf("map_hit_%d", count), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				v, ok := m[hitKey]
				benchSinkVal = v
				benchSinkBool = ok
			}
		})
		b.Run(fmt.Sprintf("properties_hit_%d", count), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				v, ok := p.Lookup(hitKey)
				benchSinkVal = v
				benchSinkBool = ok
			}
		})
		b.Run(fmt.Sprintf("map_miss_%d", count), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				v, ok := m["no_such_key"]
				benchSinkVal = v
				benchSinkBool = ok
			}
		})
		b.Run(fmt.Sprintf("properties_miss_%d", count), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				v, ok := p.Lookup("no_such_key")
				benchSinkVal = v
				benchSinkBool = ok
			}
		})
	}
}

func BenchmarkPropertyContainerCloneSet(b *testing.B) {
	for _, count := range []int{1, 4, 8, 16, 64} {
		input := propertyBenchmarkInput(count)
		m := make(map[string]any, len(input))
		for k, v := range input {
			m[k] = v
		}
		p := PropertiesFromMap(input)
		setKey := "int_0"
		b.Run(fmt.Sprintf("map_%d", count), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				cloned := maps.Clone(m)
				cloned[setKey] = int64(-999)
				benchSinkAny = cloned
			}
		})
		b.Run(fmt.Sprintf("properties_%d", count), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				cloned := p.Clone()
				cloned.Set(setKey, int64(-999))
				benchSinkProps = cloned
			}
		})
	}
}

// Each sample retains 10,000 entity containers sharing pre-existing keys and
// payloads. The metric includes slice descriptors, excludes shared payloads,
// and measures live Go heap rather than process RSS.
func BenchmarkPropertyContainerRetained(b *testing.B) {
	for _, count := range []int{1, 4, 8, 16, 64} {
		input := propertyBenchmarkInput(count)
		for _, compact := range []bool{false, true} {
			name := "map"
			if compact {
				name = "properties"
			}
			b.Run(fmt.Sprintf("%s_%d", name, count), func(b *testing.B) {
				var retained int64
				b.ReportAllocs()
				for range b.N {
					b.StopTimer()
					runtime.GC()
					var before, after runtime.MemStats
					runtime.ReadMemStats(&before)
					b.StartTimer()
					if compact {
						records := make([]Properties, 10_000)
						for i := range records {
							records[i] = PropertiesFromMap(input)
						}
						b.StopTimer()
						runtime.GC()
						runtime.ReadMemStats(&after)
						runtime.KeepAlive(records)
					} else {
						records := make([]map[string]any, 10_000)
						for i := range records {
							records[i] = maps.Clone(input)
						}
						b.StopTimer()
						runtime.GC()
						runtime.ReadMemStats(&after)
						runtime.KeepAlive(records)
					}
					retained += int64(after.HeapAlloc) - int64(before.HeapAlloc)
				}
				b.ReportMetric(float64(retained)/float64(b.N), "retained_heap_B")
				runtime.KeepAlive(input)
			})
		}
	}
}
