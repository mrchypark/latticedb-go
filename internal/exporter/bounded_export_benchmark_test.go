package exporter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"runtime"
	"testing"
	"time"

	"github.com/mrchypark/latticedb-go/internal/store"
)

var boundedExportSizes = []int{10_000, 100_000}

var boundedExportFormats = []ExportFormat{
	ExportFormatJSON,
	ExportFormatJSONL,
	ExportFormatDOT,
}

func BenchmarkBoundedExport(b *testing.B) {
	for _, size := range boundedExportSizes {
		graph := boundedExportGraph(size)
		for _, format := range boundedExportFormats {
			b.Run(fmt.Sprintf("%s/%d", format, size), func(b *testing.B) {
				outputBytes := boundedExportOutputBytes(b, graph, format)
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if err := ExportGraphContextTo(context.Background(), graph, format, io.Discard); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(outputBytes), "output-bytes/op")
				b.SetBytes(int64(outputBytes))
			})
		}
	}
}

func BenchmarkBoundedExportLatency(b *testing.B) {
	for _, size := range boundedExportSizes {
		graph := boundedExportGraph(size)
		for _, format := range boundedExportFormats {
			b.Run(fmt.Sprintf("%s/%d", format, size), func(b *testing.B) {
				writer := exportLatencyWriter{format: format}
				var firstByte, firstRecord time.Duration
				for range b.N {
					writer.reset()
					writer.started = time.Now()
					if err := ExportGraphContextTo(context.Background(), graph, format, &writer); err != nil {
						b.Fatal(err)
					}
					if !writer.haveByte || !writer.haveRecord {
						b.Fatal("export did not produce measurable first byte and first record")
					}
					firstByte += writer.firstByte
					firstRecord += writer.firstRecord
				}
				b.ReportMetric(float64(firstByte)/float64(b.N), "first-byte-ns/op")
				b.ReportMetric(float64(firstRecord)/float64(b.N), "first-record-ns/op")
			})
		}
	}
}

func BenchmarkBoundedExportLiveHeap(b *testing.B) {
	for _, size := range boundedExportSizes {
		graph := boundedExportGraph(size)
		for _, format := range boundedExportFormats {
			b.Run(fmt.Sprintf("%s/%d", format, size), func(b *testing.B) {
				b.ReportAllocs()
				var maxPeak uint64
				for range b.N {
					b.StopTimer()
					runtime.GC()
					var before runtime.MemStats
					runtime.ReadMemStats(&before)
					writer := exportLiveHeapWriter{baseHeap: before.HeapAlloc}
					b.StartTimer()
					if err := ExportGraphContextTo(context.Background(), graph, format, &writer); err != nil {
						b.Fatal(err)
					}
					b.StopTimer()
					writer.sample()
					runtime.KeepAlive(graph)
					if writer.peak > maxPeak {
						maxPeak = writer.peak
					}
				}
				b.ReportMetric(float64(maxPeak), "sampled-live-peak-B/op")
			})
		}
	}
}

func boundedExportGraph(size int) *store.GraphState {
	graph := store.NewGraphState()
	for id := uint64(1); id <= uint64(size); id++ {
		graph.Nodes.Set(id, &store.NodeRecord{ID: id, Labels: []string{"Node"}})
	}
	for source := uint64(size); source != 0; source-- {
		id := uint64(size) - source + 1
		target := id
		graph.Edges.Set(id, &store.EdgeRecord{ID: id, SourceID: source, TargetID: target, Type: "edge"})
	}
	return graph
}

type exportCountingWriter struct{ bytes uint64 }

func (writer *exportCountingWriter) Write(value []byte) (int, error) {
	writer.bytes += uint64(len(value))
	return len(value), nil
}

func boundedExportOutputBytes(b *testing.B, graph *store.GraphState, format ExportFormat) uint64 {
	b.Helper()
	var writer exportCountingWriter
	if err := ExportGraphContextTo(context.Background(), graph, format, &writer); err != nil {
		b.Fatal(err)
	}
	return writer.bytes
}

type exportLatencyWriter struct {
	format      ExportFormat
	started     time.Time
	firstByte   time.Duration
	firstRecord time.Duration
	haveByte    bool
	haveRecord  bool
	prefix      []byte
}

func (writer *exportLatencyWriter) reset() {
	writer.firstByte = 0
	writer.firstRecord = 0
	writer.haveByte = false
	writer.haveRecord = false
	writer.prefix = writer.prefix[:0]
}

func (writer *exportLatencyWriter) Write(value []byte) (int, error) {
	if len(value) == 0 {
		return 0, nil
	}
	if writer.haveByte && writer.haveRecord {
		return len(value), nil
	}
	now := time.Now()
	if !writer.haveByte {
		writer.firstByte = now.Sub(writer.started)
		writer.haveByte = true
	}
	if !writer.haveRecord {
		remaining := 8192 - len(writer.prefix)
		if remaining > len(value) {
			remaining = len(value)
		}
		writer.prefix = append(writer.prefix, value[:remaining]...)
		if boundedExportFirstRecordComplete(writer.format, writer.prefix) {
			writer.firstRecord = now.Sub(writer.started)
			writer.haveRecord = true
		}
	}
	return len(value), nil
}

func boundedExportFirstRecordComplete(format ExportFormat, value []byte) bool {
	switch format {
	case ExportFormatJSON:
		prefix := []byte(`{"nodes":[`)
		start := bytes.Index(value, prefix)
		if start < 0 {
			return false
		}
		candidate := value[start+len(prefix):]
		for end := 1; end <= len(candidate); end++ {
			if candidate[end-1] != '}' {
				continue
			}
			record := make([]byte, 1, end+2)
			record[0] = '['
			record = append(record, candidate[:end]...)
			record = append(record, ']')
			if json.Valid(record) {
				return true
			}
		}
		return false
	case ExportFormatJSONL:
		return bytes.Contains(value, []byte{'\n'})
	case ExportFormatDOT:
		prefix := []byte("digraph G {\n")
		start := bytes.Index(value, prefix)
		return start >= 0 && bytes.Contains(value[start+len(prefix):], []byte{'\n'})
	default:
		return false
	}
}

type exportLiveHeapWriter struct {
	baseHeap uint64
	peak     uint64
	writes   uint64
}

func (writer *exportLiveHeapWriter) Write(value []byte) (int, error) {
	if len(value) == 0 {
		return 0, nil
	}
	writer.writes++
	if writer.writes == 1 || writer.writes%4096 == 0 {
		writer.sample()
	}
	return len(value), nil
}

func (writer *exportLiveHeapWriter) sample() {
	var stats runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&stats)
	if stats.HeapAlloc > writer.baseHeap && stats.HeapAlloc-writer.baseHeap > writer.peak {
		writer.peak = stats.HeapAlloc - writer.baseHeap
	}
}
