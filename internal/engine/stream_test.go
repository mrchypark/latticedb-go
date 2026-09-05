package engine

import (
	"bytes"
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestReadStreamConcurrentGenerationAndPayloadIsolation(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "streams"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const recordsPerGeneration = 16
	publish := func(generation int) error {
		return db.Update(func(tx *Tx) error {
			if err := tx.TrimStream("events", uint64(generation*recordsPerGeneration)); err != nil {
				return err
			}
			payload := map[string]any{"nested": []any{bytes.Repeat([]byte{byte(generation)}, 4096)}}
			for range recordsPerGeneration {
				if err := tx.PublishStream("events", "event", payload); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if err := publish(0); err != nil {
		t.Fatal(err)
	}
	frozen, lease, err := db.SnapshotGraph()
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	check := func(records []StreamRecord) {
		t.Helper()
		if len(records) != recordsPerGeneration {
			t.Fatalf("read %d records, want one complete generation", len(records))
		}
		generation := (records[0].Sequence - 1) / recordsPerGeneration
		for index, record := range records {
			if record.Sequence != generation*recordsPerGeneration+uint64(index)+1 {
				t.Fatalf("mixed generations or ordering: sequence %d at %d", record.Sequence, index)
			}
			blob := record.Payload.(map[string]any)["nested"].([]any)[0].([]byte)
			if !bytes.Equal(blob, bytes.Repeat([]byte{byte(generation)}, 4096)) {
				t.Fatal("payload changed across generations or leaked a reader mutation")
			}
			blob[0] = 255 // A returned payload belongs to this reader alone.
		}
	}
	start := make(chan struct{})
	var workers sync.WaitGroup
	workers.Go(func() {
		<-start
		for generation := 1; generation <= 16; generation++ {
			if err := publish(generation); err != nil {
				t.Error(err)
				return
			}
		}
	})
	for range 4 {
		workers.Go(func() {
			<-start
			for range 32 {
				result, err := db.ReadStreamContext(context.Background(), "events", 0, StreamReadOptions{Limit: recordsPerGeneration})
				if err != nil {
					t.Error(err)
					return
				}
				check(result.Records)
				if result.ByteLimited || result.LastSequence != result.Records[recordsPerGeneration-1].Sequence {
					t.Errorf("read metadata = last %d, limited %v", result.LastSequence, result.ByteLimited)
					return
				}
			}
		})
	}
	close(start)
	workers.Wait()
	frozenRecords := frozen.Streams.Read("events", 0, recordsPerGeneration)
	check(frozenRecords)
	if frozenRecords[0].Sequence != 1 {
		t.Fatal("the pinned stream generation advanced")
	}
	result, err := db.ReadStream("events", 0, recordsPerGeneration, 0)
	if err != nil {
		t.Fatal(err)
	}
	check(result)
	if result[0].Sequence != 16*recordsPerGeneration+1 {
		t.Fatal("the final committed stream generation is not visible")
	}
}

func TestReadStreamImmediateDoesNotAllocateTimer(t *testing.T) {
	db := &DB{graph: store.NewGraphState(), streamNotify: make(chan struct{})}
	allocs := testing.AllocsPerRun(100, func() {
		records, err := db.ReadStream("events", 0, 1, 0)
		if err != nil || len(records) != 0 {
			t.Fatalf("read = %#v, %v", records, err)
		}
	})
	if allocs != 0 {
		t.Fatalf("immediate stream read allocations = %f", allocs)
	}
}

// This isolates the writer publication lock from WAL and disk latency while
// another goroutine reads and copies a 256 KiB stream payload.
func BenchmarkReadStreamWriterLock(b *testing.B) {
	db := &DB{graph: store.NewGraphState(), streamNotify: make(chan struct{})}
	db.graph.Streams.Publish("events", "event", bytes.Repeat([]byte{1}, 256<<10))
	ready, stop := make(chan struct{}), make(chan struct{})
	var reader sync.WaitGroup
	reader.Go(func() {
		close(ready)
		for {
			select {
			case <-stop:
				return
			default:
				if _, err := db.ReadStream("events", 0, 1, 0); err != nil {
					b.Error(err)
					return
				}
			}
		}
	})
	<-ready
	for b.Loop() {
		db.mu.Lock()
		db.mu.Unlock()
	}
	close(stop)
	reader.Wait()
}
