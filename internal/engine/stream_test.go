package engine

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

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
	db := &DB{graph: store.NewGraphState()}
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
	db := &DB{graph: store.NewGraphState()}
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

func TestReadStreamNotificationsAreStreamScopedAndReclaimed(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "streams"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	alphaContext, cancelAlpha := context.WithCancel(context.Background())
	defer cancelAlpha()
	alphaResult := make(chan struct {
		result StreamReadResult
		err    error
	}, 1)
	go func() {
		result, err := db.ReadStreamContext(alphaContext, "alpha", 0, StreamReadOptions{Limit: 1})
		alphaResult <- struct {
			result StreamReadResult
			err    error
		}{result, err}
	}()
	type blockedReader struct {
		stream string
		cancel context.CancelFunc
		err    chan error
		sub    *streamSubscription
	}
	const unrelatedStreams = 32
	readers := make([]blockedReader, unrelatedStreams)
	for index := range readers {
		stream := "other-" + strconv.Itoa(index)
		ctx, cancel := context.WithCancel(context.Background())
		readers[index] = blockedReader{stream: stream, cancel: cancel, err: make(chan error, 1)}
		go func(reader blockedReader, ctx context.Context) {
			_, err := db.ReadStreamContext(ctx, reader.stream, 0, StreamReadOptions{Limit: 1})
			reader.err <- err
		}(readers[index], ctx)
	}
	waitForStreamWaiters(t, db, "alpha", 1)
	for index := range readers {
		readers[index].sub = waitForStreamWaiters(t, db, readers[index].stream, 1)
	}

	if err := db.Update(func(tx *Tx) error {
		return tx.PublishStream("alpha", "event", "payload")
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-alphaResult:
		if got.err != nil || len(got.result.Records) != 1 || got.result.Records[0].Sequence != 1 {
			t.Fatalf("alpha read = %#v, %v", got.result, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("alpha reader was not notified")
	}
	for _, reader := range readers {
		select {
		case <-reader.sub.notify:
			t.Fatalf("%s reader was notified by an alpha commit", reader.stream)
		default:
		}
	}
	if err := db.Update(func(tx *Tx) error {
		return tx.SetStreamOffset(readers[0].stream, "consumer", 1)
	}); err != nil {
		t.Fatal(err)
	}
	for _, reader := range readers {
		select {
		case <-reader.sub.notify:
			t.Fatalf("%s reader was notified by an unrelated commit", reader.stream)
		default:
		}
	}
	waitForNoStreamSubscription(t, db, "alpha")

	for _, reader := range readers {
		reader.cancel()
		if err := <-reader.err; !errors.Is(err, context.Canceled) {
			t.Fatalf("%s read error = %v, want context.Canceled", reader.stream, err)
		}
		waitForNoStreamSubscription(t, db, reader.stream)
	}
}

func TestReadStreamNotificationFanoutWakesManyConsumers(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "streams"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const consumers = 32
	results := make(chan error, consumers)
	for range consumers {
		go func() {
			result, err := db.ReadStreamContext(context.Background(), "events", 0, StreamReadOptions{Limit: 1})
			if err == nil && (len(result.Records) != 1 || result.Records[0].Sequence != 1) {
				err = errors.New("unexpected stream result")
			}
			results <- err
		}()
	}
	waitForStreamWaiters(t, db, "events", consumers)
	if err := db.Update(func(tx *Tx) error {
		return tx.PublishStream("events", "event", nil)
	}); err != nil {
		t.Fatal(err)
	}
	for range consumers {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	waitForNoStreamSubscription(t, db, "events")
}

func TestReadStreamCloseWakesManyConsumers(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "streams"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}

	const consumers = 32
	results := make(chan error, consumers)
	for range consumers {
		go func() {
			_, err := db.ReadStreamContext(context.Background(), "events", 0, StreamReadOptions{Limit: 1})
			results <- err
		}()
	}
	waitForStreamWaiters(t, db, "events", consumers)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for range consumers {
		if err := <-results; !errors.Is(err, ErrDatabaseClosed) {
			t.Fatalf("read error = %v, want ErrDatabaseClosed", err)
		}
	}
}

func TestReadStreamDoesNotMissPublicationDuringSubscriptionWindow(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "streams"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := &streamReadWindowContext{Context: context.Background()}
	ctx.onSecondErr = func() {
		if err := db.Update(func(tx *Tx) error {
			return tx.PublishStream("events", "event", nil)
		}); err != nil {
			t.Fatal(err)
		}
	}
	result, err := db.ReadStreamContext(ctx, "events", 0, StreamReadOptions{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Records) != 1 || result.Records[0].Sequence != 1 {
		t.Fatalf("read after publication window = %#v", result)
	}
}

func TestReadStreamTimeoutSurvivesUnrelatedCommits(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "streams"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	result := make(chan error, 1)
	go func() {
		_, err := db.ReadStream("events", 0, 1, 50)
		result <- err
	}()
	waitForStreamWaiters(t, db, "events", 1)
	for index := range 32 {
		stream := "other-" + string(rune('a'+index))
		if err := db.Update(func(tx *Tx) error {
			return tx.PublishStream(stream, "event", nil)
		}); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("timed stream read error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed stream read did not finish")
	}
	waitForNoStreamSubscription(t, db, "events")
}

type streamReadWindowContext struct {
	context.Context
	onSecondErr func()
	errs        int
}

func (ctx *streamReadWindowContext) Err() error {
	ctx.errs++
	if ctx.errs == 2 && ctx.onSecondErr != nil {
		ctx.onSecondErr()
		ctx.onSecondErr = nil
	}
	return ctx.Context.Err()
}

func waitForStreamWaiters(t *testing.T, db *DB, stream string, want uint) *streamSubscription {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		db.mu.RLock()
		subscription := db.streamNotify[stream]
		var waiters uint
		if subscription != nil {
			waiters = subscription.waiters
		}
		db.mu.RUnlock()
		if waiters == want {
			return subscription
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s waiters = %d, want %d", stream, waiters, want)
		}
		runtime.Gosched()
	}
}

func waitForNoStreamSubscription(t *testing.T, db *DB, stream string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		db.mu.RLock()
		subscription := db.streamNotify[stream]
		db.mu.RUnlock()
		if subscription == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s subscription was not reclaimed", stream)
		}
		runtime.Gosched()
	}
}

func BenchmarkStreamNotificationFanout(b *testing.B) {
	const (
		streamCount        = 16
		consumersPerStream = 8
		totalConsumers     = streamCount * consumersPerStream
	)
	b.Run("per_stream_target", func(b *testing.B) {
		benchmarkStreamFanout(b, streamCount, consumersPerStream)
	})
	b.Run("global_baseline", func(b *testing.B) {
		benchmarkGlobalStreamFanout(b, totalConsumers)
	})
}

type benchmarkNotificationReader struct {
	arm   chan (<-chan struct{})
	ready chan struct{}
	ack   chan struct{}
	stop  chan struct{}
}

func newBenchmarkNotificationReaders(count int) ([]*benchmarkNotificationReader, *sync.WaitGroup) {
	readers := make([]*benchmarkNotificationReader, count)
	var done sync.WaitGroup
	done.Add(count)
	for index := range readers {
		reader := &benchmarkNotificationReader{
			arm:   make(chan (<-chan struct{}), 1),
			ready: make(chan struct{}, 1),
			ack:   make(chan struct{}, 1),
			stop:  make(chan struct{}),
		}
		readers[index] = reader
		go func() {
			defer done.Done()
			for {
				select {
				case notify := <-reader.arm:
					reader.ready <- struct{}{}
					select {
					case <-notify:
						reader.ack <- struct{}{}
					case <-reader.stop:
						return
					}
				case <-reader.stop:
					return
				}
			}
		}()
	}
	return readers, &done
}

func armBenchmarkReaders(readers []*benchmarkNotificationReader, notify <-chan struct{}) {
	for _, reader := range readers {
		reader.arm <- notify
	}
	for _, reader := range readers {
		<-reader.ready
	}
}

func waitBenchmarkAcks(readers []*benchmarkNotificationReader) {
	for _, reader := range readers {
		<-reader.ack
	}
}

func stopBenchmarkReaders(readers []*benchmarkNotificationReader, done *sync.WaitGroup) {
	for _, reader := range readers {
		close(reader.stop)
	}
	done.Wait()
}

func benchmarkStreamFanout(b *testing.B, streamCount, consumersPerStream int) {
	const totalConsumers = 128
	db := &DB{streamNotify: map[string]*streamSubscription{}}
	readers, done := newBenchmarkNotificationReaders(totalConsumers)
	streams := make([][]*benchmarkNotificationReader, streamCount)
	subscriptions := make([]*streamSubscription, streamCount)
	db.mu.Lock()
	for streamIndex := range streams {
		streams[streamIndex] = readers[streamIndex*consumersPerStream : (streamIndex+1)*consumersPerStream]
		for range consumersPerStream {
			subscriptions[streamIndex] = db.subscribeStreamLocked("stream-" + strconv.Itoa(streamIndex))
		}
	}
	db.mu.Unlock()
	for index, subscription := range subscriptions {
		armBenchmarkReaders(streams[index], subscription.notify)
	}
	target := streams[0]
	operation := []store.StreamOperation{{Type: "publish", Stream: "stream-0"}}
	b.ResetTimer()
	for b.Loop() {
		db.mu.Lock()
		db.notifyStreamsLocked(operation)
		db.mu.Unlock()
		waitBenchmarkAcks(target)
		b.StopTimer()
		db.mu.Lock()
		for range consumersPerStream {
			subscriptions[0] = db.subscribeStreamLocked("stream-0")
		}
		db.mu.Unlock()
		armBenchmarkReaders(target, subscriptions[0].notify)
		b.StartTimer()
	}
	b.StopTimer()
	db.mu.Lock()
	db.notifyAllStreamsLocked()
	db.mu.Unlock()
	stopBenchmarkReaders(readers, done)
}

func benchmarkGlobalStreamFanout(b *testing.B, consumers int) {
	readers, done := newBenchmarkNotificationReaders(consumers)
	notify := make(chan struct{})
	armBenchmarkReaders(readers, notify)
	b.ResetTimer()
	for b.Loop() {
		current := notify
		close(current)
		waitBenchmarkAcks(readers)
		b.StopTimer()
		notify = make(chan struct{})
		armBenchmarkReaders(readers, notify)
		b.StartTimer()
	}
	b.StopTimer()
	close(notify)
	stopBenchmarkReaders(readers, done)
}
