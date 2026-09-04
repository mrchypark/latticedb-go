package latticedb

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestLargePropertyChangefeedUsesBoundedSummaries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "large-changefeed.ltdb")
	db, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	oldValue := bytes.Repeat([]byte{'a'}, 1<<20)
	newValue := bytes.Repeat([]byte{'b'}, 1<<20)
	var nodeID uint64
	if err := db.Update(func(tx *Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]Value{"payload": oldValue}})
		nodeID = node.ID
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error { return tx.SetProperty(nodeID, "payload", newValue) }); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(path, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() >= 4<<20 {
		t.Fatalf("large property change WAL = %d bytes", info.Size())
	}
	changes, err := db.Changes(0, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	assertBoundedPropertyChange(t, changes[len(changes)-1], nodeID)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := SimulateCrash(path); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.View(func(tx *Tx) error {
		value, ok, err := tx.GetProperty(nodeID, "payload")
		if err == nil && (!ok || !bytes.Equal(value.([]byte), newValue)) {
			t.Fatal("recovered graph property differs")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	changes, err = db.Changes(0, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	assertBoundedPropertyChange(t, changes[len(changes)-1], nodeID)
}

func TestAutomaticChangefeedRetentionPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retained-changefeed.ltdb")
	db, err := Open(path, OpenOptions{Create: true, ChangefeedMaxBytes: 1 << 10})
	if err != nil {
		t.Fatal(err)
	}
	var nodeID uint64
	if err := db.Update(func(tx *Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]Value{"value": int64(0)}})
		nodeID = node.ID
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for value := int64(1); value <= 100; value++ {
		if err := db.Update(func(tx *Tx) error { return tx.SetProperty(nodeID, "value", value) }); err != nil {
			t.Fatal(err)
		}
	}
	changes, err := db.Changes(0, 1_000, 0)
	if err != nil || len(changes) == 0 || changes[0].Sequence == 1 || changes[len(changes)-1].Sequence != 102 {
		t.Fatalf("retained changes = first/last/len %#v/%#v/%d, %v", changes[0], changes[len(changes)-1], len(changes), err)
	}
	first := changes[0].Sequence
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	changes, err = reopened.Changes(0, 1_000, 0)
	if err != nil || len(changes) == 0 || changes[0].Sequence != first || changes[len(changes)-1].Sequence != 102 {
		t.Fatalf("recovered retained changes = %#v, %v", changes, err)
	}
}

func TestOversizedChangeRecordCannotExhaustSnapshotBudget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "oversized-change.ltdb")
	db, err := Open(path, OpenOptions{
		Create:                   true,
		MaxDatabaseSnapshotBytes: 100_000,
		ChangefeedMaxBytes:       12_500,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"v": strings.Repeat("x", 10_000)}})
		return err
	}); err != nil {
		t.Fatalf("graph write below snapshot limit failed because of changefeed: %v", err)
	}
}

func TestOffsetOnlyStreamCheckpointAndSerialization(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offset-only.ltdb")
	db, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error { return tx.SetStreamOffset("events", "worker", 42) }); err != nil {
		t.Fatal(err)
	}
	serialized, err := db.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	copyDB, err := Deserialize(serialized, OpenOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if offset, ok, err := copyDB.GetStreamOffset("events", "worker"); err != nil || !ok || offset != 42 {
		t.Fatalf("serialized offset = %d, %v, %v", offset, ok, err)
	}
	if err := copyDB.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, OpenOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if offset, ok, err := reopened.GetStreamOffset("events", "worker"); err != nil || !ok || offset != 42 {
		t.Fatalf("checkpoint offset = %d, %v, %v", offset, ok, err)
	}
}

func assertBoundedPropertyChange(t *testing.T, record StreamRecord, nodeID uint64) {
	t.Helper()
	payload, ok := record.Payload.(map[string]any)
	if !ok || record.Kind != "node.property_set" || payload["node_id"] != int64(nodeID) {
		t.Fatalf("unexpected property change = %#v", record)
	}
	for _, key := range []string{"old_value", "new_value"} {
		summary, ok := payload[key].(map[string]any)
		if !ok || summary["__lattice_value_omitted"] != true || summary["type"] != "bytes" {
			t.Fatalf("%s summary = %#v", key, payload[key])
		}
	}
}

func TestStreamChangefeedWALStaysBoundedAcrossCheckpoints(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bounded-stream-wal.ltdb")
	db, err := Open(path, OpenOptions{Create: true, WALCheckpointThresholdBytes: 8 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var nodeID uint64
	if err := db.Update(func(tx *Tx) error {
		if _, err := tx.PublishStreamGetSequence("events", "first", int64(1)); err != nil {
			return err
		}
		if err := tx.PublishStream("events", "second", int64(2)); err != nil {
			return err
		}
		if err := tx.SetStreamOffset("events", "worker", 2); err != nil {
			return err
		}
		if err := tx.TrimStream("events", 1); err != nil {
			return err
		}
		node, err := tx.CreateNode(CreateNodeOptions{})
		nodeID = node.ID
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for value := int64(1); value <= 200; value++ {
		updateWithCheckpointRetry(t, db, func(tx *Tx) error { return tx.SetProperty(nodeID, "value", value) })
	}
	info, err := os.Stat(filepath.Join(path, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() > 16<<10 {
		t.Fatalf("WAL grew to %d bytes", info.Size())
	}
	t.Logf("WAL after 200 graph commits: %d bytes", info.Size())
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := SimulateCrash(path); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	records, err := db.ReadStream("events", 0, 10, 0)
	if err != nil || len(records) != 1 || records[0].Sequence != 2 || records[0].Payload != int64(2) {
		t.Fatalf("recovered stream = %#v, %v", records, err)
	}
	if offset, ok, err := db.GetStreamOffset("events", "worker"); err != nil || !ok || offset != 2 {
		t.Fatalf("recovered offset = %d, %v, %v", offset, ok, err)
	}
	changes, err := db.Changes(0, 300, 0)
	if err != nil || len(changes) != 201 || !hasStreamChange(changes, "node.property_set", nodeID, "value") {
		t.Fatalf("recovered changes = %d, %v", len(changes), err)
	}
}

func updateWithCheckpointRetry(t *testing.T, db *DB, update func(*Tx) error) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := db.Update(update)
		if err == nil {
			return
		}
		if !errors.Is(err, ErrResourceLimit) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("checkpoint did not make progress after WAL bound")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestStreamsCommitRollbackRecoveryAndChangefeed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "streams.ltdb")
	db, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	if sequence, err := tx.PublishStreamGetSequence("events", "hidden", "no"); err != nil || sequence != 1 {
		t.Fatalf("rollback publish = (%d, %v)", sequence, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if records, err := db.ReadStream("events", 0, 10, 0); err != nil || len(records) != 0 {
		t.Fatalf("rolled back records = %#v, %v", records, err)
	}

	var nodeID uint64
	if err := db.Update(func(tx *Tx) error {
		sequence, err := tx.PublishStreamGetSequence("events", "created", map[string]Value{"id": int64(1)})
		if err != nil || sequence != 1 {
			return errors.New("unexpected sequence")
		}
		if err := tx.PublishStream("events", "updated", "two"); err != nil {
			return err
		}
		node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Person"}, Properties: map[string]Value{"name": "Ada"}})
		nodeID = node.ID
		return err
	}); err != nil {
		t.Fatal(err)
	}
	records, err := db.ReadStream("events", 1, 10, 0)
	if err != nil || len(records) != 1 || records[0].Sequence != 2 || records[0].Kind != "updated" || records[0].Payload != "two" {
		t.Fatalf("records after cursor = %#v, %v", records, err)
	}
	all, err := db.ReadStream("events", 0, 10, 0)
	if err != nil || len(all) != 2 || !reflect.DeepEqual(all[0].Payload, map[string]any{"id": int64(1)}) {
		t.Fatalf("all records = %#v, %v", all, err)
	}
	changes, err := db.Changes(0, 20, 0)
	if err != nil || !hasStreamChange(changes, "node.insert", nodeID, "") || !hasStreamChange(changes, "node.property_set", nodeID, "name") {
		t.Fatalf("changes = %#v, %v", changes, err)
	}
	if err := db.Update(func(tx *Tx) error {
		if err := tx.SetStreamOffset("events", "worker-a", 2); err != nil {
			return err
		}
		return tx.TrimStream("events", 1)
	}); err != nil {
		t.Fatal(err)
	}
	if offset, ok, err := db.GetStreamOffset("events", "worker-a"); err != nil || !ok || offset != 2 {
		t.Fatalf("live offset = %d, %v, %v", offset, ok, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	offset, ok, err := reopened.GetStreamOffset("events", "worker-a")
	if err != nil || !ok || offset != 2 {
		t.Fatalf("offset = %d, %v, %v", offset, ok, err)
	}
	records, err = reopened.ReadStream("events", 0, 10, 0)
	if err != nil || len(records) != 1 || records[0].Sequence != 2 {
		t.Fatalf("recovered records = %#v, %v", records, err)
	}
}

func TestStreamValidationReadOnlyAndWakeup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "streams.ltdb")
	db, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		if err := tx.PublishStream("__lattice_user", "message", "no"); !errors.Is(err, ErrInvalidArgument) {
			return errors.New("reserved stream was accepted")
		}
		if _, err := tx.PublishStreamGetSequence("events", "", "no"); !errors.Is(err, ErrInvalidArgument) {
			return errors.New("empty kind was accepted")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ReadStream("events", 0, 0, 0); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("zero limit error = %v", err)
	}
	result := make(chan []StreamRecord, 1)
	errs := make(chan error, 1)
	go func() {
		records, err := db.ReadStream("events", 0, 10, 500)
		if err != nil {
			errs <- err
			return
		}
		result <- records
	}()
	time.Sleep(10 * time.Millisecond)
	if err := db.Update(func(tx *Tx) error { return tx.PublishStream("events", "message", "wake") }); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errs:
		t.Fatal(err)
	case records := <-result:
		if len(records) != 1 || records[0].Payload != "wake" {
			t.Fatalf("wakeup records = %#v", records)
		}
	case <-time.After(time.Second):
		t.Fatal("stream waiter did not wake")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := db.GetStreamOffset("events", "worker"); !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("closed get offset error = %v", err)
	}
	readOnly, err := Open(path, OpenOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer readOnly.Close()
	if err := readOnly.Update(func(tx *Tx) error { return tx.PublishStream("events", "message", "no") }); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("read-only publish error = %v", err)
	}
}

func TestReadStreamContextByteLimit(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "stream-bounds.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		if err := tx.PublishStream("events", "message", strings.Repeat("x", 1_024)); err != nil {
			return err
		}
		return tx.PublishStream("events", "message", strings.Repeat("y", 1_024))
	}); err != nil {
		t.Fatal(err)
	}
	result, err := db.ReadStreamContext(context.Background(), "events", 0, StreamReadOptions{Limit: 2, MaxBytes: 100})
	if err != nil || len(result.Records) != 0 || result.LastSequence != 0 || !result.ByteLimited {
		t.Fatalf("small budget result = %#v, %v", result, err)
	}
	result, err = db.ReadStreamContext(context.Background(), "events", 0, StreamReadOptions{Limit: 2, MaxBytes: 10_000})
	if err != nil || len(result.Records) != 1 || result.LastSequence != 1 || !result.ByteLimited {
		t.Fatalf("partial budget result = %#v, %v", result, err)
	}
	legacy, err := db.ReadStream("events", 0, 2, 0)
	if err != nil || len(legacy) != 2 {
		t.Fatalf("legacy read = %#v, %v", legacy, err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := db.ReadStreamContext(canceled, "events", 2, StreamReadOptions{Limit: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled read error = %v", err)
	}
}

func hasStreamChange(records []StreamRecord, kind string, nodeID uint64, key string) bool {
	for _, record := range records {
		if record.Kind != kind {
			continue
		}
		payload, ok := record.Payload.(map[string]any)
		if !ok || payload["node_id"] != int64(nodeID) {
			continue
		}
		if key == "" || payload["key"] == key {
			return true
		}
	}
	return false
}
