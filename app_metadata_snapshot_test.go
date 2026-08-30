package latticedb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestAppMetadataIsTransactionalAndDurable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata.ltdb")
	db, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	key := []byte{0xff, 0, 1}
	value := []byte("committed")
	if err := db.Update(func(tx *Tx) error {
		if _, err := tx.Query("CREATE (:Item {id: 1})", nil); err != nil {
			return err
		}
		if err := tx.PutAppMetadata(key, value); err != nil {
			return err
		}
		key[0], value[0] = 0, 'X'
		got, ok, err := tx.GetAppMetadata([]byte{0xff, 0, 1})
		if err != nil || !ok || string(got) != "committed" {
			t.Fatalf("metadata in transaction = %q, %t, %v", got, ok, err)
		}
		got[0] = 'X'
		again, _, err := tx.GetAppMetadata([]byte{0xff, 0, 1})
		if err != nil || string(again) != "committed" {
			t.Fatalf("metadata defensive read = %q, %v", again, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	rolledBack, err := db.BeginWrite()
	if err != nil {
		t.Fatal(err)
	}
	if err := rolledBack.PutAppMetadata([]byte("rolled-back"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := rolledBack.DeleteAppMetadata([]byte{0xff, 0, 1}); err != nil {
		t.Fatal(err)
	}
	if err := rolledBack.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *Tx) error {
		if err := tx.PutAppMetadata([]byte("read-only"), nil); !errors.Is(err, ErrReadOnly) {
			t.Fatalf("read-only metadata write = %v", err)
		}
		if _, ok, err := tx.GetAppMetadata([]byte("rolled-back")); err != nil || ok {
			t.Fatalf("rolled-back metadata = %t, %v", ok, err)
		}
		got, ok, err := tx.GetAppMetadata([]byte{0xff, 0, 1})
		if err != nil || !ok || string(got) != "committed" {
			t.Fatalf("committed metadata = %q, %t, %v", got, ok, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	serialized, err := db.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	copyDB, err := Deserialize(serialized, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	assertMetadataAndItem(t, copyDB, []byte{0xff, 0, 1}, "committed", 1)
	if err := copyDB.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	assertMetadataAndItem(t, reopened, []byte{0xff, 0, 1}, "committed", 1)
}

func TestAppMetadataAndGraphRecoverTogetherFromWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata-recovery.ltdb")
	if os.Getenv("LATTICEDB_METADATA_RECOVERY_CHILD") == "1" {
		db, err := Open(os.Getenv("LATTICEDB_METADATA_RECOVERY_PATH"), OpenOptions{})
		if err != nil {
			os.Exit(2)
		}
		err = db.Update(func(tx *Tx) error {
			if _, err := tx.Query("CREATE (:Recovered {id: 7})", nil); err != nil {
				return err
			}
			return tx.PutAppMetadata([]byte("tip"), []byte{7})
		})
		if err != nil {
			os.Exit(3)
		}
		os.Exit(0)
	}
	seed, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestAppMetadataAndGraphRecoverTogetherFromWAL$")
	cmd.Env = append(os.Environ(), "LATTICEDB_METADATA_RECOVERY_CHILD=1", "LATTICEDB_METADATA_RECOVERY_PATH="+path)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("recovery child: %v\n%s", err, output)
	}
	db, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	assertMetadataAndItem(t, db, []byte("tip"), string([]byte{7}), 1)
}

func TestTxQueryContextLimitsAreStatementAtomic(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "query-context.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.BeginWrite()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Query("CREATE (:Kept {id: 1})", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Query("MATCH (n:Kept) SET n.name = 'before'", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.QueryContext(&cancelAfterChecks{after: 3}, "MATCH (n:Kept) SET n.name = 'leaked'", nil, QueryOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled SET = %v", err)
	}
	kept, err := tx.Query("MATCH (n:Kept) RETURN n.name", nil)
	if err != nil || kept.Rows[0]["n.name"] != "before" {
		t.Fatalf("cancelled SET leaked into transaction: result=%+v err=%v", kept, err)
	}
	_, err = tx.QueryContext(context.Background(), "CREATE (n:Partial {id: 1}) RETURN id(n) AS id", nil, QueryOptions{MaxBytes: 1})
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("limited mutation = %v", err)
	}
	result, err := tx.Query("MATCH (n:Partial) RETURN count(n) AS total", nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows[0]["total"] != int64(0) {
		t.Fatalf("failed statement left mutations: %#v", result.Rows)
	}
	result, err = tx.Query("MATCH (n:Kept) RETURN count(n) AS total", nil)
	if err != nil || result.Rows[0]["total"] != int64(1) {
		t.Fatalf("failed statement damaged prior transaction state: result=%+v err=%v", result, err)
	}
	if _, err := tx.QueryContext(context.Background(), "CREATE (:Committed {id: 1})", nil, QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := tx.PutAppMetadata([]byte("same-tx"), []byte("yes")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	kept, err = db.Query("MATCH (n:Kept) RETURN n.name", nil)
	if err != nil || kept.Rows[0]["n.name"] != "before" {
		t.Fatalf("cancelled SET reached commit: result=%+v err=%v", kept, err)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := db.View(func(tx *Tx) error {
		_, err := tx.QueryContext(cancelled, "MATCH (n) RETURN n", nil, QueryOptions{})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled query = %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *Tx) error {
		if _, err := tx.QueryContext(context.Background(), "UNWIND $items AS value RETURN value", map[string]Value{"items": []any{int64(1), int64(2)}}, QueryOptions{MaxRows: 1}); !errors.Is(err, ErrResourceLimit) {
			t.Fatalf("row-limited query = %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

type cancelAfterChecks struct {
	checks atomic.Int32
	after  int32
}

func (*cancelAfterChecks) Deadline() (time.Time, bool) { return time.Time{}, false }
func (*cancelAfterChecks) Done() <-chan struct{}       { return nil }
func (ctx *cancelAfterChecks) Err() error {
	if ctx.checks.Add(1) >= ctx.after {
		return context.Canceled
	}
	return nil
}
func (*cancelAfterChecks) Value(any) any { return nil }

func TestParameterizedPatternsAndOrderBy(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "query.db"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for _, id := range []string{"two", "one"} {
		if _, err := db.Query("CREATE (:Item {id: $id})", map[string]Value{"id": id}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Query("CREATE (:Item {id: 'zero'})", nil); err != nil {
		t.Fatal(err)
	}
	matched, err := db.Query("MATCH (n:Item {id: $id}) RETURN n.id", map[string]Value{"id": "one"})
	if err != nil || len(matched.Rows) != 1 || matched.Rows[0]["n.id"] != "one" {
		t.Fatalf("parameterized MATCH result=%+v err=%v", matched, err)
	}
	ordered, err := db.Query("MATCH (n:Item) RETURN n.id ORDER BY n.id", nil)
	if err != nil || len(ordered.Rows) != 3 || ordered.Rows[0]["n.id"] != "one" || ordered.Rows[1]["n.id"] != "two" || ordered.Rows[2]["n.id"] != "zero" {
		t.Fatalf("ordered result=%+v err=%v", ordered, err)
	}
	if err := db.CreateNodePropertyIndex("Item", "id"); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginWrite()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Query("CREATE (:Item {id: 'same-tx'})", nil); err != nil {
		t.Fatal(err)
	}
	matched, err = tx.Query("MATCH (n:Item {id: $id}) RETURN n.id", map[string]Value{"id": "same-tx"})
	if err != nil || len(matched.Rows) != 1 || matched.Rows[0]["n.id"] != "same-tx" {
		t.Fatalf("same-transaction indexed read result=%+v err=%v", matched, err)
	}
}

func TestStreamCommitWakesReaderAfterMetadataReadYourWrites(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "stream.db"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	type streamResult struct {
		records []StreamRecord
		err     error
	}
	waiting := make(chan streamResult, 1)
	go func() {
		records, err := db.ReadStream("events", 0, 10, 2_000)
		waiting <- streamResult{records: records, err: err}
	}()

	if err := db.Update(func(tx *Tx) error {
		if _, err := tx.QueryContext(context.Background(), "CREATE (:Item {id: '1'})", nil, QueryOptions{MaxRows: 10_000, MaxBytes: 16 << 20}); err != nil {
			return err
		}
		if err := tx.PublishStream("events", "created", "one"); err != nil {
			return err
		}
		if err := tx.PutAppMetadata([]byte("tip"), []byte("1")); err != nil {
			return err
		}
		value, ok, err := tx.GetAppMetadata([]byte("tip"))
		if err != nil || !ok || string(value) != "1" {
			return fmt.Errorf("metadata read-your-writes value=%q ok=%v err=%v", value, ok, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case result := <-waiting:
		if result.err != nil || len(result.records) != 1 || result.records[0].Kind != "created" || result.records[0].Payload != "one" {
			t.Fatalf("stream result=%+v err=%v", result.records, result.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stream reader was not notified after commit")
	}
}

func TestSnapshotPinsGenerationWhileWritersContinue(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.ltdb")
	db, err := Open(sourcePath, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		if _, err := tx.Query("CREATE (:Item {id: 1})", nil); err != nil {
			return err
		}
		if err := tx.PutAppMetadata([]byte("tip"), []byte{1}); err != nil {
			return err
		}
		return tx.PublishStream("events", "created", map[string]Value{"id": int64(1)})
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := db.BeginSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.BeginSnapshot(); !errors.Is(err, ErrSnapshotActive) {
		t.Fatalf("second snapshot = %v", err)
	}
	if err := db.Close(); !errors.Is(err, ErrSnapshotActive) || !db.IsOpen() {
		t.Fatalf("close with snapshot = %v, open=%t", err, db.IsOpen())
	}
	if err := snapshot.Backup(sourcePath); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("backup over source = %v", err)
	}

	backupPath := filepath.Join(dir, "backup.ltdb")
	backupDone := make(chan error, 1)
	go func() { backupDone <- snapshot.Backup(backupPath) }()
	if err := db.Update(func(tx *Tx) error {
		if _, err := tx.Query("CREATE (:Item {id: 2})", nil); err != nil {
			return err
		}
		if err := tx.PutAppMetadata([]byte("tip"), []byte{2}); err != nil {
			return err
		}
		return tx.PublishStream("events", "created", map[string]Value{"id": int64(2)})
	}); err != nil {
		t.Fatal(err)
	}
	if err := <-backupDone; err != nil {
		t.Fatal(err)
	}

	copyDB, err := Open(backupPath, OpenOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	assertMetadataAndItem(t, copyDB, []byte("tip"), string([]byte{1}), 1)
	records, err := copyDB.ReadStream("events", 0, 10, 0)
	if err != nil || len(records) != 1 {
		t.Fatalf("snapshot stream records = %d, %v", len(records), err)
	}
	if err := copyDB.Close(); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatalf("idempotent snapshot close = %v", err)
	}
	if err := snapshot.Backup(filepath.Join(dir, "closed.ltdb")); !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("backup after close = %v", err)
	}
	again, err := db.BeginSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := again.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func assertMetadataAndItem(t *testing.T, db *DB, key []byte, value string, count int64) {
	t.Helper()
	if err := db.View(func(tx *Tx) error {
		got, ok, err := tx.GetAppMetadata(key)
		if err != nil || !ok || string(got) != value {
			t.Fatalf("metadata = %q, %t, %v", got, ok, err)
		}
		result, err := tx.Query("MATCH (n) RETURN count(n) AS total", nil)
		if err != nil {
			return err
		}
		if result.Rows[0]["total"] != count {
			t.Fatalf("node count = %#v, want %d", result.Rows, count)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
