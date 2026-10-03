package latticedb

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestMemoryDatabaseUsesNoFiles(t *testing.T) {
	work := t.TempDir()
	t.Chdir(work)
	t.Setenv("TMPDIR", work)
	t.Setenv("TMP", work)
	t.Setenv("TEMP", work)

	db, err := Open(":memory:", OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query("CREATE (:Memory {value: 'resident'})", nil); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	assertMemoryTestDirEmpty(t, work)
}

func TestMemoryDatabaseSerializeDeserializeRoundTrip(t *testing.T) {
	work := t.TempDir()
	t.Chdir(work)
	t.Setenv("TMPDIR", work)
	t.Setenv("TMP", work)
	t.Setenv("TEMP", work)
	db, err := Open(":memory:", OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query("CREATE (:Memory {value: 'roundtrip'})", nil); err != nil {
		t.Fatal(err)
	}
	data, err := db.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	copyDB, err := Deserialize(data, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer copyDB.Close()
	result, err := copyDB.Query("MATCH (n:Memory) RETURN n.value AS value", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["value"] != "roundtrip" {
		t.Fatalf("unexpected deserialized data: %#v", result.Rows)
	}
	if copyDB.Path() != "<deserialized>" {
		t.Fatalf("unexpected deserialized path %q", copyDB.Path())
	}
	assertMemoryTestDirEmpty(t, work)
	if err := copyDB.Close(); err != nil {
		t.Fatal(err)
	}
	readOnlyDB, err := Deserialize(data, OpenOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer readOnlyDB.Close()
	if _, err := readOnlyDB.Query("CREATE (:Forbidden)", nil); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("read-only deserialized database accepted a write: %v", err)
	}
	if err := readOnlyDB.Close(); err != nil {
		t.Fatal(err)
	}
	assertMemoryTestDirEmpty(t, work)
}

func TestMemoryDatabaseLifecycleContracts(t *testing.T) {
	db, err := Open(":memory:", OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	snapshot, err := db.BeginSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	stats, err := db.OperationalStats()
	if err != nil || stats.ActiveSnapshots != 1 {
		t.Fatalf("snapshot lease stats = %+v, %v", stats, err)
	}

	rolledBack, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	rolledBackNode, err := rolledBack.CreateNode(CreateNodeOptions{Labels: []string{"Item"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := rolledBack.Rollback(); err != nil {
		t.Fatal(err)
	}
	committed, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	node, err := committed.CreateNode(CreateNodeOptions{Labels: []string{"Item"}})
	if err != nil {
		t.Fatal(err)
	}
	if node.ID <= rolledBackNode.ID {
		t.Fatalf("ID reused after rollback: rolled back %d, next %d", rolledBackNode.ID, node.ID)
	}
	if _, err := committed.PublishStreamGetSequence("events", "created", map[string]Value{"id": node.ID}); err != nil {
		t.Fatal(err)
	}
	if err := committed.Commit(); err != nil {
		t.Fatal(err)
	}
	stream, err := db.ReadStream("events", 0, 10, 0)
	if err != nil || len(stream) != 1 {
		t.Fatalf("stream read = %#v, %v", stream, err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	stats, err = db.OperationalStats()
	if err != nil || stats.ActiveSnapshots != 0 {
		t.Fatalf("snapshot lease after close = %+v, %v", stats, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := OpenContext(ctx, ":memory:", OpenOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled memory open returned %v", err)
	}
	if _, err := db.BeginWriteContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled write begin returned %v", err)
	}
	if _, err := db.ReadStreamContext(ctx, "missing", 0, StreamReadOptions{Limit: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled stream read returned %v", err)
	}
}

func TestMemorySnapshotBackupUsesNoSyntheticSourcePath(t *testing.T) {
	work := t.TempDir()
	t.Chdir(work)
	db, err := Open(":memory:", OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query("CREATE (:BackupCheck)", nil); err != nil {
		t.Fatal(err)
	}
	snapshot, err := db.BeginSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(work, ".pages")
	if err := snapshot.Backup(destination); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	backup, err := Open(destination, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer backup.Close()
	result, err := backup.Query("MATCH (n:BackupCheck) RETURN count(n) AS count", nil)
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["count"] != int64(1) {
		t.Fatalf("backup contents = %#v, %v", result.Rows, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func assertMemoryTestDirEmpty(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("in-memory operation created files under %s: %v", dir, entries)
	}
}

func TestMemoryVectorMaintenanceUsesNoFiles(t *testing.T) {
	work := t.TempDir()
	t.Chdir(work)
	t.Setenv("TMPDIR", work)
	t.Setenv("TMP", work)
	t.Setenv("TEMP", work)
	db, err := Open(":memory:", OpenOptions{EnableVector: true, VectorDimensions: 2, VectorIndexMode: VectorIndexHNSWSynchronous})
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Update(func(tx *Tx) error {
		for i := 0; i < 4; i++ {
			if _, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"embedding": []float32{float32(i), 0}}}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err = db.RebuildVectorIndexContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	hits, err := db.VectorSearch([]float32{0, 0}, VectorSearchOptions{K: 1})
	if err != nil || len(hits) != 1 {
		t.Fatalf("hits=%+v err=%v", hits, err)
	}
	if err = db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	assertMemoryTestDirEmpty(t, work)
}
