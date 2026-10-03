package latticedb

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestPublicCompactAndStorageOptions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	db, err := Open(path, OpenOptions{Create: true, PageSize: 8192, CacheSizeMB: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Query("CREATE (n:Compact {name: 'kept'})", nil); err != nil {
		t.Fatal(err)
	}
	snapshot, err := db.BeginSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Compact(); !errors.Is(err, ErrTransactionsActive) {
		t.Fatalf("snapshot: %v", err)
	}
	if err = snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err = db.CompactContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, OpenOptions{PageSize: 8192})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query("MATCH (n:Compact) RETURN n.name AS name", nil)
	if err != nil || len(rows.Rows) != 1 {
		t.Fatalf("%#v %v", rows, err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	if invalid, err := Open(path, OpenOptions{PageSize: 4096}); err == nil {
		invalid.Close()
		t.Fatal("accepted mismatched page size")
	}
}

func TestPageSizeAppliesToLegacyMigration(t *testing.T) {
	source, err := Open(t.TempDir(), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = source.Query("CREATE (:Migrated {value: 7})", nil); err != nil {
		t.Fatal(err)
	}
	data, err := source.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	if err = source.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "legacy")
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	migrated, err := Open(path, OpenOptions{PageSize: 8192, CacheSizeMB: 1})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := migrated.Query("MATCH (n:Migrated) RETURN n.value AS value", nil)
	if err != nil || len(rows.Rows) != 1 {
		t.Fatalf("%#v %v", rows, err)
	}
	if err = migrated.Close(); err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(original, data) {
		t.Fatal("migration modified original")
	}
	reopened, err := Open(path, OpenOptions{PageSize: 8192})
	if err != nil {
		t.Fatal(err)
	}
	if err = reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCompactRequiresPathLock(t *testing.T) {
	db, err := Open(t.TempDir(), OpenOptions{Create: true, DisableLock: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Compact(); !errors.Is(err, ErrUnsupportedOption) {
		t.Fatalf("unlocked compaction: %v", err)
	}
	if _, err := db.Query("CREATE (:StillWritable)", nil); err != nil {
		t.Fatalf("rejected compaction changed database state: %v", err)
	}
}

func TestCompactPreservesBackupChainAndAllocatedIDs(t *testing.T) {
	archive := filepath.Join(t.TempDir(), "archive")
	db, err := Open(t.TempDir(), OpenOptions{Create: true, BackupDirectory: archive})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Query("CREATE (:BeforeCompact)", nil); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	consumed, err := tx.CreateNode(CreateNodeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadDir(archive)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Compact(); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadDir(archive)
	if err != nil || !reflect.DeepEqual(archiveCheckpointNames(before), archiveCheckpointNames(after)) {
		t.Fatalf("compaction changed archive: %v", err)
	}
	var created Node
	if err = db.Update(func(tx *Tx) error {
		var err error
		created, err = tx.CreateNode(CreateNodeOptions{Labels: []string{"AfterCompact"}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if created.ID <= consumed.ID {
		t.Fatalf("allocated ID went backward: %d <= %d", created.ID, consumed.ID)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(t.TempDir(), "restored")
	if _, err = RestoreBackup(context.Background(), archive, destination, BackupRestoreOptions{}); err != nil {
		t.Fatal(err)
	}
	restored, err := Open(destination, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	rows, err := restored.Query("MATCH (n) RETURN n", nil)
	if err != nil || len(rows.Rows) != 2 {
		t.Fatalf("restored rows: %#v %v", rows, err)
	}
}
