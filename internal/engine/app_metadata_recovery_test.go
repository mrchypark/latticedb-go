package engine

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestAppMetadataRecoveryPreservesCommittedChangesAcrossCheckpointAndWALTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metadata.ltdb")
	db, err := Open(path, OpenOptions{Create: true, WALCheckpointThresholdBytes: ^uint64(0)})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	overwritten, deleted, stable := []byte("overwritten"), []byte{0xff, 0, 1}, []byte("stable")
	if err := db.Update(func(tx *Tx) error {
		if err := tx.PutAppMetadata(overwritten, []byte("before")); err != nil {
			return err
		}
		if err := tx.PutAppMetadata(deleted, []byte("delete me")); err != nil {
			return err
		}
		return tx.PutAppMetadata(stable, []byte("unchanged"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		if err := tx.PutAppMetadata(overwritten, []byte("after")); err != nil {
			return err
		}
		return tx.DeleteAppMetadata(deleted)
	}); err != nil {
		t.Fatal(err)
	}
	rolledBack, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	if err := rolledBack.PutAppMetadata(overwritten, []byte("rolled back")); err != nil {
		t.Fatal(err)
	}
	if err := rolledBack.DeleteAppMetadata(stable); err != nil {
		t.Fatal(err)
	}
	if err := rolledBack.PutAppMetadata([]byte("rollback-only"), []byte("missing")); err != nil {
		t.Fatal(err)
	}
	if err := rolledBack.Rollback(); err != nil {
		t.Fatal(err)
	}

	first := recoverAppMetadataFiles(t, db, path)
	assertAppMetadataRecoveryState(t, first, overwritten, []byte("after"), true)
	assertAppMetadataRecoveryState(t, first, deleted, nil, false)
	assertAppMetadataRecoveryState(t, first, stable, []byte("unchanged"), true)
	assertAppMetadataRecoveryState(t, first, []byte("rollback-only"), nil, false)
	assertAppMetadataSnapshotBytes(t, first)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	tailDeleted := []byte("tail-deleted")
	if err := db.Update(func(tx *Tx) error {
		if err := tx.PutAppMetadata(overwritten, []byte("tail value")); err != nil {
			return err
		}
		return tx.PutAppMetadata(tailDeleted, []byte("remove from tail"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error { return tx.DeleteAppMetadata(tailDeleted) }); err != nil {
		t.Fatal(err)
	}

	second := recoverAppMetadataFiles(t, db, path)
	defer second.Close()
	assertAppMetadataRecoveryState(t, second, overwritten, []byte("tail value"), true)
	assertAppMetadataRecoveryState(t, second, deleted, nil, false)
	assertAppMetadataRecoveryState(t, second, tailDeleted, nil, false)
	assertAppMetadataRecoveryState(t, second, stable, []byte("unchanged"), true)
	assertAppMetadataRecoveryState(t, second, []byte("rollback-only"), nil, false)
	assertAppMetadataSnapshotBytes(t, db)
	assertAppMetadataSnapshotBytes(t, second)
}

func recoverAppMetadataFiles(t *testing.T, db *DB, path string) *DB {
	t.Helper()
	db.writeMu.Lock()
	crashPath := copyRecoveryFiles(t, path)
	db.writeMu.Unlock()
	recovered, err := Open(crashPath, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return recovered
}

func assertAppMetadataRecoveryState(t *testing.T, db *DB, key, want []byte, found bool) {
	t.Helper()
	if err := db.View(func(tx *Tx) error {
		got, ok, err := tx.GetAppMetadata(key)
		if err != nil {
			return err
		}
		if ok != found || found && !bytes.Equal(got, want) {
			return fmt.Errorf("metadata %x = %q, %t; want %q, %t", key, got, ok, want, found)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func assertAppMetadataSnapshotBytes(t *testing.T, db *DB) {
	t.Helper()
	db.mu.RLock()
	defer db.mu.RUnlock()
	estimated, err := store.EstimateSnapshotBytes(db.graph)
	if err != nil {
		t.Fatal(err)
	}
	if db.graph.SnapshotBytes != estimated {
		t.Fatalf("snapshot bytes = %d, want %d", db.graph.SnapshotBytes, estimated)
	}
}
