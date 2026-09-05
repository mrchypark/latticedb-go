package latticedb

import (
	"bytes"
	"path/filepath"
	"testing"
)

func TestAppMetadataUpdateAndDeleteRollback(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "metadata.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	binaryKey := []byte{0, 0xff, 1}
	nilKey, emptyKey := []byte("nil"), []byte("empty")
	if err := db.Update(func(tx *Tx) error {
		if err := tx.PutAppMetadata(binaryKey, []byte{0xff, 0}); err != nil {
			return err
		}
		if err := tx.PutAppMetadata(nilKey, nil); err != nil {
			return err
		}
		return tx.PutAppMetadata(emptyKey, []byte{})
	}); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginWrite()
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.PutAppMetadata(binaryKey, []byte("changed")); err != nil {
		t.Fatal(err)
	}
	if err := tx.DeleteAppMetadata(nilKey); err != nil {
		t.Fatal(err)
	}
	if err := tx.DeleteAppMetadata(emptyKey); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *Tx) error {
		value, ok, err := tx.GetAppMetadata(binaryKey)
		if err != nil || !ok || !bytes.Equal(value, []byte{0xff, 0}) {
			t.Fatalf("rolled-back update = %q, %t, %v", value, ok, err)
		}
		value, ok, err = tx.GetAppMetadata(nilKey)
		if err != nil || !ok || value != nil {
			t.Fatalf("rolled-back nil delete = %q, %t, %v", value, ok, err)
		}
		value, ok, err = tx.GetAppMetadata(emptyKey)
		if err != nil || !ok || value == nil || len(value) != 0 {
			t.Fatalf("rolled-back empty delete = %q, %t, %v", value, ok, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestAppMetadataReadAndSnapshotRemainIsolatedAcrossUpdateAndDelete(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "metadata.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	updatedKey, deletedKey := []byte{0xff, 0, 1}, []byte("deleted")
	if err := db.Update(func(tx *Tx) error {
		if err := tx.PutAppMetadata(updatedKey, []byte("before")); err != nil {
			return err
		}
		return tx.PutAppMetadata(deletedKey, []byte("present"))
	}); err != nil {
		t.Fatal(err)
	}
	reader, err := db.BeginRead()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Rollback()
	snapshot, err := db.BeginSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	if err := db.Update(func(tx *Tx) error {
		if err := tx.PutAppMetadata(updatedKey, []byte("after")); err != nil {
			return err
		}
		return tx.DeleteAppMetadata(deletedKey)
	}); err != nil {
		t.Fatal(err)
	}
	assertAppMetadata(t, reader, updatedKey, []byte("before"), true)
	assertAppMetadata(t, reader, deletedKey, []byte("present"), true)
	if err := db.View(func(tx *Tx) error {
		assertAppMetadata(t, tx, updatedKey, []byte("after"), true)
		assertAppMetadata(t, tx, deletedKey, nil, false)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	backupPath := filepath.Join(dir, "snapshot.ltdb")
	if err := snapshot.Backup(backupPath); err != nil {
		t.Fatal(err)
	}
	backup, err := Open(backupPath, OpenOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer backup.Close()
	if err := backup.View(func(tx *Tx) error {
		assertAppMetadata(t, tx, updatedKey, []byte("before"), true)
		assertAppMetadata(t, tx, deletedKey, []byte("present"), true)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func assertAppMetadata(t *testing.T, tx *Tx, key, want []byte, found bool) {
	t.Helper()
	got, ok, err := tx.GetAppMetadata(key)
	if err != nil || ok != found || found && !bytes.Equal(got, want) {
		t.Fatalf("metadata %x = %q, %t, %v; want %q, %t", key, got, ok, err, want, found)
	}
}
