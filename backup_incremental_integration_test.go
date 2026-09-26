package latticedb

import (
	"context"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// Exercise the public API without depending on the archive's file layout.
func TestIncrementalBackupSmallCommitAndResumeAfterCheckpoint(t *testing.T) {
	source, archive := filepath.Join(t.TempDir(), "source"), filepath.Join(t.TempDir(), "archive")
	db, err := Open(source, OpenOptions{Create: true, BackupDirectory: archive})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var id uint64
	if err := db.Update(func(tx *Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Document"}, Properties: map[string]Value{"body": strings.Repeat("x", 1<<20), "revision": int64(1)}})
		id = node.ID
		return err
	}); err != nil {
		t.Fatal(err)
	}
	archiveSize := func() int64 {
		t.Helper()
		var total int64
		if err := filepath.WalkDir(archive, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return nil
			}
			info, err := entry.Info()
			if err == nil {
				total += info.Size()
			}
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return total
	}
	before := archiveSize()
	if err := db.Update(func(tx *Tx) error {
		if err := tx.SetProperty(id, "revision", int64(2)); err != nil {
			return err
		}
		return tx.PublishStream("audit", "updated", int64(2))
	}); err != nil {
		t.Fatal(err)
	}
	growth := archiveSize() - before
	if growth <= 0 || growth >= 128<<10 {
		t.Fatalf("small commit added %d archive bytes; expected a delta, not another 1 MiB checkpoint", growth)
	}
	t.Logf("1 MiB document property update and stream event added %d archive bytes", growth)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// A maintenance checkpoint while backup is disabled removes the old WAL.
	// Resume must retain older points and create an honest new base, not invent
	// the missing commit history or require throwing the whole archive away.
	db, err = Open(source, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error { return tx.SetProperty(id, "revision", int64(3)) }); err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(source, OpenOptions{BackupDirectory: archive})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error { return tx.SetProperty(id, "revision", int64(4)) }); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	for _, commit := range []uint64{1, 2, 3, 4} {
		destination := filepath.Join(t.TempDir(), "restored.ltdb")
		metadata, err := RestoreBackup(context.Background(), archive, destination, BackupRestoreOptions{CommitID: &commit})
		if err != nil {
			t.Fatalf("restore commit %d: %v", commit, err)
		}
		if metadata.CommitID != commit {
			t.Fatalf("restored %d, want %d", metadata.CommitID, commit)
		}
		restored, err := Open(destination, OpenOptions{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		if err := restored.View(func(tx *Tx) error {
			value, found, err := tx.GetProperty(id, "revision")
			if err == nil && (!found || value != int64(commit)) {
				t.Errorf("commit %d revision=%v, found=%v", commit, value, found)
			}
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if commit >= 2 {
			events, err := restored.ReadStream("audit", 0, 10, 0)
			if err != nil || len(events) != 1 || events[0].Payload != int64(2) {
				t.Fatalf("commit %d stream=%v, err=%v", commit, events, err)
			}
		}
		if err := restored.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
