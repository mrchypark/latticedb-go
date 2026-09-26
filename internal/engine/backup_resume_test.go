package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBackupResumeRejectsTimeInsideCoverageGap(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "archive")
	db, err := Open(filepath.Join(t.TempDir(), "source"), OpenOptions{Create: true, BackupDirectory: directory})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	old := db.backupArchive.head
	// No historical frames are supplied: only the resumed endpoint is known.
	resumed, err := db.backupArchive.captureAt(old.CapturedAt.Add(time.Hour), db.graph, db.nextNodeID, db.nextEdgeID, 3, db.maxDatabaseSnapshotBytes)
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(t.TempDir(), "gap")
	_, err = RestoreBackup(context.Background(), directory, destination, BackupRestoreOptions{Before: old.CapturedAt.Add(time.Minute)})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("gap selection error = %v", err)
	}
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("gap published destination: %v", err)
	}
	missing := uint64(2)
	if _, err := RestoreBackup(context.Background(), directory, destination, BackupRestoreOptions{CommitID: &missing}); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing commit = %v", err)
	}
	got, err := RestoreBackup(context.Background(), directory, destination, BackupRestoreOptions{Before: resumed.CapturedAt})
	if err != nil || got.CommitID != resumed.CommitID || !got.CapturedAt.Equal(resumed.CapturedAt) {
		t.Fatalf("resumed endpoint = %+v, %v", got, err)
	}
}

func TestBackupPublicationHonorsDestinationLock(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "archive")
	db, err := Open(filepath.Join(t.TempDir(), "source"), OpenOptions{Create: true, BackupDirectory: directory})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	snapshot, err := db.BeginSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	destination := filepath.Join(t.TempDir(), "destination")
	lock, err := acquireFlatDestinationLock(destination)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.close()
	if err := snapshot.Backup(destination); !errors.Is(err, ErrDatabaseLocked) {
		t.Fatalf("snapshot destination lock = %v", err)
	}
	if _, err := RestoreBackup(context.Background(), directory, destination, BackupRestoreOptions{}); !errors.Is(err, ErrDatabaseLocked) {
		t.Fatalf("restore destination lock = %v", err)
	}
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("locked destination published: %v", err)
	}
}
