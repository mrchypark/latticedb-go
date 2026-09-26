package engine

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBackupArchiveCaptureClockSurvivesReopen(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "source"), OpenOptions{Create: true, BackupDirectory: filepath.Join(t.TempDir(), "archive")})
	if err != nil {
		t.Fatal(err)
	}
	archive := db.backupArchive
	initial := archive.head
	regressed := initial.CapturedAt.Add(-time.Hour)
	first, err := archive.captureAt(regressed, db.graph, db.nextNodeID, db.nextEdgeID, 1, db.maxDatabaseSnapshotBytes)
	if err != nil || !first.CapturedAt.Equal(initial.CapturedAt.Add(time.Nanosecond)) {
		t.Fatalf("regressed capture = %v, %v", first, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopen the archive directly to verify its persisted clock. The source DB
	// deliberately stays at commit 0; production Open would reject that lineage.
	archive, err = openBackupArchive(archive.directory, db.files.State, db.graph.DatabaseID)
	if err != nil {
		t.Fatal(err)
	}
	defer archive.close()
	same, err := archive.captureAt(regressed, db.graph, db.nextNodeID, db.nextEdgeID, 1, db.maxDatabaseSnapshotBytes)
	if err != nil || same.CommitID != first.CommitID || !same.CapturedAt.Equal(first.CapturedAt) {
		t.Fatalf("idempotent capture = %v, %v, want %v", same, err, first)
	}
	next, err := archive.captureAt(regressed, db.graph, db.nextNodeID, db.nextEdgeID, 2, db.maxDatabaseSnapshotBytes)
	if err != nil || !next.CapturedAt.Equal(first.CapturedAt.Add(time.Nanosecond)) {
		t.Fatalf("reopened capture clock = %v, %v", next, err)
	}
}

func archivePoints(t *testing.T, directory string) []os.DirEntry {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	var points []os.DirEntry
	for _, entry := range entries {
		if !entry.IsDir() && len(entry.Name()) > len(backupPrefix)+len(backupSuffix) && entry.Name()[:len(backupPrefix)] == backupPrefix && filepath.Ext(entry.Name()) == backupSuffix {
			points = append(points, entry)
		}
	}
	return points
}

func TestBackupArchiveDoesNotPublishFailedWALCommit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source")
	archive := filepath.Join(t.TempDir(), "archive")
	db, err := Open(path, OpenOptions{Create: true, BackupDirectory: archive, walWrite: func(*os.File, []byte) (int, error) {
		return 0, errors.New("injected WAL failure")
	}})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.CreateNode(CreateNodeOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err == nil {
		t.Fatal("commit unexpectedly succeeded")
	}
	if len(archivePoints(t, archive)) != 1 {
		t.Fatal("failed WAL commit published an archive checkpoint")
	}
	_ = db.Close()
}

func TestBackupArchiveFailureAfterWALRequiresRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source")
	archive := filepath.Join(t.TempDir(), "archive")
	db, err := Open(path, OpenOptions{Create: true, BackupDirectory: archive})
	if err != nil {
		t.Fatal(err)
	}
	invalidDirectory := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(invalidDirectory, []byte("file"), 0o600); err != nil {
		t.Fatal(err)
	}
	db.backupArchive.directory = invalidDirectory
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.CreateNode(CreateNodeOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrCommitOutcomeUnknown) {
		t.Fatalf("commit error = %v, want ErrCommitOutcomeUnknown", err)
	}
	if err := tx.Rollback(); !errors.Is(err, ErrInactiveTx) {
		t.Fatalf("rollback after failed commit error = %v, want ErrInactiveTx", err)
	}
	if _, err := db.Begin(false); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("next Begin error = %v, want ErrRecoveryRequired", err)
	}
	if err := db.Checkpoint(); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("Checkpoint error = %v, want ErrRecoveryRequired", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.commitID != 1 {
		t.Fatalf("recovered commit = %d, want durable commit 1", reopened.commitID)
	}
	if node, err := reopened.Begin(true); err != nil {
		t.Fatal(err)
	} else {
		defer node.Rollback()
		found, err := node.GetNode(1)
		if err != nil || found == nil {
			t.Fatalf("durable WAL commit missing after reopen: node=%v err=%v", found, err)
		}
	}
}

func TestBackupArchiveRejectsStaleHeadForSameSource(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source")
	archive := filepath.Join(t.TempDir(), "archive")
	db, err := Open(path, OpenOptions{Create: true, BackupDirectory: archive})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	points := archivePoints(t, archive)
	if len(points) != 2 {
		t.Fatalf("archive points = %d, want 2", len(points))
	}
	if err := os.Remove(filepath.Join(archive, points[len(points)-1].Name())); err != nil {
		t.Fatal(err)
	}
	if reopened, err := Open(path, OpenOptions{BackupDirectory: archive}); err == nil {
		_ = reopened.Close()
		t.Fatal("reopen accepted a source generation ahead of the archive head")
	}
	// A new archive can start at the current generation without claiming the missing history.
	if reopened, err := Open(path, OpenOptions{BackupDirectory: filepath.Join(t.TempDir(), "new-archive")}); err != nil {
		t.Fatal(err)
	} else if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}
