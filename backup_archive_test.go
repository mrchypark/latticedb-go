package latticedb

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBackupArchiveRestoresSelectedCommit(t *testing.T) {
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
	commit := uint64(0)
	destination := filepath.Join(t.TempDir(), "restored")
	metadata, err := RestoreBackup(context.Background(), archive, destination, BackupRestoreOptions{CommitID: &commit})
	if err != nil {
		t.Fatal(err)
	}
	if metadata.CommitID != 0 {
		t.Fatalf("restored commit = %d, want 0", metadata.CommitID)
	}
	restored, err := Open(destination, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	var found bool
	if err := restored.View(func(tx *Tx) error {
		node, err := tx.GetNode(1)
		found = node != nil
		return err
	}); err != nil || found {
		t.Fatal("commit-zero restore included later node")
	}
	if _, err := os.Stat(destination); err != nil {
		t.Fatal(err)
	}
	if err := restored.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := restored.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(destination, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.View(func(tx *Tx) error {
		node, err := tx.GetNode(1)
		if err == nil && node == nil {
			t.Fatal("write after archive-only restore was lost on reopen")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBackupArchiveReopenRejectsMismatchedSameCommit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source")
	archive := filepath.Join(t.TempDir(), "archive")
	db, err := Open(path, OpenOptions{Create: true, BackupDirectory: archive})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(archive)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) != ".ltdb" {
			continue
		}
		point := filepath.Join(archive, entry.Name())
		data, err := os.ReadFile(point)
		if err != nil {
			t.Fatal(err)
		}
		data[len(data)-1] ^= 1
		if err := os.WriteFile(point, data, 0o600); err != nil {
			t.Fatal(err)
		}
		break
	}
	if reopened, err := Open(path, OpenOptions{BackupDirectory: archive}); err == nil {
		_ = reopened.Close()
		t.Fatal("reopen accepted a mismatched same-commit archive point")
	}
}

func TestBackupArchiveReopenSameCommitIsIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source")
	archive := filepath.Join(t.TempDir(), "archive")
	db, err := Open(path, OpenOptions{Create: true, BackupDirectory: archive})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadDir(archive)
	if err != nil {
		t.Fatal(err)
	}
	if db, err = Open(path, OpenOptions{BackupDirectory: archive}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadDir(archive)
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != len(after) {
		t.Fatalf("same-commit reopen archive entries = %d, want %d", len(after), len(before))
	}
	for index := range before {
		if before[index].Name() != after[index].Name() {
			t.Fatalf("same-commit reopen changed archive entry %q to %q", before[index].Name(), after[index].Name())
		}
	}
}

func TestBackupArchiveReopenAfterRolledBackIDReservation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source")
	archive := filepath.Join(t.TempDir(), "archive")
	db, err := Open(path, OpenOptions{Create: true, BackupDirectory: archive})
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
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadDir(archive)
	if err != nil {
		t.Fatal(err)
	}
	beforeNames := archiveCheckpointNames(before)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, OpenOptions{BackupDirectory: archive})
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadDir(archive)
	if err != nil {
		t.Fatal(err)
	}
	afterNames := archiveCheckpointNames(after)
	if len(afterNames) != len(beforeNames) {
		t.Fatalf("reopen after rolled-back ID reservation changed archive count from %d to %d", len(beforeNames), len(afterNames))
	}
	for i := range beforeNames {
		if beforeNames[i] != afterNames[i] {
			t.Fatalf("reopen after rolled-back ID reservation changed archive timestamp/name %q to %q", beforeNames[i], afterNames[i])
		}
	}
}

func TestBackupArchiveCapturesEachCommitAndKeepsSameGenerationStable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source")
	archive := filepath.Join(t.TempDir(), "archive")
	db, err := Open(path, OpenOptions{Create: true, BackupDirectory: archive})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	reserved, err := tx.CreateNode(CreateNodeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var committedNodeID uint64
	if err := db.Update(func(tx *Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{})
		if err == nil && node.ID <= reserved.ID {
			t.Fatalf("committed node ID = %d, want greater than rolled-back reservation %d", node.ID, reserved.ID)
		}
		committedNodeID = node.ID
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(archive)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(archiveCheckpointNames(entries)); got != 2 {
		t.Fatalf("archive checkpoints = %d, want commit 0 and commit 1", got)
	}
	metadata := make([]BackupMetadata, 2)
	for commit := uint64(0); commit <= 1; commit++ {
		destination := filepath.Join(t.TempDir(), "restored")
		metadata[commit], err = RestoreBackup(context.Background(), archive, destination, BackupRestoreOptions{CommitID: &commit})
		if err != nil {
			t.Fatal(err)
		}
		if commit == 1 {
			restored, err := Open(destination, OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if err := restored.View(func(tx *Tx) error {
				node, err := tx.GetNode(committedNodeID)
				if err == nil && node == nil {
					t.Fatalf("commit-1 archive omitted node ID %d after rolled-back reservation %d", committedNodeID, reserved.ID)
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if err := restored.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if !metadata[1].CapturedAt.After(metadata[0].CapturedAt) {
		t.Fatalf("capture timestamps are not increasing: %v then %v", metadata[0].CapturedAt, metadata[1].CapturedAt)
	}
	floor, err := RestoreBackup(context.Background(), archive, filepath.Join(t.TempDir(), "floor"), BackupRestoreOptions{Before: metadata[0].CapturedAt})
	if err != nil || floor != metadata[0] {
		t.Fatalf("capture floor = %v, %v, want %v", floor, err, metadata[0])
	}
	if _, err := RestoreBackup(context.Background(), archive, filepath.Join(t.TempDir(), "too-early"), BackupRestoreOptions{Before: metadata[0].CapturedAt.Add(-time.Nanosecond)}); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("selection before first capture = %v", err)
	}
	before := archiveCheckpointNames(entries)
	db, err = Open(path, OpenOptions{BackupDirectory: archive})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	afterEntries, err := os.ReadDir(archive)
	if err != nil {
		t.Fatal(err)
	}
	after := archiveCheckpointNames(afterEntries)
	if len(after) != len(before) {
		t.Fatalf("same-generation reopen changed checkpoint count from %d to %d", len(before), len(after))
	}
	for i := range before {
		if before[i] != after[i] {
			t.Fatalf("same-generation reopen changed timestamp/name %q to %q", before[i], after[i])
		}
	}
}

func archiveCheckpointNames(entries []os.DirEntry) []string {
	var names []string
	for _, entry := range entries {
		if !entry.IsDir() && (filepath.Ext(entry.Name()) == ".ltdb" || filepath.Ext(entry.Name()) == ".wal") {
			names = append(names, entry.Name())
		}
	}
	return names
}

func TestRestoreBackupRejectsTamperedFilenameDigest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source")
	archive := filepath.Join(t.TempDir(), "archive")
	db, err := Open(path, OpenOptions{Create: true, BackupDirectory: archive})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error { _, err := tx.CreateNode(CreateNodeOptions{}); return err }); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(archive)
	if err != nil {
		t.Fatal(err)
	}
	points := archiveCheckpointNames(entries)
	if len(points) != 2 {
		t.Fatalf("points = %v", points)
	}
	cutoff := time.Now()
	selected, err := RestoreBackup(context.Background(), archive, filepath.Join(t.TempDir(), "control"), BackupRestoreOptions{Before: cutoff})
	if err != nil || selected.CommitID != 1 {
		t.Fatalf("control selection = %v, %v", selected, err)
	}
	name := points[1]
	start := len("commit-") + 20 + 1
	if filepath.Ext(name) == ".wal" {
		start += 20 + 1
	}
	// Keep the filename syntactically valid, but move the newest point beyond
	// the cutoff without updating its metadata checksum. Older commit 0 exists.
	tampered := name[:start] + "09000000000000000000" + name[start+20:]
	if err := os.Rename(filepath.Join(archive, name), filepath.Join(archive, tampered)); err != nil {
		t.Fatal(err)
	}
	if _, err := RestoreBackup(context.Background(), archive, filepath.Join(t.TempDir(), "restored"), BackupRestoreOptions{Before: cutoff}); err == nil || errors.Is(err, os.ErrNotExist) {
		t.Fatalf("timestamp corruption silently selected an older point or vanished: %v", err)
	}
}

func TestRestoreBackupRejectsExistingAndSymlinkDestinations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source")
	archive := filepath.Join(t.TempDir(), "archive")
	db, err := Open(path, OpenOptions{Create: true, BackupDirectory: archive})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	existing := filepath.Join(t.TempDir(), "existing")
	if err := os.WriteFile(existing, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := RestoreBackup(context.Background(), archive, existing, BackupRestoreOptions{}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("restore to existing destination error = %v, want ErrInvalidArgument", err)
	}
	contents, err := os.ReadFile(existing)
	if err != nil || string(contents) != "keep" {
		t.Fatalf("existing destination changed: contents=%q err=%v", contents, err)
	}
	target := filepath.Join(t.TempDir(), "target")
	if err := os.WriteFile(target, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := RestoreBackup(context.Background(), archive, link, BackupRestoreOptions{}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("restore to symlink destination error = %v, want ErrInvalidArgument", err)
	}
	contents, err = os.ReadFile(target)
	if err != nil || string(contents) != "keep" {
		t.Fatalf("symlink target changed: contents=%q err=%v", contents, err)
	}
}

func TestRestoreBackupEnforcesSnapshotLimitAndDoesNotFallback(t *testing.T) {
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
	if _, err := RestoreBackup(context.Background(), archive, filepath.Join(t.TempDir(), "limited"), BackupRestoreOptions{MaxDatabaseSnapshotBytes: 1}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("restore over snapshot limit error = %v, want ErrResourceLimit", err)
	}
	entries, err := os.ReadDir(archive)
	if err != nil {
		t.Fatal(err)
	}
	points := archiveCheckpointNames(entries)
	if len(points) != 2 {
		t.Fatalf("archive checkpoints = %d, want 2", len(points))
	}
	latest := filepath.Join(archive, points[len(points)-1])
	data, err := os.ReadFile(latest)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 1
	if err := os.WriteFile(latest, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := RestoreBackup(context.Background(), archive, filepath.Join(t.TempDir(), "corrupt"), BackupRestoreOptions{}); err == nil {
		t.Fatal("restore fell back to an older checkpoint after selected checkpoint corruption")
	}
}
