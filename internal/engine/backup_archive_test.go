package engine

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"testing/iotest"
	"time"
)

func TestBackupSegmentFilenameRoundTripAndLength(t *testing.T) {
	var digest, previous [sha256.Size]byte
	if _, err := rand.Read(digest[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := rand.Read(previous[:]); err != nil {
		t.Fatal(err)
	}
	metadata := BackupMetadata{CommitID: ^uint64(0), CapturedAt: time.Unix(0, int64(^uint64(0)>>1))}
	name := backupSegmentFilename(^uint64(0)-1, metadata, digest, previous)
	if len(name) > 255 {
		t.Fatalf("backup segment filename is %d bytes, exceeds filesystem limit", len(name))
	}
	parsed, err := parseBackupSegment(name)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.start != ^uint64(0)-1 || parsed.metadata != metadata || parsed.digest != digest || parsed.previous != previous {
		t.Fatalf("segment round trip = %+v", parsed)
	}
}

func TestWriteArchiveHeadPreservesUnrecognizedFile(t *testing.T) {
	directory := t.TempDir()
	headPath := filepath.Join(directory, backupHeadFile)
	if err := os.WriteFile(headPath, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	entry := backupEntry{metadata: BackupMetadata{CommitID: 2, CapturedAt: time.Now()}, path: filepath.Join(directory, "commit-entry")}
	if err := writeArchiveHead(directory, entry); err == nil {
		t.Fatal("unrecognized head file was replaced")
	}
	data, err := os.ReadFile(headPath)
	if err != nil || string(data) != "keep" {
		t.Fatalf("unrecognized head changed: %q, %v", data, err)
	}
}

func writeArchiveHeadTestEntry(t testing.TB, directory string, commitID uint64, size int) backupEntry {
	t.Helper()
	content := make([]byte, size)
	for i := range content {
		content[i] = byte(i)
	}
	digest := sha256.Sum256(content)
	metadata := BackupMetadata{CommitID: commitID, CapturedAt: time.Unix(int64(commitID), 0)}
	path := filepath.Join(directory, backupFilename(metadata, digest))
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
	return backupEntry{metadata: metadata, digest: digest, path: path}
}

func TestWriteArchiveHeadRejectsCorruptPreviousFileAndPreservesHead(t *testing.T) {
	directory := t.TempDir()
	previous := writeArchiveHeadTestEntry(t, directory, 1, 4<<10)
	if err := writeArchiveHead(directory, previous); err != nil {
		t.Fatal(err)
	}
	headPath := filepath.Join(directory, backupHeadFile)
	headBefore, err := os.ReadFile(headPath)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(previous.path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{0xff}, 0); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := writeArchiveHead(directory, writeArchiveHeadTestEntry(t, directory, 2, 1)); err == nil {
		t.Fatal("corrupt previous file replaced the archive head")
	}
	headAfter, err := os.ReadFile(headPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(headAfter, headBefore) {
		t.Fatal("corrupt previous file changed the archive head")
	}
}

func TestHashArchiveReaderCountsAndDigestsFiles(t *testing.T) {
	for _, size := range []int{0, 2*backupIOChunk + 17} {
		t.Run(fmt.Sprintf("%dB", size), func(t *testing.T) {
			content := bytes.Repeat([]byte{byte(size)}, size)
			path := filepath.Join(t.TempDir(), "archive")
			if err := os.WriteFile(path, content, 0o600); err != nil {
				t.Fatal(err)
			}
			file, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			digest, copied, hashErr := hashArchiveReader(context.Background(), file)
			if err := errors.Join(hashErr, file.Close()); err != nil {
				t.Fatal(err)
			}
			if copied != int64(size) || digest != sha256.Sum256(content) {
				t.Fatalf("hash = %x, copied = %d, want digest with %d bytes", digest, copied, size)
			}
		})
	}
}

func TestHashArchiveFileCancellationAndErrors(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "archive")
	if err := os.WriteFile(path, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := hashArchiveFile(ctx, path); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled hash error = %v", err)
	}
	if _, err := hashArchiveFile(context.Background(), filepath.Join(directory, "missing")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing hash error = %v", err)
	}
	wantErr := errors.New("injected read failure")
	reader := io.MultiReader(bytes.NewReader([]byte("partial")), iotest.ErrReader(wantErr))
	digest, copied, err := hashArchiveReader(context.Background(), reader)
	if !errors.Is(err, wantErr) || copied != 7 || digest != [sha256.Size]byte{} {
		t.Fatalf("failed hash = %x, copied = %d, err = %v", digest, copied, err)
	}
}

func BenchmarkArchivePreviousHeadHashBytes(b *testing.B) {
	for _, size := range []int{1 << 10, 1 << 20} {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			previous := writeArchiveHeadTestEntry(b, b.TempDir(), 1, size)
			var copied int64
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				file, err := os.Open(previous.path)
				if err != nil {
					b.Fatal(err)
				}
				_, hashed, hashErr := hashArchiveReader(context.Background(), file)
				if err := errors.Join(hashErr, file.Close()); err != nil {
					b.Fatal(err)
				}
				copied += hashed
			}
			b.StopTimer()
			b.ReportMetric(float64(copied)/float64(b.N), "previous_rehash_bytes")
		})
	}
}

func TestBackupArchiveCaptureClockSurvivesReopen(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "source"), OpenOptions{Create: true, BackupDirectory: filepath.Join(t.TempDir(), "archive")})
	if err != nil {
		t.Fatal(err)
	}
	archive := db.backupArchive
	initial := archive.head
	regressed := initial.CapturedAt.Add(-time.Hour)
	first, err := archive.captureAt(context.Background(), regressed, db.graph, db.nextNodeID, db.nextEdgeID, 1, db.maxDatabaseSnapshotBytes)
	if err != nil || !first.CapturedAt.Equal(initial.CapturedAt.Add(time.Nanosecond)) {
		t.Fatalf("regressed capture = %v, %v", first, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopen the archive directly to verify its persisted clock. The source DB
	// deliberately stays at commit 0; production Open would reject that lineage.
	archive, err = openBackupArchive(context.Background(), archive.directory, db.files.State, db.graph.DatabaseID)
	if err != nil {
		t.Fatal(err)
	}
	defer archive.close()
	same, err := archive.captureAt(context.Background(), regressed, db.graph, db.nextNodeID, db.nextEdgeID, 1, db.maxDatabaseSnapshotBytes)
	if err != nil || same.CommitID != first.CommitID || !same.CapturedAt.Equal(first.CapturedAt) {
		t.Fatalf("idempotent capture = %v, %v, want %v", same, err, first)
	}
	next, err := archive.captureAt(context.Background(), regressed, db.graph, db.nextNodeID, db.nextEdgeID, 2, db.maxDatabaseSnapshotBytes)
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
		if !entry.IsDir() && len(entry.Name()) > len(backupPrefix)+len(backupSuffix) && entry.Name()[:len(backupPrefix)] == backupPrefix && (filepath.Ext(entry.Name()) == backupSuffix || filepath.Ext(entry.Name()) == backupSegmentSuffix) {
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
