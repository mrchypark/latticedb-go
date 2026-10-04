package engine

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestBackupEntryCancellationBeforePublication(t *testing.T) {
	directory := t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	_, err := publishArchiveEntry(ctx, directory, BackupMetadata{CapturedAt: time.Unix(1, 0)}, 0, [sha256.Size]byte{}, false, time.Time{}, func(output io.Writer) error {
		if _, err := output.Write([]byte("first write")); err != nil {
			return err
		}
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("publish error=%v", err)
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 0 {
		t.Fatalf("canceled entry or staging remains: %v, %v", entries, err)
	}
}

func TestBackupBaseCancellationAndRetry(t *testing.T) {
	graph := store.NewGraphState()
	if err := store.EnsureDatabaseID(graph); err != nil {
		t.Fatal(err)
	}
	graph.Nodes.Set(1, &store.NodeRecord{ID: 1, Properties: store.PropertiesFromMap(map[string]any{"text": strings.Repeat("x", 2<<20)})})
	archive := &backupArchive{directory: t.TempDir()}
	ctx := &cancelAfterQueryChecks{remaining: 12, done: make(chan struct{})}
	_, err := archive.publishBase(ctx, time.Unix(1, 0), graph, 2, 1, 0)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("base cancellation=%v", err)
	}
	entries, err := os.ReadDir(archive.directory)
	if err != nil || len(entries) != 0 || archive.headPath != "" {
		t.Fatalf("canceled base retained state: %v, %v", entries, err)
	}
	if _, err = archive.publishBase(t.Context(), time.Unix(1, 0), graph, 2, 1, 0); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if err = validateArchiveFile(t.Context(), archive.headPath, archive.head, 64<<20, graph.DatabaseID); err != nil {
		t.Fatal(err)
	}
}

func TestBackupArchiveOpenAndRestoreHonorValidationContext(t *testing.T) {
	root := t.TempDir()
	source, archivePath := filepath.Join(root, "db"), filepath.Join(root, "archive")
	db, err := Open(source, OpenOptions{Create: true, BackupDirectory: archivePath})
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"text": strings.Repeat("x", 2<<20)}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	id := db.graph.DatabaseID
	source = db.files.State
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	ctx := &cancelAfterQueryChecks{remaining: 10, done: make(chan struct{})}
	opened, err := openBackupArchive(ctx, archivePath, source, id)
	if opened != nil || !errors.Is(err, context.Canceled) {
		if opened != nil {
			_ = opened.close()
		}
		t.Fatalf("archive open=%v, %v", opened, err)
	}
	// Failed validation must release the archive lock.
	opened, err = openBackupArchive(t.Context(), archivePath, source, id)
	if err != nil {
		t.Fatal(err)
	}
	if err = opened.close(); err != nil {
		t.Fatal(err)
	}
	ctx = &cancelAfterQueryChecks{remaining: 12, done: make(chan struct{})}
	destination := filepath.Join(root, "restored")
	if _, err = RestoreBackup(ctx, archivePath, destination, BackupRestoreOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("restore validation=%v", err)
	}
	if _, err = os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canceled restore published: %v", err)
	}
}
