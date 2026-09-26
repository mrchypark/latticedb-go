package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type cancelBackupPublicationContext struct {
	context.Context
	files    DatabaseFiles
	observed bool
}

func (ctx *cancelBackupPublicationContext) Err() error {
	matches, _ := filepath.Glob(filepath.Join(ctx.files.Directory, databaseTempPattern(ctx.files, "state-payload")))
	if len(matches) != 0 {
		ctx.observed = true
		return context.Canceled
	}
	return nil
}

func TestBackupRestoreCancellationDuringPublicationLeavesNoDatabase(t *testing.T) {
	base := FlatDatabaseFiles(filepath.Join(t.TempDir(), "base"))
	if err := CreateCheckpointGraphStateFiles(base, NewGraphState(), 1, 1, 0); err != nil {
		t.Fatal(err)
	}
	destination := FlatDatabaseFiles(filepath.Join(t.TempDir(), "restored"))
	ctx := &cancelBackupPublicationContext{Context: context.Background(), files: destination}
	err := CreateBackupRecoveryFilesFrom(ctx, destination, base.State, nil, 1<<20, 0)
	if !ctx.observed || !errors.Is(err, context.Canceled) {
		t.Fatalf("publication cancellation = %v, observed=%v", err, ctx.observed)
	}
	if _, err := os.Stat(destination.State); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("published canceled restore: %v", err)
	}
	entries, err := os.ReadDir(destination.Directory)
	if err != nil || len(entries) != 0 {
		t.Fatalf("restore staging remains: %v, %v", entries, err)
	}
}
