package store

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// Cancel only after the private serialization payload exists, so this cannot
// accidentally exercise just the public method's initial context check.
type cancelCheckpointPayloadContext struct {
	context.Context
	files    DatabaseFiles
	observed bool
}

func (ctx *cancelCheckpointPayloadContext) Err() error {
	matches, _ := filepath.Glob(filepath.Join(ctx.files.Directory, "*checkpoint*", "*payload*"))
	if len(matches) != 0 {
		ctx.observed = true
		return context.Canceled
	}
	return nil
}

func TestCheckpointPreparationCancellationCleansStaging(t *testing.T) {
	files := DirectoryDatabaseFiles(t.TempDir())
	graph := NewGraphState()
	if err := EnsureDatabaseID(graph); err != nil {
		t.Fatal(err)
	}
	if err := CheckpointGraphStateAndWALFiles(files, graph, 1, 1, 1); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(files.State)
	if err != nil {
		t.Fatal(err)
	}
	ctx := &cancelCheckpointPayloadContext{Context: context.Background(), files: files}
	prepared, err := PrepareCheckpointFilesContext(ctx, files, graph, 1, 1, 2)
	if prepared != nil {
		defer prepared.Cleanup()
	}
	if !ctx.observed || !errors.Is(err, context.Canceled) {
		t.Fatalf("during-payload cancellation = %v, observed=%v", err, ctx.observed)
	}
	after, err := os.ReadFile(files.State)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("live checkpoint changed: %v", err)
	}
	entries, err := os.ReadDir(files.Directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			t.Fatalf("staging remains: %s", entry.Name())
		}
	}
}
