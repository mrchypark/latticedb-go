package engine

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestMemoryCloseReleasesReferences(t *testing.T) {
	db, err := Open(":memory:", OpenOptions{EnableVector: true, VectorDimensions: 2, VectorIndexMode: VectorIndexHNSWSynchronous})
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"text": strings.Repeat("x", 1<<20), "vector": []float32{1, 0}}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Query("RETURN 1", nil); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); !errors.Is(err, ErrTransactionsActive) {
		t.Fatalf("active transaction close = %v", err)
	}
	if db.graph == nil {
		t.Fatal("failed close released active graph")
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	db.vectorRebuildBeforeBuild = func() { close(started); <-release }
	result := make(chan error, 1)
	go func() { result <- db.RebuildVectorIndexContext(context.Background()) }()
	select {
	case <-started:
	case err := <-result:
		t.Fatalf("rebuild failed before start: %v", err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	if db.graph != nil || db.queryCache != nil || db.queryCacheSources != nil || db.vectorRebuild != nil || db.adjacencyCompactorGraph != nil || db.generationLeases != nil {
		t.Fatal("closed memory DB retained graph or cache")
	}
	close(release)
	if err = <-result; !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("rebuild error = %v", err)
	}
	if db.graph != nil {
		t.Fatal("rebuild published after close")
	}
	if err = db.CloseContext(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.GetNode(1); err == nil {
		t.Fatalf("retained transaction error = %v", err)
	}
	if _, err = db.Query("RETURN 1", nil); !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("closed DB query = %v", err)
	}
}
