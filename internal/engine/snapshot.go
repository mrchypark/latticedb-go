package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/mrchypark/latticedb-go/internal/store"
)

// Snapshot pins one committed database generation while writers continue.
type Snapshot struct {
	mu         sync.Mutex
	db         *DB
	graph      *store.GraphState
	files      store.DatabaseFiles
	nextNodeID uint64
	nextEdgeID uint64
	commitID   uint64
	closed     bool
}

func (db *DB) BeginSnapshot() (*Snapshot, error) {
	if db == nil {
		return nil, ErrDatabaseClosed
	}
	if !db.writeMu.TryLock() {
		return nil, ErrWriteTxActive
	}
	defer db.writeMu.Unlock()
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil, ErrDatabaseClosed
	}
	if db.recoveryRequired {
		return nil, ErrRecoveryRequired
	}
	if db.activeSnapshot {
		return nil, ErrSnapshotActive
	}
	db.activeSnapshot = true
	return &Snapshot{
		db:         db,
		graph:      db.graph,
		files:      db.files,
		nextNodeID: db.nextNodeID,
		nextEdgeID: db.nextEdgeID,
		commitID:   db.commitID,
	}, nil
}

func (snapshot *Snapshot) Backup(path string) error {
	if snapshot == nil {
		return ErrDatabaseClosed
	}
	snapshot.mu.Lock()
	defer snapshot.mu.Unlock()
	if snapshot.closed {
		return ErrDatabaseClosed
	}
	if path == "" {
		return fmt.Errorf("%w: backup path is empty", ErrInvalidArgument)
	}
	target, err := canonicalSnapshotPath(path)
	if err != nil {
		return err
	}
	for _, source := range []string{snapshot.files.State, snapshot.files.WAL, snapshot.files.WALBase, snapshot.files.IDs, snapshot.files.State + ".lock", snapshot.files.State + ".layout"} {
		canonicalSource, err := canonicalSnapshotPath(source)
		if err != nil {
			return err
		}
		if target == canonicalSource {
			return fmt.Errorf("%w: backup path belongs to the source database", ErrInvalidArgument)
		}
	}
	return store.CheckpointGraphStateFiles(store.FlatDatabaseFiles(target), snapshot.graph, snapshot.nextNodeID, snapshot.nextEdgeID, snapshot.commitID)
}

func canonicalSnapshotPath(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	absolute = filepath.Clean(absolute)
	resolved, err := filepath.EvalSymlinks(absolute)
	if err == nil {
		return resolved, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return absolute, nil
		}
		return "", err
	}
	return filepath.Join(parent, filepath.Base(absolute)), nil
}

func (snapshot *Snapshot) Close() error {
	if snapshot == nil {
		return nil
	}
	snapshot.mu.Lock()
	defer snapshot.mu.Unlock()
	if snapshot.closed {
		return nil
	}
	snapshot.db.mu.Lock()
	snapshot.db.activeSnapshot = false
	snapshot.db.mu.Unlock()
	snapshot.closed = true
	snapshot.graph = nil
	return nil
}
