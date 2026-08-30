package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestCommitFailureDoesNotExposeWrites(t *testing.T) {
	root := t.TempDir()
	dbPath := filepath.Join(root, "commit_failure.ltdb")

	db, err := Open(dbPath, OpenOptions{Create: true})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	tx, err := db.Begin(false)
	if err != nil {
		t.Fatalf("begin write tx: %v", err)
	}

	node, err := tx.CreateNode(CreateNodeOptions{
		Labels:     []string{"Person"},
		Properties: map[string]any{"name": "Alice"},
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	if err := db.wal.Close(); err != nil {
		t.Fatalf("close WAL writer: %v", err)
	}

	if err := tx.Commit(); err == nil {
		t.Fatalf("expected commit to fail")
	}
	if _, err := tx.CreateNode(CreateNodeOptions{}); !errors.Is(err, ErrInactiveTx) {
		t.Fatalf("mutation after failed commit = %v, want ErrInactiveTx", err)
	}

	if err := db.View(func(view *Tx) error {
		exists, err := view.NodeExists(node.ID)
		if err != nil {
			return err
		}
		if exists {
			t.Fatalf("expected failed commit node %d to remain invisible", node.ID)
		}
		return nil
	}); err != nil {
		t.Fatalf("view after failed commit: %v", err)
	}

	if err := tx.Rollback(); !errors.Is(err, ErrInactiveTx) {
		t.Fatalf("rollback after failed commit = %v, want ErrInactiveTx", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close original db: %v", err)
	}

	reopened, err := Open(dbPath, OpenOptions{})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer reopened.Close()

	if err := reopened.View(func(view *Tx) error {
		exists, err := view.NodeExists(node.ID)
		if err != nil {
			return err
		}
		if exists {
			t.Fatalf("expected failed commit node %d to remain absent after reopen", node.ID)
		}
		return nil
	}); err != nil {
		t.Fatalf("view after reopen: %v", err)
	}
}

func TestReaderStartsDuringWALSync(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	db, err := Open(filepath.Join(t.TempDir(), "concurrent-read.ltdb"), OpenOptions{
		Create:     true,
		Durability: DurabilityFull,
		walSync: func(file *os.File) error {
			close(started)
			<-release
			return file.Sync()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	commitDone := make(chan error, 1)
	var node Node
	go func() {
		commitDone <- db.Update(func(tx *Tx) error {
			var err error
			node, err = tx.CreateNode(CreateNodeOptions{})
			return err
		})
	}()
	<-started

	readDone := make(chan error, 1)
	go func() {
		readDone <- db.View(func(tx *Tx) error {
			exists, err := tx.NodeExists(node.ID)
			if err == nil && exists {
				return errors.New("uncommitted node is visible")
			}
			return err
		})
	}()
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		close(release)
		t.Fatal("reader blocked on WAL sync")
	}
	close(release)
	if err := <-commitDone; err != nil {
		t.Fatal(err)
	}
}

func TestCommitSyncUnknownFencesDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "unknown-sync.ltdb")
	db, err := Open(path, OpenOptions{
		Create: true,
		walSync: func(*os.File) error {
			return errors.New("injected sync failure")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{})
		return err
	})
	if !errors.Is(err, ErrCommitOutcomeUnknown) {
		t.Fatalf("commit error = %v, want ErrCommitOutcomeUnknown", err)
	}
	if _, err := db.Begin(true); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("read Begin error = %v, want ErrRecoveryRequired", err)
	}
	if _, err := db.Begin(false); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("write Begin error = %v, want ErrRecoveryRequired", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCommitSyncUnknownRecoversDurableOrDiscardedFrame(t *testing.T) {
	for name, durable := range map[string]bool{"durable": true, "discarded": false} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "unknown-sync.ltdb")
			var oldSize int64
			db, err := Open(path, OpenOptions{
				Create: true,
				walSync: func(file *os.File) error {
					if durable {
						if err := file.Sync(); err != nil {
							return err
						}
					} else {
						if err := file.Truncate(oldSize); err != nil {
							return err
						}
						if err := file.Sync(); err != nil {
							return err
						}
					}
					return errors.New("injected acknowledgement loss")
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(filepath.Join(path, "wal.log"))
			if err != nil {
				t.Fatal(err)
			}
			oldSize = info.Size()
			var node Node
			err = db.Update(func(tx *Tx) error {
				var err error
				node, err = tx.CreateNode(CreateNodeOptions{})
				return err
			})
			if !errors.Is(err, ErrCommitOutcomeUnknown) {
				t.Fatalf("commit error = %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = Open(path, OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if err := db.View(func(tx *Tx) error {
				exists, err := tx.NodeExists(node.ID)
				if err == nil && exists != durable {
					t.Fatalf("node exists = %v, durable frame = %v", exists, durable)
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWALShortWrite_TruncateSucceeds(t *testing.T) {
	testWALWriteFailure(t, false, func(*OpenOptions) {})
}

func TestWALShortWrite_TruncateFailsAndFences(t *testing.T) {
	testWALWriteFailure(t, true, func(options *OpenOptions) {
		options.walTruncate = func(*os.File, int64) error { return errors.New("injected truncate failure") }
	})
}

func TestWALShortWrite_TruncateSyncFailsAndFences(t *testing.T) {
	testWALWriteFailure(t, true, func(options *OpenOptions) {
		options.walCleanupSync = func(*os.File) error { return errors.New("injected cleanup sync failure") }
	})
}

func TestWALFullWriteWithErrorIsOutcomeUnknown(t *testing.T) {
	testWALWriteFailure(t, true, func(options *OpenOptions) {
		options.walWrite = func(file *os.File, data []byte) (int, error) {
			count, err := file.Write(data)
			if err != nil {
				return count, err
			}
			return count, errors.New("injected acknowledgement loss")
		}
	})
}

func TestWALGrowthIsBoundedWithoutClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bounded-wal.ltdb")
	db, err := Open(path, OpenOptions{Create: true, WALCheckpointThresholdBytes: 8 << 10})
	if err != nil {
		t.Fatal(err)
	}
	var node Node
	if err := db.Update(func(tx *Tx) error {
		node, err = tx.CreateNode(CreateNodeOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for value := int64(1); value <= 200; value++ {
		if err := db.Update(func(tx *Tx) error { return tx.SetProperty(node.ID, "value", value) }); err != nil {
			t.Fatal(err)
		}
	}
	info, err := os.Stat(filepath.Join(path, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() > 16<<10 {
		t.Fatalf("WAL grew to %d bytes", info.Size())
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.View(func(tx *Tx) error {
		value, _, err := tx.GetProperty(node.ID, "value")
		if err == nil && value != int64(200) {
			t.Fatalf("recovered value = %v", value)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestWALThresholdCountsOnlyDeltaTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delta-tail.ltdb")
	seed, err := Open(path, OpenOptions{Create: true, WALCheckpointThresholdBytes: ^uint64(0)})
	if err != nil {
		t.Fatal(err)
	}
	var target Node
	if err := seed.Update(func(tx *Tx) error {
		for index := 0; index < 1_000; index++ {
			node, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"padding": strings.Repeat("x", 128)}})
			if err != nil {
				return err
			}
			if index == 0 {
				target = node
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := Open(path, OpenOptions{WALCheckpointThresholdBytes: 2 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for value := int64(0); value < 100; value++ {
		if err := db.Update(func(tx *Tx) error { return tx.SetProperty(target.ID, "value", value) }); err != nil {
			t.Fatal(err)
		}
	}
	if db.checkpointCount == 0 || db.checkpointCount >= 100 {
		t.Fatalf("checkpoint count = %d; base snapshot was included in trigger", db.checkpointCount)
	}
}

func TestSnapshotLimitRejectsCommitBeforeWALAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot-limit.ltdb")
	db, err := Open(path, OpenOptions{Create: true, MaxDatabaseSnapshotBytes: 5 << 10})
	if err != nil {
		t.Fatal(err)
	}
	err = db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"large": strings.Repeat("x", 1_000)}})
		return err
	})
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("commit error = %v, want ErrResourceLimit", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, OpenOptions{MaxDatabaseSnapshotBytes: 5 << 10})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	result, err := db.Query("MATCH (n) RETURN count(n) AS count", nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows[0]["count"] != int64(0) {
		t.Fatalf("recovered rejected node: %v", result.Rows)
	}
}

func TestCheckpointPostRotationAppendsSurviveCrashWithoutClose(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprintf("explicit=%v", explicit), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "rotation.ltdb")
			threshold := uint64(1)
			if explicit {
				threshold = ^uint64(0)
			}
			db, err := Open(path, OpenOptions{Create: true, WALCheckpointThresholdBytes: threshold})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var node Node
			if err := db.Update(func(tx *Tx) error {
				node, err = tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"value": int64(0)}})
				return err
			}); err != nil {
				t.Fatal(err)
			}
			for value := int64(1); value <= 3; value++ {
				if explicit {
					if err := db.Checkpoint(); err != nil {
						t.Fatal(err)
					}
				}
				if err := db.Update(func(tx *Tx) error { return tx.SetProperty(node.ID, "value", value) }); err != nil {
					t.Fatal(err)
				}
				matches, err := db.wal.MatchesPath(path)
				if err != nil || !matches {
					t.Fatalf("append handle does not match current WAL: %v, %v", matches, err)
				}
			}
			crashPath := copyRecoveryFiles(t, path)
			recovered, err := Open(crashPath, OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer recovered.Close()
			if err := recovered.View(func(tx *Tx) error {
				value, _, err := tx.GetProperty(node.ID, "value")
				if err == nil && value != int64(3) {
					t.Fatalf("post-rotation value = %v", value)
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCheckpointDoesNotReclaimLiveAllocatorTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "live-tail.ltdb")
	db, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var left, right Node
	if err := db.Update(func(tx *Tx) error {
		left, err = tx.CreateNode(CreateNodeOptions{})
		if err != nil {
			return err
		}
		right, err = tx.CreateNode(CreateNodeOptions{})
		if err != nil {
			return err
		}
		_, err = tx.CreateEdge(left.ID, right.ID, "LINK", CreateEdgeOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	nodeCeiling, edgeCeiling, err := store.LoadIDReservation(path, db.graph.DatabaseID)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	afterNode, afterEdge, err := store.LoadIDReservation(path, db.graph.DatabaseID)
	if err != nil {
		t.Fatal(err)
	}
	if afterNode != nodeCeiling || afterEdge != edgeCeiling {
		t.Fatalf("live checkpoint reclaimed reservation (%d,%d) -> (%d,%d)", nodeCeiling, edgeCeiling, afterNode, afterEdge)
	}
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	issuedNode, err := tx.CreateNode(CreateNodeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	issuedEdge, err := tx.CreateEdge(left.ID, right.ID, "LINK", CreateEdgeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	crashPath := copyRecoveryFiles(t, path)
	recovered, err := Open(crashPath, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	if err := recovered.Update(func(tx *Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{})
		if err != nil {
			return err
		}
		edge, err := tx.CreateEdge(left.ID, right.ID, "LINK", CreateEdgeOptions{})
		if err != nil {
			return err
		}
		if node.ID <= issuedNode.ID || edge.ID <= issuedEdge.ID {
			t.Fatalf("post-checkpoint IDs reused: node %d/%d edge %d/%d", issuedNode.ID, node.ID, issuedEdge.ID, edge.ID)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func copyRecoveryFiles(t *testing.T, source string) string {
	t.Helper()
	target := filepath.Join(t.TempDir(), "recovered.ltdb")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"state.json", "wal.log", "ids.json"} {
		data, err := os.ReadFile(filepath.Join(source, name))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(target, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return target
}

func TestOpenDerivedIndexBudgetFailureReleasesPathLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "derived-budget.ltdb")
	db, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{})
		if err != nil {
			return err
		}
		return tx.FTSIndex(node.ID, "one two three")
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenContext(context.Background(), path, OpenOptions{DerivedIndexBuildMaxLogicalBytes: 1}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("derived index byte error = %v", err)
	}
	reopened, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("reopen after resource error: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFTSIndexContextHonorsCancellationAndBudget(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "fts-budget.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var node Node
	if err := db.Update(func(tx *Tx) error {
		var err error
		node, err = tx.CreateNode(CreateNodeOptions{})
		if err != nil {
			return err
		}
		return tx.FTSIndex(node.ID, "oldtoken")
	}); err != nil {
		t.Fatal(err)
	}
	db.derivedIndexBuildMaxLogicalBytes = 1024
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := tx.FTSIndexContext(canceled, node.ID, "text"); !errors.Is(err, context.Canceled) {
		t.Fatalf("FTS cancellation = %v", err)
	}
	if err := tx.FTSIndex(node.ID, strings.Repeat("A ", 1_000)); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("FTS byte budget = %v", err)
	}
	if err := tx.SetProperty(node.ID, "committed", true); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	old, err := db.FTSSearch("oldtoken", FTSSearchOptions{})
	if err != nil || len(old) != 1 {
		t.Fatalf("old FTS after failed replacement = %#v, %v", old, err)
	}
	failed, err := db.FTSSearch("a", FTSSearchOptions{})
	if err != nil || len(failed) != 0 {
		t.Fatalf("failed FTS replacement became visible = %#v, %v", failed, err)
	}
}

func TestOpenDecodeFailuresReleasePathLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "open-cleanup.ltdb")
	db, err := Open(path, OpenOptions{Create: true})
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
	statePath := filepath.Join(path, "state.json")
	walPath := filepath.Join(path, "wal.log")
	idsPath := filepath.Join(path, "ids.json")
	state, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	wal, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}
	corruptWAL := slices.Clone(wal)
	corruptWAL[len(corruptWAL)-1] ^= 1
	ids, err := os.ReadFile(idsPath)
	if err != nil {
		t.Fatal(err)
	}

	for name, testCase := range map[string]struct{ corrupt, restore func() error }{
		"state decode": {
			func() error {
				if err := os.WriteFile(statePath, []byte("{"), 0o600); err != nil {
					return err
				}
				return os.Remove(walPath)
			},
			func() error {
				if err := os.WriteFile(statePath, state, 0o600); err != nil {
					return err
				}
				return os.WriteFile(walPath, wal, 0o600)
			},
		},
		"wal decode": {
			func() error { return os.WriteFile(walPath, corruptWAL, 0o600) },
			func() error { return os.WriteFile(walPath, wal, 0o600) },
		},
		"ids decode": {
			func() error { return os.WriteFile(idsPath, []byte("{"), 0o600) },
			func() error { return os.WriteFile(idsPath, ids, 0o600) },
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := testCase.corrupt(); err != nil {
				t.Fatal(err)
			}
			if opened, err := Open(path, OpenOptions{}); err == nil {
				_ = opened.Close()
				t.Fatal("corrupt database opened")
			}
			if err := testCase.restore(); err != nil {
				t.Fatal(err)
			}
			reopened, err := Open(path, OpenOptions{})
			if err != nil {
				t.Fatalf("path lock leaked: %v", err)
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCloseFailureFullyUnlocksForRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "close-cleanup.ltdb")
	injected := errors.New("injected close checkpoint failure")
	db, err := Open(path, OpenOptions{Create: true, checkpoint: func(string, *store.GraphState, uint64, uint64, uint64) error {
		return injected
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	closeErr := db.Close()
	if !errors.Is(closeErr, injected) {
		t.Fatalf("Close error = %v, want %v", closeErr, injected)
	}
	if _, err := db.Begin(true); !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("closed handle Begin error = %v", err)
	}
	reopened, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("reopen after Close failure: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCloseUnlockErrorStillReleasesProcessRegistry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "unlock-error.ltdb")
	db, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.pathLock.file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err == nil {
		t.Fatal("Close succeeded with an already closed OS lock handle")
	}
	reopened, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("unlock failure retained process registry entry: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCloseCheckpointFailureBeforeAndAfterPublicationRecoversLatestCommit(t *testing.T) {
	if path := os.Getenv("LATTICEDB_CLOSE_RECOVERY_HELPER"); path != "" {
		db, err := Open(path, OpenOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if err := db.View(func(tx *Tx) error {
			node, err := tx.GetNode(1)
			if err == nil && (node == nil || node.Properties["latest"] != true) {
				t.Fatalf("subprocess latest node = %#v", node)
			}
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		return
	}
	for _, mode := range []string{"before", "state-published", "state-and-wal-published"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "close-publication.ltdb")
			injected := errors.New("injected post-side-effect failure")
			db, err := Open(path, OpenOptions{Create: true, checkpoint: func(path string, graph *store.GraphState, nextNodeID, nextEdgeID, commitID uint64) error {
				switch mode {
				case "state-published":
					if err := store.CheckpointGraphState(path, graph, nextNodeID, nextEdgeID, commitID); err != nil {
						return err
					}
				case "state-and-wal-published":
					if err := store.CheckpointGraphStateAndWAL(path, graph, nextNodeID, nextEdgeID, commitID); err != nil {
						return err
					}
				}
				return injected
			}})
			if err != nil {
				t.Fatal(err)
			}
			var node Node
			if err := db.Update(func(tx *Tx) error {
				node, err = tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"latest": true}})
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); !errors.Is(err, injected) {
				t.Fatalf("Close error = %v", err)
			}
			command := exec.Command(os.Args[0], "-test.run=^TestCloseCheckpointFailureBeforeAndAfterPublicationRecoversLatestCommit$")
			command.Env = append(os.Environ(), "LATTICEDB_CLOSE_RECOVERY_HELPER="+path)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("subprocess reopen after %s: %v\n%s", mode, err, output)
			}
			reopened, err := Open(path, OpenOptions{})
			if err != nil {
				t.Fatalf("reopen after %s failure: %v", mode, err)
			}
			if err := reopened.View(func(tx *Tx) error {
				got, err := tx.GetNode(node.ID)
				if err == nil && (got == nil || got.Properties["latest"] != true) {
					t.Fatalf("latest commit missing after %s failure: %#v", mode, got)
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestV3CloseFaultMatrixAllCheckpointStages(t *testing.T) {
	stages := []string{"state-create", "state-write", "state-sync", "state-close", "state-rename", "state-dir-sync", "wal-create", "wal-write", "wal-sync", "wal-close", "wal-rename", "wal-dir-sync"}
	for _, stage := range stages {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s-after-%v", stage, after), func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "close-fault-matrix.ltdb")
				injected := errors.New("injected checkpoint stage failure")
				db, err := Open(path, OpenOptions{Create: true, checkpoint: func(path string, graph *store.GraphState, nextNodeID, nextEdgeID, commitID uint64) error {
					return store.CheckpointGraphStateAndWALWithFault(path, graph, nextNodeID, nextEdgeID, commitID, func(gotStage string, gotAfter bool) error {
						if gotStage == stage && gotAfter == after {
							return injected
						}
						return nil
					})
				}})
				if err != nil {
					t.Fatal(err)
				}
				if err := db.Update(func(tx *Tx) error {
					_, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"latest": true}})
					return err
				}); err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); !errors.Is(err, injected) {
					t.Fatalf("Close error = %v", err)
				}
				if _, err := db.Begin(true); !errors.Is(err, ErrDatabaseClosed) {
					t.Fatalf("closed DB Begin error = %v", err)
				}
				reopened, err := Open(path, OpenOptions{})
				if err != nil {
					t.Fatalf("reopen: %v", err)
				}
				if err := reopened.View(func(tx *Tx) error {
					node, err := tx.GetNode(1)
					if err == nil && (node == nil || node.Properties["latest"] != true) {
						t.Fatalf("latest commit missing: %#v", node)
					}
					return err
				}); err != nil {
					t.Fatal(err)
				}
				if err := reopened.Close(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func testWALWriteFailure(t *testing.T, fenced bool, configure func(*OpenOptions)) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "write-failure.ltdb")
	failWrite := true
	options := OpenOptions{Create: true}
	options.walWrite = func(file *os.File, data []byte) (int, error) {
		if !failWrite {
			return file.Write(data)
		}
		count, err := file.Write(data[:len(data)/2])
		if err != nil {
			return count, err
		}
		return count, errors.New("injected short write")
	}
	configure(&options)
	db, err := Open(path, options)
	if err != nil {
		t.Fatal(err)
	}
	var failedNode Node
	err = db.Update(func(tx *Tx) error {
		failedNode, err = tx.CreateNode(CreateNodeOptions{})
		return err
	})
	if fenced {
		if !errors.Is(err, ErrCommitOutcomeUnknown) {
			t.Fatalf("commit error = %v, want ErrCommitOutcomeUnknown", err)
		}
		if _, err := db.Begin(true); !errors.Is(err, ErrRecoveryRequired) {
			t.Fatalf("read Begin error = %v, want ErrRecoveryRequired", err)
		}
		if _, err := db.Begin(false); !errors.Is(err, ErrRecoveryRequired) {
			t.Fatalf("write Begin error = %v, want ErrRecoveryRequired", err)
		}
	} else if err == nil || errors.Is(err, ErrCommitOutcomeUnknown) {
		t.Fatalf("commit error = %v, want deterministic write failure", err)
	}
	failWrite = false
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.View(func(tx *Tx) error {
		exists, err := tx.NodeExists(failedNode.ID)
		if err == nil && exists {
			t.Fatalf("failed node %d was recovered", failedNode.ID)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{})
		if err == nil && node.ID <= failedNode.ID {
			t.Fatalf("issued node ID %d was reused as %d", failedNode.ID, node.ID)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestQueryContextCancellationLinearizesBeforeWALWrite(t *testing.T) {
	for name, cancelDuringSync := range map[string]bool{"before WAL": false, "during WAL sync": true} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			path := filepath.Join(t.TempDir(), "cancel-linearization.ltdb")
			db, err := Open(path, OpenOptions{
				Create: true,
				walSync: func(file *os.File) error {
					if err := file.Sync(); err != nil {
						return err
					}
					cancel()
					return nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			if !cancelDuringSync {
				cancel()
			}
			_, err = db.QueryContext(ctx, "CREATE (n:Item)", nil, QueryOptions{})
			if cancelDuringSync && err != nil {
				t.Fatalf("cancellation after WAL write changed commit result: %v", err)
			}
			if !cancelDuringSync && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation before WAL write = %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = Open(path, OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			result, err := db.Query("MATCH (n:Item) RETURN count(n) AS count", nil)
			if err != nil {
				t.Fatal(err)
			}
			want := int64(0)
			if cancelDuringSync {
				want = 1
			}
			if result.Rows[0]["count"] != want {
				t.Fatalf("recovered count = %#v, want %d", result.Rows[0]["count"], want)
			}
		})
	}
}

func TestUpdateContextCancellationReleasesWriter(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "update-context.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	if err := db.UpdateContext(cancelled, func(*Tx) error { called = true; return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("UpdateContext error = %v", err)
	}
	if called {
		t.Fatal("cancelled UpdateContext invoked callback")
	}
	if err := db.Update(func(tx *Tx) error { _, err := tx.CreateNode(CreateNodeOptions{}); return err }); err != nil {
		t.Fatalf("writer remained locked: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWALReplaysDeltaSnapshotDelta(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mixed-wal.ltdb")
	db, err := Open(path, OpenOptions{Create: true, Durability: DurabilityStandard})
	if err != nil {
		t.Fatal(err)
	}
	var node Node
	if err := db.Update(func(tx *Tx) error {
		var err error
		node, err = tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"step": int64(1)}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query("MATCH (n:Item) WHERE id(n) = 1 SET n.step = 2", nil); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error { return tx.SetProperty(node.ID, "final", true) }); err != nil {
		t.Fatal(err)
	}

	// Preserve the live WAL chain exactly as a crash would; Close must not compact it for this test.
	db.mu.Lock()
	db.recoveryRequired = true
	db.mu.Unlock()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.SimulateCrash(path); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.View(func(tx *Tx) error {
		step, ok, err := tx.GetProperty(node.ID, "step")
		if err != nil {
			return err
		}
		if !ok || step != int64(2) {
			t.Fatalf("step = %#v, ok=%v", step, ok)
		}
		final, ok, err := tx.GetProperty(node.ID, "final")
		if err != nil {
			return err
		}
		if !ok || final != true {
			t.Fatalf("final = %#v, ok=%v", final, ok)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestExplicitPropertyIndexesRecoverFromWALSnapshotAndDelta(t *testing.T) {
	path := filepath.Join(t.TempDir(), "property-index-wal.ltdb")
	db, err := Open(path, OpenOptions{Create: true, WALCheckpointThresholdBytes: ^uint64(0)})
	if err != nil {
		t.Fatal(err)
	}
	var alice, bob Node
	var knows Edge
	if err := db.Update(func(tx *Tx) error {
		var err error
		alice, err = tx.CreateNode(CreateNodeOptions{Labels: []string{"Person"}, Properties: map[string]any{"email": "alice@example.com"}})
		if err != nil {
			return err
		}
		bob, err = tx.CreateNode(CreateNodeOptions{Labels: []string{"Person"}, Properties: map[string]any{"email": "bob@example.com"}})
		if err != nil {
			return err
		}
		knows, err = tx.CreateEdge(alice.ID, bob.ID, "KNOWS", CreateEdgeOptions{Properties: map[string]any{"since": int64(2024)}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("Person", "email"); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateEdgePropertyIndex("KNOWS", "since"); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		if err := tx.SetProperty(bob.ID, "email", "alice@example.com"); err != nil {
			return err
		}
		return tx.SetEdgeProperty(knows.ID, "since", int64(2025))
	}); err != nil {
		t.Fatal(err)
	}

	// Preserve the WAL snapshot and delta; Close must not replace them with a checkpoint.
	db.mu.Lock()
	db.recoveryRequired = true
	db.mu.Unlock()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.SimulateCrash(path); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.View(func(tx *Tx) error {
		nodes, err := tx.FindNodesByLabelProperty("Person", "email", "alice@example.com", 100)
		if err != nil {
			return err
		}
		if !slices.Equal(nodes, []uint64{alice.ID, bob.ID}) {
			t.Fatalf("recovered node property index = %v", nodes)
		}
		edges, err := tx.FindEdgesByTypeProperty("KNOWS", "since", int64(2025), 100)
		if err != nil {
			return err
		}
		if !slices.Equal(edges, []uint64{knows.ID}) {
			t.Fatalf("recovered edge property index = %v", edges)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestReadTransactionKeepsSnapshotAfterCommit(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "snapshot.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	read, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Rollback()

	var node Node
	if err := db.Update(func(tx *Tx) error {
		var createErr error
		node, createErr = tx.CreateNode(CreateNodeOptions{Labels: []string{"Person"}})
		return createErr
	}); err != nil {
		t.Fatal(err)
	}

	exists, err := read.NodeExists(node.ID)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatalf("read transaction observed node %d committed after its snapshot", node.ID)
	}
}

func TestReadTransactionKeepsWholeGraphSnapshot(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "whole-snapshot.ltdb"), OpenOptions{Create: true, EnableVector: true, VectorDimensions: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var first, second Node
	var edge Edge
	if err := db.Update(func(tx *Tx) error {
		var err error
		first, err = tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"vector": []float32{1, 0}}})
		if err != nil {
			return err
		}
		second, err = tx.CreateNode(CreateNodeOptions{})
		if err != nil {
			return err
		}
		edge, err = tx.CreateEdge(first.ID, second.ID, "LINK", CreateEdgeOptions{})
		if err != nil {
			return err
		}
		return tx.FTSIndex(first.ID, "before")
	}); err != nil {
		t.Fatal(err)
	}

	read, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Rollback()
	if err := db.Update(func(tx *Tx) error {
		if err := tx.SetVector(first.ID, "vector", []float32{0, 1}); err != nil {
			return err
		}
		if err := tx.FTSIndex(first.ID, "after"); err != nil {
			return err
		}
		tx.deleteEdge(edge.ID)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if got := read.graph.Nodes.Get(first.ID).Properties["vector"].([]float32); got[0] != 1 || got[1] != 0 {
		t.Fatalf("old vector = %v", got)
	}
	if got := read.graph.FTS.Get(first.ID).Text; got != "before" {
		t.Fatalf("old FTS = %q", got)
	}
	if got := read.graph.Outgoing.Get(first.ID).IDs(); len(got) != 1 || got[0] != edge.ID || read.graph.Edges.Get(edge.ID) == nil {
		t.Fatalf("old adjacency = %v, edge = %#v", got, read.graph.Edges.Get(edge.ID))
	}
	db.mu.RLock()
	current := db.graph
	db.mu.RUnlock()
	if got := current.Nodes.Get(first.ID).Properties["vector"].([]float32); got[0] != 0 || got[1] != 1 {
		t.Fatalf("current vector = %v", got)
	}
	if current.FTS.Get(first.ID).Text != "after" || current.Edges.Get(edge.ID) != nil || current.Outgoing.Get(first.ID).Len() != 0 {
		t.Fatalf("current graph did not publish atomically")
	}
}

func TestReadTransactionKeepsPropertyIndexSnapshotAcrossPostingPromotion(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "property-index-snapshot.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var first Node
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 64; i++ {
			node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Person"}, Properties: map[string]any{"email": "shared@example.com", "name": "before"}})
			if err != nil {
				return err
			}
			if i == 0 {
				first = node
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("Person", "email"); err != nil {
		t.Fatal(err)
	}

	reader, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Rollback()
	if err := db.Update(func(tx *Tx) error {
		if err := tx.SetProperty(first.ID, "name", "after"); err != nil {
			return err
		}
		_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Person"}, Properties: map[string]any{"email": "shared@example.com", "name": "new"}})
		return err
	}); err != nil {
		t.Fatal(err)
	}

	oldIDs, err := reader.FindNodesByLabelProperty("Person", "email", "shared@example.com", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(oldIDs) != 64 || oldIDs[0] != first.ID {
		t.Fatalf("old reader property index = %v", oldIDs)
	}
	oldName, ok, err := reader.GetProperty(first.ID, "name")
	if err != nil || !ok || oldName != "before" {
		t.Fatalf("old reader property = %#v, ok=%v, err=%v", oldName, ok, err)
	}
	if err := db.View(func(tx *Tx) error {
		ids, err := tx.FindNodesByLabelProperty("Person", "email", "shared@example.com", 100)
		if err != nil {
			return err
		}
		if len(ids) != 65 {
			t.Fatalf("current property index count = %d", len(ids))
		}
		name, ok, err := tx.GetProperty(first.ID, "name")
		if err != nil || !ok || name != "after" {
			t.Fatalf("current property = %#v, ok=%v, err=%v", name, ok, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestQueryMutationUsesRecordCOW(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "query-cow.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]any{"version": int64(1)}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	read, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Rollback()
	if _, err := db.Query("MATCH (n:Item) WHERE id(n) = 1 SET n.version = 2", nil); err != nil {
		t.Fatal(err)
	}
	old, _, err := read.GetProperty(1, "version")
	if err != nil || old != int64(1) {
		t.Fatalf("old reader version = %#v, err=%v", old, err)
	}
	if err := db.View(func(tx *Tx) error {
		current, _, err := tx.GetProperty(1, "version")
		if err == nil && current != int64(2) {
			t.Fatalf("current version = %#v", current)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestWriteTransactionsDoNotLoseCommittedChanges(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "writers.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	first, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	firstNode, err := first.CreateNode(CreateNodeOptions{Labels: []string{"First"}})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := db.Begin(false); !errors.Is(err, ErrWriteTxActive) {
		t.Fatalf("second writer error = %v, want ErrWriteTxActive", err)
	}

	if err := first.Commit(); err != nil {
		t.Fatal(err)
	}

	second, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	secondNode, err := second.CreateNode(CreateNodeOptions{Labels: []string{"Second"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Commit(); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *Tx) error {
		for _, id := range []uint64{firstNode.ID, secondNode.ID} {
			exists, err := tx.NodeExists(id)
			if err != nil {
				return err
			}
			if !exists {
				t.Fatalf("committed node %d was lost", id)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestManagedTransactionCannotCompleteItself(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "managed.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(tx *Tx) error {
		if err := tx.Commit(); !errors.Is(err, ErrManagedTransaction) {
			t.Fatalf("Commit error = %v, want ErrManagedTransaction", err)
		}
		if err := tx.Rollback(); !errors.Is(err, ErrManagedTransaction) {
			t.Fatalf("Rollback error = %v, want ErrManagedTransaction", err)
		}
		_, err := tx.CreateNode(CreateNodeOptions{})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); !errors.Is(err, ErrInactiveTx) {
		t.Fatalf("second Rollback error = %v, want ErrInactiveTx", err)
	}
	if _, err := tx.NodeExists(1); !errors.Is(err, ErrInactiveTx) {
		t.Fatalf("NodeExists error = %v, want ErrInactiveTx", err)
	}
}

func TestManagedTransactionPanicReleasesWriter(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "managed-panic.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic")
			}
		}()
		_ = db.Update(func(tx *Tx) error {
			if _, err := tx.CreateNode(CreateNodeOptions{}); err != nil {
				t.Fatal(err)
			}
			panic("boom")
		})
	}()
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{})
		return err
	}); err != nil {
		t.Fatalf("writer remained locked after panic: %v", err)
	}
}

func TestSameDatabasePathCannotBeOpenedTwice(t *testing.T) {
	path := filepath.Join(t.TempDir(), "locked.ltdb")
	first, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path, OpenOptions{}); !errors.Is(err, ErrDatabaseLocked) {
		t.Fatalf("second Open error = %v, want ErrDatabaseLocked", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("Open after Close: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDisableLockExplicitlyAllowsSecondOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "unlocked.ltdb")
	first, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	second, err := Open(path, OpenOptions{DisableLock: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCreateNestedDatabasePath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing", "parents", "database.ltdb")
	db, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenFailureReleasesPathLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "open-failure-lock.ltdb")
	db, err := Open(path, OpenOptions{Create: true, EnableVector: true, VectorDimensions: 3})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path, OpenOptions{EnableVector: true, VectorDimensions: 2}); err == nil {
		t.Fatal("expected vector mismatch")
	}
	db, err = Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("path lock leaked after failed Open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestManagedViewPanicReleasesReader(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "view-panic.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() { _ = recover() }()
		_ = db.View(func(*Tx) error { panic("boom") })
	}()
	if err := db.Close(); err != nil {
		t.Fatalf("reader leaked after panic: %v", err)
	}
}

func TestOpenCloseWithoutCommitDoesNotRewriteDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "no-rewrite.ltdb")
	db, err := Open(path, OpenOptions{Create: true})
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
	readFiles := func() map[string]string {
		contents := map[string]string{}
		for _, name := range []string{"state.json", "wal.log", "ids.json"} {
			data, err := os.ReadFile(filepath.Join(path, name))
			if err != nil {
				t.Fatal(err)
			}
			contents[name] = string(data)
		}
		return contents
	}
	want := readFiles()
	for _, options := range []OpenOptions{{}, {ReadOnly: true}} {
		db, err := Open(path, options)
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		got := readFiles()
		for name, before := range want {
			if after := got[name]; after != before {
				t.Fatalf("%s changed after Open/Close with options %+v", name, options)
			}
		}
	}
}

func TestCheckpointLifecycleContract(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checkpoint-contract.ltdb")
	db, err := Open(path, OpenOptions{Create: true})
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

	readFiles := func() map[string]string {
		files := map[string]string{}
		for _, name := range []string{"state.json", "wal.log", "ids.json"} {
			data, err := os.ReadFile(filepath.Join(path, name))
			if err != nil {
				t.Fatal(err)
			}
			files[name] = string(data)
		}
		return files
	}
	want := readFiles()
	readOnly, err := Open(path, OpenOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := readOnly.Checkpoint(); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("read-only Checkpoint error = %v", err)
	}
	if err := readOnly.Close(); err != nil {
		t.Fatal(err)
	}
	if got := readFiles(); !reflect.DeepEqual(got, want) {
		t.Fatal("read-only Checkpoint changed persistent files")
	}
	if err := readOnly.Checkpoint(); !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("closed Checkpoint error = %v", err)
	}

	db, err = Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	writer, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Checkpoint(); !errors.Is(err, ErrWriteTxActive) {
		t.Fatalf("active writer Checkpoint error = %v", err)
	}
	if err := writer.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	fencedPath := filepath.Join(t.TempDir(), "checkpoint-fenced.ltdb")
	fenced, err := Open(fencedPath, OpenOptions{Create: true, walSync: func(*os.File) error { return errors.New("injected") }})
	if err != nil {
		t.Fatal(err)
	}
	_ = fenced.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{})
		return err
	})
	if err := fenced.Checkpoint(); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("fenced Checkpoint error = %v", err)
	}
	if err := fenced.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointSerializationDoesNotBlockReaders(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	path := filepath.Join(t.TempDir(), "checkpoint-readers.ltdb")
	db, err := Open(path, OpenOptions{
		Create: true,
		checkpoint: func(path string, graph *store.GraphState, nextNodeID uint64, nextEdgeID uint64, commitID uint64) error {
			close(started)
			<-release
			return store.CheckpointGraphState(path, graph, nextNodeID, nextEdgeID, commitID)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var node Node
	if err := db.Update(func(tx *Tx) error {
		node, err = tx.CreateNode(CreateNodeOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	checkpointDone := make(chan error, 1)
	go func() { checkpointDone <- db.Checkpoint() }()
	<-started
	readDone := make(chan error, 1)
	go func() {
		readDone <- db.View(func(tx *Tx) error {
			_, err := tx.GetNode(node.ID)
			return err
		})
	}()
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("reader blocked on checkpoint serialization")
	}
	close(release)
	if err := <-checkpointDone; err != nil {
		t.Fatal(err)
	}
}

func TestDatabasePathLockAcrossProcesses(t *testing.T) {
	if path := os.Getenv("LATTICEDB_LOCK_HELPER_PATH"); path != "" {
		db, err := Open(path, OpenOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if err := os.WriteFile(os.Getenv("LATTICEDB_LOCK_HELPER_READY"), nil, 0o600); err != nil {
			t.Fatal(err)
		}
		for {
			if _, err := os.Stat(os.Getenv("LATTICEDB_LOCK_HELPER_RELEASE")); err == nil {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	root := t.TempDir()
	path := filepath.Join(root, "process-lock.ltdb")
	db, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	ready := filepath.Join(root, "ready")
	release := filepath.Join(root, "release")
	cmd := exec.Command(os.Args[0], "-test.run=^TestDatabasePathLockAcrossProcesses$")
	cmd.Env = append(os.Environ(),
		"LATTICEDB_LOCK_HELPER_PATH="+path,
		"LATTICEDB_LOCK_HELPER_READY="+ready,
		"LATTICEDB_LOCK_HELPER_RELEASE="+release,
	)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = os.WriteFile(release, nil, 0o600)
		_ = cmd.Wait()
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("subprocess did not acquire database lock")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := Open(path, OpenOptions{}); !errors.Is(err, ErrDatabaseLocked) {
		t.Fatalf("Open while subprocess holds lock = %v, want ErrDatabaseLocked", err)
	}
}

func TestCloseRefusesActiveWriter(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "active-writer.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); !errors.Is(err, ErrWriteTxActive) {
		t.Fatalf("Close error = %v, want ErrWriteTxActive", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCloseRefusesActiveReader(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "active-reader.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); !errors.Is(err, ErrTransactionsActive) {
		t.Fatalf("Close error = %v, want ErrTransactionsActive", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRolledBackEdgeIDIsNotReusedAfterReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "edge-ids.ltdb")
	db, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	var left, right Node
	if err := db.Update(func(tx *Tx) error {
		var err error
		left, err = tx.CreateNode(CreateNodeOptions{})
		if err != nil {
			return err
		}
		right, err = tx.CreateNode(CreateNodeOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	rolledBack, err := tx.CreateEdge(left.ID, right.ID, "TEST", CreateEdgeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		edge, err := tx.CreateEdge(left.ID, right.ID, "TEST", CreateEdgeOptions{})
		if err != nil {
			return err
		}
		if edge.ID <= rolledBack.ID {
			t.Fatalf("edge ID %d reused rolled-back ID %d", edge.ID, rolledBack.ID)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRolledBackNodeIDSurvivesQuerySnapshotAndCompaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node-ids.ltdb")
	db, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	rolledBack, err := tx.CreateNode(CreateNodeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var committed Node
	if err := db.Update(func(tx *Tx) error {
		var err error
		committed, err = tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query("MATCH (n:Item) SET n.changed = true", nil); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{})
		if err == nil && (node.ID <= rolledBack.ID || node.ID <= committed.ID) {
			t.Fatalf("node ID %d reused issued IDs rollback=%d committed=%d", node.ID, rolledBack.ID, committed.ID)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestFTSIndexDoesNotOverwriteTextProperty(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "fts-property.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	var node Node
	if err := db.Update(func(tx *Tx) error {
		var err error
		node, err = tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"text": "original"}})
		if err != nil {
			return err
		}
		return tx.FTSIndex(node.ID, "search corpus")
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(db.path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.View(func(tx *Tx) error {
		value, ok, err := tx.GetProperty(node.ID, "text")
		if err != nil {
			return err
		}
		if !ok || value != "original" {
			t.Fatalf("text property = %#v, ok=%v", value, ok)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	results, err := db.FTSSearch("corpus", FTSSearchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].NodeID != node.ID {
		t.Fatalf("FTS results = %#v", results)
	}
}

func TestQueryAndCacheRejectClosedOrOversizedUse(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "limits.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query(string(make([]byte, maxQueryBytes+1)), nil); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("oversized query error = %v, want ErrResourceLimit", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.CacheClear(); !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("CacheClear error = %v, want ErrDatabaseClosed", err)
	}
	if _, err := db.CacheStats(); !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("CacheStats error = %v, want ErrDatabaseClosed", err)
	}
}

func TestQueryContextCancellationAndBudget(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "query-budget.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		for range 3 {
			if _, err := tx.CreateNode(CreateNodeOptions{}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := db.QueryContext(ctx, "MATCH (n) RETURN n", nil, QueryOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled query error = %v", err)
	}
	if _, err := db.QueryContext(context.Background(), "MATCH (n) RETURN n", nil, QueryOptions{MaxWork: 1}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("budget query error = %v, want ErrResourceLimit", err)
	}
	if _, err := db.QueryContext(context.Background(), "MATCH (n) RETURN n", nil, QueryOptions{MaxBytes: 127}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("byte budget query error = %v, want ErrResourceLimit", err)
	}
	if _, err := db.QueryContext(ctx, "MATCH (n) SET n.cancelled = true", nil, QueryOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled mutation error = %v", err)
	}
	if err := db.View(func(tx *Tx) error {
		_, exists, err := tx.GetProperty(1, "cancelled")
		if exists {
			t.Fatal("canceled mutation was published")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error { return tx.SetProperty(1, "writer", "released") }); err != nil {
		t.Fatalf("writer token was not released: %v", err)
	}
}

func TestVectorConfigurationPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vector-config.ltdb")
	db, err := Open(path, OpenOptions{Create: true, EnableVector: true, VectorDimensions: 3})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path, OpenOptions{EnableVector: true, VectorDimensions: 2}); err == nil {
		t.Fatal("expected vector dimension mismatch")
	}
	db, err = Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if db.vectorDimensions != 3 || !db.enableVector {
		t.Fatalf("reopened vector config: enabled=%v dimensions=%d", db.enableVector, db.vectorDimensions)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestQueryMatchesNodeByParameterizedID(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "query-id.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var wanted Node
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 3; i++ {
			node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Person"}, Properties: map[string]any{"index": i}})
			if err != nil {
				return err
			}
			if i == 1 {
				wanted = node
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	result, err := db.Query("MATCH (n:Person) WHERE id(n) = $id RETURN n.index", map[string]any{"id": int64(wanted.ID)})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["n.index"] != int64(1) {
		t.Fatalf("unexpected query result: %#v", result)
	}
}

func TestDirectIDLookupPreservesAllPredicates(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "id-predicates.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var node Node
	if err := db.Update(func(tx *Tx) error {
		var err error
		node, err = tx.CreateNode(CreateNodeOptions{Labels: []string{"Person"}, Properties: map[string]any{"kind": "wanted"}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for query, want := range map[string]int64{
		"MATCH (n:Person) WHERE id(n) = $id AND n.kind = \"wanted\" RETURN count(n) AS count": 1,
		"MATCH (n:Other) WHERE id(n) = $id AND n.kind = \"wanted\" RETURN count(n) AS count":  0,
		"MATCH (n:Person) WHERE id(n) = $id AND n.kind = \"other\" RETURN count(n) AS count":  0,
	} {
		result, err := db.Query(query, map[string]any{"id": int64(node.ID)})
		if err != nil {
			t.Fatal(err)
		}
		if got := result.Rows[0]["count"]; got != want {
			t.Fatalf("query %q count = %#v, want %d", query, got, want)
		}
	}
	result, err := db.Query("MATCH (n:Person) WHERE id(n) = -1 RETURN count(n) AS count", nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows[0]["count"] != int64(0) {
		t.Fatalf("negative ID matched: %#v", result.Rows)
	}
}

func TestQueryAppliesLimitAfterAggregation(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "aggregate-limit.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		for range 3 {
			if _, err := tx.CreateNode(CreateNodeOptions{}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	result, err := db.Query("MATCH (n) RETURN count(n) AS count LIMIT 1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := result.Rows[0]["count"]; got != int64(3) {
		t.Fatalf("count = %v, want 3", got)
	}
	result, err = db.Query("MATCH (n) RETURN count(n) AS count LIMIT 0", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 0 {
		t.Fatalf("LIMIT 0 returned %d rows", len(result.Rows))
	}
	result, err = db.Query("UNWIND $items AS x RETURN count(x) AS count", map[string]any{"items": []any{nil, 1}})
	if err != nil {
		t.Fatal(err)
	}
	if got := result.Rows[0]["count"]; got != int64(1) {
		t.Fatalf("count(x) = %v, want 1", got)
	}
}

func TestQueryNumericAndNullEquality(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "value-equality.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"number": int64(1), "nothing": nil}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	result, err := db.Query("MATCH (n) WHERE n.number = 1.0 RETURN count(n) AS count", nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows[0]["count"] != int64(1) {
		t.Fatalf("numeric count = %v", result.Rows[0]["count"])
	}
	result, err = db.Query("MATCH (n) WHERE n.nothing = null RETURN count(n) AS count", nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows[0]["count"] != int64(0) {
		t.Fatalf("null equality count = %v", result.Rows[0]["count"])
	}
}

func TestQuerySingleQuotedStructuralCharacters(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "single-quoted-query.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	value := "a,b) RETURN value AND more"
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"text": value}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	result, err := db.Query("MATCH (n) WHERE n.text = 'a,b) RETURN value AND more' RETURN count(n) AS count", nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows[0]["count"] != int64(1) {
		t.Fatalf("count = %v, want 1", result.Rows[0]["count"])
	}
}

func TestQueryQuotedOperatorsAreNotStructural(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "quoted-operators.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for index, quote := range []string{"'", `"`} {
		value := fmt.Sprintf("value-%d )->[ -> += <=> @@ = :", index)
		literal := quote + value + quote
		if _, err := db.Query("CREATE (n {text: "+literal+"})", nil); err != nil {
			t.Fatalf("create with %s quote: %v", quote, err)
		}
		result, err := db.Query("MATCH (n {text: "+literal+"}) RETURN count(n) AS count", nil)
		if err != nil || result.Rows[0]["count"] != int64(1) {
			t.Fatalf("match with %s quote = %#v, %v", quote, result, err)
		}
		updated := quote + value + " updated += <=> @@" + quote
		if _, err := db.Query("MATCH (n {text: "+literal+"}) SET n.text = "+updated, nil); err != nil {
			t.Fatalf("set with %s quote: %v", quote, err)
		}
		result, err = db.Query("MATCH (n) WHERE n.text = "+updated+" RETURN count(n) AS count", nil)
		if err != nil || result.Rows[0]["count"] != int64(1) {
			t.Fatalf("where with %s quote = %#v, %v", quote, result, err)
		}
	}
}

func TestQuerySemanticValidationFailsClosed(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "query-semantics.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, query := range []string{
		"MATCH (n) RETURN missing.value",
		"MATCH (n) SET missing.value = 1",
		"MATCH (n) CREATE (n)-[:LINK]->(missing)",
		"MATCH (n) DELETE missing",
		"MATCH (n)-[n]->(m) RETURN id(n)",
		"MATCH (n) RETURN n.value trailing",
	} {
		if _, err := db.Query(query, nil); err == nil {
			t.Fatalf("query %q unexpectedly passed semantic validation", query)
		}
	}
	stats, err := db.CacheStats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.Entries != 0 {
		t.Fatalf("invalid plans entered cache: %+v", stats)
	}
}

func TestQueryCacheIsBounded(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "bounded-cache.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 1; i <= queryCacheEntries+1; i++ {
		if _, err := db.Query(fmt.Sprintf("MATCH (n) RETURN n.value LIMIT %d", i), nil); err != nil {
			t.Fatal(err)
		}
	}
	stats, err := db.CacheStats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.Entries != queryCacheEntries {
		t.Fatalf("cache grew to %d entries, want %d", stats.Entries, queryCacheEntries)
	}
}

func TestWriteRollbackDoesNotMutatePublishedSnapshot(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "copy-on-write.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var node Node
	if err := db.Update(func(tx *Tx) error {
		var createErr error
		node, createErr = tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"name": "before"}})
		return createErr
	}); err != nil {
		t.Fatal(err)
	}

	writer, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.SetProperty(node.ID, "name", "after"); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(reader *Tx) error {
		value, _, err := reader.GetProperty(node.ID, "name")
		if err != nil {
			return err
		}
		if value != "before" {
			t.Fatalf("uncommitted value leaked into read snapshot: %v", value)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func TestDerivedPostingsTrackPublishedGeneration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "postings.ltdb")
	db, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	var first Node
	if err := db.Update(func(tx *Tx) error {
		first, err = tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}})
		if err != nil {
			return err
		}
		second, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}})
		if err != nil {
			return err
		}
		if _, err := tx.CreateEdge(first.ID, second.ID, "LINK", CreateEdgeOptions{}); err != nil {
			return err
		}
		return tx.FTSIndex(first.ID, "rare token")
	}); err != nil {
		t.Fatal(err)
	}
	reader, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query("MATCH (n) WHERE id(n) = $id REMOVE n:Item", map[string]any{"id": int64(first.ID)}); err != nil {
		t.Fatal(err)
	}
	if len(reader.graph.Labels.Get("Item")) != 2 || len(reader.graph.EdgeTypes.Get("LINK")) != 1 || len(reader.graph.FTSTokens.Get("rare")) != 1 {
		t.Fatal("reader postings changed across publish")
	}
	if err := reader.Rollback(); err != nil {
		t.Fatal(err)
	}
	result, err := db.Query("MATCH (n:Item) RETURN count(n) AS count", nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows[0]["count"] != int64(1) {
		t.Fatalf("indexed label count = %v", result.Rows[0]["count"])
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if results, err := db.FTSSearch("rare", FTSSearchOptions{}); err != nil || len(results) != 1 || results[0].NodeID != first.ID {
		t.Fatalf("recovered FTS postings = %v, %v", results, err)
	}
}
