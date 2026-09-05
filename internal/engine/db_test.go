package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/mrchypark/latticedb-go/internal/store"
)

var testENOSPC = syscall.Errno(28)

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

func TestOpenCleansAbandonedDirectoryTempFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cleanup.ltdb")
	db, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	abandoned := filepath.Join(path, ".wal-abandoned.tmp")
	if err := os.WriteFile(abandoned, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := os.Stat(abandoned); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("abandoned temp still exists: %v", err)
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

func TestIDReservationDoesNotBlockReadersOrWriters(t *testing.T) {
	tests := map[string]struct {
		reserve func(*testing.T, *OpenOptions, <-chan struct{}, chan<- struct{})
		mutate  func(*Tx) error
	}{
		"node": {
			reserve: func(t *testing.T, options *OpenOptions, release <-chan struct{}, started chan<- struct{}) {
				var startedOnce sync.Once
				options.reserveIDs = func(store.DatabaseFiles, string, uint64, uint64) error {
					startedOnce.Do(func() { close(started) })
					<-release
					return nil
				}
			},
			mutate: func(tx *Tx) error {
				_, err := tx.CreateNode(CreateNodeOptions{})
				return err
			},
		},
		"edge": {
			reserve: func(t *testing.T, options *OpenOptions, release <-chan struct{}, started chan<- struct{}) {
				var startedOnce sync.Once
				options.reserveIDs = func(files store.DatabaseFiles, databaseID string, nextNodeID, nextEdgeID uint64) error {
					if nextEdgeID > 1 {
						startedOnce.Do(func() { close(started) })
						<-release
					}
					return store.ReserveIDsFiles(files, databaseID, nextNodeID, nextEdgeID)
				}
			},
			mutate: func(tx *Tx) error {
				node, err := tx.CreateNode(CreateNodeOptions{})
				if err != nil {
					return err
				}
				_, err = tx.CreateEdge(node.ID, node.ID, "loop", CreateEdgeOptions{})
				return err
			},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			release := make(chan struct{})
			var releaseOnce sync.Once
			releaseReservation := func() { releaseOnce.Do(func() { close(release) }) }
			started := make(chan struct{})
			options := OpenOptions{Create: true}
			test.reserve(t, &options, release, started)
			db, err := Open(filepath.Join(t.TempDir(), "reservation.ltdb"), options)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			mutationDone := make(chan error, 1)
			go func() { mutationDone <- db.Update(test.mutate) }()
			<-started

			readDone := make(chan error, 1)
			go func() {
				readDone <- db.View(func(tx *Tx) error {
					exists, err := tx.NodeExists(1)
					if err == nil && exists {
						return errors.New("uncommitted mutation is visible")
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
				releaseReservation()
				t.Fatal("reader blocked on ID reservation")
			}

			writerDone := make(chan error, 1)
			go func() {
				_, err := db.Begin(false)
				writerDone <- err
			}()
			select {
			case err := <-writerDone:
				if !errors.Is(err, ErrWriteTxActive) {
					t.Fatalf("concurrent writer error = %v, want ErrWriteTxActive", err)
				}
			case <-time.After(time.Second):
				releaseReservation()
				t.Fatal("concurrent writer was not serialized")
			}
			releaseReservation()
			if err := <-mutationDone; err != nil {
				t.Fatal(err)
			}
		})
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

func TestWALShortWriteENOSPCIsReopenable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "write-enospc.ltdb")
	db, err := Open(path, OpenOptions{
		Create: true,
		walWrite: func(file *os.File, data []byte) (int, error) {
			count, writeErr := file.Write(data[:len(data)/2])
			if writeErr != nil {
				return count, writeErr
			}
			return count, testENOSPC
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var issued Node
	err = db.Update(func(tx *Tx) error {
		issued, err = tx.CreateNode(CreateNodeOptions{})
		return err
	})
	if !errors.Is(err, testENOSPC) || errors.Is(err, ErrCommitOutcomeUnknown) {
		t.Fatalf("short write error = %v, want deterministic ENOSPC", err)
	}
	read, err := db.Begin(true)
	if err != nil {
		t.Fatalf("short write fenced database: %v", err)
	}
	if err := read.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.View(func(tx *Tx) error {
		exists, err := tx.NodeExists(issued.ID)
		if err == nil && exists {
			t.Fatalf("short-written node %d was recovered", issued.ID)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestWALSyncENOSPCFencesAndRecovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sync-enospc.ltdb")
	db, err := Open(path, OpenOptions{
		Create: true,
		walSync: func(file *os.File) error {
			if err := file.Sync(); err != nil {
				return err
			}
			return testENOSPC
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var issued Node
	err = db.Update(func(tx *Tx) error {
		issued, err = tx.CreateNode(CreateNodeOptions{})
		return err
	})
	if !errors.Is(err, testENOSPC) || !errors.Is(err, ErrCommitOutcomeUnknown) {
		t.Fatalf("sync error = %v, want ENOSPC and unknown outcome", err)
	}
	if _, err := db.Begin(true); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("sync failure Begin error = %v, want ErrRecoveryRequired", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.View(func(tx *Tx) error {
		exists, err := tx.NodeExists(issued.ID)
		if err == nil && !exists {
			t.Fatalf("durable sync frame was discarded")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestIDReservationENOSPCIsAtomicAndReopenable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ids-enospc.ltdb")
	db, err := Open(path, OpenOptions{
		Create: true,
		reserveIDs: func(store.DatabaseFiles, string, uint64, uint64) error {
			return testENOSPC
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{})
		return err
	}); !errors.Is(err, testENOSPC) {
		t.Fatalf("reservation error = %v, want ENOSPC", err)
	}
	if _, err := db.Begin(true); !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("reservation failure Begin error = %v, want ErrRecoveryRequired", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.Update(func(tx *Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{})
		if err == nil && node.ID != 1 {
			t.Fatalf("reservation failure consumed ID %d", node.ID)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestIDReservationPostWriteENOSPCPreservesHighWaterMark(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ids-delayed-enospc.ltdb")
	db, err := Open(path, OpenOptions{
		Create: true,
		reserveIDs: func(files store.DatabaseFiles, databaseID string, nextNodeID, nextEdgeID uint64) error {
			if err := store.ReserveIDsFiles(files, databaseID, nextNodeID, nextEdgeID); err != nil {
				return err
			}
			return testENOSPC
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{})
		return err
	}); !errors.Is(err, testENOSPC) {
		t.Fatalf("post-write reservation error = %v, want ENOSPC", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.Update(func(tx *Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{})
		if err == nil && node.ID <= 1 {
			t.Fatalf("post-write reservation reused ID %d", node.ID)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestIDReservationFaultMatrixPreservesPublicationBoundary(t *testing.T) {
	stages := []string{"ids-create", "ids-write", "ids-sync", "ids-close", "ids-rename", "ids-dir-sync"}
	for _, stage := range stages {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s-after-%v", stage, after), func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "ids-fault-matrix.ltdb")
				db, err := Open(path, OpenOptions{
					Create: true,
					reserveIDs: func(files store.DatabaseFiles, databaseID string, nextNodeID, nextEdgeID uint64) error {
						return store.ReserveIDsFilesWithFault(files, databaseID, nextNodeID, nextEdgeID, func(gotStage string, gotAfter bool) error {
							if gotStage == stage && gotAfter == after {
								return testENOSPC
							}
							return nil
						})
					},
				})
				if err != nil {
					t.Fatal(err)
				}
				if err := db.Update(func(tx *Tx) error {
					_, err := tx.CreateNode(CreateNodeOptions{})
					return err
				}); !errors.Is(err, testENOSPC) {
					t.Fatalf("reservation fault = %v, want ENOSPC", err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				reopened, err := Open(path, OpenOptions{})
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.Close()
				if err := reopened.View(func(tx *Tx) error {
					exists, err := tx.NodeExists(1)
					if err == nil && exists {
						t.Fatal("failed graph mutation was published")
					}
					return err
				}); err != nil {
					t.Fatal(err)
				}
				if err := reopened.Update(func(tx *Tx) error {
					node, err := tx.CreateNode(CreateNodeOptions{})
					published := stage == "ids-dir-sync" || (stage == "ids-rename" && after)
					want := uint64(1)
					if published {
						want = 1 + idReservationBlock
					}
					if err == nil && node.ID != want {
						t.Fatalf("next ID = %d, want %d", node.ID, want)
					}
					return err
				}); err != nil {
					t.Fatal(err)
				}
				entries, err := os.ReadDir(path)
				if err != nil {
					t.Fatal(err)
				}
				for _, entry := range entries {
					if strings.HasSuffix(entry.Name(), ".tmp") {
						t.Fatalf("temporary reservation file remains: %s", entry.Name())
					}
				}
			})
		}
	}
}

func TestWALGrowthIsBoundedWithoutClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bounded-wal.ltdb")
	checkpointDone := make(chan struct{}, 32)
	db, err := Open(path, OpenOptions{Create: true, WALCheckpointThresholdBytes: 8 << 10, ChangefeedMaxBytes: 1 << 10, checkpointComplete: checkpointDone})
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
		for {
			db.mu.RLock()
			checkpointTarget := db.checkpointCount + 1
			db.mu.RUnlock()
			err := db.Update(func(tx *Tx) error { return tx.SetProperty(node.ID, "value", value) })
			if err == nil {
				break
			}
			if !errors.Is(err, ErrResourceLimit) {
				t.Fatal(err)
			}
			waitForBackgroundCheckpointReady(t, db, checkpointTarget, checkpointDone)
		}
	}
	waitForBackgroundCheckpointReady(t, db, 1, checkpointDone)
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
	checkpointDone := make(chan struct{}, 32)
	db, err := Open(path, OpenOptions{WALCheckpointThresholdBytes: 2 << 10, checkpointComplete: checkpointDone})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for value := int64(0); value < 100; value++ {
		for {
			db.mu.RLock()
			checkpointTarget := db.checkpointCount + 1
			db.mu.RUnlock()
			err := db.Update(func(tx *Tx) error { return tx.SetProperty(target.ID, "value", value) })
			if err == nil {
				break
			}
			if !errors.Is(err, ErrResourceLimit) {
				t.Fatal(err)
			}
			waitForBackgroundCheckpointReady(t, db, checkpointTarget, checkpointDone)
		}
	}
	waitForBackgroundCheckpointReady(t, db, 1, checkpointDone)
	if db.checkpointCount == 0 || db.checkpointCount >= 100 {
		t.Fatalf("checkpoint count = %d; base snapshot was included in trigger", db.checkpointCount)
	}
}

func TestBackgroundCheckpointDoesNotBlockNextCommit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "background-checkpoint.ltdb")
	started := make(chan struct{})
	release := make(chan struct{})
	checkpointDone := make(chan struct{}, 32)
	var once sync.Once
	db, err := Open(path, OpenOptions{
		Create:                      true,
		WALCheckpointThresholdBytes: 1,
		checkpointComplete:          checkpointDone,
		checkpointPrepare: func(string, *store.GraphState, uint64, uint64, uint64) error {
			once.Do(func() {
				close(started)
				<-release
			})
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	committed := make(chan error, 1)
	go func() {
		committed <- db.Update(func(tx *Tx) error {
			_, err := tx.CreateNode(CreateNodeOptions{})
			return err
		})
	}()
	select {
	case err := <-committed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("next commit blocked by background checkpoint preparation")
	}
	close(release)
	waitForBackgroundCheckpoint(t, db, 1, checkpointDone)
	if err := db.View(func(tx *Tx) error {
		result, err := tx.Query("MATCH (n) RETURN count(n) AS count", nil)
		if err != nil {
			return err
		}
		if got := result.Rows[0]["count"]; got != int64(2) {
			t.Fatalf("committed node count = %v, want 2", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBackgroundCheckpointStaleCandidateRecoversLatestCommit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "background-stale.ltdb")
	started := make(chan struct{})
	release := make(chan struct{})
	checkpointDone := make(chan struct{}, 32)
	var once sync.Once
	db, err := Open(path, OpenOptions{
		Create:                      true,
		WALCheckpointThresholdBytes: 1,
		checkpointComplete:          checkpointDone,
		checkpointPrepare: func(string, *store.GraphState, uint64, uint64, uint64) error {
			once.Do(func() {
				close(started)
				<-release
			})
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var node Node
	if err := db.Update(func(tx *Tx) error {
		node, err = tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"revision": int64(1)}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := db.Update(func(tx *Tx) error {
		return tx.SetProperty(node.ID, "revision", int64(2))
	}); err != nil {
		t.Fatal(err)
	}
	close(release)
	waitForBackgroundCheckpoint(t, db, 1, checkpointDone)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.View(func(tx *Tx) error {
		value, ok, err := tx.GetProperty(node.ID, "revision")
		if err != nil {
			return err
		}
		if !ok || value != int64(2) {
			t.Fatalf("recovered stale-candidate value = %#v, ok=%v", value, ok)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBackgroundCheckpointProgressWithContinuousWrites(t *testing.T) {
	const staleCandidates = 4
	path := filepath.Join(t.TempDir(), "background-progress.ltdb")
	checkpointStarted := make(chan struct {
		number   int
		commitID uint64
	}, staleCandidates+1)
	checkpointRelease := make(chan struct{}, staleCandidates+1)
	checkpointDone := make(chan struct{}, staleCandidates+1)
	attempts := 0
	db, err := Open(path, OpenOptions{
		Create:                      true,
		WALCheckpointThresholdBytes: 1,
		checkpointComplete:          checkpointDone,
		checkpointPrepare: func(_ string, _ *store.GraphState, _ uint64, _ uint64, commitID uint64) error {
			attempts++
			checkpointStarted <- struct {
				number   int
				commitID uint64
			}{
				number:   attempts,
				commitID: commitID,
			}
			if attempts <= staleCandidates+1 {
				<-checkpointRelease
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	var node Node
	if err := db.Update(func(tx *Tx) error {
		node, err = tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"value": int64(0)}})
		return err
	}); err != nil {
		t.Fatal(err)
	}

	waitStarted := func(want int) uint64 {
		t.Helper()
		select {
		case attempt := <-checkpointStarted:
			if attempt.number != want {
				t.Fatalf("checkpoint attempt = %d, want %d", attempt.number, want)
			}
			return attempt.commitID
		case <-time.After(5 * time.Second):
			t.Fatalf("checkpoint attempt %d did not start", want)
			return 0
		}
	}
	waitDone := func() {
		t.Helper()
		select {
		case <-checkpointDone:
		case <-time.After(5 * time.Second):
			t.Fatal("background checkpoint attempt did not finish")
		}
	}

	for number := 1; number <= staleCandidates; number++ {
		candidateCommitID := waitStarted(number)
		committed := make(chan error, 1)
		go func() {
			committed <- db.Update(func(tx *Tx) error {
				return tx.SetProperty(node.ID, "value", int64(number))
			})
		}()
		select {
		case err := <-committed:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("commit blocked by checkpoint preparation")
		}
		db.mu.RLock()
		currentCommitID := db.commitID
		db.mu.RUnlock()
		if currentCommitID == candidateCommitID {
			t.Fatalf("commit %d did not make candidate %d stale", currentCommitID, number)
		}
		checkpointRelease <- struct{}{}
		waitDone()
	}

	// Keep the next candidate from succeeding before the assertion below. The
	// N commits above leave every completed candidate stale on origin/main.
	waitStarted(staleCandidates + 1)
	db.mu.RLock()
	count, dirty := db.checkpointCount, db.dirty
	db.mu.RUnlock()
	if count == 0 && dirty {
		t.Errorf("checkpoint made no progress after %d stale candidates while writes continued", staleCandidates)
	}

	checkpointRelease <- struct{}{}
	waitDone()
	db.mu.RLock()
	count, dirty = db.checkpointCount, db.dirty
	db.mu.RUnlock()
	if count == 0 || dirty {
		t.Fatalf("checkpoint did not succeed after writes stopped: count=%d dirty=%v", count, dirty)
	}
	if attempts != staleCandidates+1 {
		t.Fatalf("checkpoint preparation attempts = %d, want %d", attempts, staleCandidates+1)
	}
	info, err := os.Stat(filepath.Join(path, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() > 16<<10 {
		t.Fatalf("active WAL grew to %d bytes after progress", info.Size())
	}
	if _, err := os.Stat(filepath.Join(path, "wal.base")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("completed checkpoint left wal.base: %v", err)
	}
}

func TestBackgroundCheckpointBoundsTailDuringBlockedPreparation(t *testing.T) {
	const threshold = uint64(1)
	path := filepath.Join(t.TempDir(), "background-tail-bound.ltdb")
	started := make(chan struct{})
	release := make(chan struct{})
	checkpointDone := make(chan struct{}, 16)
	var once sync.Once
	db, err := Open(path, OpenOptions{
		Create:                      true,
		WALCheckpointThresholdBytes: threshold,
		checkpointComplete:          checkpointDone,
		checkpointPrepare: func(string, *store.GraphState, uint64, uint64, uint64) error {
			once.Do(func() { close(started) })
			select {
			case <-release:
			case <-time.After(5 * time.Second):
				return errors.New("checkpoint preparation did not release")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var node Node
	if err := db.Update(func(tx *Tx) error {
		node, err = tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"value": int64(0)}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("checkpoint preparation did not start")
	}

	for value := int64(1); ; value++ {
		db.mu.RLock()
		tail, tailErr := db.wal.TailSize()
		inFlight := db.checkpointInFlight.Load()
		db.mu.RUnlock()
		if tailErr != nil {
			t.Fatal(tailErr)
		}
		if !inFlight {
			t.Fatal("checkpoint finished while preparation was blocked")
		}
		if uint64(tail) >= threshold {
			break
		}
		if err := db.Update(func(tx *Tx) error { return tx.SetProperty(node.ID, "value", value) }); err != nil {
			t.Fatal(err)
		}
	}

	db.mu.RLock()
	beforeCommit, err := db.wal.TailSize()
	beforeGeneration := db.commitID
	db.mu.RUnlock()
	if err != nil {
		t.Fatal(err)
	}
	beforeFile, err := os.Stat(filepath.Join(path, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	err = db.Update(func(tx *Tx) error { return tx.SetProperty(node.ID, "value", int64(999999)) })
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("commit beyond in-flight WAL bound = %v, want ErrResourceLimit", err)
	}
	db.mu.RLock()
	afterGeneration := db.commitID
	db.mu.RUnlock()
	afterFile, statErr := os.Stat(filepath.Join(path, "wal.log"))
	if statErr != nil {
		t.Fatal(statErr)
	}
	if beforeCommit < int64(threshold) {
		t.Fatalf("tail before bounded rejection = %d, want >= %d", beforeCommit, threshold)
	}
	if beforeFile.Size() != afterFile.Size() || beforeGeneration != afterGeneration {
		t.Fatalf("rejected commit changed WAL/generation: size %d->%d, commit %d->%d", beforeFile.Size(), afterFile.Size(), beforeGeneration, afterGeneration)
	}

	close(release)
	db.mu.RLock()
	checkpointCount := db.checkpointCount
	db.mu.RUnlock()
	waitForBackgroundCheckpoint(t, db, checkpointCount+1, checkpointDone)
	if err := db.Update(func(tx *Tx) error { return tx.SetProperty(node.ID, "value", int64(999999)) }); err != nil {
		t.Fatalf("retry after checkpoint = %v", err)
	}
	db.writeMu.Lock()
	crashPath := copyRecoveryFiles(t, path)
	db.writeMu.Unlock()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(crashPath, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.View(func(tx *Tx) error {
		value, ok, err := tx.GetProperty(node.ID, "value")
		if err == nil && (!ok || value != int64(999999)) {
			t.Fatalf("recovered retry value = %#v, ok=%v", value, ok)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCloseWaitsForBackgroundCheckpointPreparation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "background-close.ltdb")
	started := make(chan struct{})
	release := make(chan struct{})
	db, err := Open(path, OpenOptions{
		Create:                      true,
		WALCheckpointThresholdBytes: 1,
		checkpointPrepare: func(string, *store.GraphState, uint64, uint64, uint64) error {
			select {
			case <-started:
			default:
				close(started)
			}
			<-release
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Close returned before preparation completed: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not drain background checkpoint worker")
	}
	if _, err := db.Begin(true); !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("closed DB Begin error = %v", err)
	}
}

func TestBackgroundCheckpointPreparationFailureRetriesOnNextCommit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "background-prepare-failure.ltdb")
	checkpointDone := make(chan struct{}, 8)
	attempts := 0
	db, err := Open(path, OpenOptions{
		Create:                      true,
		WALCheckpointThresholdBytes: 1,
		checkpointComplete:          checkpointDone,
		checkpointPrepare: func(string, *store.GraphState, uint64, uint64, uint64) error {
			attempts++
			if attempts == 1 {
				return errors.New("injected checkpoint preparation failure")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-checkpointDone:
	case <-time.After(time.Second):
		t.Fatal("background checkpoint preparation did not finish")
	}
	db.mu.RLock()
	if !db.dirty || db.checkpointCount != 0 {
		db.mu.RUnlock()
		t.Fatalf("failed preparation state: dirty=%v count=%d", db.dirty, db.checkpointCount)
	}
	db.mu.RUnlock()
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	waitForBackgroundCheckpoint(t, db, 1, checkpointDone)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	result, err := reopened.Query("MATCH (n) RETURN count(n) AS count", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := result.Rows[0]["count"]; got != int64(2) {
		t.Fatalf("recovered node count = %v, want 2", got)
	}
}

func TestExplicitCheckpointWhileBackgroundPreparationRuns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "background-explicit-checkpoint.ltdb")
	started := make(chan struct{})
	release := make(chan struct{})
	checkpointDone := make(chan struct{}, 8)
	var once sync.Once
	db, err := Open(path, OpenOptions{
		Create:                      true,
		WALCheckpointThresholdBytes: 1,
		checkpointComplete:          checkpointDone,
		checkpointPrepare: func(string, *store.GraphState, uint64, uint64, uint64) error {
			once.Do(func() { close(started) })
			<-release
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("background checkpoint did not start")
	}
	explicitDone := make(chan error, 1)
	go func() { explicitDone <- db.Checkpoint() }()
	select {
	case err := <-explicitDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("explicit checkpoint blocked on background preparation")
	}
	close(release)
	select {
	case <-checkpointDone:
	case <-time.After(time.Second):
		t.Fatal("background checkpoint attempt did not finish")
	}
}

func TestForegroundUpdateWaitsForCheckpointPublication(t *testing.T) {
	path := filepath.Join(t.TempDir(), "background-foreground-priority.ltdb")
	started := make(chan struct{})
	release := make(chan struct{})
	db, err := Open(path, OpenOptions{
		Create:                      true,
		WALCheckpointThresholdBytes: 1,
		checkpointPublish: func() {
			close(started)
			<-release
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("background checkpoint did not reach publication")
	}
	committed := make(chan error, 1)
	go func() {
		committed <- db.Update(func(tx *Tx) error {
			_, err := tx.CreateNode(CreateNodeOptions{})
			return err
		})
	}()
	select {
	case err := <-committed:
		t.Fatalf("foreground update returned while publication was blocked: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-committed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("foreground update did not complete after publication")
	}
}

func TestForegroundWriterStillConflictsWithForegroundWriter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "foreground-writer-conflict.ltdb")
	db, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Begin(false); !errors.Is(err, ErrWriteTxActive) {
		t.Fatalf("concurrent foreground Begin error = %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func TestForegroundBeginRetriesAfterCheckpointAttemptFinishes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "foreground-begin-after-checkpoint.ltdb")
	published := make(chan struct{})
	release := make(chan struct{})
	failed := make(chan struct{})
	allowState := make(chan struct{})
	checkpointDone := make(chan struct{}, 8)
	var publishOnce sync.Once
	var failedOnce sync.Once
	db, err := Open(path, OpenOptions{
		Create:                      true,
		WALCheckpointThresholdBytes: 1,
		checkpointComplete:          checkpointDone,
		checkpointPublish: func() {
			publishOnce.Do(func() { close(published) })
			<-release
		},
		checkpointTryLockFailed: func() {
			failedOnce.Do(func() {
				close(failed)
				<-allowState
			})
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-published:
	case <-time.After(time.Second):
		t.Fatal("background checkpoint did not reach publication")
	}
	type beginResult struct {
		tx  *Tx
		err error
	}
	resultCh := make(chan beginResult, 1)
	go func() {
		tx, err := db.Begin(false)
		resultCh <- beginResult{tx: tx, err: err}
	}()
	select {
	case <-failed:
	case <-time.After(time.Second):
		t.Fatal("foreground Begin did not observe checkpoint contention")
	}
	close(release)
	select {
	case <-checkpointDone:
	case <-time.After(time.Second):
		t.Fatal("background checkpoint did not finish")
	}
	close(allowState)
	result := <-resultCh
	if result.err != nil {
		t.Fatalf("foreground Begin after completed checkpoint = %v", result.err)
	}
	if err := result.tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

func TestForegroundBeginRetriesWorkerHandoffAfterInactiveObservation(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "foreground-worker-handoff.ltdb"), OpenOptions{
		Create: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	db.writeMu.Lock()
	firstFailed := make(chan struct{})
	releaseFirst := make(chan struct{})
	beforeFinal := make(chan struct{})
	releaseFinal := make(chan struct{})
	workerReady := make(chan struct{})
	releaseWorker := make(chan struct{})
	var firstOnce, finalOnce sync.Once
	db.checkpointTryLockFailed = func() {
		firstOnce.Do(func() {
			close(firstFailed)
			<-releaseFirst
		})
	}
	db.checkpointBeforeFinalTryLock = func() {
		finalOnce.Do(func() {
			close(beforeFinal)
			<-releaseFinal
		})
	}
	resultCh := make(chan error, 1)
	go func() {
		tx, err := db.Begin(false)
		if err == nil {
			err = tx.Rollback()
		}
		resultCh <- err
	}()
	select {
	case <-firstFailed:
	case <-time.After(time.Second):
		t.Fatal("foreground Begin did not observe writer contention")
	}
	close(releaseFirst)
	select {
	case <-beforeFinal:
	case <-time.After(time.Second):
		t.Fatal("foreground Begin did not reach final TryLock")
	}
	db.writeMu.Unlock()
	db.announceCheckpointAttempt()
	go func() {
		db.writeMu.Lock()
		close(workerReady)
		<-releaseWorker
		db.writeMu.Unlock()
		db.finishCheckpointAttempt()
	}()
	select {
	case <-workerReady:
	case <-time.After(time.Second):
		t.Fatal("worker did not acquire writeMu")
	}
	close(releaseFinal)
	select {
	case err := <-resultCh:
		t.Fatalf("foreground Begin returned while worker owned writeMu: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(releaseWorker)
	select {
	case err := <-resultCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("foreground Begin did not complete after worker handoff")
	}
}

func TestRollbackAfterCheckpointPublicationContentionReschedules(t *testing.T) {
	path := filepath.Join(t.TempDir(), "background-rollback-reschedule.ltdb")
	started := make(chan struct{})
	release := make(chan struct{})
	checkpointDone := make(chan struct{}, 8)
	var once sync.Once
	db, err := Open(path, OpenOptions{
		Create:                      true,
		WALCheckpointThresholdBytes: 1,
		checkpointComplete:          checkpointDone,
		checkpointPrepare: func(string, *store.GraphState, uint64, uint64, uint64) error {
			once.Do(func() { close(started) })
			<-release
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("background checkpoint did not start")
	}
	foreground, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	close(release)
	select {
	case <-checkpointDone:
	case <-time.After(time.Second):
		t.Fatal("contended background checkpoint did not finish")
	}
	if err := foreground.Rollback(); err != nil {
		t.Fatal(err)
	}
	waitForBackgroundCheckpoint(t, db, 1, checkpointDone)
	info, err := os.Stat(filepath.Join(path, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() > 16<<10 {
		t.Fatalf("WAL after rollback reschedule = %d bytes", info.Size())
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	result, err := reopened.Query("MATCH (n) RETURN count(n) AS count", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := result.Rows[0]["count"]; got != int64(1) {
		t.Fatalf("recovered node count = %v, want 1", got)
	}
}

func TestCloseWithActiveReadTransactionLeavesCheckpointWorkerRunning(t *testing.T) {
	path := filepath.Join(t.TempDir(), "background-read-close.ltdb")
	checkpointStarted := make(chan struct{})
	checkpointRelease := make(chan struct{})
	checkpointDone := make(chan struct{}, 8)
	var once sync.Once
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(checkpointRelease) }) }
	db, err := Open(path, OpenOptions{
		Create:                      true,
		WALCheckpointThresholdBytes: 1,
		checkpointComplete:          checkpointDone,
		checkpointPrepare: func(string, *store.GraphState, uint64, uint64, uint64) error {
			once.Do(func() { close(checkpointStarted) })
			<-checkpointRelease
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	defer release()
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-checkpointStarted:
	case <-time.After(time.Second):
		t.Fatal("background checkpoint did not start")
	}
	if err := db.Close(); !errors.Is(err, ErrTransactionsActive) {
		t.Fatalf("Close with active read transaction = %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	release()
	waitForBackgroundCheckpoint(t, db, 1, checkpointDone)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCloseWithActiveSnapshotLeavesCheckpointWorkerRunning(t *testing.T) {
	path := filepath.Join(t.TempDir(), "background-snapshot-close.ltdb")
	checkpointDone := make(chan struct{}, 8)
	db, err := Open(path, OpenOptions{Create: true, WALCheckpointThresholdBytes: 1, checkpointComplete: checkpointDone})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	snapshot, err := db.BeginSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); !errors.Is(err, ErrSnapshotActive) {
		t.Fatalf("Close with active snapshot = %v", err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	waitForBackgroundCheckpoint(t, db, 1, checkpointDone)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func waitForBackgroundCheckpoint(t *testing.T, db *DB, minimum uint64, complete <-chan struct{}) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-complete:
		case <-timer.C:
			db.mu.RLock()
			count, dirty := db.checkpointCount, db.dirty
			db.mu.RUnlock()
			t.Fatalf("background checkpoint did not finish: count=%d dirty=%v", count, dirty)
		}
		db.mu.RLock()
		count := db.checkpointCount
		dirty := db.dirty
		db.mu.RUnlock()
		if count >= minimum && !dirty {
			return
		}
	}
}

func waitForBackgroundCheckpointReady(t *testing.T, db *DB, minimum uint64, complete <-chan struct{}) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		db.mu.RLock()
		count := db.checkpointCount
		inFlight := db.checkpointInFlight.Load()
		db.mu.RUnlock()
		if count >= minimum && !inFlight {
			return
		}
		select {
		case <-complete:
		case <-timer.C:
			t.Fatalf("background checkpoint did not become ready: count=%d in-flight=%v", count, inFlight)
		}
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
			checkpointDone := make(chan struct{}, 16)
			db, err := Open(path, OpenOptions{Create: true, WALCheckpointThresholdBytes: threshold, checkpointComplete: checkpointDone})
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
				for {
					db.mu.RLock()
					checkpointTarget := db.checkpointCount + 1
					db.mu.RUnlock()
					err := db.Update(func(tx *Tx) error { return tx.SetProperty(node.ID, "value", value) })
					if err == nil {
						break
					}
					if !errors.Is(err, ErrResourceLimit) {
						t.Fatal(err)
					}
					waitForBackgroundCheckpointReady(t, db, checkpointTarget, checkpointDone)
				}
				db.mu.RLock()
				matches, err := db.wal.MatchesPath(path)
				db.mu.RUnlock()
				if err != nil || !matches {
					t.Fatalf("append handle does not match current WAL: %v, %v", matches, err)
				}
			}
			db.writeMu.Lock()
			crashPath := copyRecoveryFiles(t, path)
			db.writeMu.Unlock()
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

func TestOpenCompactsRecoveredRotationBeforeRemovingWALBase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rotation-open-crash.ltdb")
	started := make(chan struct{})
	release := make(chan struct{})
	db, err := Open(path, OpenOptions{
		Create:                      true,
		WALCheckpointThresholdBytes: 1,
		checkpointPrepare: func(string, *store.GraphState, uint64, uint64, uint64) error {
			select {
			case <-started:
			default:
				close(started)
			}
			<-release
			return nil
		},
	})
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
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("checkpoint did not reach pre-prepare rotation boundary")
	}

	db.writeMu.Lock()
	firstPath := copyRecoveryFiles(t, path)
	db.writeMu.Unlock()
	first, err := Open(firstPath, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.View(func(tx *Tx) error {
		exists, err := tx.NodeExists(node.ID)
		if err == nil && !exists {
			t.Fatal("first recovery lost rotated commit")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	secondPath := copyRecoveryFiles(t, firstPath)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := Open(secondPath, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := second.View(func(tx *Tx) error {
		exists, err := tx.NodeExists(node.ID)
		if err == nil && !exists {
			t.Fatal("second recovery lost rotated commit")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCloseCleansPreparedCheckpointAfterPublicationContention(t *testing.T) {
	path := filepath.Join(t.TempDir(), "close-prepared-checkpoint.ltdb")
	prepared := make(chan struct{})
	releasePrepare := make(chan struct{})
	publicationContended := make(chan struct{})
	releasePublication := make(chan struct{})
	var prepareOnce, contentionOnce sync.Once
	db, err := Open(path, OpenOptions{
		Create:                      true,
		WALCheckpointThresholdBytes: 1,
		checkpointPrepare: func(string, *store.GraphState, uint64, uint64, uint64) error {
			prepareOnce.Do(func() { close(prepared) })
			<-releasePrepare
			return nil
		},
		checkpointTryLockFailed: func() {
			contentionOnce.Do(func() { close(publicationContended) })
			<-releasePublication
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-prepared:
	case <-time.After(5 * time.Second):
		t.Fatal("checkpoint preparation did not start")
	}
	foreground, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	close(releasePrepare)
	select {
	case <-publicationContended:
	case <-time.After(5 * time.Second):
		t.Fatal("checkpoint publication did not observe writer contention")
	}
	if err := foreground.Rollback(); err != nil {
		t.Fatal(err)
	}

	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	deadline := time.NewTimer(5 * time.Second)
	for db.IsOpen() {
		select {
		case <-deadline.C:
			t.Fatal("Close did not stop the database")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	deadline.Stop()
	close(releasePublication)
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.Contains(entry.Name(), "-checkpoint-") {
			t.Fatalf("prepared checkpoint staging remains: %s", entry.Name())
		}
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
	db.writeMu.Lock()
	crashPath := copyRecoveryFiles(t, path)
	db.writeMu.Unlock()
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
	for _, name := range []string{"state.json", "wal.log", "wal.base", "ids.json"} {
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

func TestOpenRecoveryBudgetFailureReleasesPathLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recovery-budget.ltdb")
	graph := store.NewGraphState()
	if err := store.EnsureDatabaseID(graph); err != nil {
		t.Fatal(err)
	}
	if err := store.CheckpointGraphState(path, graph, 1, 1, 0); err != nil {
		t.Fatal(err)
	}
	for id := uint64(1); id <= 2; id++ {
		next := store.NewGraphState()
		next.DatabaseID = graph.DatabaseID
		next.Nodes.Set(id, &store.NodeRecord{ID: id})
		if err := store.AppendWALCommit(path, next, id+1, 1, id); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := OpenContext(context.Background(), path, OpenOptions{RecoveryMaxFrames: 1}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("recovery budget error = %v, want ErrResourceLimit", err)
	}
	reopened, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("reopen after recovery budget error: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDeserializeChecksRecoveryBytesBeforeDecode(t *testing.T) {
	graph := store.NewGraphState()
	graph.Nodes.Set(1, &store.NodeRecord{ID: 1})
	graph.Nodes.Set(2, &store.NodeRecord{ID: 2})
	data, err := store.SerializeGraphState(graph, 1, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Deserialize(data, OpenOptions{RecoveryMaxDecodedBytes: 1}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("deserialize recovery budget error = %v, want ErrResourceLimit", err)
	}
	if _, err := Deserialize(data, OpenOptions{RecoveryMaxWork: 1}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("deserialize recovery work error = %v, want ErrResourceLimit", err)
	}
	db, err := Deserialize(data, OpenOptions{RecoveryMaxDecodedBytes: uint64(len(data))})
	if err != nil {
		t.Fatalf("deserialize at recovery byte boundary: %v", err)
	}
	if err := db.Close(); err != nil {
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
	if current.FTS.Get(first.ID).Text != "after" || current.Edges.Get(edge.ID) != nil || current.Outgoing.Has(first.ID) || current.Incoming.Has(second.ID) {
		t.Fatalf("current graph did not publish atomically")
	}
}

func TestChunkedAdjacencySnapshotAndRecoveryAfterMassDelete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "chunked-adjacency.ltdb")
	db, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}

	const totalEdges = 512
	const deletedEdges = 384
	var hub Node
	allEdgeIDs := make([]uint64, 0, totalEdges)
	edgeIDs := func(edges []Edge) []uint64 {
		ids := make([]uint64, len(edges))
		for i, edge := range edges {
			ids[i] = edge.ID
		}
		return ids
	}
	if err := db.Update(func(tx *Tx) error {
		var err error
		hub, err = tx.CreateNode(CreateNodeOptions{})
		if err != nil {
			return err
		}
		for i := 0; i < totalEdges; i++ {
			target, err := tx.CreateNode(CreateNodeOptions{})
			if err != nil {
				return err
			}
			edge, err := tx.CreateEdge(hub.ID, target.ID, "LINK", CreateEdgeOptions{})
			if err != nil {
				return err
			}
			allEdgeIDs = append(allEdgeIDs, edge.ID)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	read, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	oldEdges, err := read.GetOutgoingEdges(hub.ID)
	if err != nil {
		read.Rollback()
		t.Fatal(err)
	}
	if got := edgeIDs(oldEdges); !slices.Equal(got, allEdgeIDs) {
		read.Rollback()
		t.Fatalf("initial snapshot adjacency = %v, want %v", got, allEdgeIDs)
	}

	if err := db.Update(func(tx *Tx) error {
		for _, edgeID := range allEdgeIDs[:deletedEdges] {
			tx.deleteEdge(edgeID)
		}
		return nil
	}); err != nil {
		read.Rollback()
		t.Fatal(err)
	}
	oldEdges, err = read.GetOutgoingEdges(hub.ID)
	if err != nil {
		read.Rollback()
		t.Fatal(err)
	}
	if got := edgeIDs(oldEdges); !slices.Equal(got, allEdgeIDs) {
		read.Rollback()
		t.Fatalf("old snapshot adjacency = %v, want %v", got, allEdgeIDs)
	}

	wantLive := allEdgeIDs[deletedEdges:]
	check := func(tx *Tx) error {
		edges, err := tx.GetOutgoingEdges(hub.ID)
		if err != nil {
			return err
		}
		if got := edgeIDs(edges); !slices.Equal(got, wantLive) {
			t.Fatalf("current adjacency = %v, want %v", got, wantLive)
		}
		return nil
	}
	if err := db.View(check); err != nil {
		read.Rollback()
		t.Fatal(err)
	}
	if err := read.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.View(check); err != nil {
		t.Fatal(err)
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

func TestReadOnlyDatabasePathCanBeOpenedTwice(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shared-readers.ltdb")
	first, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	reader1, err := Open(path, OpenOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	reader2, err := Open(path, OpenOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path, OpenOptions{}); !errors.Is(err, ErrDatabaseLocked) {
		t.Fatalf("writer Open while readers are active = %v, want ErrDatabaseLocked", err)
	}
	if err := reader1.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path, OpenOptions{}); !errors.Is(err, ErrDatabaseLocked) {
		t.Fatalf("writer Open after one reader closes = %v, want ErrDatabaseLocked", err)
	}
	if err := reader2.Close(); err != nil {
		t.Fatal(err)
	}
	writer, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatalf("writer Open after last reader closes: %v", err)
	}
	if err := writer.Close(); err != nil {
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
		db, err := Open(path, OpenOptions{ReadOnly: os.Getenv("LATTICEDB_LOCK_HELPER_READONLY") == "1"})
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

func TestReadOnlyDatabasePathLockAcrossProcesses(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "process-shared-readers.ltdb")
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
		"LATTICEDB_LOCK_HELPER_READONLY=1",
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
			t.Fatal("subprocess did not acquire read-only database lock")
		}
		time.Sleep(10 * time.Millisecond)
	}
	reader, err := Open(path, OpenOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("second read-only Open while subprocess holds lock: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path, OpenOptions{}); !errors.Is(err, ErrDatabaseLocked) {
		t.Fatalf("writer Open while cross-process readers are active = %v, want ErrDatabaseLocked", err)
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

func TestQueryFTSSearchesNamedPropertyOnly(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "query-fts-properties.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var first, second Node
	if err := db.Update(func(tx *Tx) error {
		var err error
		first, err = tx.CreateNode(CreateNodeOptions{Properties: map[string]any{
			"title": "alpha", "body": "beta",
		}})
		if err != nil {
			return err
		}
		second, err = tx.CreateNode(CreateNodeOptions{Properties: map[string]any{
			"title": "beta", "body": "alpha",
		}})
		if err != nil {
			return err
		}
		// A node-level index must not make an unrelated or missing property match.
		return tx.FTSIndex(first.ID, "alpha")
	}); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, query string
		want        uint64
	}{
		{"title", `MATCH (n) WHERE n.title @@ "alpha" RETURN n`, first.ID},
		{"body", `MATCH (n) WHERE n.body @@ "alpha" RETURN n`, second.ID},
	} {
		result, err := db.Query(test.query, nil)
		if err != nil {
			t.Fatalf("%s query: %v", test.name, err)
		}
		if len(result.Rows) != 1 || result.Rows[0]["n"].(Node).ID != test.want {
			t.Fatalf("%s rows = %#v, want node %d", test.name, result.Rows, test.want)
		}
	}
	result, err := db.Query(`MATCH (n) WHERE n.typo @@ "alpha" RETURN n`, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 0 {
		t.Fatalf("missing property unexpectedly matched: %#v", result.Rows)
	}
	direct, err := db.FTSSearch("alpha", FTSSearchOptions{})
	if err != nil || len(direct) != 1 || direct[0].NodeID != first.ID {
		t.Fatalf("direct FTS = %#v, %v", direct, err)
	}
}

func TestCanonicalTextValidationAndRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "canonical-text.ltdb")
	db, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	invalid := string([]byte{0xff})
	var first, second Node
	var edge Edge
	if err := db.Update(func(tx *Tx) error {
		var err error
		first, err = tx.CreateNode(CreateNodeOptions{Labels: []string{"첫", "둘"}, Properties: map[string]any{"키": "값"}})
		if err != nil {
			return err
		}
		second, err = tx.CreateNode(CreateNodeOptions{})
		if err != nil {
			return err
		}
		edge, err = tx.CreateEdge(first.ID, second.ID, "관계", CreateEdgeOptions{})
		if err != nil {
			return err
		}
		return tx.FTSIndex(first.ID, "검색 본문")
	}); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Tx) error{
		"invalid label": func(tx *Tx) error { _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{invalid}}); return err },
		"duplicate label": func(tx *Tx) error {
			_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"same", "same"}})
			return err
		},
		"invalid edge type": func(tx *Tx) error {
			_, err := tx.CreateEdge(first.ID, second.ID, invalid, CreateEdgeOptions{})
			return err
		},
		"empty edge type":      func(tx *Tx) error { _, err := tx.CreateEdge(first.ID, second.ID, "", CreateEdgeOptions{}); return err },
		"invalid property key": func(tx *Tx) error { return tx.SetProperty(first.ID, invalid, "value") },
		"invalid edge key":     func(tx *Tx) error { return tx.SetEdgeProperty(edge.ID, invalid, "value") },
		"invalid removal key":  func(tx *Tx) error { return tx.RemoveEdgeProperty(edge.ID, invalid) },
		"invalid FTS text":     func(tx *Tx) error { return tx.FTSIndex(first.ID, invalid) },
	} {
		t.Run(name, func(t *testing.T) {
			if err := db.Update(mutate); err == nil {
				t.Fatal("invalid canonical text accepted")
			}
		})
	}
	if err := db.CreateNodePropertyIndex(invalid, "key"); err == nil {
		t.Fatal("invalid node index scope accepted")
	}
	if err := db.CreateEdgePropertyIndex("관계", invalid); err == nil {
		t.Fatal("invalid edge index property accepted")
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
		node, ok, err := tx.GetNodeValue(first.ID)
		if err != nil {
			return err
		}
		if !ok || !slices.Equal(node.Labels, []string{"첫", "둘"}) || node.Properties["키"] != "값" {
			t.Fatalf("reopened node = %#v, ok=%v", node, ok)
		}
		edges, err := tx.GetOutgoingEdges(first.ID)
		if err != nil {
			return err
		}
		if len(edges) != 1 || edges[0].ID != edge.ID || edges[0].Type != "관계" {
			t.Fatalf("reopened edges = %#v", edges)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	results, err := db.FTSSearch("본문", FTSSearchOptions{})
	if err != nil || len(results) != 1 || results[0].NodeID != first.ID {
		t.Fatalf("reopened FTS results = %#v, %v", results, err)
	}
}

func TestSetVectorRejectsInvalidPropertyKey(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "vector-key.ltdb"), OpenOptions{Create: true, EnableVector: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{})
		if err != nil {
			return err
		}
		return tx.SetVector(node.ID, string([]byte{0xff}), make([]float32, 128))
	}); err == nil {
		t.Fatal("invalid vector property key accepted")
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

func TestQueryVectorBudgetChargesDimension(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "query-vector-budget.ltdb"), OpenOptions{Create: true, EnableVector: true, VectorDimensions: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"embedding": []float32{1, 0, 0, 0}}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	query := "MATCH (n) WHERE n.embedding <=> $vector RETURN n"
	params := map[string]any{"vector": []float32{1, 0, 0, 0}}
	if result, err := db.QueryContext(context.Background(), query, params, QueryOptions{MaxWork: 5}); err != nil || len(result.Rows) != 1 {
		t.Fatalf("dimension boundary query = %#v, %v", result, err)
	}
	if result, err := db.QueryContext(context.Background(), query, params, QueryOptions{MaxWork: 4}); !errors.Is(err, ErrResourceLimit) || len(result.Rows) != 0 {
		t.Fatalf("dimension budget query = %#v, %v", result, err)
	}
}

func TestQueryVectorBudget4096Dimensions(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "query-vector-4096.ltdb"), OpenOptions{Create: true, EnableVector: true, VectorDimensions: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	vector := make([]float32, 4096)
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"embedding": vector}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	query := "MATCH (n) WHERE n.embedding <=> $vector RETURN n"
	params := map[string]any{"vector": vector}
	if result, err := db.QueryContext(context.Background(), query, params, QueryOptions{MaxWork: 4097}); err != nil || len(result.Rows) != 1 {
		t.Fatalf("4096D query = %#v, %v", result, err)
	}
	if result, err := db.QueryContext(context.Background(), query, params, QueryOptions{MaxWork: 4096}); !errors.Is(err, ErrResourceLimit) || len(result.Rows) != 0 {
		t.Fatalf("4096D budget query = %#v, %v", result, err)
	}
}

func TestQueryVectorBudgetZeroDimensionHasUnitCost(t *testing.T) {
	graph := store.NewGraphState()
	for id := uint64(1); id <= 2; id++ {
		graph.Nodes.Set(id, &store.NodeRecord{ID: id, Properties: map[string]any{"embedding": []float32{}}})
	}
	tx := &Tx{graph: graph}
	clause := &whereClause{Kind: whereVector, Var: "n", Property: "embedding", Expr: paramExpr{Name: "vector"}}
	rows := []queryRow{
		{slots: []boundValue{{Node: graph.Nodes.Get(1)}}, bound: []bool{true}, index: map[string]int{"n": 0}},
		{slots: []boundValue{{Node: graph.Nodes.Get(2)}}, bound: []bool{true}, index: map[string]int{"n": 0}},
	}
	budget := newQueryBudget(context.Background(), QueryOptions{MaxWork: 1})
	filtered, err := clause.apply(tx, rows, map[string]any{"vector": []float32{}}, budget)
	if !errors.Is(err, ErrResourceLimit) || filtered != nil {
		t.Fatalf("zero-dimension budget = %#v, %v", filtered, err)
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
		first, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"number": int64(1), "nothing": nil, "nested": map[string]any{"scores": []any{int64(1)}}}})
		if err != nil {
			return err
		}
		second, err := tx.CreateNode(CreateNodeOptions{})
		if err != nil {
			return err
		}
		_, err = tx.CreateEdge(first.ID, second.ID, "SCORED", CreateEdgeOptions{Properties: map[string]any{"nested": map[string]any{"score": int64(1)}}})
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
	result, err = db.Query("MATCH (n {number: 1.0}) RETURN count(n) AS count", nil)
	if err != nil || result.Rows[0]["count"] != int64(1) {
		t.Fatalf("inline numeric count = %#v, %v", result.Rows, err)
	}
	result, err = db.Query("MATCH (n {nested: $nested}) RETURN count(n) AS count", map[string]any{"nested": map[string]any{"scores": []any{float64(1)}}})
	if err != nil || result.Rows[0]["count"] != int64(1) {
		t.Fatalf("nested node numeric count = %#v, %v", result.Rows, err)
	}
	result, err = db.Query("MATCH ()-[r:SCORED {nested: $nested}]->() RETURN count(r) AS count", map[string]any{"nested": map[string]any{"score": float64(1)}})
	if err != nil || result.Rows[0]["count"] != int64(1) {
		t.Fatalf("nested edge numeric count = %#v, %v", result.Rows, err)
	}
	result, err = db.Query("MATCH (n) WHERE n.nothing = null RETURN count(n) AS count", nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows[0]["count"] != int64(0) {
		t.Fatalf("null equality count = %v", result.Rows[0]["count"])
	}
}

func TestQueryFTSHonorsTokenizationBudgets(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "query-fts-budget.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"text": strings.Repeat("alpha ", 100)}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	query := "MATCH (n) WHERE n.text @@ $query RETURN n"
	if result, err := db.QueryContext(context.Background(), query, map[string]any{"query": "!!!"}, QueryOptions{MaxBytes: 256}); err != nil || len(result.Rows) != 0 {
		t.Fatalf("query punctuation-only FTS rows = %#v, %v", result.Rows, err)
	}
	if _, err := db.QueryContext(context.Background(), query, map[string]any{"query": strings.Repeat("alpha ", 100)}, QueryOptions{MaxBytes: 128}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("query FTS token byte error = %v", err)
	}
	if _, err := db.QueryContext(context.Background(), query, map[string]any{"query": "alpha"}, QueryOptions{MaxWork: 20}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("query FTS fallback work error = %v", err)
	}
	if _, err := db.QueryContext(&cancelAfterChecks{limit: 10}, query, map[string]any{"query": strings.Repeat("alpha ", 1_000)}, QueryOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("query FTS mid-tokenization cancellation = %v", err)
	}
}

func TestQueryFTSHoistsInvariantQueryTokenization(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "query-fts-hoist.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		for i := 0; i < 1_000; i++ {
			if _, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"text": "indexed text"}}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	// A punctuation-only literal still incurs tokenization input work. With
	// row-wise tokenization this exceeds the budget as rows scale.
	query := `MATCH (n) WHERE n.text @@ "` + strings.Repeat("!", 128) + `" RETURN count(n) AS count`
	result, err := db.QueryContext(context.Background(), query, nil, QueryOptions{MaxWork: 3_000})
	if err != nil {
		t.Fatalf("invariant FTS query: %v", err)
	}
	if got := result.Rows[0]["count"]; got != int64(0) {
		t.Fatalf("punctuation-only FTS count = %v, want 0", got)
	}
	result, err = db.QueryContext(context.Background(), `MATCH (n) WHERE n.text @@ $query RETURN count(n) AS count`, map[string]any{"query": strings.Repeat("!", 128)}, QueryOptions{MaxWork: 3_000})
	if err != nil || result.Rows[0]["count"] != int64(0) {
		t.Fatalf("parameter FTS query = %#v, %v", result.Rows, err)
	}
}

func TestQueryFTSRowDependentRHSAndEmptyRows(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "query-fts-row-dependent.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		for _, node := range []map[string]any{{"text": "alpha", "query": "alpha"}, {"text": "alpha", "query": "beta"}} {
			if _, err := tx.CreateNode(CreateNodeOptions{Properties: node}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	result, err := db.Query(`MATCH (n) WHERE n.text @@ n.query RETURN count(n) AS count`, nil)
	if err != nil || result.Rows[0]["count"] != int64(1) {
		t.Fatalf("row-dependent FTS query = %#v, %v", result.Rows, err)
	}
	empty, err := Open(filepath.Join(t.TempDir(), "query-fts-empty.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer empty.Close()
	for _, params := range []map[string]any{nil, {"query": 42}} {
		result, err := empty.Query(`MATCH (n) WHERE n.text @@ $query RETURN count(n) AS count`, params)
		if err != nil || result.Rows[0]["count"] != int64(0) {
			t.Fatalf("empty-row FTS query params %#v = %#v, %v", params, result.Rows, err)
		}
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

func TestAnonymousParameterizedMatchSupportsTraversal(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "anonymous-match.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		alice, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"name": "Alice"}})
		if err != nil {
			return err
		}
		bob, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"name": "Bob"}})
		if err != nil {
			return err
		}
		_, err = tx.CreateEdge(alice.ID, bob.ID, "KNOWS", CreateEdgeOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}

	params := map[string]any{"name": "Alice"}
	result, err := db.Query("MATCH ({name: $name})-[:KNOWS]->(b) RETURN b.name", params)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["b.name"] != "Bob" {
		t.Fatalf("anonymous traversal result = %#v", result.Rows)
	}
	result, err = db.Query("MATCH ({name: $name}) RETURN count(*) AS count", params)
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["count"] != int64(1) {
		t.Fatalf("anonymous node result = %#v, %v", result.Rows, err)
	}
}

func TestMatchPathSyntaxNormalizesToExistingTraversal(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "path-syntax.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		alice, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"name": "Alice"}})
		if err != nil {
			return err
		}
		bob, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"name": "Bob"}})
		if err != nil {
			return err
		}
		carol, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"name": "Carol"}})
		if err != nil {
			return err
		}
		if _, err = tx.CreateEdge(alice.ID, bob.ID, "KNOWS", CreateEdgeOptions{Properties: map[string]any{"since": int64(2024)}}); err != nil {
			return err
		}
		_, err = tx.CreateEdge(bob.ID, carol.ID, "KNOWS", CreateEdgeOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}

	queries := []string{
		`MATCH (b {name: "Bob"})<-[r:KNOWS]-(a) RETURN a.name AS name`,
		`MATCH (a)-[:KNOWS {since: $since}]->(b) RETURN b.name AS name`,
		`MATCH (a {name: "Alice"})-[:KNOWS]->()-[:KNOWS]->(c) RETURN c.name AS name`,
	}
	for _, query := range queries {
		result, err := db.Query(query, map[string]any{"since": int64(2024)})
		if err != nil {
			t.Fatalf("Query(%q): %v", query, err)
		}
		want := "Alice"
		if strings.Contains(query, "since") {
			want = "Bob"
		} else if strings.Contains(query, "->()") {
			want = "Carol"
		}
		if len(result.Rows) != 1 || result.Rows[0]["name"] != want {
			t.Fatalf("Query(%q) = %#v, want %q", query, result.Rows, want)
		}
	}
}

func TestReturnAliasAndParameterizedPagination(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "pagination.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		for _, value := range []int64{3, 1, 2} {
			if _, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"value": value}}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	result, err := db.Query("MATCH (n) RETURN n.value AS value ORDER BY value SKIP $skip LIMIT $limit", map[string]any{"skip": 1, "limit": 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["value"] != int64(2) {
		t.Fatalf("paginated rows = %#v", result.Rows)
	}
	for _, params := range []map[string]any{{"skip": -1, "limit": 1}, {"skip": 0, "limit": "one"}} {
		if _, err := db.Query("MATCH (n) RETURN n.value SKIP $skip LIMIT $limit", params); err == nil {
			t.Fatalf("pagination unexpectedly accepted %#v", params)
		}
	}
}

func TestMutationCompositionReturnsUpdatedBindings(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "mutation-return.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Source"}, Properties: map[string]any{"old": true}}); err != nil {
			return err
		}
		_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Target"}})
		return err
	}); err != nil {
		t.Fatal(err)
	}

	result, err := db.Query("MATCH (n:Source) SET n.a = 1, n.b = 2 RETURN n.a AS a, n.b AS b", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["a"] != int64(1) || result.Rows[0]["b"] != int64(2) {
		t.Fatalf("SET RETURN rows = %#v", result.Rows)
	}
	result, err = db.Query("MATCH (n:Source) REMOVE n.old RETURN n.old AS old", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["old"] != nil {
		t.Fatalf("REMOVE RETURN rows = %#v", result.Rows)
	}
	result, err = db.Query("MATCH (a:Source), (b:Target) CREATE (a)-[r:LINK {weight: 1}]->(b) RETURN id(r) AS edge", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["edge"] == nil {
		t.Fatalf("CREATE RETURN rows = %#v", result.Rows)
	}
}

func TestBooleanWherePredicatesAndComparisons(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "where-predicate.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		for _, props := range []map[string]any{
			{"name": "ten", "age": int64(10), "active": true},
			{"name": "twenty", "age": int64(20), "active": false, "admin": true},
			{"name": "thirty", "age": int64(30)},
		} {
			if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Person"}, Properties: props}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		where string
		want  []string
	}{
		{"n.age >= 20 AND n.age < 30", []string{"twenty"}},
		{"n.age <> 20", []string{"ten", "thirty"}},
		{"n.active = true OR n.admin = true", []string{"ten", "twenty"}},
		{"NOT (n.active = true OR n.age >= 30)", []string{"twenty"}},
		{"NOT n.admin = true", nil},
	}
	for _, test := range tests {
		result, err := db.Query("MATCH (n:Person) WHERE "+test.where+" RETURN n.name AS name ORDER BY name", nil)
		if err != nil {
			t.Fatalf("WHERE %s: %v", test.where, err)
		}
		got := make([]string, len(result.Rows))
		for index, row := range result.Rows {
			got[index] = row["name"].(string)
		}
		if !slices.Equal(got, test.want) {
			t.Fatalf("WHERE %s = %v, want %v", test.where, got, test.want)
		}
	}
}

func TestMixedNumericComparisonPreservesIntegerPrecision(t *testing.T) {
	tests := []struct {
		left  any
		right any
		want  int
	}{
		{int64(9_007_199_254_740_993), float64(9_007_199_254_740_992), 1},
		{int64(1), 1.5, -1},
		{int64(-1), -1.5, 1},
	}
	for _, test := range tests {
		got, ok := compareQueryValues(test.left, test.right)
		if !ok || got != test.want {
			t.Fatalf("compareQueryValues(%v, %v) = %d, %v; want %d, true", test.left, test.right, got, ok, test.want)
		}
	}
}

func TestMixedNumericOrderUsesPrecisionSafeComparison(t *testing.T) {
	negativeZero := math.Copysign(0, -1)
	tests := []struct {
		name  string
		left  any
		right any
		want  int
		equal bool
	}{
		{name: "negative nonintegral", left: int64(-1), right: -1.5, want: 1},
		{name: "negative reverse", left: -1.5, right: int64(-1), want: -1},
		{name: "integral float", left: int64(2), right: 2.0, equal: true},
		{name: "signed zero", left: int64(0), right: negativeZero, equal: true},
		{name: "precision boundary", left: int64(9_007_199_254_740_993), right: float64(9_007_199_254_740_992), want: 1},
	}
	for _, test := range tests {
		if got := compareOrderValues(test.left, test.right); got != test.want {
			t.Errorf("compareOrderValues(%v, %v) = %d, want %d", test.left, test.right, got, test.want)
		}
		if test.equal && !queryValuesEqual(test.left, test.right) {
			t.Errorf("queryValuesEqual(%v, %v) = false, want true", test.left, test.right)
		}
	}

	db, err := Open(filepath.Join(t.TempDir(), "mixed-numeric-order.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	result, err := db.Query("UNWIND $values AS value RETURN value ORDER BY value", map[string]any{
		"values": []any{
			float64(1.5),
			int64(-1),
			float64(-1.5),
			int64(0),
			int64(9_007_199_254_740_993),
			float64(9_007_199_254_740_992),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	got := make([]any, len(result.Rows))
	for index, row := range result.Rows {
		got[index] = row["value"]
	}
	want := []any{
		float64(-1.5),
		int64(-1),
		int64(0),
		float64(1.5),
		float64(9_007_199_254_740_992),
		int64(9_007_199_254_740_993),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mixed numeric ORDER BY = %#v, want %#v", got, want)
	}
}

func TestCypherCompatibilitySyntaxBatch(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "cypher-compatibility.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for _, query := range []string{
		"CREATE\t(:Item {name: 'Alpha', kind: 'first', text: 'A  B'})\n;",
		"CREATE (:Item {name: 'Beta', kind: 'second'});",
	} {
		if _, err := db.Query(query, nil); err != nil {
			t.Fatalf("Query(%q): %v", query, err)
		}
	}
	result, err := db.Query("MATCH\n(n:Item)\nWHERE n.text = 'A  B'\nRETURN n.name AS name;", nil)
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["name"] != "Alpha" {
		t.Fatalf("normalized query result = %#v, %v", result.Rows, err)
	}

	result, err = db.Query("MATCH (a:Item {name: 'Alpha'}), (b:Item {name: 'Beta'}) CREATE (a)<-[r:LINK]-(b) RETURN id(r) AS edge;", nil)
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["edge"] == nil {
		t.Fatalf("incoming CREATE result = %#v, %v", result.Rows, err)
	}
	result, err = db.Query("MATCH (a:Item {name: 'Alpha'})<-[r:LINK]-(b:Item) RETURN b.name AS name", nil)
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["name"] != "Beta" {
		t.Fatalf("incoming edge result = %#v, %v", result.Rows, err)
	}

	result, err = db.Query("MATCH (n:Item {name: 'Alpha'}) SET n:Active RETURN n.name AS name", nil)
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["name"] != "Alpha" {
		t.Fatalf("SET label result = %#v, %v", result.Rows, err)
	}
	result, err = db.Query("MATCH (n:Active) RETURN count(n) AS count", nil)
	if err != nil || result.Rows[0]["count"] != int64(1) {
		t.Fatalf("SET label count = %#v, %v", result.Rows, err)
	}

	predicateTests := []struct {
		where string
		args  map[string]any
		want  []string
	}{
		{"n.kind IN $kinds", map[string]any{"kinds": []any{"second"}}, []string{"Beta"}},
		{"n.name STARTS WITH $text", map[string]any{"text": "Al"}, []string{"Alpha"}},
		{"n.name ENDS WITH $text", map[string]any{"text": "ta"}, []string{"Beta"}},
		{"n.name CONTAINS $text", map[string]any{"text": "ph"}, []string{"Alpha"}},
	}
	for _, test := range predicateTests {
		result, err := db.Query("MATCH (n:Item) WHERE "+test.where+" RETURN n.name AS name ORDER BY name", test.args)
		if err != nil {
			t.Fatalf("WHERE %s: %v", test.where, err)
		}
		got := make([]string, len(result.Rows))
		for index, row := range result.Rows {
			got[index] = row["name"].(string)
		}
		if !slices.Equal(got, test.want) {
			t.Fatalf("WHERE %s = %v, want %v", test.where, got, test.want)
		}
	}
	result, err = db.Query("MATCH (n:Item) WHERE n.name STARTS WITH 'Al' OR n.name ENDS WITH 'ta' RETURN n.name AS name ORDER BY name", nil)
	if err != nil || len(result.Rows) != 2 {
		t.Fatalf("combined string predicate rows = %#v, %v", result.Rows, err)
	}
	result, err = db.Query("MATCH (n:Item) WHERE NOT (n.kind IN $kinds) RETURN n.name", map[string]any{"kinds": []any{"missing", nil}})
	if err != nil || len(result.Rows) != 0 {
		t.Fatalf("NULL IN predicate rows = %#v, %v", result.Rows, err)
	}
	if _, err := db.Query("MATCH (n:Item) WHERE n.kind IN $kinds RETURN n", map[string]any{"kinds": "first"}); err == nil {
		t.Fatal("IN accepted a non-list parameter")
	}

	if _, err := db.Query("MATCH (n:Active) DETACH DELETE n;", nil); err != nil {
		t.Fatal(err)
	}
	result, err = db.Query("MATCH ()-[r]->() RETURN count(r) AS count", nil)
	if err != nil || result.Rows[0]["count"] != int64(0) {
		t.Fatalf("DETACH DELETE edge count = %#v, %v", result.Rows, err)
	}
}

func TestDeleteRequiresDetachForIncidentEdges(t *testing.T) {
	for _, test := range []struct {
		name   string
		match  string
		plain  string
		detach string
	}{
		{name: "outgoing", match: "MATCH (a), (b) CREATE (a)-[:LINK]->(b)", plain: "MATCH (a) DELETE a", detach: "MATCH (a) DETACH DELETE a"},
		{name: "incoming", match: "MATCH (a), (b) CREATE (a)-[:LINK]->(b)", plain: "MATCH (b) DELETE b", detach: "MATCH (b) DETACH DELETE b"},
		{name: "self-loop", match: "MATCH (a), (b) CREATE (a)-[:LINK]->(a)", plain: "MATCH (a) DELETE a", detach: "MATCH (a) DETACH DELETE a"},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, err := Open(filepath.Join(t.TempDir(), "delete.ltdb"), OpenOptions{Create: true})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := db.Query("CREATE (:Node)", nil); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Query("CREATE (:Node)", nil); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Query(test.match, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Query(test.plain, nil); err == nil {
				t.Fatal("plain DELETE unexpectedly removed a node with an incident edge")
			}
			result, err := db.Query("MATCH (n) RETURN count(n) AS count", nil)
			if err != nil || result.Rows[0]["count"] != int64(2) {
				t.Fatalf("plain DELETE was not atomic: %#v, %v", result.Rows, err)
			}
			if _, err := db.Query(test.detach, nil); err != nil {
				t.Fatal(err)
			}
			result, err = db.Query("MATCH (n) RETURN count(n) AS count", nil)
			if err != nil || result.Rows[0]["count"] != int64(0) {
				t.Fatalf("DETACH DELETE node count = %#v, %v", result.Rows, err)
			}
		})
	}

	t.Run("edge-and-node-same-query", func(t *testing.T) {
		db, err := Open(filepath.Join(t.TempDir(), "delete-edge-node.ltdb"), OpenOptions{Create: true})
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if _, err := db.Query("CREATE (:Node)", nil); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Query("CREATE (:Node)", nil); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Query("MATCH (a), (b) CREATE (a)-[r:LINK]->(b)", nil); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Query("MATCH (a)-[r:LINK]->(b) DELETE r, a, b", nil); err != nil {
			t.Fatalf("explicit edge deletion should permit plain node deletion: %v", err)
		}
		result, err := db.Query("MATCH (n) RETURN count(n) AS count", nil)
		if err != nil || result.Rows[0]["count"] != int64(0) {
			t.Fatalf("edge+node DELETE count = %#v, %v", result.Rows, err)
		}
	})
}

func TestDeleteNodeBatchesIncidentEdges(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "batch-delete.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var node Node
	if err := db.Update(func(tx *Tx) error {
		var err error
		node, err = tx.CreateNode(CreateNodeOptions{Labels: []string{"Node"}})
		if err != nil {
			return err
		}
		other, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Node"}})
		if err != nil {
			return err
		}
		if _, err = tx.CreateEdge(node.ID, other.ID, "LINK", CreateEdgeOptions{Properties: map[string]any{"k": int64(1)}}); err != nil {
			return err
		}
		if _, err = tx.CreateEdge(other.ID, node.ID, "LINK", CreateEdgeOptions{Properties: map[string]any{"k": int64(2)}}); err != nil {
			return err
		}
		_, err = tx.CreateEdge(node.ID, node.ID, "LINK", CreateEdgeOptions{Properties: map[string]any{"k": int64(3)}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateEdgePropertyIndex("LINK", "k"); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error { return tx.DeleteNode(node.ID) }); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *Tx) error {
		if exists, err := tx.NodeExists(node.ID); err != nil || exists {
			t.Fatalf("deleted node exists=%v err=%v", exists, err)
		}
		if got := tx.graph.Edges.Len(); got != 0 {
			t.Fatalf("incident edges remain: %d", got)
		}
		for _, value := range []int64{1, 2, 3} {
			ids, err := tx.FindEdgesByTypeProperty("LINK", "k", value, 1)
			if err != nil {
				return err
			}
			if len(ids) != 0 {
				t.Fatalf("deleted edge index value %d = %v", value, ids)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestAdditionalCypherSyntaxBatch(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "additional-cypher.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Update(func(tx *Tx) error {
		var nodes []Node
		for _, props := range []map[string]any{
			{"id": int64(1), "name": "Alpha", "min": int64(1), "value": int64(2), "group": "same", "numeric": int64(1), "nested": []any{int64(1), map[string]any{"value": int64(2)}}, "bucket": "all"},
			{"id": int64(2), "name": "Beta", "min": int64(3), "value": int64(2), "group": "same", "numeric": float64(1), "nested": []any{float64(1), map[string]any{"value": float64(2)}}, "bucket": "all"},
			{"id": int64(3), "name": "Gamma", "min": int64(0), "value": int64(0), "group": "other", "numeric": float64(2), "bucket": "all"},
		} {
			node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: props})
			if err != nil {
				return err
			}
			nodes = append(nodes, node)
		}
		if _, err := tx.CreateEdge(nodes[0].ID, nodes[1].ID, "LINK", CreateEdgeOptions{}); err != nil {
			return err
		}
		if _, err := tx.CreateEdge(nodes[0].ID, nodes[0].ID, "LINK", CreateEdgeOptions{}); err != nil {
			return err
		}
		_, err := tx.CreateEdge(nodes[0].ID, nodes[0].ID, "SELF", CreateEdgeOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("Item", "bucket"); err != nil {
		t.Fatal(err)
	}

	result, err := db.Query("MATCH (n:Item) WHERE n.min <= n.value RETURN n.id AS id ORDER BY id", nil)
	if err != nil || len(result.Rows) != 2 || result.Rows[0]["id"] != int64(1) || result.Rows[1]["id"] != int64(3) {
		t.Fatalf("property comparison rows = %#v, %v", result.Rows, err)
	}
	result, err = db.Query("MATCH (n:Item {id: 1}) SET n.copy = n.name RETURN n.copy AS copy", nil)
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["copy"] != "Alpha" {
		t.Fatalf("property SET rows = %#v, %v", result.Rows, err)
	}
	result, err = db.Query("MATCH (a:Item {id: 1}), (b:Item {id: 2}) CREATE (a)-[r:WEIGHTED {weight: b.value}]->(b) RETURN r.weight AS weight", nil)
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["weight"] != int64(2) {
		t.Fatalf("property CREATE rows = %#v, %v", result.Rows, err)
	}

	result, err = db.Query("UNWIND $items AS item CREATE (n:Item {id: item.id}) RETURN n.id AS id ORDER BY id", map[string]any{"items": []any{map[string]any{"id": int64(4)}, map[string]any{"id": int64(5)}}})
	if err != nil || len(result.Rows) != 2 || result.Rows[0]["id"] != int64(4) || result.Rows[1]["id"] != int64(5) {
		t.Fatalf("UNWIND CREATE rows = %#v, %v", result.Rows, err)
	}
	result, err = db.Query("UNWIND $ids AS wanted MATCH (n:Item) WHERE n.id = wanted SET n.selected = true RETURN DISTINCT n.id AS id ORDER BY id", map[string]any{"ids": []any{int64(1), int64(1), int64(2)}})
	if err != nil || len(result.Rows) != 2 || result.Rows[0]["id"] != int64(1) || result.Rows[1]["id"] != int64(2) {
		t.Fatalf("UNWIND MATCH SET rows = %#v, %v", result.Rows, err)
	}
	if _, err := db.Query("UNWIND $ids AS wanted MATCH (n:Item) WHERE n.id = wanted REMOVE n.selected", map[string]any{"ids": []any{int64(2)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query("UNWIND $ids AS wanted MATCH (n:Item) WHERE n.id = wanted DELETE n", map[string]any{"ids": []any{int64(4), int64(5)}}); err != nil {
		t.Fatal(err)
	}

	result, err = db.Query("MATCH (a:Item)-[:LINK]-(b:Item) RETURN a.id AS a, b.id AS b ORDER BY a, b", nil)
	if err != nil || len(result.Rows) != 3 || result.Rows[0]["a"] != int64(1) || result.Rows[0]["b"] != int64(1) || result.Rows[1]["b"] != int64(2) || result.Rows[2]["a"] != int64(2) {
		t.Fatalf("undirected rows = %#v, %v", result.Rows, err)
	}
	result, err = db.Query("MATCH ()-[r:WEIGHTED {weight: 2.0}]->() RETURN count(r) AS count", nil)
	if err != nil || result.Rows[0]["count"] != int64(1) {
		t.Fatalf("inline edge numeric count = %#v, %v", result.Rows, err)
	}
	result, err = db.Query("MATCH (a:Item)-[:LINK]-(a) RETURN count(*) AS count", nil)
	if err != nil || result.Rows[0]["count"] != int64(1) {
		t.Fatalf("undirected repeated binding count = %#v, %v", result.Rows, err)
	}
	result, err = db.Query("MATCH (a:Item)-[:LINK]-(b:Item) RETURN DISTINCT a.id AS id ORDER BY id SKIP 1 LIMIT 1", nil)
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["id"] != int64(2) {
		t.Fatalf("DISTINCT rows = %#v, %v", result.Rows, err)
	}
	result, err = db.Query("MATCH (n:Item) WHERE n.numeric IS NOT NULL RETURN DISTINCT n.numeric AS numeric ORDER BY numeric", nil)
	if err != nil || len(result.Rows) != 2 {
		t.Fatalf("numeric DISTINCT rows = %#v, %v", result.Rows, err)
	}
	result, err = db.Query("MATCH (n:Item) WHERE n.nested IS NOT NULL RETURN DISTINCT n.nested AS nested", nil)
	if err != nil || len(result.Rows) != 1 {
		t.Fatalf("nested numeric DISTINCT rows = %#v, %v", result.Rows, err)
	}
	result, err = db.Query("MATCH (n:Item) RETURN DISTINCT n.absent AS absent", nil)
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["absent"] != nil {
		t.Fatalf("nil DISTINCT rows = %#v, %v", result.Rows, err)
	}
	result, err = db.Query("MATCH (n:Item) WHERE n.bucket = 'all' RETURN DISTINCT n.group AS group LIMIT 2", nil)
	if err != nil || len(result.Rows) != 2 {
		t.Fatalf("indexed DISTINCT rows = %#v, %v", result.Rows, err)
	}

	mutation := "MATCH (a:Item)-[:WEIGHTED]-(b:Item) SET a.marked = true RETURN a.marked AS a, b.marked AS b"
	if _, err := db.QueryContext(context.Background(), mutation, nil, QueryOptions{MaxRows: 1}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("undirected row budget error = %v, want ErrResourceLimit", err)
	}
	result, err = db.Query("MATCH (n:Item) WHERE n.marked IS NOT NULL RETURN count(n) AS count", nil)
	if err != nil || result.Rows[0]["count"] != int64(0) {
		t.Fatalf("budget failure persisted mutation: %#v, %v", result.Rows, err)
	}
	result, err = db.QueryContext(context.Background(), mutation, nil, QueryOptions{MaxRows: 2})
	if err != nil || len(result.Rows) != 2 {
		t.Fatalf("undirected mutation rows = %#v, %v", result.Rows, err)
	}
	for _, row := range result.Rows {
		if row["a"] != true || row["b"] != true {
			t.Fatalf("stale mutation binding row = %#v", row)
		}
	}
	result, err = db.Query("MATCH (a:Item)-[:WEIGHTED]-(b:Item) SET a.x = 1, b.y = a.x RETURN b.y AS y", nil)
	if err != nil || len(result.Rows) != 2 || result.Rows[0]["y"] != int64(1) || result.Rows[1]["y"] != int64(1) {
		t.Fatalf("stale SET expression rows = %#v, %v", result.Rows, err)
	}
	result, err = db.QueryContext(context.Background(), "MATCH (a:Item)-[:SELF]-(b:Item) RETURN a.id AS a, b.id AS b", nil, QueryOptions{MaxRows: 1})
	if err != nil || len(result.Rows) != 1 {
		t.Fatalf("undirected self-loop rows = %#v, %v", result.Rows, err)
	}
	if _, err := db.QueryContext(context.Background(), "UNWIND $items AS item MATCH (n:Item) WHERE n.bucket = 'all' RETURN item", map[string]any{"items": []any{int64(1), int64(2)}}, QueryOptions{MaxRows: 2}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("indexed cartesian row budget error = %v, want ErrResourceLimit", err)
	}
}

func TestUnsupportedCreatePatternFailsBeforeMutation(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "partial-create.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, query := range []string{
		"CREATE (a:Person {name: $a})-[e:KNOWS]->(b:Person {name: $b})",
		"CREATE (:Person)-[:KNOWS]->(:Person)",
	} {
		if _, err := db.Query(query, map[string]any{"a": "Alice", "b": "Bob"}); err == nil {
			t.Fatalf("unsupported create pattern unexpectedly succeeded: %s", query)
		}
	}
	result, err := db.Query("MATCH (n) RETURN count(n) AS count", nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows[0]["count"] != int64(0) {
		t.Fatalf("unsupported CREATE partially mutated graph: %#v", result.Rows)
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
