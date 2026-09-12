package engine

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

const (
	batchCrashFixtureEnv = "LATTICEDB_BATCH_CRASH_FIXTURE"
	batchCrashModeEnv    = "LATTICEDB_BATCH_CRASH_MODE"
	batchCrashMarkerEnv  = "LATTICEDB_BATCH_CRASH_MARKER"
)

const (
	markerBatchPersist  = 42
	markerBatchTruncate = 43
)

// TestBatchCrashChild is the subprocess entry point for batch crash tests.
func TestBatchCrashChild(t *testing.T) {
	fixturePath := os.Getenv(batchCrashFixtureEnv)
	if fixturePath == "" {
		return
	}
	mode := os.Getenv(batchCrashModeEnv)
	markerCode, err := strconv.Atoi(os.Getenv(batchCrashMarkerEnv))
	if err != nil {
		t.Fatal(err)
	}
	switch mode {
	case "persist":
		batchCrashPersist(t, fixturePath, markerCode)
	case "truncate":
		batchCrashTruncate(t, fixturePath, markerCode)
	default:
		t.Fatalf("unknown batch crash mode: %s", mode)
	}
}

func batchCrashPersist(t *testing.T, fixturePath string, markerCode int) {
	db, err := Open(fixturePath, OpenOptions{
		Create: true,
		walSync: func(file *os.File) error {
			if err := file.Sync(); err != nil {
				return err
			}
			os.Exit(markerCode)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	requests := makeBatchRequests()
	db.batchMu.Lock()
	db.batchPending = requests
	db.batchRunning = true
	db.batchMu.Unlock()

	go db.runBatch()
	select {} // exit via os.Exit in walSync
}

func batchCrashTruncate(t *testing.T, fixturePath string, markerCode int) {
	var walBaseSize int64

	db, err := Open(fixturePath, OpenOptions{
		Create: true,
		walSync: func(file *os.File) error {
			if err := file.Truncate(walBaseSize); err != nil {
				return err
			}
			if err := file.Sync(); err != nil {
				return err
			}
			os.Exit(markerCode)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	info, err := os.Stat(db.files.WAL)
	if err != nil {
		t.Fatal(err)
	}
	walBaseSize = info.Size()

	requests := makeBatchRequests()
	db.batchMu.Lock()
	db.batchPending = requests
	db.batchRunning = true
	db.batchMu.Unlock()

	go db.runBatch()
	select {} // exit via os.Exit in walSync
}

func makeBatchRequests() []*batchRequest {
	fn := func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{})
		return err
	}
	return []*batchRequest{
		{ctx: context.Background(), fn: fn, done: make(chan error, 1)},
		{ctx: context.Background(), fn: fn, done: make(chan error, 1)},
	}
}

func runBatchCrashSubprocess(t *testing.T, fixturePath, mode string, markerCode int) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestBatchCrashChild$", "-test.v")
	cmd.Env = append(os.Environ(),
		batchCrashFixtureEnv+"="+fixturePath,
		batchCrashModeEnv+"="+mode,
		batchCrashMarkerEnv+"="+strconv.Itoa(markerCode),
	)
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("subprocess %s exited cleanly, want exit code %d\n%s", mode, markerCode, output)
	}
	exitErr, ok := err.(*exec.ExitError)
	if !ok {
		t.Fatalf("subprocess %s: unexpected error type: %v\n%s", mode, err, output)
	}
	return exitErr.ExitCode()
}

func TestBatchCrashGroupPersist(t *testing.T) {
	root := t.TempDir()
	fixturePath := filepath.Join(root, "batch-crash.ltdb")

	exitCode := runBatchCrashSubprocess(t, fixturePath, "persist", markerBatchPersist)
	if exitCode != markerBatchPersist {
		t.Fatalf("exit code = %d, want %d", exitCode, markerBatchPersist)
	}

	db, err := Open(fixturePath, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if db.commitID != 1 {
		t.Fatalf("commit=%d, want 1", db.commitID)
	}

	if err := db.View(func(tx *Tx) error {
		exists1, err := tx.NodeExists(1)
		if err != nil {
			return err
		}
		exists2, err := tx.NodeExists(2)
		if err != nil {
			return err
		}
		if !exists1 || !exists2 {
			t.Fatalf("expected both nodes persisted, got exists1=%v exists2=%v", exists1, exists2)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBatchCrashGroupTruncateDiscards(t *testing.T) {
	root := t.TempDir()
	fixturePath := filepath.Join(root, "batch-crash-truncate.ltdb")

	exitCode := runBatchCrashSubprocess(t, fixturePath, "truncate", markerBatchTruncate)
	if exitCode != markerBatchTruncate {
		t.Fatalf("exit code = %d, want %d", exitCode, markerBatchTruncate)
	}

	db, err := Open(fixturePath, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if db.commitID != 0 {
		t.Fatalf("commit=%d, want 0", db.commitID)
	}

	if err := db.View(func(tx *Tx) error {
		exists1, err := tx.NodeExists(1)
		if err != nil {
			return err
		}
		exists2, err := tx.NodeExists(2)
		if err != nil {
			return err
		}
		if exists1 || exists2 {
			t.Fatalf("expected no batch nodes persisted, got exists1=%v exists2=%v", exists1, exists2)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
