package exporter

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestReleaseExportRejectsSymlinkLockDirectory(t *testing.T) {
	for _, lexicalAlias := range []bool{false, true} {
		name := "lock-directory"
		if lexicalAlias {
			name = "reserved-lexical-alias"
		}
		t.Run(name, func(t *testing.T) {
			parent := t.TempDir()
			target := filepath.Join(parent, "ordinary")
			if err := os.Mkdir(target, 0700); err != nil {
				t.Fatal(err)
			}
			sentinel := filepath.Join(target, "graph.lock")
			const contents = "existing lock target"
			if err := os.WriteFile(sentinel, []byte(contents), 0600); err != nil {
				t.Fatal(err)
			}
			before, err := os.Stat(sentinel)
			if err != nil {
				t.Fatal(err)
			}
			reserved := filepath.Join(parent, exportLockDirectory)
			if err := os.Symlink(target, reserved); err != nil {
				t.Skipf("symlinks unavailable: %v", err)
			}
			output := filepath.Join(parent, "graph")
			if lexicalAlias {
				output = filepath.Join(reserved, "graph.lock")
			}
			if _, err := ExportGraph(store.NewGraphState(), ExportFormatJSON, output); err == nil {
				t.Error("export accepted a symlink lock directory or its reserved lexical alias")
			}
			after, err := os.Stat(sentinel)
			if err != nil || !os.SameFile(before, after) {
				t.Fatalf("target inode changed: %v", err)
			}
			data, err := os.ReadFile(sentinel)
			if err != nil || string(data) != contents {
				t.Fatalf("target contents changed: %q, %v", data, err)
			}
			entries, err := os.ReadDir(target)
			if err != nil || len(entries) != 1 {
				t.Fatalf("target directory changed: %v, %v", entries, err)
			}
			if _, err := os.Lstat(filepath.Join(parent, "graph")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("rejected export published output: %v", err)
			}
		})
	}
}

func TestReleaseExportNormalLockPreservesInodeAndExclusion(t *testing.T) {
	output := filepath.Join(t.TempDir(), "graph")
	unlock, err := acquireExportLock(output)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	lockPath, err := exportLockPath(output)
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ExportGraph(store.NewGraphState(), ExportFormatJSON, lockPath); err == nil {
		t.Error("export accepted an active reserved lock path")
	}
	if _, err := ExportGraph(store.NewGraphState(), ExportFormatJSON, output+".lock"); err != nil {
		t.Fatalf("distinct lock-named output: %v", err)
	}
	after, err := os.Stat(lockPath)
	if err != nil || !os.SameFile(before, after) {
		t.Fatalf("active lock inode changed: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := ExportGraphContext(ctx, store.NewGraphState(), ExportFormatJSON, output); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("same-output export bypassed exclusion: %v", err)
	}
}

func TestReleaseExportRejectsSymlinkLockFile(t *testing.T) {
	parent := t.TempDir()
	output := filepath.Join(parent, "graph")
	lockPath, err := exportLockPath(output)
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(parent, "ordinary")
	if err := os.WriteFile(target, []byte("unchanged"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, lockPath); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, err := ExportGraph(store.NewGraphState(), ExportFormatJSON, output); err == nil {
		t.Fatal("export accepted a symlink lock file")
	}
	if data, err := os.ReadFile(target); err != nil || string(data) != "unchanged" {
		t.Fatalf("lock target changed: %q, %v", data, err)
	}
	if _, err := os.Stat(output); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected export published output: %v", err)
	}
}

func TestReleaseExportReservedNamespaceCaseFolding(t *testing.T) {
	for _, name := range []string{".latticedb-export-locks", ".LATTICEDB-EXPORT-LOCKS", ".latticedb-export-lockſ", ".LATTICEDB-EXPORT-LOCKS.INIT-123", ".latticedb-export-lockſ.init-123"} {
		if !exportPathIsReserved(filepath.Join(t.TempDir(), name, "output")) {
			t.Errorf("reserved alias accepted: %s", name)
		}
	}
}
