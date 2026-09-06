package engine

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestPathLockRejectsWriterThroughLockFileAlias(t *testing.T) {
	directory := t.TempDir()
	primary := filepath.Join(directory, "primary.ltdb")
	first, _, _, err := acquirePathLock(primary, true, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.close() })
	alias := linkPathLock(t, directory, primary)

	if _, _, _, err := acquirePathLock(alias, true, false); !errors.Is(err, ErrDatabaseLocked) {
		t.Fatalf("aliased writer error = %v", err)
	}
}

func TestPathLockSharesReadersThroughLockFileAliasUntilLastClose(t *testing.T) {
	directory := t.TempDir()
	primary := filepath.Join(directory, "primary.ltdb")
	db, err := Open(primary, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	first, _, _, err := acquirePathLock(primary, false, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.close() })
	alias := linkPathLock(t, directory, primary)
	second, _, _, err := acquirePathLock(alias, true, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.close() })
	if second.path != first.path {
		t.Fatalf("alias registry key = %q, want %q", second.path, first.path)
	}
	if err := first.close(); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestPathLockAliasProcessProbe$")
	command.Env = append(os.Environ(), "LATTICEDB_LOCK_ALIAS_PROBE="+primary)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("separate-process writer admission: %v\n%s", err, output)
	}
	if _, _, _, err := acquirePathLock(primary, true, false); !errors.Is(err, ErrDatabaseLocked) {
		t.Fatalf("writer while aliased reader remains error = %v", err)
	}
	if err := second.close(); err != nil {
		t.Fatal(err)
	}
	last, _, _, err := acquirePathLock(primary, true, false)
	if err != nil {
		t.Fatalf("writer after last aliased reader closes: %v", err)
	}
	t.Cleanup(func() { _ = last.close() })
	if err := last.close(); err != nil {
		t.Fatal(err)
	}
}

func TestPathLockAliasProcessProbe(t *testing.T) {
	path := os.Getenv("LATTICEDB_LOCK_ALIAS_PROBE")
	if path == "" {
		return
	}
	if db, err := Open(path, OpenOptions{}); err == nil {
		_ = db.Close()
		t.Fatal("separate-process writer acquired an aliased reader lock")
	} else if !errors.Is(err, ErrDatabaseLocked) {
		t.Fatalf("separate-process writer error = %v", err)
	}
}

func linkPathLock(t *testing.T, directory, primary string) string {
	t.Helper()
	alias := filepath.Join(directory, "alias.ltdb")
	if err := os.Mkdir(alias, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(primary, "state.json.lock"), filepath.Join(alias, "state.json.lock")); err != nil {
		t.Skipf("create lock-file alias: %v", err)
	}
	return alias
}
