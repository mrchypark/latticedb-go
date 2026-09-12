package exporter

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func createTestGeneration(t *testing.T, root, name string) string {
	t.Helper()
	genPath := filepath.Join(root, name)
	if err := os.MkdirAll(genPath, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, file := range []string{"nodes.csv", "edges.csv"} {
		if err := os.WriteFile(filepath.Join(genPath, file), []byte("id\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return genPath
}

func TestCleanupCSVLeasesConcurrentRemoval(t *testing.T) {
	for _, stage := range []string{"stat", "open", "remove"} {
		t.Run(stage, func(t *testing.T) {
			genPath := createTestGeneration(t, t.TempDir(), "generation-0001")
			leasePath := filepath.Join(genPath, ".lease-race")
			if err := os.WriteFile(leasePath, []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
			called := false
			remove := func(path string) {
				called = true
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			}
			hooks := &csvLeaseCleanupHooks{}
			switch stage {
			case "stat":
				hooks.beforeLstat = remove
			case "open":
				hooks.beforeOpen = remove
			case "remove":
				hooks.beforeRemove = remove
			}
			active, err := cleanupCSVLeasesWithHooks(genPath, hooks)
			if !called || active || err != nil {
				t.Fatalf("called=%v active=%v err=%v", called, active, err)
			}
			if _, err := os.Stat(leasePath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("removed lease: %v", err)
			}
		})
	}
}

func TestCleanupCSVLeasesPreservesActiveLease(t *testing.T) {
	root := t.TempDir()
	genPath := createTestGeneration(t, root, "generation-0004")

	file, err := os.CreateTemp(genPath, ".lease-active-")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}

	lease := &CSVGenerationLease{
		Generation: "generation-0004",
		file:       file,
		info:       info,
	}
	csvGenerationLeases.Lock()
	csvGenerationLeases.entries[lease] = struct{}{}
	csvGenerationLeases.Unlock()
	defer func() {
		csvGenerationLeases.Lock()
		delete(csvGenerationLeases.entries, lease)
		csvGenerationLeases.Unlock()
	}()

	active, err := cleanupCSVLeases(genPath)
	if err != nil {
		t.Fatalf("cleanupCSVLeases returned error: %v", err)
	}
	if !active {
		t.Fatal("cleanupCSVLeases did not detect active in-process lease")
	}
	if _, err := os.Stat(file.Name()); err != nil {
		t.Fatalf("active lease file was removed: %v", err)
	}
}

func TestCleanupCSVLeasesPropagatesReadDirError(t *testing.T) {
	_, err := cleanupCSVLeases(filepath.Join(t.TempDir(), "does-not-exist"))
	if err == nil {
		t.Fatal("cleanupCSVLeases did not return error for missing generation path")
	}
}

func TestCleanupCSVLeasesPropagatesLstatPermissionError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root bypasses permission checks")
	}
	if runtime.GOOS == "windows" || runtime.GOOS == "plan9" {
		t.Skip("permission semantics differ on " + runtime.GOOS)
	}
	root := t.TempDir()
	genPath := createTestGeneration(t, root, "generation-perm")

	leasePath := filepath.Join(genPath, ".lease-perm")
	if err := os.WriteFile(leasePath, []byte("p"), 0o600); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(genPath, 0o700)
	_, err := cleanupCSVLeasesWithHooks(genPath, &csvLeaseCleanupHooks{
		beforeLstat: func(string) {
			if err := os.Chmod(genPath, 0o000); err != nil {
				t.Fatal(err)
			}
		},
	})
	if !errors.Is(err, ErrInvalidCSVGeneration) {
		t.Fatal("cleanupCSVLeases did not return error for permission-denied Lstat")
	}
}

func TestCleanupCSVLeasesCleansUpStaleLease(t *testing.T) {
	root := t.TempDir()
	genPath := createTestGeneration(t, root, "generation-0005")

	leasePath := filepath.Join(genPath, ".lease-stale")
	if err := os.WriteFile(leasePath, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}

	active, err := cleanupCSVLeases(genPath)
	if err != nil {
		t.Fatalf("cleanupCSVLeases returned error: %v", err)
	}
	if active {
		t.Fatal("cleanupCSVLeases reported active for stale lease")
	}
	if _, err := os.Stat(leasePath); !os.IsNotExist(err) {
		t.Fatalf("stale lease file was not removed: %v", err)
	}
}
