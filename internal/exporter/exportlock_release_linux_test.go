//go:build linux

package exporter

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestReleaseExportSharedParentAcrossUIDs(t *testing.T) {
	if output := os.Getenv("LATTICEDB_RELEASE_EXPORT_OUTPUT"); output != "" {
		uid, err := strconv.Atoi(os.Getenv("LATTICEDB_RELEASE_EXPORT_UID"))
		if err != nil || os.Geteuid() != uid {
			t.Fatalf("helper UID=%d, expected %d: %v", os.Geteuid(), uid, err)
		}
		if err := os.WriteFile(output+".control", []byte("parent is writable"), 0600); err != nil {
			t.Fatalf("control write to shared parent: %v", err)
		}
		syscall.Umask(0o077)
		data, err := ExportGraph(store.NewGraphState(), ExportFormatJSON, output)
		if err != nil {
			t.Fatalf("UID %d export: %v", uid, err)
		}
		info, err := os.Stat(filepath.Join(filepath.Dir(output), exportLockDirectory))
		if err != nil || info.Mode().Perm() != 0o777 || info.Mode()&os.ModeSticky == 0 {
			t.Fatalf("shared lock directory permissions: %v, %v", info, err)
		}
		written, err := os.ReadFile(output)
		if err != nil || !bytes.Equal(data, written) {
			t.Fatalf("published export differs: %v", err)
		}
		return
	}
	if os.Geteuid() != 0 {
		t.Skip("requires root to start helpers with distinct UIDs")
	}
	// Copy the test executable out of Go's private build directory for both UIDs.
	root, err := os.MkdirTemp("", "latticedb-export-uids-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	if err := os.Chmod(root, 0755); err != nil {
		t.Fatal(err)
	}
	shared := filepath.Join(root, "shared")
	if err := os.Mkdir(shared, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(shared, os.ModeSticky|0777); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	source, err := os.Open(executable)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	helper := filepath.Join(root, "exporter.test")
	destination, err := os.OpenFile(helper, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0755)
	if err != nil {
		t.Fatal(err)
	}
	_, copyErr := io.Copy(destination, source)
	closeErr := destination.Close()
	if copyErr != nil || closeErr != nil {
		t.Fatalf("copy test helper: %v, %v", copyErr, closeErr)
	}
	if err := os.Chmod(helper, 0755); err != nil {
		t.Fatal(err)
	}
	for _, uid := range []uint32{65533, 65534} {
		output := filepath.Join(shared, "graph-"+strconv.FormatUint(uint64(uid), 10)+".json")
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		command := exec.CommandContext(ctx, helper, "-test.run=^TestReleaseExportSharedParentAcrossUIDs$", "-test.v")
		command.Dir = shared
		command.Env = append(os.Environ(), "LATTICEDB_RELEASE_EXPORT_OUTPUT="+output, "LATTICEDB_RELEASE_EXPORT_UID="+strconv.FormatUint(uint64(uid), 10))
		command.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: uid, Gid: uid, Groups: []uint32{}}}
		log, err := command.CombinedOutput()
		cancel()
		if err != nil {
			t.Fatalf("UID %d helper: %v\n%s", uid, err, log)
		}
	}
}

func TestReleaseExportRejectsIncompatibleLockPermissions(t *testing.T) {
	for _, mode := range []os.FileMode{0700, 0777} {
		t.Run(mode.String(), func(t *testing.T) {
			parent := t.TempDir()
			directory := filepath.Join(parent, exportLockDirectory)
			if err := os.Mkdir(directory, mode); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(directory, mode); err != nil {
				t.Fatal(err)
			}
			output := filepath.Join(parent, "graph")
			if _, err := ExportGraph(store.NewGraphState(), ExportFormatJSON, output); err == nil {
				t.Fatal("export accepted incompatible existing lock permissions")
			}
			info, err := os.Stat(directory)
			if err != nil || info.Mode().Perm() != mode || info.Mode()&os.ModeSticky != 0 {
				t.Fatalf("existing directory permissions changed: %v, %v", info, err)
			}
			if _, err := os.Stat(output); !os.IsNotExist(err) {
				t.Fatalf("rejected export published output: %v", err)
			}
		})
	}
}
