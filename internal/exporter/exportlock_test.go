package exporter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestCSVConcurrentSubprocessPublicationAndOwnerDeath(t *testing.T) {
	if os.Getenv("LATTICEDB_EXPORT_HELPER") != "" {
		runExportHelper(t)
		return
	}
	output := filepath.Join(t.TempDir(), "graph.csv")
	commands := make([]*exec.Cmd, 3)
	for index := range commands {
		commands[index] = exportHelperCommand(t, "export", output, "")
		if err := commands[index].Start(); err != nil {
			t.Fatal(err)
		}
	}
	for _, command := range commands {
		if err := command.Wait(); err != nil {
			t.Fatalf("export helper: %v", err)
		}
	}
	assertCSVManifestPathsExist(t, output)

	ready := filepath.Join(t.TempDir(), "ready")
	holder := exportHelperCommand(t, "hold", output, ready)
	if err := holder.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = holder.Process.Kill()
			t.Fatal("lock holder did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	manifestBefore, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	graph := store.NewGraphState()
	if _, err := ExportGraphContext(ctx, graph, ExportFormatCSV, output); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("context waiter error = %v", err)
	}
	manifestAfter, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if string(manifestAfter) != string(manifestBefore) {
		t.Fatal("cancelled waiter changed the published manifest")
	}
	waiter := exportHelperCommand(t, "export", output, "")
	if err := waiter.Start(); err != nil {
		t.Fatal(err)
	}
	waited := make(chan error, 1)
	go func() { waited <- waiter.Wait() }()
	select {
	case err := <-waited:
		t.Fatalf("waiter did not block on the OS lock: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := holder.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = holder.Wait()
	select {
	case err := <-waited:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiter did not recover after lock owner death")
	}
	assertCSVManifestPathsExist(t, output)
}

func TestExportPathLockRegistryReclaimsEntries(t *testing.T) {
	base := t.TempDir()
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	for index := 0; index < 10_000; index++ {
		if _, err := acquireExportLockContext(cancelled, filepath.Join(base, fmt.Sprintf("cancelled-%d", index))); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}
	path := filepath.Join(base, "shared")
	unlock, err := acquireExportLock(path)
	if err != nil {
		t.Fatal(err)
	}
	waiter, waiterCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer waiterCancel()
	if _, err := acquireExportLockContext(waiter, path); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiter error = %v", err)
	}
	exportPathLocks.Lock()
	entries, refs := len(exportPathLocks.entries), exportPathLocks.entries[path].refs
	exportPathLocks.Unlock()
	if entries != 1 || refs != 1 {
		t.Fatalf("registry while owned: entries=%d refs=%d", entries, refs)
	}
	unlock()
	exportPathLocks.Lock()
	entries = len(exportPathLocks.entries)
	exportPathLocks.Unlock()
	if entries != 0 {
		t.Fatalf("registry retained %d entries", entries)
	}
}

func TestExportContextCancelsBodyBeforeManifest(t *testing.T) {
	output := filepath.Join(t.TempDir(), "cancel-body.csv")
	base := store.NewGraphState()
	base.Nodes.Set(1, &store.NodeRecord{ID: 1, Properties: map[string]any{"version": int64(1)}})
	if _, err := ExportGraph(base, ExportFormatCSV, output); err != nil {
		t.Fatal(err)
	}
	manifestBefore, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	large := store.NewGraphState()
	for id := uint64(1); id <= 2_000; id++ {
		large.Nodes.Set(id, &store.NodeRecord{ID: id, Properties: map[string]any{"value": int64(id)}})
	}
	ctx := &cancelExportAfterChecks{limit: 20}
	if _, err := ExportGraphContext(ctx, large, ExportFormatCSV, output); !errors.Is(err, context.Canceled) {
		t.Fatalf("body cancellation error = %v", err)
	}
	manifestAfter, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if string(manifestAfter) != string(manifestBefore) {
		t.Fatal("canceled body changed the manifest")
	}
	building, err := filepath.Glob(filepath.Join(strings.TrimSuffix(output, filepath.Ext(output))+"_csv_generations", ".building-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(building) != 0 {
		t.Fatalf("canceled body retained building directories: %v", building)
	}
	if _, err := DumpGraphContext(&cancelExportAfterChecks{limit: 20}, large); !errors.Is(err, context.Canceled) {
		t.Fatalf("dump cancellation error = %v", err)
	}
}

type cancelExportAfterChecks struct {
	checks atomic.Int32
	limit  int32
}

func (*cancelExportAfterChecks) Deadline() (time.Time, bool) { return time.Time{}, false }
func (*cancelExportAfterChecks) Done() <-chan struct{}       { return nil }
func (ctx *cancelExportAfterChecks) Err() error {
	if ctx.checks.Add(1) >= ctx.limit {
		return context.Canceled
	}
	return nil
}
func (*cancelExportAfterChecks) Value(any) any { return nil }

func exportHelperCommand(t *testing.T, mode, output, ready string) *exec.Cmd {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestCSVConcurrentSubprocessPublicationAndOwnerDeath$")
	command.Env = append(os.Environ(), "LATTICEDB_EXPORT_HELPER="+mode, "LATTICEDB_EXPORT_OUTPUT="+output, "LATTICEDB_EXPORT_READY="+ready)
	return command
}

func runExportHelper(t *testing.T) {
	output := os.Getenv("LATTICEDB_EXPORT_OUTPUT")
	if os.Getenv("LATTICEDB_EXPORT_HELPER") == "hold" {
		unlock, err := acquireExportLock(output)
		if err != nil {
			t.Fatal(err)
		}
		defer unlock()
		if err := os.WriteFile(os.Getenv("LATTICEDB_EXPORT_READY"), nil, 0o600); err != nil {
			t.Fatal(err)
		}
		for {
			time.Sleep(time.Hour)
		}
	}
	graph := store.NewGraphState()
	graph.Nodes.Set(1, &store.NodeRecord{ID: 1, Properties: map[string]any{}})
	if _, err := ExportGraph(graph, ExportFormatCSV, output); err != nil {
		t.Fatal(err)
	}
}

func assertCSVManifestPathsExist(t *testing.T, output string) {
	t.Helper()
	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	var manifest csvManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{manifest.Nodes, manifest.Edges} {
		if _, err := os.Stat(filepath.Join(filepath.Dir(output), path)); err != nil {
			t.Fatal(err)
		}
	}
}
