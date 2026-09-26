package engine

import (
	"bytes"
	"context"
	"errors"
	"github.com/mrchypark/latticedb-go/internal/store"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

func TestPageDBCommitReopenAndRollback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	db, err := Open(path, OpenOptions{Create: true, PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	var first, second uint64
	err = db.Update(func(tx *Tx) error {
		a, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Person"}, Properties: map[string]any{"name": "a"}})
		if err != nil {
			return err
		}
		first = a.ID
		b, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Person"}})
		if err != nil {
			return err
		}
		second = b.ID
		_, err = tx.CreateEdge(a.ID, b.ID, "KNOWS", CreateEdgeOptions{})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if db.graph.Nodes.Len() != 0 || db.graph.Edges.Len() != 0 {
		t.Fatal("committed records remained resident")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, OpenOptions{PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	sentinel := errors.New("rollback")
	err = db.Update(func(tx *Tx) error {
		if err := tx.SetProperty(first, "name", "discard"); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	err = db.View(func(tx *Tx) error {
		value, ok, err := tx.GetProperty(first, "name")
		if err != nil {
			return err
		}
		if !ok || value != "a" {
			t.Fatalf("property %v %v", value, ok)
		}
		node, err := tx.GetNode(second)
		if err != nil {
			return err
		}
		if node == nil {
			t.Fatal("second node missing")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Update(func(tx *Tx) error { return tx.DeleteNode(first) }); err != nil {
		t.Fatal(err)
	}
	nodes, err := db.graph.NodeCount()
	if err != nil || nodes != 1 {
		t.Fatalf("nodes %d %v", nodes, err)
	}
	edges, err := db.graph.EdgeCount()
	if err != nil || edges != 0 {
		t.Fatalf("edges %d %v", edges, err)
	}
}

// TestPageDBMemoryCheck is run by the Linux cgroup check with an explicit
// memory/swap ceiling. Normal unit runs skip the multi-gigabyte disk workload.
func TestPageDBMemoryCheck(t *testing.T) {
	if os.Getenv("LATTICEDB_PAGING_CHECK") != "1" {
		t.Skip("opt-in constrained-memory paging check")
	}
	path := filepath.Join(t.TempDir(), "large")
	db, err := Open(path, OpenOptions{Create: true, PageStorage: true, ChangefeedMaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	const small = 100000
	for first := 0; first < small; first += 256 {
		err = db.Update(func(tx *Tx) error {
			for n := first; n < min(first+256, small); n++ {
				node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Small"}, Properties: map[string]any{"n": int64(n), "payload": "a small independently indexed record"}})
				if err != nil {
					return err
				}
				if n != 0 {
					if _, err := tx.CreateEdge(node.ID-1, node.ID, "NEXT", CreateEdgeOptions{}); err != nil {
						return err
					}
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("small batch %d: %v", first, err)
		}
	}
	payload := bytes.Repeat([]byte("paged-data-"), 100000) // 1.1 MB per independent record.
	const large = 1024
	for n := 0; n < large; n++ {
		err = db.Update(func(tx *Tx) error {
			_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Large"}, Properties: map[string]any{"payload": payload, "n": int64(n)}})
			return err
		})
		if err != nil {
			t.Fatalf("large record %d: %v", n, err)
		}
	}
	if db.graph.Nodes.Len() != 0 || db.graph.Edges.Len() != 0 {
		t.Fatal("resident record table")
	}
	t.Log("large DB writes complete; building persistent property index")
	if err := db.CreateNodePropertyIndex("Small", "n"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(store.DirectoryDatabaseFiles(path).State)
	if err != nil {
		// The file naming is supplied by the shared layout resolver.
		info, err = os.Stat(store.DirectoryDatabaseFiles(path).State + ".pages")
	}
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() < 1<<30 {
		t.Fatalf("DB too small for paging proof: %d", info.Size())
	}
	t.Logf("disk database bytes=%d", info.Size())
	archive := filepath.Join(t.TempDir(), "archive")
	db, err = Open(path, OpenOptions{PageStorage: true, ChangefeedMaxBytes: 1 << 20, BackupDirectory: archive})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *Tx) error {
		ids, err := tx.FindNodesByLabelProperty("Small", "n", int64(99999), 1)
		if err != nil {
			return err
		}
		if len(ids) != 1 || ids[0] != 100000 {
			t.Fatalf("persisted index %v", ids)
		}
		for n := 1; n < small; n += 997 {
			node, err := tx.GetNode(uint64(n))
			if err != nil {
				return err
			}
			if node == nil {
				t.Fatalf("missing %d", n)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		if err := tx.SetProperty(50000, "n", int64(-1)); err != nil {
			return err
		}
		return tx.DeleteNode(50001)
	}); err != nil {
		t.Fatal(err)
	}
	nodes, err := db.graph.NodeCount()
	if err != nil || nodes != small+large-1 {
		t.Fatalf("count %d %v", nodes, err)
	}
	snapshot, err := db.BeginSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	backup := filepath.Join(t.TempDir(), "large-backup.ltdb")
	if err := snapshot.Backup(backup); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	t.Logf("heap after close=%d", memory.HeapAlloc)
	t.Log("restoring streamed checkpoint larger than RAM")
	restored, err := Open(backup, OpenOptions{PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	count, err := restored.graph.NodeCount()
	if err != nil || count != small+large-1 {
		t.Fatalf("restored count %d %v", count, err)
	}
	if err := restored.Close(); err != nil {
		t.Fatal(err)
	}
	t.Log("restoring incremental archive larger than RAM")
	destination := filepath.Join(t.TempDir(), "archive-restored.pages")
	if _, err := RestoreBackup(context.Background(), archive, destination, BackupRestoreOptions{}); err != nil {
		t.Fatal(err)
	}
	restored, err = Open(destination, OpenOptions{PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.View(func(tx *Tx) error {
		ids, err := tx.FindNodesByLabelProperty("Small", "n", int64(-1), 1)
		if err != nil {
			return err
		}
		if len(ids) != 1 || ids[0] != 50000 {
			t.Fatalf("restored delta index %v", ids)
		}
		deleted, err := tx.GetNode(50001)
		if err != nil || deleted != nil {
			t.Fatalf("restored deleted node: %v, %v", deleted, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := restored.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPageDBRecoversAfterProcessExit(t *testing.T) {
	if path := os.Getenv("LATTICEDB_PAGE_CRASH_CHILD"); path != "" {
		db, err := Open(path, OpenOptions{Create: true, PageStorage: true})
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Update(func(tx *Tx) error {
			_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Committed"}})
			return err
		}); err != nil {
			t.Fatal(err)
		}
		tx, err := db.Begin(false)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Uncommitted"}}); err != nil {
			t.Fatal(err)
		}
		os.Exit(0) // Deliberately bypass transaction/database cleanup.
	}
	path := filepath.Join(t.TempDir(), "crash")
	command := exec.Command(os.Args[0], "-test.run=^TestPageDBRecoversAfterProcessExit$")
	command.Env = append(os.Environ(), "LATTICEDB_PAGE_CRASH_CHILD="+path)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("child: %v\n%s", err, output)
	}
	db, err := Open(path, OpenOptions{PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	count, err := db.graph.NodeCount()
	if err != nil || count != 1 {
		t.Fatalf("recovered count %d %v", count, err)
	}
	if err := db.View(func(tx *Tx) error {
		node, err := tx.GetNode(1)
		if err != nil {
			return err
		}
		if node == nil {
			t.Fatal("durable commit missing")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
