package engine

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestSnapshotBackupRejectsPageSidecars(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "source"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	snapshot, err := db.BeginSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()

	for _, suffix := range []string{".pages", ".pages.layout"} {
		t.Run(suffix, func(t *testing.T) {
			target := filepath.Join(t.TempDir(), "backup")
			want := []byte("preserve existing sidecar")
			if err := os.WriteFile(target+suffix, want, 0600); err != nil {
				t.Fatal(err)
			}
			if err := snapshot.Backup(target); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("Backup error = %v, want ErrInvalidArgument", err)
			}
			if _, err := os.Lstat(target); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("checkpoint was published: %v", err)
			}
			got, err := os.ReadFile(target + suffix)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("sidecar = %q, %v; want preserved %q", got, err, want)
			}
		})
	}
}

func TestSnapshotBackupRejectsValidStalePageSidecar(t *testing.T) {
	source := filepath.Join(t.TempDir(), "source")
	db, err := Open(source, OpenOptions{Create: true, PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var nodeID uint64
	if err := db.Update(func(tx *Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]any{"name": "source"}})
		if err == nil {
			nodeID = node.ID
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	databaseID := db.graph.DatabaseID
	staleCommitID := db.commitID
	pageBytes, err := os.ReadFile(db.files.State)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error { return tx.SetProperty(nodeID, "name", "later") }); err != nil {
		t.Fatal(err)
	}
	if db.graph.DatabaseID != databaseID || db.commitID == staleCommitID {
		t.Fatal("page sidecar and snapshot do not represent different generations of the same database")
	}
	snapshot, err := db.BeginSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()

	target := filepath.Join(t.TempDir(), "backup")
	if err := os.WriteFile(target+".pages", pageBytes, 0600); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Backup(target); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Backup error = %v, want ErrInvalidArgument", err)
	}
	if _, err := os.Lstat(target); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("checkpoint was published: %v", err)
	}
	got, err := os.ReadFile(target + ".pages")
	if err != nil || !bytes.Equal(got, pageBytes) {
		t.Fatalf("stale page sidecar changed: equal=%v, err=%v", bytes.Equal(got, pageBytes), err)
	}
}
