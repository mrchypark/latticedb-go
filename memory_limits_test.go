package latticedb

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func TestMemoryInitialSnapshotLimit(t *testing.T) {
	for _, path := range []string{":memory:", filepath.Join(t.TempDir(), "disk")} {
		db, err := Open(path, OpenOptions{Create: true, MaxDatabaseSnapshotBytes: 1})
		if db != nil {
			_ = db.Close()
		}
		if !errors.Is(err, ErrResourceLimit) {
			t.Errorf("Open(%q) = %v, want resource limit", path, err)
		}
	}
	for _, populated := range []bool{false, true} {
		name := "empty"
		if populated {
			name = "populated"
		}
		t.Run(name, func(t *testing.T) {
			source, err := Open(":memory:", OpenOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer source.Close()
			if populated {
				if err := source.Update(func(tx *Tx) error {
					_, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]Value{"body": strings.Repeat("x", 32<<10)}})
					return err
				}); err != nil {
					t.Fatal(err)
				}
			}
			data, err := source.Serialize()
			if err != nil {
				t.Fatal(err)
			}
			for _, readOnly := range []bool{false, true} {
				copyDB, err := Deserialize(data, OpenOptions{ReadOnly: readOnly, MaxDatabaseSnapshotBytes: uint64(len(data)) * 2})
				if copyDB != nil {
					_ = copyDB.Close()
				}
				if !errors.Is(err, ErrResourceLimit) {
					t.Errorf("Deserialize(readOnly=%v, limit=%d)=%v, want resource limit", readOnly, len(data)*2, err)
				}
			}
			copyDB, err := Deserialize(data, OpenOptions{MaxDatabaseSnapshotBytes: 1 << 20})
			if err != nil {
				t.Fatal(err)
			}
			defer copyDB.Close()
			if err := copyDB.Update(func(*Tx) error { return nil }); err != nil {
				t.Fatalf("no-op commit after admitted deserialize: %v", err)
			}
		})
	}
}
