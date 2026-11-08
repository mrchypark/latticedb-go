package engine

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func createConstraintRecoveryFixture(t *testing.T, db *DB) {
	t.Helper()
	if err := db.Update(func(tx *Tx) error {
		if _, err := tx.CreateNode(CreateNodeOptions{
			Labels:     []string{"Item"},
			Properties: map[string]any{"active": true, "payload": strings.Repeat("x", 20<<20)},
		}); err != nil {
			return err
		}
		return tx.CreateGraphConstraint(store.GraphConstraint{
			Name: "active-required", Label: "Item", Property: "active", Kind: store.ConstraintKindRequired,
		})
	}); err != nil {
		t.Fatal(err)
	}
}

func TestGraphConstraintRecoveryBudgetPageReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	opts := OpenOptions{
		Create: true, PageStorage: true,
	}
	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	createConstraintRecoveryFixture(t, db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	opts.Create = false
	db, err = Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	opts.RecoveryMaxWork = 16 << 20
	db, err = Open(path, opts)
	if db != nil {
		_ = db.Close()
	}
	if !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("low RecoveryMaxWork error = %v", err)
	}
}

func TestGraphConstraintRecoveryBudgetStagedMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	db, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	createConstraintRecoveryFixture(t, db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, OpenOptions{PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestGraphConstraintRecoveryBudgetBackupRestore(t *testing.T) {
	root := t.TempDir()
	source, archive, destination := filepath.Join(root, "source"), filepath.Join(root, "archive"), filepath.Join(root, "restored")
	db, err := Open(source, OpenOptions{Create: true, PageStorage: true, BackupDirectory: archive})
	if err != nil {
		t.Fatal(err)
	}
	createConstraintRecoveryFixture(t, db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := RestoreBackup(context.Background(), archive, destination, BackupRestoreOptions{}); err != nil {
		t.Fatal(err)
	}
	db, err = Open(destination, OpenOptions{PageStorage: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
