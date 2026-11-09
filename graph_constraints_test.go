package latticedb

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
)

func TestGraphConstraintsFinalState(t *testing.T) {
	for _, backend := range []string{"memory", "page"} {
		t.Run(backend, func(t *testing.T) {
			path := ":memory:"
			if backend == "page" {
				path = filepath.Join(t.TempDir(), "db")
			}
			db, err := Open(path, OpenOptions{Create: true, EnableVector: true, VectorDimensions: 2})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var first, second uint64
			if err := db.Update(func(tx *Tx) error {
				for i, email := range []string{"first", "second"} {
					n, err := tx.CreateNode(CreateNodeOptions{
						Labels: []string{"User", "Member"}, Properties: map[string]Value{"email": email},
					})
					if err != nil {
						return err
					}
					if i == 0 {
						first = n.ID
					} else {
						second = n.ID
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			early, err := db.BeginRead()
			if err != nil {
				t.Fatal(err)
			}
			defer early.Rollback()
			defs := []GraphConstraint{
				{Name: "email-required", Label: "User", Property: "email", Kind: ConstraintRequired},
				{Name: "email-type", Label: "Member", Property: "email", Kind: ConstraintType, Type: ConstraintString},
				{Name: "email-unique", Label: "User", Property: "email", Kind: ConstraintUnique},
			}
			if err := db.Update(func(tx *Tx) error {
				for _, def := range defs {
					if err := tx.CreateGraphConstraint(def); err != nil {
						return err
					}
				}
				if err := tx.CreateGraphConstraint(defs[0]); !errors.Is(err, ErrAlreadyExists) {
					t.Fatalf("duplicate name: %v", err)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if visible, err := early.GraphConstraints(); err != nil || len(visible) != 0 {
				t.Fatalf("old reader saw new rules: %v, %v", visible, err)
			}
			if err := db.Update(func(tx *Tx) error {
				if err := tx.SetProperty(first, "email", "second"); err != nil {
					return err
				}
				return tx.SetProperty(second, "email", "first")
			}); err != nil {
				t.Fatalf("swap: %v", err)
			}
			before, err := db.Changes(0, 100, 0)
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Batch(func(tx *Tx) error {
				if err := tx.DropGraphConstraint(defs[1].Name); err != nil {
					return err
				}
				return tx.SetProperty(second, "email", "second")
			}); !errors.Is(err, ErrConstraintViolation) {
				t.Fatalf("batch duplicate: %v", err)
			}
			if err := db.Update(func(tx *Tx) error {
				_, err := tx.BatchInsertVectors("User", [][]float32{{1, 0}})
				return err
			}); !errors.Is(err, ErrConstraintViolation) {
				t.Fatalf("vector bulk insert without required property: %v", err)
			}
			result, err := db.Query("MATCH (n:User) WHERE n.email = 'second' SET n.email = null RETURN n", nil)
			var violation *ConstraintViolation
			if !errors.As(err, &violation) || !errors.Is(err, ErrConstraintViolation) || len(result.Rows) != 0 {
				t.Fatalf("query violation: rows=%v, err=%v", result.Rows, err)
			}
			if violation.Name != defs[0].Name || violation.NodeID != first {
				t.Fatalf("wrong failing rule or node: %+v", violation)
			}
			if err := db.View(func(tx *Tx) error {
				visible, err := tx.GraphConstraints()
				if err != nil || !reflect.DeepEqual(visible, defs) {
					t.Fatalf("failed batch changed definitions: %v, %v", visible, err)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			after, err := db.Changes(0, 100, 0)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("failed transaction changed changefeed: %v", err)
			}
			if _, err := db.Query("CREATE (n:Other {email: 'second'})", nil); err != nil {
				t.Fatalf("other label: %v", err)
			}
			if _, err := db.Query("MATCH (n:Other) SET n:User RETURN n", nil); !errors.Is(err, ErrConstraintViolation) {
				t.Fatalf("label mutation bypassed unique: %v", err)
			}
			if err := db.Update(func(tx *Tx) error {
				_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Member"}, Properties: map[string]Value{"email": int64(1)}})
				return err
			}); !errors.Is(err, ErrConstraintViolation) {
				t.Fatalf("type mismatch: %v", err)
			}
			if err := db.Update(func(tx *Tx) error {
				for _, key := range []string{"latticedb:graph-constraints", "latticedb:graph-constraint:email-required"} {
					if !errors.Is(tx.PutAppMetadata([]byte(key), []byte("bypass")), ErrInvalidArgument) ||
						!errors.Is(tx.DeleteAppMetadata([]byte(key)), ErrInvalidArgument) {
						t.Fatalf("reserved key writable: %s", key)
					}
				}
				if err := tx.DropGraphConstraint(defs[0].Name); err != nil {
					return err
				}
				return tx.CreateGraphConstraint(defs[0])
			}); err != nil {
				t.Fatalf("drop and recreate: %v", err)
			}
		})
	}
}

func TestGraphConstraintsAdmissionBudgetAndRoundTrip(t *testing.T) {
	root := t.TempDir()
	path, archive := filepath.Join(root, "db"), filepath.Join(root, "archive")
	db, err := Open(path, OpenOptions{Create: true, BackupDirectory: archive})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	unique := GraphConstraint{Name: "item-code", Label: "Item", Property: "code", Kind: ConstraintUnique}
	required := GraphConstraint{Name: "item-name", Label: "Item", Property: "name", Kind: ConstraintRequired}
	if err := db.Update(func(tx *Tx) error {
		for i := range 80 {
			_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]Value{"code": fmt.Sprint(i)}})
			if err != nil {
				return err
			}
		}
		return tx.CreateGraphConstraint(unique)
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error { return tx.CreateGraphConstraint(required) }); !errors.Is(err, ErrConstraintViolation) {
		t.Fatalf("admitting required rule over invalid data: %v", err)
	}
	for _, opts := range []QueryOptions{{MaxWork: 50}, {MaxBytes: 1024}} {
		rows, err := db.QueryContext(t.Context(), "CREATE (n:Item {code: 'new'}) RETURN n", nil, opts)
		if !errors.Is(err, ErrResourceLimit) || len(rows.Rows) != 0 {
			t.Fatalf("validation limit %+v: %+v, %v", opts, rows, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	err = db.UpdateContext(ctx, func(tx *Tx) error {
		if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]Value{"code": "new"}}); err != nil {
			return err
		}
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled write: %v", err)
	}
	verify := func(target *DB) {
		t.Helper()
		if _, err := target.Query("CREATE (n:Item {code: 'new'})", nil); err != nil {
			t.Fatalf("failed query published data: %v", err)
		}
		if _, err := target.Query("CREATE (n:Item {code: '0'})", nil); !errors.Is(err, ErrConstraintViolation) {
			t.Fatalf("round trip lost constraint: %v", err)
		}
	}
	data, err := db.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	memory, err := Deserialize(data, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer memory.Close()
	verify(memory)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	restoredPath := filepath.Join(root, "restored")
	if _, err := RestoreBackup(t.Context(), archive, restoredPath, BackupRestoreOptions{}); err != nil {
		t.Fatal(err)
	}
	for _, candidate := range []string{path, restoredPath} {
		reopened, err := Open(candidate, OpenOptions{})
		if err != nil {
			t.Fatal(err)
		}
		verify(reopened)
		if err := reopened.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
