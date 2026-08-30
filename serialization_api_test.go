package latticedb

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestSerializeDeserializePublicAPI(t *testing.T) {
	if Version() == "" {
		t.Fatal("Version returned an empty string")
	}
	path := t.TempDir()
	db, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	if !db.IsOpen() || db.Path() != path {
		t.Fatalf("open state = open:%v path:%q", db.IsOpen(), db.Path())
	}
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Person"}, Properties: map[string]Value{"name": "Ada"}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	data, err := db.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	data[0] ^= 1
	copyData, err := db.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	if data[0] == copyData[0] {
		t.Fatal("Serialize returned caller-owned bytes")
	}
	readTx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Serialize(); !errors.Is(err, ErrTransactionsActive) {
		t.Fatalf("Serialize with active transaction = %v", err)
	}
	if err := readTx.Rollback(); err != nil {
		t.Fatal(err)
	}
	writeTx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Serialize(); !errors.Is(err, ErrWriteTxActive) {
		t.Fatalf("Serialize with active write transaction = %v", err)
	}
	if err := writeTx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if db.IsOpen() {
		t.Fatal("closed DB remains open")
	}
	if _, err := db.Serialize(); !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("Serialize after Close = %v", err)
	}

	restored, err := Deserialize(copyData, OpenOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	copyData[0] ^= 1
	if !restored.IsOpen() || restored.Path() != "<deserialized>" {
		t.Fatalf("deserialized state = open:%v path:%q", restored.IsOpen(), restored.Path())
	}
	if err := restored.View(func(tx *Tx) error {
		node, err := tx.GetNode(1)
		if err != nil || node == nil || node.Properties["name"] != "Ada" {
			t.Fatalf("restored node = %#v, %v", node, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := restored.Update(func(tx *Tx) error { _, err := tx.CreateNode(CreateNodeOptions{}); return err }); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("read-only deserialized write = %v", err)
	}
	if _, err := Deserialize([]byte("corrupt"), OpenOptions{}); err == nil {
		t.Fatal("Deserialize accepted corrupt data")
	}
}

func TestSerializeBytesOpenAsWritableFile(t *testing.T) {
	source, err := Open(filepath.Join(t.TempDir(), "source"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Person"}, Properties: map[string]Value{"name": "Ada"}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	data, err := source.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(t.TempDir(), "database.ltdb")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	db, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	if duplicate, err := Open(path, OpenOptions{}); !errors.Is(err, ErrDatabaseLocked) {
		if duplicate != nil {
			_ = duplicate.Close()
		}
		t.Fatalf("second Open error = %v, want ErrDatabaseLocked", err)
	}
	alias := path + ".alias"
	if err := os.Symlink(path, alias); err == nil {
		if duplicate, err := Open(alias, OpenOptions{}); !errors.Is(err, ErrDatabaseLocked) {
			if duplicate != nil {
				_ = duplicate.Close()
			}
			t.Fatalf("symlink Open error = %v, want ErrDatabaseLocked", err)
		}
	}
	if db.Path() != path {
		t.Fatalf("Path() = %q, want %q", db.Path(), path)
	}
	if err := db.View(func(tx *Tx) error {
		node, err := tx.GetNode(1)
		if err != nil || node == nil || node.Properties["name"] != "Ada" {
			t.Fatalf("serialized node = %#v, %v", node, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Person"}, Properties: map[string]Value{"name": "Grace"}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() {
		t.Fatalf("serialized path mode = %v, want regular file", info.Mode())
	}

	reopened, err := Open(path, OpenOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.View(func(tx *Tx) error {
		node, err := tx.GetNode(2)
		if err != nil || node == nil || node.Properties["name"] != "Grace" {
			t.Fatalf("persisted node = %#v, %v", node, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDirectoryAndFlatStateAliasesConflict(t *testing.T) {
	directoryPath := t.TempDir()
	directory, err := Open(directoryPath, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := directory.Close(); err != nil {
		t.Fatal(err)
	}
	if alias, err := Open(filepath.Join(directoryPath, "state.json"), OpenOptions{ReadOnly: true}); !errors.Is(err, ErrDatabaseLayoutConflict) {
		if alias != nil {
			_ = alias.Close()
		}
		t.Fatalf("directory state flat alias error = %v, want ErrDatabaseLayoutConflict", err)
	}

	source, err := Open(filepath.Join(t.TempDir(), "source"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	data, err := source.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	flatParent := t.TempDir()
	flatPath := filepath.Join(flatParent, "state.json")
	if err := os.WriteFile(flatPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	flat, err := Open(flatPath, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := flat.Close(); err != nil {
		t.Fatal(err)
	}
	if alias, err := Open(flatParent, OpenOptions{ReadOnly: true}); !errors.Is(err, ErrDatabaseLayoutConflict) {
		if alias != nil {
			_ = alias.Close()
		}
		t.Fatalf("flat state directory alias error = %v, want ErrDatabaseLayoutConflict", err)
	}
}

func TestSerializedFileRejectsReservedStatePaths(t *testing.T) {
	source, err := Open(filepath.Join(t.TempDir(), "source"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	data, err := source.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"database.lock", "database.layout", "database-wal", "database-wal.base", "database-ids", "wal.log", "wal.base", "ids.json", ".state-123.tmp", ".latticedb-layout-123"} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), name)
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			if db, err := Open(path, OpenOptions{}); !errors.Is(err, ErrDatabaseLayoutConflict) {
				if db != nil {
					_ = db.Close()
				}
				t.Fatalf("Open error = %v, want ErrDatabaseLayoutConflict", err)
			}
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(after, data) {
				t.Fatal("reserved state path was modified")
			}
		})
	}
}

func TestDatabaseLayoutsSurviveRelocation(t *testing.T) {
	base := t.TempDir()
	directoryPath := filepath.Join(base, "directory-original")
	directory, err := Open(directoryPath, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := directory.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	data, err := directory.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	if err := directory.Close(); err != nil {
		t.Fatal(err)
	}
	directoryMarker, err := os.ReadFile(filepath.Join(directoryPath, "state.json.layout"))
	if err != nil {
		t.Fatal(err)
	}
	movedDirectory := filepath.Join(base, "directory-moved")
	if err := os.Rename(directoryPath, movedDirectory); err != nil {
		t.Fatal(err)
	}
	reopenedDirectory, err := Open(movedDirectory, OpenOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := reopenedDirectory.Close(); err != nil {
		t.Fatal(err)
	}
	afterDirectoryMarker, err := os.ReadFile(filepath.Join(movedDirectory, "state.json.layout"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(afterDirectoryMarker, directoryMarker) {
		t.Fatal("read-only directory relocation changed the layout marker")
	}

	flatParent := filepath.Join(base, "flat-original")
	if err := os.Mkdir(flatParent, 0o700); err != nil {
		t.Fatal(err)
	}
	flatPath := filepath.Join(flatParent, "database.ltdb")
	if err := os.WriteFile(flatPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	flat, err := Open(flatPath, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := flat.Close(); err != nil {
		t.Fatal(err)
	}
	flatMarker, err := os.ReadFile(flatPath + ".layout")
	if err != nil {
		t.Fatal(err)
	}
	movedFlatParent := filepath.Join(base, "flat-moved")
	if err := os.Rename(flatParent, movedFlatParent); err != nil {
		t.Fatal(err)
	}
	movedFlatPath := filepath.Join(movedFlatParent, "database.ltdb")
	reopenedFlat, err := Open(movedFlatPath, OpenOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := reopenedFlat.View(func(tx *Tx) error {
		_, err := tx.GetNode(1)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := reopenedFlat.Close(); err != nil {
		t.Fatal(err)
	}
	afterFlatMarker, err := os.ReadFile(movedFlatPath + ".layout")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(afterFlatMarker, flatMarker) {
		t.Fatal("read-only flat relocation changed the layout marker")
	}
}

func TestSerializedFileRejectsSidecarsFromReplacedDatabase(t *testing.T) {
	first, err := Open(filepath.Join(t.TempDir(), "first"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	firstData, err := first.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "replace.ltdb")
	if err := os.WriteFile(path, firstData, 0o600); err != nil {
		t.Fatal(err)
	}
	opened, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := opened.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := Open(filepath.Join(t.TempDir(), "second"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	secondData, err := second.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, secondData, 0o600); err != nil {
		t.Fatal(err)
	}
	if db, err := Open(path, OpenOptions{}); err == nil {
		_ = db.Close()
		t.Fatal("Open accepted WAL sidecars from a replaced database")
	}
}

func TestSerializedFileRejectsHardLinkAlias(t *testing.T) {
	source, err := Open(filepath.Join(t.TempDir(), "source"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	data, err := source.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	path := filepath.Join(directory, "database.ltdb")
	alias := filepath.Join(directory, "alias.ltdb")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(path, alias); err != nil {
		t.Skipf("hard links unavailable: %v", err)
	}
	if db, err := Open(path, OpenOptions{}); err == nil {
		_ = db.Close()
		t.Fatal("Open accepted a multiply linked database file")
	}
}

func TestSerializedFileReadOnlyDoesNotCreateRecoverySidecars(t *testing.T) {
	source, err := Open(filepath.Join(t.TempDir(), "source"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	data, err := source.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "readonly.ltdb")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	db, err := Open(path, OpenOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, data) {
		t.Fatal("read-only Open changed the database file")
	}
	for _, sidecar := range []string{path + "-wal", path + "-wal.base", path + "-ids"} {
		if _, err := os.Stat(sidecar); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("read-only Open sidecar %q: %v", sidecar, err)
		}
	}
}

func TestSerializedFileRecoversCommittedWALAfterProcessExit(t *testing.T) {
	if path := os.Getenv("LATTICEDB_FLAT_CRASH_PATH"); path != "" {
		db, err := Open(path, OpenOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Update(func(tx *Tx) error {
			_, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]Value{"name": "committed"}})
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return
	}

	source, err := Open(filepath.Join(t.TempDir(), "source"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	data, err := source.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "crash.ltdb")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	command := exec.Command(os.Args[0], "-test.run=^TestSerializedFileRecoversCommittedWALAfterProcessExit$")
	command.Env = append(os.Environ(), "LATTICEDB_FLAT_CRASH_PATH="+path)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("crash helper: %v\n%s", err, output)
	}

	readOnlyRecovered, err := Open(path, OpenOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := readOnlyRecovered.View(func(tx *Tx) error {
		node, err := tx.GetNode(1)
		if err != nil || node == nil || node.Properties["name"] != "committed" {
			t.Fatalf("recovered node = %#v, %v", node, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := readOnlyRecovered.Close(); err != nil {
		t.Fatal(err)
	}

	recovered, err := Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path, OpenOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.View(func(tx *Tx) error {
		_, err := tx.GetNode(1)
		return err
	}); err != nil {
		t.Fatal(err)
	}
}
