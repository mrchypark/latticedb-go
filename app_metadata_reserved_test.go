package latticedb

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"

	"github.com/mrchypark/latticedb-go/internal/store"
	"testing"
)

func TestAppMetadataRejectsFTSCatalogMutation(t *testing.T) {
	db, err := Open(":memory:", OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tx, err := db.BeginWrite()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	key := []byte("latticedb:fts-index:documents")
	if err := tx.PutAppMetadata(key, []byte("invalid")); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("put: %v", err)
	}
	if err := tx.DeleteAppMetadata(key); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("delete: %v", err)
	}
	if err := tx.PutAppMetadata([]byte("application:key"), []byte("allowed")); err != nil {
		t.Fatal(err)
	}
}

func TestFTSSchemaRejectsUnknownVersionsAndLegacyKeyCollisions(t *testing.T) {
	for _, value := range []string{`{"version":2,"definition":{"name":"future","kind":"node","scope":"Doc","property":"body"}}`, `existing application value`} {
		graph := store.NewGraphState()
		if err := store.EnsureDatabaseID(graph); err != nil {
			t.Fatal(err)
		}
		graph.AppMetadata.Set("latticedb:fts-index:future", []byte(value))
		data, err := store.SerializeGraphState(graph, 1, 1, 0)
		if err != nil {
			t.Fatal(err)
		}
		if db, err := Deserialize(data, OpenOptions{}); err == nil {
			db.Close()
			t.Fatal("memory accepted an unsupported catalog")
		}
		path := filepath.Join(t.TempDir(), "legacy")
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		if db, err := Open(path, OpenOptions{}); err == nil {
			db.Close()
			t.Fatal("disk accepted an unsupported catalog")
		}
		original, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(original, data) {
			t.Fatal("rejected catalog changed original snapshot")
		}
	}
}
