package latticedb

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"testing"
)

func TestDBMethodsRejectNilAndZeroWithoutPanicking(t *testing.T) {
	cases := []struct {
		name string
		call func(*DB) error
	}{
		{"checkpoint", func(db *DB) error { return db.Checkpoint() }},
		{"serialize", func(db *DB) error { _, err := db.Serialize(); return err }},
		{"snapshot", func(db *DB) error { _, err := db.BeginSnapshot(); return err }},
		{"begin", func(db *DB) error { _, err := db.Begin(true); return err }},
		{"begin read alias", func(db *DB) error { _, err := db.BeginRead(); return err }},
		{"begin write alias", func(db *DB) error { _, err := db.BeginWrite(); return err }},
		{"begin write context", func(db *DB) error { _, err := db.BeginWriteContext(context.Background()); return err }},
		{"view", func(db *DB) error {
			return db.View(func(*Tx) error { t.Fatal("nil/zero View callback invoked"); return nil })
		}},
		{"update", func(db *DB) error {
			return db.Update(func(*Tx) error { t.Fatal("nil/zero Update callback invoked"); return nil })
		}},
		{"update context", func(db *DB) error {
			return db.UpdateContext(context.Background(), func(*Tx) error { t.Fatal("nil/zero UpdateContext callback invoked"); return nil })
		}},
		{"query", func(db *DB) error { _, err := db.Query("MATCH (n) RETURN n", nil); return err }},
		{"query context", func(db *DB) error {
			_, err := db.QueryContext(context.Background(), "MATCH (n) RETURN n", nil, QueryOptions{})
			return err
		}},
		{"vector search", func(db *DB) error { _, err := db.VectorSearch(nil, VectorSearchOptions{}); return err }},
		{"vector search context", func(db *DB) error {
			_, err := db.VectorSearchContext(context.Background(), nil, VectorSearchOptions{})
			return err
		}},
		{"fts search", func(db *DB) error { _, err := db.FTSSearch("x", FTSSearchOptions{}); return err }},
		{"fts search context", func(db *DB) error {
			_, err := db.FTSSearchContext(context.Background(), "x", FTSSearchOptions{})
			return err
		}},
		{"fts fuzzy alias", func(db *DB) error { _, err := db.FTSSearchFuzzy("x", FTSSearchOptions{}); return err }},
		{"read stream", func(db *DB) error { _, err := db.ReadStream("s", 0, 1, 0); return err }},
		{"read stream context", func(db *DB) error {
			_, err := db.ReadStreamContext(context.Background(), "s", 0, StreamReadOptions{Limit: 1})
			return err
		}},
		{"stream offset", func(db *DB) error { _, _, err := db.GetStreamOffset("s", "c"); return err }},
		{"changes", func(db *DB) error { _, err := db.Changes(0, 1, 0); return err }},
		{"changes context", func(db *DB) error {
			_, err := db.ChangesContext(context.Background(), 0, StreamReadOptions{Limit: 1})
			return err
		}},
		{"cache clear", func(db *DB) error { return db.CacheClear() }},
		{"cache stats", func(db *DB) error { _, err := db.CacheStats(); return err }},
		{"node index", func(db *DB) error { return db.CreateNodePropertyIndex("N", "p") }},
		{"node index context", func(db *DB) error { return db.CreateNodePropertyIndexContext(context.Background(), "N", "p") }},
		{"drop node index", func(db *DB) error { return db.DropNodePropertyIndex("N", "p") }},
		{"edge index", func(db *DB) error { return db.CreateEdgePropertyIndex("E", "p") }},
		{"edge index context", func(db *DB) error { return db.CreateEdgePropertyIndexContext(context.Background(), "E", "p") }},
		{"drop edge index", func(db *DB) error { return db.DropEdgePropertyIndex("E", "p") }},
		{"vector stats", func(db *DB) error { _, err := db.VectorIndexStats(); return err }},
		{"vector rebuild", func(db *DB) error { return db.RebuildVectorIndexContext(context.Background()) }},
		{"get nodes", func(db *DB) error { _, err := db.GetNodesByLabel("N"); return err }},
		{"export", func(db *DB) error {
			_, err := db.Export(ExportFormatJSON, filepath.Join(t.TempDir(), "out"))
			return err
		}},
		{"export context", func(db *DB) error {
			_, err := db.ExportContext(context.Background(), ExportFormatJSON, filepath.Join(t.TempDir(), "out"))
			return err
		}},
		{"export file", func(db *DB) error { return db.ExportFile(ExportFormatJSON, filepath.Join(t.TempDir(), "out")) }},
		{"export file context", func(db *DB) error {
			return db.ExportFileContext(context.Background(), ExportFormatJSON, filepath.Join(t.TempDir(), "out"))
		}},
		{"dump", func(db *DB) error { _, err := db.Dump(); return err }},
		{"dump context", func(db *DB) error { _, err := db.DumpContext(context.Background()); return err }},
		{"dump to", func(db *DB) error { return db.DumpTo(io.Discard) }},
		{"dump to context", func(db *DB) error { return db.DumpToContext(context.Background(), io.Discard) }},
		{"export to", func(db *DB) error { return db.ExportTo(ExportFormatJSON, io.Discard) }},
		{"export to context", func(db *DB) error { return db.ExportToContext(context.Background(), ExportFormatJSON, io.Discard) }},
	}
	for _, db := range []*DB{nil, {}, closedTestDB(t)} {
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				defer func() {
					if recovered := recover(); recovered != nil {
						t.Fatalf("panic: %v", recovered)
					}
				}()
				err := tc.call(db)
				if !errors.Is(err, ErrDatabaseClosed) {
					t.Fatalf("error = %v, want ErrDatabaseClosed", err)
				}
				var structured *Error
				if !errors.As(err, &structured) {
					t.Fatalf("error = %T, want structured wrapper", err)
				}
			})
		}
	}
}

func closedTestDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "closed.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestDBCloseAndValueMethodsAreIdempotent(t *testing.T) {
	var nilDB *DB
	if err := nilDB.Close(); err != nil {
		t.Fatalf("nil Close = %v", err)
	}
	zero := &DB{}
	if err := zero.Close(); err != nil || zero.IsOpen() || zero.Path() != "" {
		t.Fatalf("zero DB state: close=%v open=%v path=%q", err, zero.IsOpen(), zero.Path())
	}
	db, err := Open(filepath.Join(t.TempDir(), "closed.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	path := db.Path()
	if err := db.Close(); err != nil {
		t.Fatalf("repeated Close = %v", err)
	}
	if db.IsOpen() || db.Path() != path {
		t.Fatalf("closed DB state: open=%v path=%q want %q", db.IsOpen(), db.Path(), path)
	}
}
