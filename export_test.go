package latticedb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

type shortWriter struct{}

func TestExportNilContext(t *testing.T) {
	db := openExportLimitDB(t)
	want, err := db.Dump()
	if err != nil {
		t.Fatal(err)
	}
	for _, withOptions := range []bool{false, true} {
		var got []byte
		if withOptions {
			got, err = db.DumpContextWithOptions(nil, ExportOptions{})
		} else {
			got, err = db.DumpContext(nil)
		}
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("dump options=%v: %s, %v", withOptions, got, err)
		}
		var output bytes.Buffer
		if withOptions {
			err = db.DumpToContextWithOptions(nil, &output, ExportOptions{})
		} else {
			err = db.DumpToContext(nil, &output)
		}
		if err != nil || !bytes.Equal(output.Bytes(), want) {
			t.Fatalf("dump writer options=%v: %s, %v", withOptions, output.Bytes(), err)
		}
		for _, format := range []ExportFormat{ExportFormatJSON, ExportFormatJSONL, ExportFormatDOT} {
			var expected bytes.Buffer
			if err := db.ExportTo(format, &expected); err != nil {
				t.Fatal(err)
			}
			output.Reset()
			if withOptions {
				err = db.ExportToContextWithOptions(nil, format, &output, ExportOptions{})
			} else {
				err = db.ExportToContext(nil, format, &output)
			}
			if err != nil || !bytes.Equal(output.Bytes(), expected.Bytes()) {
				t.Fatalf("export %s options=%v: %s, %v", format, withOptions, output.Bytes(), err)
			}
		}
	}
	if err := db.DumpToContextWithOptions(nil, io.Discard, ExportOptions{MaxBytes: 1}); !errors.Is(err, ErrExportOutputLimit) {
		t.Fatalf("nil context byte limit: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := db.DumpToContext(ctx, io.Discard); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled dump: %v", err)
	}
	if err := db.ExportToContext(ctx, ExportFormatJSONL, io.Discard); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled export: %v", err)
	}
	if _, err := db.ReadStreamContext(nil, "events", 0, StreamReadOptions{Limit: 1}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nil stream context: %v", err)
	}
}

func (shortWriter) Write(data []byte) (int, error) { return len(data) / 2, nil }

type recordingWriter struct{ writes int }

func (writer *recordingWriter) Write(data []byte) (int, error) {
	writer.writes++
	return len(data), nil
}

func openExportLimitDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "export-limits.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Update(func(tx *Tx) error {
		left, err := tx.CreateNode(CreateNodeOptions{Properties: map[string]Value{"text": "large enough for a limit"}})
		if err != nil {
			return err
		}
		right, err := tx.CreateNode(CreateNodeOptions{})
		if err != nil {
			return err
		}
		_, err = tx.CreateEdge(left.ID, right.ID, "LINK", CreateEdgeOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestExportRecordLimitPreflightsAllFileFormats(t *testing.T) {
	db := openExportLimitDB(t)
	for _, format := range []ExportFormat{ExportFormatJSON, ExportFormatJSONL, ExportFormatCSV, ExportFormatDOT} {
		t.Run(string(format), func(t *testing.T) {
			output := filepath.Join(t.TempDir(), "graph."+string(format))
			if err := os.WriteFile(output, []byte("published"), 0o600); err != nil {
				t.Fatal(err)
			}
			err := db.ExportFileContextWithOptions(context.Background(), format, output, ExportOptions{MaxRecords: 2})
			if !errors.Is(err, ErrExportOutputLimit) {
				t.Fatalf("record limit error = %v", err)
			}
			if data, err := os.ReadFile(output); err != nil || string(data) != "published" {
				t.Fatalf("published output changed: %q, %v", data, err)
			}
			if builds, err := filepath.Glob(output + "_generations/.building-*"); err != nil || len(builds) != 0 {
				t.Fatalf("temporary CSV builds = %v, %v", builds, err)
			}
		})
	}
}

func TestExportByteLimitKeepsFilePublicationsAtomic(t *testing.T) {
	db := openExportLimitDB(t)
	for _, format := range []ExportFormat{ExportFormatJSON, ExportFormatJSONL, ExportFormatCSV, ExportFormatDOT} {
		t.Run(string(format), func(t *testing.T) {
			directory := t.TempDir()
			output := filepath.Join(directory, "graph."+string(format))
			if err := os.WriteFile(output, []byte("published"), 0o600); err != nil {
				t.Fatal(err)
			}
			err := db.ExportFileContextWithOptions(context.Background(), format, output, ExportOptions{MaxBytes: 1})
			if !errors.Is(err, ErrExportOutputLimit) {
				t.Fatalf("byte limit error = %v", err)
			}
			if data, err := os.ReadFile(output); err != nil || string(data) != "published" {
				t.Fatalf("published output changed: %q, %v", data, err)
			}
			if temporary, err := filepath.Glob(filepath.Join(directory, ".latticedb-export-*.tmp")); err != nil || len(temporary) != 0 {
				t.Fatalf("temporary outputs = %v, %v", temporary, err)
			}
			if builds, err := filepath.Glob(output + "_generations/.building-*"); err != nil || len(builds) != 0 {
				t.Fatalf("temporary CSV builds = %v, %v", builds, err)
			}
		})
	}
}

func TestExportLimitsCoverBufferedAndStreamingAPIs(t *testing.T) {
	db := openExportLimitDB(t)
	if output, err := db.DumpContextWithOptions(context.Background(), ExportOptions{MaxBytes: 10}); !errors.Is(err, ErrExportOutputLimit) || output != nil {
		t.Fatalf("buffered dump = %q, %v", output, err)
	}
	writer := &recordingWriter{}
	if err := db.DumpToContextWithOptions(context.Background(), writer, ExportOptions{MaxRecords: 2}); !errors.Is(err, ErrExportOutputLimit) || writer.writes != 0 {
		t.Fatalf("record preflight = writes %d, error %v", writer.writes, err)
	}
	for _, format := range []ExportFormat{ExportFormatJSON, ExportFormatJSONL, ExportFormatDOT} {
		t.Run(string(format), func(t *testing.T) {
			var output bytes.Buffer
			err := db.ExportToContextWithOptions(context.Background(), format, &output, ExportOptions{MaxBytes: 10})
			if !errors.Is(err, ErrExportOutputLimit) || output.Len() > 10 {
				t.Fatalf("streaming output = %q, error %v", output.Bytes(), err)
			}
		})
	}
}

func TestExportOutputLimitUsesPublicErrorFull(t *testing.T) {
	db := openExportLimitDB(t)
	assertErrorFull := func(t *testing.T, err error) {
		t.Helper()
		if !errors.Is(err, ErrExportOutputLimit) {
			t.Fatalf("output limit sentinel = %v", err)
		}
		var latticeErr *Error
		if !errors.As(err, &latticeErr) || latticeErr.Code != ErrorFull {
			t.Fatalf("public error = %#v", err)
		}
	}
	_, err := db.DumpContextWithOptions(context.Background(), ExportOptions{MaxBytes: 1})
	assertErrorFull(t, err)
	err = db.ExportFileContextWithOptions(context.Background(), ExportFormatJSON, filepath.Join(t.TempDir(), "graph.json"), ExportOptions{MaxBytes: 1})
	assertErrorFull(t, err)
	var output bytes.Buffer
	err = db.ExportToContextWithOptions(context.Background(), ExportFormatJSON, &output, ExportOptions{MaxBytes: 1})
	assertErrorFull(t, err)
}

func TestStreamingDumpMatchesConvenienceDump(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "stream-export.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]Value{"nested": map[string]any{"value": int64(1)}}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	want, err := db.Dump()
	if err != nil {
		t.Fatal(err)
	}
	var got bytes.Buffer
	if err := db.DumpTo(&got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Bytes(), want) {
		t.Fatalf("streamed dump differs:\n%s\n%s", got.Bytes(), want)
	}
	if err := db.DumpTo(shortWriter{}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short writer error = %v", err)
	}
}

func TestCSVManifestKeepsPublishedGenerations(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "csv-export.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		left, err := tx.CreateNode(CreateNodeOptions{})
		if err != nil {
			return err
		}
		right, err := tx.CreateNode(CreateNodeOptions{})
		if err != nil {
			return err
		}
		_, err = tx.CreateEdge(left.ID, right.ID, "LINK", CreateEdgeOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(t.TempDir(), "graph.csv")
	manifestBytes, err := db.Export(ExportFormatCSV, output)
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct{ Nodes, Edges string }
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	paths := []string{manifest.Nodes, manifest.Edges}
	for _, path := range paths {
		if _, err := os.Stat(filepath.Join(filepath.Dir(output), path)); err != nil {
			t.Fatal(err)
		}
	}
	for range 5 {
		if _, err := db.Export(ExportFormatCSV, output); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range paths {
		if _, err := os.Stat(filepath.Join(filepath.Dir(output), path)); err != nil {
			t.Fatalf("published generation was reclaimed: %v", err)
		}
	}
}

func TestConcurrentCSVExportsNeverPublishDanglingManifest(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "csv-concurrent.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(t.TempDir(), "graph.csv")
	var wait sync.WaitGroup
	errors := make(chan error, 8)
	for range 8 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := db.Export(ExportFormatCSV, output)
			errors <- err
		}()
	}
	wait.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct{ Nodes, Edges string }
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{manifest.Nodes, manifest.Edges} {
		if _, err := os.Stat(filepath.Join(filepath.Dir(output), path)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDumpUsesSingleGenerationDuringConcurrentWrites(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "export-snapshot.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *Tx) error {
		for range 2 {
			if _, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Item"}, Properties: map[string]Value{"version": int64(0)}}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		for version := int64(1); version <= 50; version++ {
			if _, err := db.Query("MATCH (n:Item) SET n.version = $version", map[string]Value{"version": version}); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	for {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			return
		default:
		}
		data, err := db.Dump()
		if err != nil {
			t.Fatal(err)
		}
		var dump struct {
			Nodes []struct {
				Properties map[string]struct {
					Int int64 `json:"int"`
				} `json:"properties"`
			} `json:"nodes"`
		}
		if err := json.Unmarshal(data, &dump); err != nil {
			t.Fatal(err)
		}
		if len(dump.Nodes) != 2 || dump.Nodes[0].Properties["version"].Int != dump.Nodes[1].Properties["version"].Int {
			t.Fatalf("torn dump: %s", data)
		}
	}
}
