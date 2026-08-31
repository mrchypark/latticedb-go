package latticedb

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

type shortWriter struct{}

func (shortWriter) Write(data []byte) (int, error) { return len(data) / 2, nil }

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
