package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

type cancelBackupWriter struct {
	cancel context.CancelFunc
	writes int
}

func TestWALPayloadPreparationCancellation(t *testing.T) {
	for _, stage := range []string{"payload copy", "WAL copy"} {
		t.Run(stage, func(t *testing.T) {
			files := DirectoryDatabaseFiles(t.TempDir())
			payload, err := os.CreateTemp(t.TempDir(), "payload")
			if err != nil {
				t.Fatal(err)
			}
			defer payload.Close()
			if _, err := payload.WriteString(strings.Repeat("x", 2<<20)); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var workCtx context.Context = ctx
			var fault CheckpointFault
			if stage == "payload copy" {
				workCtx = &cancelLoadAfterChecks{limit: 4}
			} else {
				fault = func(stage string, after bool) error {
					if stage == "wal-write" && !after {
						cancel()
					}
					return nil
				}
			}
			err = rewriteWALStatePayloadContext(workCtx, files, payload, strings.Repeat("a", 32), 0, fault, files.WAL)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("error=%v", err)
			}
			entries, err := os.ReadDir(files.Directory)
			if err != nil || len(entries) != 0 {
				t.Fatalf("temporary WAL files remain: %v, %v", entries, err)
			}
		})
	}
}

func TestCheckpointCancellationAfterStatePublication(t *testing.T) {
	for _, private := range []bool{false, true} {
		t.Run(strconv.FormatBool(private), func(t *testing.T) {
			files := DirectoryDatabaseFiles(t.TempDir())
			graph := NewGraphState()
			graph.Nodes.Set(1, &NodeRecord{ID: 1, Properties: PropertiesFromMap(map[string]any{"text": strings.Repeat("x", 1<<20)})})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			fault := func(stage string, after bool) error {
				if stage == "state-rename" && after {
					cancel()
				}
				return nil
			}
			err := writeCheckpointGraphStateAndWALFilesContext(ctx, files, graph, 2, 1, 0, 0, fault, private)
			if private {
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("private files: %v", err)
				}
				if _, err := os.Stat(files.WAL); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("private WAL was published: %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				recovered, _, _, _, err := LoadGraphStateFilesContext(t.Context(), files, ^uint64(0), ^uint64(0), ^uint64(0))
				if err != nil {
					t.Fatalf("durable recovery: %v", err)
				}
				if recovered.Nodes.Len() != 1 {
					t.Fatalf("durable node count=%d", recovered.Nodes.Len())
				}
			}
			matches, err := filepath.Glob(filepath.Join(files.Directory, "*.tmp"))
			if err != nil || len(matches) != 0 {
				t.Fatalf("temporary files remain: %v, %v", matches, err)
			}
		})
	}
}

func (writer *cancelBackupWriter) Write(data []byte) (int, error) {
	writer.writes++
	writer.cancel()
	return len(data), nil
}

func TestWriteGraphStateContextStopsAfterFirstOutputWrite(t *testing.T) {
	temporary := t.TempDir()
	t.Setenv("TMPDIR", temporary)
	graph := NewGraphState()
	graph.Nodes.Set(1, &NodeRecord{ID: 1, Properties: PropertiesFromMap(map[string]any{"text": strings.Repeat("x", 1<<20)})})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	writer := &cancelBackupWriter{cancel: cancel}
	err := WriteGraphStateContext(ctx, writer, graph, 2, 1, 0)
	if !errors.Is(err, context.Canceled) || writer.writes != 1 {
		t.Fatalf("error=%v writes=%d", err, writer.writes)
	}
	entries, err := os.ReadDir(temporary)
	if err != nil || len(entries) != 0 {
		t.Fatalf("temporary payload cleanup: %v, %v", entries, err)
	}
}

func TestBackupSegmentCRCChecksContextWithinFrame(t *testing.T) {
	path := t.TempDir()
	graph := NewGraphState()
	if err := CheckpointGraphState(path, graph, 1, 1, 0); err != nil {
		t.Fatal(err)
	}
	graph.Nodes.Set(1, &NodeRecord{ID: 1, Properties: PropertiesFromMap(map[string]any{"text": strings.Repeat("x", 1<<20)})})
	delta, err := buildPersistedDelta(graph, 2, 1, 1, GraphDelta{UpsertNodes: []uint64{1}})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := encodeBinaryWALPayload(walPayload{Kind: "delta", Delta: &delta})
	if err != nil {
		t.Fatal(err)
	}
	header, err := encodeWALHeader(graph.DatabaseID, 1, payload)
	if err != nil {
		t.Fatal(err)
	}
	file := path + "/segment.wal"
	if err = os.WriteFile(file, append(header[:], payload...), 0600); err != nil {
		t.Fatal(err)
	}
	if err = ValidateBackupWALSegmentFile(file, graph.DatabaseID, 0, 1); err != nil {
		t.Fatal(err)
	}
	ctx := &cancelLoadAfterChecks{limit: 4}
	if err = ValidateBackupWALSegmentFileContext(ctx, file, graph.DatabaseID, 0, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("CRC cancellation=%v", err)
	}
}

func TestFTSCountContextCancellation(t *testing.T) {
	graph := NewGraphState()
	for id := uint64(1); id <= 2048; id++ {
		graph.FTS.Set(id, &FTSRecord{Text: "record"})
	}
	count, err := graph.FTSCountContext(&cancelLoadAfterChecks{limit: 3})
	if !errors.Is(err, context.Canceled) || count >= 2048 {
		t.Fatalf("count=%d, error=%v", count, err)
	}
}
