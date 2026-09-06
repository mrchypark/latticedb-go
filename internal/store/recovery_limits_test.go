package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"testing"
)

func TestLegacyWALReadStopsAtCanonicalAndCumulativeLimits(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "legacy-wal")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := file.Write(bytes.Repeat([]byte{'x'}, 8<<10)); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name               string
		canonical, decoded uint64
	}{
		{name: "canonical", canonical: 1024},
		{name: "cumulative", canonical: 16 << 10, decoded: 1024},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := file.Seek(0, 0); err != nil {
				t.Fatal(err)
			}
			budget := &recoveryBudget{limits: RecoveryLimits{MaxDecodedBytes: test.decoded}}
			if _, err := loadLatestLegacyWALContextWithRecoveryBudget(context.Background(), file, test.canonical, budget); !errors.Is(err, ErrLoadResourceLimit) {
				t.Fatalf("load error = %v, want ErrLoadResourceLimit", err)
			}
			offset, err := file.Seek(0, io.SeekCurrent)
			if err != nil {
				t.Fatal(err)
			}
			if offset >= 8<<10 {
				t.Fatalf("reader consumed %d bytes before rejecting an 8KiB line", offset)
			}
		})
	}
}

func TestRecoveryReportsWhetherCurrentWALCanAppend(t *testing.T) {
	path := t.TempDir()
	graph := NewGraphState()
	if err := AppendWALCommit(path, graph, 1, 1, 0); err != nil {
		t.Fatal(err)
	}
	files := DirectoryDatabaseFiles(path)
	_, _, _, _, ready, err := LoadGraphStateFilesContextWithRecoveryLimitsAndWALAppendReady(context.Background(), files, ^uint64(0), ^uint64(0), ^uint64(0), RecoveryLimits{})
	if err != nil || !ready {
		t.Fatalf("complete WAL = ready %v, error %v", ready, err)
	}
	wal, err := os.OpenFile(files.WAL, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wal.Write([]byte{1}); err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	_, _, _, _, ready, err = LoadGraphStateFilesContextWithRecoveryLimitsAndWALAppendReady(context.Background(), files, ^uint64(0), ^uint64(0), ^uint64(0), RecoveryLimits{})
	if err != nil || ready {
		t.Fatalf("incomplete WAL = ready %v, error %v", ready, err)
	}
}

func TestRecoveryDoesNotMarkMixedFormatWALAppendReady(t *testing.T) {
	path := t.TempDir()
	graph := NewGraphState()
	if err := AppendWALCommit(path, graph, 1, 1, 0); err != nil {
		t.Fatal(err)
	}
	graph.Nodes.Set(1, &NodeRecord{ID: 1})
	if err := AppendWALCommit(path, graph, 2, 1, 1); err != nil {
		t.Fatal(err)
	}
	files := DirectoryDatabaseFiles(path)
	wal, err := os.ReadFile(files.WAL)
	if err != nil {
		t.Fatal(err)
	}
	firstLength := binary.BigEndian.Uint64(wal[20:28])
	second := walHeaderSize + int(firstLength)
	copy(wal[second:second+8], legacyWALMagic[:])
	binary.BigEndian.PutUint16(wal[second+8:second+10], legacyWALVersion)
	if err := os.WriteFile(files.WAL, wal, 0o600); err != nil {
		t.Fatal(err)
	}
	_, _, _, _, ready, err := LoadGraphStateFilesContextWithRecoveryLimitsAndWALAppendReady(context.Background(), files, ^uint64(0), ^uint64(0), ^uint64(0), RecoveryLimits{})
	if err != nil || ready {
		t.Fatalf("mixed-format WAL = ready %v, error %v", ready, err)
	}
}
