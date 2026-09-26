package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"strings"
	"testing"
)

func TestReadWALHeaderReusesBufferAcrossFormats(t *testing.T) {
	header, err := encodeWALHeader(strings.Repeat("a", 32), 1, []byte{1})
	if err != nil {
		t.Fatal(err)
	}
	legacy := bytes.Clone(header[:legacyWALHeaderSize])
	copy(legacy[:8], binaryWALMagic[:])
	binary.BigEndian.PutUint16(legacy[8:10], binaryWALVersion)
	binary.BigEndian.PutUint16(legacy[10:12], legacyWALHeaderSize)
	var buffer [walHeaderSize]byte
	reader := bytes.NewReader(nil)
	frames := [][]byte{header[:], legacy, header[:]}
	allocations := testing.AllocsPerRun(100, func() {
		for _, frame := range frames {
			reader.Reset(frame)
			got, err := readWALHeader(reader, &buffer)
			if err != nil || !bytes.Equal(got, frame) || !validWALHeader(got) {
				t.Fatalf("reused header = %x, %v", got, err)
			}
		}
	})
	if allocations != 0 {
		t.Fatalf("reading into a reusable header buffer allocated %g times", allocations)
	}
}

func TestWALHeaderChecksumRejectsLengthCorruption(t *testing.T) {
	files := DirectoryDatabaseFiles(t.TempDir())
	base := NewGraphState()
	if err := CheckpointGraphState(files.Directory, base, 1, 1, 0); err != nil {
		t.Fatal(err)
	}
	if err := AppendWALCommitFiles(files, base, 1, 1, 0); err != nil {
		t.Fatal(err)
	}
	updated := CloneGraphState(base)
	updated.Nodes.Set(1, &NodeRecord{ID: 1, Properties: PropertiesFromMap(map[string]any{"v": "committed"})})
	if err := AppendWALDeltaFiles(files, updated, 2, 1, 1, GraphDelta{UpsertNodes: []uint64{1}}); err != nil {
		t.Fatal(err)
	}
	wal, err := os.ReadFile(files.WAL)
	if err != nil {
		t.Fatal(err)
	}
	firstLength := binary.BigEndian.Uint64(wal[20:28])
	second := walHeaderSize + int(firstLength)
	wal[second+27]++ // Payload length increases by one while remaining below every budget.
	if err := os.WriteFile(files.WAL, wal, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := LoadGraphState(files.Directory); err == nil {
		t.Fatal("corrupt v5 header length silently discarded the committed frame")
	}
	if WALFilesReadyForAppend(files) {
		t.Fatal("corrupt v5 header is append-ready")
	}
}

func TestWALV5RejectsEveryHeaderBitCorruption(t *testing.T) {
	files := DirectoryDatabaseFiles(t.TempDir())
	graph := NewGraphState()
	if err := AppendWALCommitFiles(files, graph, 1, 1, 0); err != nil {
		t.Fatal(err)
	}
	record, err := os.ReadFile(files.WAL)
	if err != nil {
		t.Fatal(err)
	}
	for byteIndex := 0; byteIndex < walHeaderSize; byteIndex++ {
		for bit := byte(0); bit < 8; bit++ {
			corrupt := append([]byte(nil), record...)
			corrupt[byteIndex] ^= 1 << bit
			if err := os.WriteFile(files.WAL, corrupt, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, _, _, _, err := LoadGraphState(files.Directory); err == nil {
				t.Fatalf("header byte %d bit %d was accepted", byteIndex, bit)
			}
		}
	}
}

func TestValidWALHeaderRejectsShortBuffers(t *testing.T) {
	for length := 0; length < walHeaderPrefixSize; length++ {
		if validWALHeader(make([]byte, length)) {
			t.Fatalf("short header length %d was accepted", length)
		}
	}
}

func TestWALV5RejectsCompletePayloadChecksumMismatch(t *testing.T) {
	files := DirectoryDatabaseFiles(t.TempDir())
	graph := NewGraphState()
	if err := AppendWALCommitFiles(files, graph, 1, 1, 0); err != nil {
		t.Fatal(err)
	}
	record, err := os.ReadFile(files.WAL)
	if err != nil {
		t.Fatal(err)
	}
	record[len(record)-1] ^= 1
	if err := os.WriteFile(files.WAL, record, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := LoadGraphState(files.Directory); err == nil {
		t.Fatal("complete payload checksum mismatch was accepted")
	}
}

func TestWALV5TruncationReturnsOnlyCompletePrefix(t *testing.T) {
	files := DirectoryDatabaseFiles(t.TempDir())
	base := NewGraphState()
	if err := AppendWALCommitFiles(files, base, 1, 1, 0); err != nil {
		t.Fatal(err)
	}
	updated := CloneGraphState(base)
	updated.Nodes.Set(1, &NodeRecord{ID: 1, Properties: PropertiesFromMap(map[string]any{"v": "committed"})})
	if err := AppendWALDeltaFiles(files, updated, 2, 1, 1, GraphDelta{UpsertNodes: []uint64{1}}); err != nil {
		t.Fatal(err)
	}
	wal, err := os.ReadFile(files.WAL)
	if err != nil {
		t.Fatal(err)
	}
	second := walHeaderSize + int(binary.BigEndian.Uint64(wal[20:28]))
	for length := 0; length < len(wal); length++ {
		if err := os.WriteFile(files.WAL, wal[:length], 0o600); err != nil {
			t.Fatal(err)
		}
		graph, _, _, commit, err := LoadGraphState(files.Directory)
		if length < second {
			if !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("truncate at %d: error = %v, want os.ErrNotExist", length, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("truncate at %d: %v", length, err)
		}
		if commit != 0 || graph.Nodes.Get(1) != nil {
			t.Fatalf("truncate at %d recovered commit %d, node=%v", length, commit, graph.Nodes.Get(1) != nil)
		}
	}
	if err := os.WriteFile(files.WAL, wal, 0o600); err != nil {
		t.Fatal(err)
	}
	graph, _, _, commit, err := LoadGraphState(files.Directory)
	if err != nil || commit != 1 || graph.Nodes.Get(1) == nil {
		t.Fatalf("complete v5 frame = commit %d, node=%v, err=%v", commit, graph.Nodes.Get(1) != nil, err)
	}
}

func TestWALV5VersionDowngradeDoesNotReinterpretHeaderLayout(t *testing.T) {
	files := DirectoryDatabaseFiles(t.TempDir())
	graph := NewGraphState()
	if err := AppendWALCommitFiles(files, graph, 1, 1, 0); err != nil {
		t.Fatal(err)
	}
	record, err := os.ReadFile(files.WAL)
	if err != nil {
		t.Fatal(err)
	}
	binary.BigEndian.PutUint16(record[8:10], binaryWALVersion)
	binary.BigEndian.PutUint32(record[walHeaderChecksumAt:walHeaderSize], crc32.ChecksumIEEE(record[:walHeaderChecksumAt]))
	if err := os.WriteFile(files.WAL, record, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := LoadGraphState(files.Directory); err == nil {
		t.Fatal("v5 header was reinterpreted as v4 after a version downgrade")
	}
}

func TestBinaryWALV4LoadsAndMigratesToV5(t *testing.T) {
	files := DirectoryDatabaseFiles(t.TempDir())
	graph := NewGraphState()
	if err := EnsureDatabaseID(graph); err != nil {
		t.Fatal(err)
	}
	snapshot, err := buildPersistedState(graph, 1, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := encodeBinaryWALPayload(walPayload{Kind: "snapshot", Snapshot: &snapshot})
	if err != nil {
		t.Fatal(err)
	}
	header := make([]byte, legacyWALHeaderSize)
	copy(header[:8], binaryWALMagic[:])
	binary.BigEndian.PutUint16(header[8:10], binaryWALVersion)
	binary.BigEndian.PutUint16(header[10:12], legacyWALHeaderSize)
	binary.BigEndian.PutUint64(header[20:28], uint64(len(payload)))
	binary.BigEndian.PutUint32(header[28:32], crc32.ChecksumIEEE(payload))
	copy(header[walDatabaseIDAt:legacyWALHeaderSize], graph.DatabaseID)
	if err := os.WriteFile(files.WAL, append(header, payload...), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, commit, err := LoadGraphState(files.Directory); err != nil || commit != 0 {
		t.Fatalf("load v4 WAL = commit %d, err=%v", commit, err)
	}
	if err := AppendWALCommitFiles(files, graph, 1, 1, 1); err != nil {
		t.Fatal(err)
	}
	migrated, err := os.ReadFile(files.WAL)
	if err != nil {
		t.Fatal(err)
	}
	if !validCurrentWALHeader(migrated) {
		t.Fatal("v4 WAL was not rewritten as an integrity-protected v5 frame")
	}
}

func TestBinaryCorruptStateLengthStillFallsBackToValidWAL(t *testing.T) {
	files := DirectoryDatabaseFiles(t.TempDir())
	graph := NewGraphState()
	graph.Nodes.Set(1, &NodeRecord{ID: 1, Properties: PropertiesFromMap(map[string]any{"kept": "value"})})
	if err := CheckpointGraphStateAndWALFiles(files, graph, 2, 1, 1); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(files.State)
	if err != nil {
		t.Fatal(err)
	}
	data[stateHeaderSize] = 0xff // damaged DB-ID length; checksum intentionally stale
	if err := os.WriteFile(files.State, data, 0600); err != nil {
		t.Fatal(err)
	}
	recovered, _, _, commit, err := LoadGraphState(files.Directory)
	if err != nil {
		t.Fatal(err)
	}
	if commit != 1 || recovered.Nodes.Get(1).Properties.Get("kept") != "value" {
		t.Fatal("valid WAL fallback lost state")
	}
}

func TestBinaryLogicalEOFDoesNotDiscardCompleteWALFrame(t *testing.T) {
	files := DirectoryDatabaseFiles(t.TempDir())
	graph := NewGraphState()
	if err := AppendWALCommitFiles(files, graph, 1, 1, 0); err != nil {
		t.Fatal(err)
	}
	delta := persistedDelta{DatabaseID: graph.DatabaseID, CommitID: 1, NextNodeID: 2, NextEdgeID: 1,
		UpsertNodes: []persistedNode{{ID: 1, Properties: map[string]persistedValue{
			"a": {Kind: "vector", Vector: []float32{1, 2}},
			"z": {Kind: "string", String: strings.Repeat("z", 8192)},
		}}}}
	payload, err := encodeBinaryWALPayload(walPayload{Kind: "delta", Delta: &delta})
	if err != nil {
		t.Fatal(err)
	}
	pattern := []byte{1, 'a', binaryVectorValue, 3, 0x3f, 0x80, 0, 0, 0x40, 0, 0, 0}
	offset := bytes.Index(payload, pattern)
	if offset < 0 {
		t.Fatal("vector fixture missing")
	}
	offset += 3
	// Count fits the remaining byte count, but not its four-byte elements.
	// The later string keeps the frame larger than the decoder's read buffer.
	malformed := append([]byte(nil), payload[:offset]...)
	malformed = binary.AppendUvarint(malformed, 3001)
	malformed = append(malformed, payload[offset+1:]...)
	header, err := encodeWALHeader(graph.DatabaseID, 1, malformed)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(files.WAL, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := file.Write(append(header[:], malformed...))
	closeErr := file.Close()
	if writeErr != nil {
		t.Fatal(writeErr)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	if state, err := loadLatestWALSnapshot(files.Directory); err == nil {
		t.Fatalf("complete malformed frame silently discarded; recovered commit %d", state.CommitID)
	}
}
