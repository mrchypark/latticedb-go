package store

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"testing"
)

func buildLegacyGraph(t *testing.T) *GraphState {
	t.Helper()
	graph := NewGraphState()
	graph.Nodes.Set(1, &NodeRecord{
		ID:         1,
		Labels:     []string{"Person"},
		Properties: PropertiesFromMap(map[string]any{"name": "alice", "age": int64(30)}),
	})
	graph.Nodes.Set(2, &NodeRecord{
		ID:         2,
		Labels:     []string{"Person"},
		Properties: PropertiesFromMap(map[string]any{"name": "bob"}),
	})
	graph.Edges.Set(1, &EdgeRecord{
		ID:         1,
		SourceID:   1,
		TargetID:   2,
		Type:       "KNOWS",
		Properties: PropertiesFromMap(map[string]any{"since": int64(2020)}),
	})
	graph.FTS.Set(1, &FTSRecord{Text: "alice bob"})
	graph.AppMetadata.Set("schema_version", []byte("1"))
	return graph
}

func verifyLegacyGraph(t *testing.T, loaded *GraphState) {
	t.Helper()
	if loaded.Nodes.Get(1).Properties.Get("name") != "alice" {
		t.Fatal("node 1 name lost")
	}
	if loaded.Nodes.Get(1).Properties.Get("age") != int64(30) {
		t.Fatal("node 1 age lost")
	}
	if loaded.Nodes.Get(2).Properties.Get("name") != "bob" {
		t.Fatal("node 2 name lost")
	}
	edge := loaded.Edges.Get(1)
	if edge == nil || edge.Properties.Get("since") != int64(2020) {
		t.Fatal("edge 1 missing or corrupted")
	}
	fts := loaded.FTS.Get(1)
	if fts == nil || fts.Text != "alice bob" {
		t.Fatal("FTS record missing or corrupted")
	}
	val, ok := loaded.AppMetadata.Get("schema_version")
	if !ok || string(val) != "1" {
		t.Fatal("app metadata lost")
	}
}

func writeLegacyStateFile(t *testing.T, path string, graph *GraphState, commit uint64, version uint16) {
	t.Helper()
	data := jsonStateTestBytes(t, graph, 3, 2, commit, version)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeLegacyWALFile(t *testing.T, path string, graph *GraphState, commit uint64, version uint16) {
	t.Helper()
	snapshot, err := buildPersistedState(graph, 3, 2, commit)
	if err != nil {
		t.Fatal(err)
	}
	record := jsonWALTestRecord(t, graph.DatabaseID, commit, walPayload{Kind: "snapshot", Snapshot: &snapshot}, version)
	if err := os.WriteFile(path, record, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyStateV3AppendWALDeltaFilesMigration(t *testing.T) {
	dbPath := t.TempDir()
	files := DirectoryDatabaseFiles(dbPath)
	graph := buildLegacyGraph(t)
	writeLegacyStateFile(t, files.State, graph, 0, legacyStateVersion)
	writeLegacyWALFile(t, files.WAL, graph, 0, legacyWALVersion)

	graph.Nodes.Set(3, &NodeRecord{
		ID:         3,
		Labels:     []string{"Person"},
		Properties: PropertiesFromMap(map[string]any{"name": "carol"}),
	})
	if err := AppendWALDeltaFiles(files, graph, 4, 2, 1, GraphDelta{UpsertNodes: []uint64{3}}); err != nil {
		t.Fatal(err)
	}

	loaded, _, _, commitID, err := LoadGraphStateFilesContext(context.Background(), files, maxStateFileBytes, ^uint64(0), ^uint64(0))
	if err != nil {
		t.Fatal(err)
	}
	if commitID != 1 {
		t.Fatalf("commit = %d, want 1", commitID)
	}
	verifyLegacyGraph(t, loaded)
	if loaded.Nodes.Get(3) == nil || loaded.Nodes.Get(3).Properties.Get("name") != "carol" {
		t.Fatal("new node 3 missing or corrupted")
	}
}

func TestLegacyStateV4AppendWALDeltaFilesMigration(t *testing.T) {
	dbPath := t.TempDir()
	files := DirectoryDatabaseFiles(dbPath)
	graph := buildLegacyGraph(t)
	writeLegacyStateFile(t, files.State, graph, 0, jsonStateVersion)
	writeLegacyWALFile(t, files.WAL, graph, 0, jsonWALVersion)

	graph.Nodes.Set(3, &NodeRecord{
		ID:         3,
		Labels:     []string{"Person"},
		Properties: PropertiesFromMap(map[string]any{"name": "carol"}),
	})
	if err := AppendWALDeltaFiles(files, graph, 4, 2, 1, GraphDelta{UpsertNodes: []uint64{3}}); err != nil {
		t.Fatal(err)
	}

	loaded, _, _, _, err := LoadGraphStateFilesContext(context.Background(), files, maxStateFileBytes, ^uint64(0), ^uint64(0))
	if err != nil {
		t.Fatal(err)
	}
	verifyLegacyGraph(t, loaded)
	if loaded.Nodes.Get(3) == nil {
		t.Fatal("new node 3 missing after v4/v3 migration")
	}
}

func TestCheckpointGraphStateAndWALFilesMigratesLegacyState(t *testing.T) {
	dbPath := t.TempDir()
	files := DirectoryDatabaseFiles(dbPath)
	graph := buildLegacyGraph(t)
	writeLegacyStateFile(t, files.State, graph, 0, legacyStateVersion)
	writeLegacyWALFile(t, files.WAL, graph, 0, legacyWALVersion)

	if err := CheckpointGraphStateAndWALFiles(files, graph, 3, 2, 1); err != nil {
		t.Fatal(err)
	}

	sf, err := os.Open(files.State)
	if err != nil {
		t.Fatal(err)
	}
	defer sf.Close()
	var header [stateHeaderSize]byte
	if _, err := io.ReadFull(sf, header[:]); err != nil {
		t.Fatal(err)
	}
	if string(header[:8]) != string(stateBinaryMagic[:]) {
		t.Fatalf("state magic = %s, want %s", header[:8], stateBinaryMagic[:])
	}
	if binary.BigEndian.Uint16(header[8:10]) != stateVersion {
		t.Fatalf("state version = %d, want %d", binary.BigEndian.Uint16(header[8:10]), stateVersion)
	}

	wf, err := os.Open(files.WAL)
	if err != nil {
		t.Fatal(err)
	}
	defer wf.Close()
	var walHdr [walHeaderSize]byte
	if _, err := io.ReadFull(wf, walHdr[:]); err != nil {
		t.Fatal(err)
	}
	if string(walHdr[:8]) != string(walMagic[:]) {
		t.Fatalf("WAL magic = %s, want %s", walHdr[:8], walMagic[:])
	}

	loaded, _, _, commitID, err := LoadGraphStateFilesContext(context.Background(), files, maxStateFileBytes, ^uint64(0), ^uint64(0))
	if err != nil {
		t.Fatal(err)
	}
	if commitID != 1 {
		t.Fatalf("commit = %d, want 1", commitID)
	}
	verifyLegacyGraph(t, loaded)
}

func TestFutureStateMagicReturnsUnsupportedOverWALRecovery(t *testing.T) {
	dbPath := t.TempDir()
	files := DirectoryDatabaseFiles(dbPath)
	graph := buildLegacyGraph(t)

	if err := ensureDatabaseID(graph); err != nil {
		t.Fatal(err)
	}
	snapshot := persistedState{
		DatabaseID:       graph.DatabaseID,
		VectorDimensions: graph.VectorDimensions,
		CommitID:         0,
		NextNodeID:       3,
		NextEdgeID:       2,
	}
	payload, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	header, err := encodeStateHeader(graph.DatabaseID, 0, uint64(len(payload)), crc32.ChecksumIEEE(payload))
	if err != nil {
		t.Fatal(err)
	}
	copy(header[:8], []byte("LDBSTAT6"))
	binary.BigEndian.PutUint16(header[8:10], 99)
	if err := os.WriteFile(files.State, append(header[:], payload...), 0o600); err != nil {
		t.Fatal(err)
	}

	writeLegacyWALFile(t, files.WAL, graph, 0, legacyWALVersion)

	_, _, _, _, err = LoadGraphStateFilesContext(context.Background(), files, maxStateFileBytes, ^uint64(0), ^uint64(0))
	if err == nil {
		t.Fatal("expected error for future state magic, got nil")
	}
	if !errors.Is(err, errUnsupportedStorageFormat) {
		t.Fatalf("err = %v, want errUnsupportedStorageFormat", err)
	}
}

// wal-rename fault fires before the WAL file rename. The state file was
// already renamed successfully (commit 1) but the WAL is still the old
// legacy v2 file (commit 0). Recovery picks commit 1 from the state file.
func TestMixedOldNewStateAfterCheckpointFaultWalRename(t *testing.T) {
	dbPath := t.TempDir()
	files := DirectoryDatabaseFiles(dbPath)
	graph := buildLegacyGraph(t)
	writeLegacyStateFile(t, files.State, graph, 0, legacyStateVersion)
	writeLegacyWALFile(t, files.WAL, graph, 0, legacyWALVersion)

	injected := errors.New("wal-rename fault")
	fault := func(stage string, after bool) error {
		if stage == "wal-rename" && !after {
			return injected
		}
		return nil
	}
	err := checkpointGraphStateAndWALFiles(files, graph, 3, 2, 1, 0, fault)
	if !errors.Is(err, injected) {
		t.Fatalf("checkpoint err = %v, want %v", err, injected)
	}

	loaded, _, _, commitID, loadErr := LoadGraphStateFilesContext(context.Background(), files, maxStateFileBytes, ^uint64(0), ^uint64(0))
	if loadErr != nil {
		t.Fatalf("recovery after wal-rename fault: %v", loadErr)
	}
	if commitID != 1 {
		t.Fatalf("commit = %d, want 1 after wal-rename fault", commitID)
	}
	verifyLegacyGraph(t, loaded)
}

// state-rename fault fires before the state file rename. Neither state nor
// WAL was updated. Recovery reads the original legacy files (commit 0).
func TestMixedOldNewStateAfterCheckpointFaultStateRename(t *testing.T) {
	dbPath := t.TempDir()
	files := DirectoryDatabaseFiles(dbPath)
	graph := buildLegacyGraph(t)
	writeLegacyStateFile(t, files.State, graph, 0, legacyStateVersion)
	writeLegacyWALFile(t, files.WAL, graph, 0, legacyWALVersion)

	injected := errors.New("state-rename fault")
	fault := func(stage string, after bool) error {
		if stage == "state-rename" && !after {
			return injected
		}
		return nil
	}
	err := checkpointGraphStateAndWALFiles(files, graph, 3, 2, 1, 0, fault)
	if !errors.Is(err, injected) {
		t.Fatalf("checkpoint err = %v, want %v", err, injected)
	}

	loaded, _, _, commitID, loadErr := LoadGraphStateFilesContext(context.Background(), files, maxStateFileBytes, ^uint64(0), ^uint64(0))
	if loadErr != nil {
		t.Fatalf("recovery after state-rename fault: %v", loadErr)
	}
	if commitID != 0 {
		t.Fatalf("commit = %d, want 0 after state-rename fault", commitID)
	}
	verifyLegacyGraph(t, loaded)
}

func TestLegacyCheckpointThenDeltaPreservesAllProperties(t *testing.T) {
	dbPath := t.TempDir()
	files := DirectoryDatabaseFiles(dbPath)
	graph := buildLegacyGraph(t)
	writeLegacyStateFile(t, files.State, graph, 0, legacyStateVersion)
	writeLegacyWALFile(t, files.WAL, graph, 0, legacyWALVersion)

	if err := CheckpointGraphStateAndWALFiles(files, graph, 3, 2, 1); err != nil {
		t.Fatal(err)
	}

	graph.Edges.Set(1, &EdgeRecord{
		ID:         1,
		SourceID:   1,
		TargetID:   2,
		Type:       "KNOWS",
		Properties: PropertiesFromMap(map[string]any{"since": int64(2021), "weight": float64(0.9)}),
	})
	if err := AppendWALDeltaFiles(files, graph, 3, 2, 2, GraphDelta{UpsertEdges: []uint64{1}}); err != nil {
		t.Fatal(err)
	}

	loaded, _, _, commitID, err := LoadGraphStateFilesContext(context.Background(), files, maxStateFileBytes, ^uint64(0), ^uint64(0))
	if err != nil {
		t.Fatal(err)
	}
	if commitID != 2 {
		t.Fatalf("commit = %d, want 2", commitID)
	}
	edge := loaded.Edges.Get(1)
	if edge == nil {
		t.Fatal("edge 1 missing")
	}
	if edge.Properties.Get("since") != int64(2021) {
		t.Fatalf("edge since = %v, want 2021", edge.Properties.Get("since"))
	}
	if edge.Properties.Get("weight") != float64(0.9) {
		t.Fatalf("edge weight = %v, want 0.9", edge.Properties.Get("weight"))
	}
}
