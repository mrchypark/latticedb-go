package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestDecodePersistedStateContextCancelsFTSRebuild(t *testing.T) {
	snapshot := persistedState{DatabaseID: "00000000000000000000000000000001", NextNodeID: 1001}
	for id := uint64(1); id <= 1000; id++ {
		snapshot.Nodes = append(snapshot.Nodes, persistedNode{ID: id})
		snapshot.FTS = append(snapshot.FTS, persistedFTS{NodeID: id, Text: "unique token text"})
	}
	ctx := &cancelLoadAfterChecks{limit: 10}
	if _, _, _, _, err := decodePersistedStateContext(ctx, snapshot, ^uint64(0), ^uint64(0)); !errors.Is(err, context.Canceled) {
		t.Fatalf("FTS rebuild cancellation = %v", err)
	}
	if _, _, _, _, err := decodePersistedStateContext(context.Background(), snapshot, ^uint64(0), 1); !errors.Is(err, ErrDerivedIndexResourceLimit) {
		t.Fatalf("FTS rebuild byte limit = %v", err)
	}
}

type cancelLoadAfterChecks struct {
	checks atomic.Int32
	limit  int32
}

func (*cancelLoadAfterChecks) Deadline() (time.Time, bool) { return time.Time{}, false }
func (*cancelLoadAfterChecks) Done() <-chan struct{}       { return nil }
func (ctx *cancelLoadAfterChecks) Err() error {
	if ctx.checks.Add(1) >= ctx.limit {
		return context.Canceled
	}
	return nil
}
func (*cancelLoadAfterChecks) Value(any) any { return nil }

func TestLoadGraphStateRecoversLatestCommitFromWAL(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "recover.ltdb")

	empty := NewGraphState()
	if err := CheckpointGraphState(dbPath, empty, 1, 1, 0); err != nil {
		t.Fatalf("checkpoint initial state: %v", err)
	}

	committed := NewGraphState()
	committed.Nodes.Set(1, &NodeRecord{
		ID:         1,
		Labels:     []string{"Person"},
		Properties: map[string]any{"name": "Alice"},
	})
	if err := AppendWALCommit(dbPath, committed, 2, 1, 1); err != nil {
		t.Fatalf("append wal commit: %v", err)
	}

	if err := SimulateCrash(dbPath); err != nil {
		t.Fatalf("simulate crash: %v", err)
	}

	graph, nextNodeID, nextEdgeID, commitID, err := LoadGraphState(dbPath)
	if err != nil {
		t.Fatalf("load recovered state: %v", err)
	}
	if commitID != 1 {
		t.Fatalf("unexpected recovered commit id %d", commitID)
	}
	if nextNodeID != 2 || nextEdgeID != 1 {
		t.Fatalf("unexpected recovered id counters node=%d edge=%d", nextNodeID, nextEdgeID)
	}
	node := graph.Nodes.Get(1)
	if node == nil {
		t.Fatalf("expected recovered node 1")
	}
	if got := node.Properties["name"]; got != "Alice" {
		t.Fatalf("unexpected recovered property %#v", got)
	}
}

func TestSnapshotEstimatorIsConservativeForStreamedPayload(t *testing.T) {
	graph := NewGraphState()
	graph.DatabaseID = "0123456789abcdef0123456789abcdef"
	for id := uint64(1); id <= 100; id++ {
		graph.Nodes.Set(id, &NodeRecord{
			ID:     id,
			Labels: []string{"문서", "item"},
			Properties: map[string]any{
				"bytes":  []byte{0, 1, 2, byte(id)},
				"nested": map[string]any{"list": []any{int64(id), "quoted\nvalue", true}},
				"vector": []float32{float32(id), float32(id) / 3},
			},
		})
		graph.FTS.Set(id, &FTSRecord{Text: "한글 quoted text", Tokens: []string{"한글", "quoted", "text"}})
	}
	for id := uint64(1); id < 100; id++ {
		graph.Edges.Set(id, &EdgeRecord{ID: id, SourceID: id, TargetID: id + 1, Type: "연결", Properties: map[string]any{"weight": float64(id) / 7}})
	}
	estimated, err := EstimateSnapshotBytes(graph)
	if err != nil {
		t.Fatal(err)
	}
	var payload bytes.Buffer
	if err := writePersistedStateJSON(&payload, graph, 101, 100, 999); err != nil {
		t.Fatal(err)
	}
	if uint64(payload.Len()) > estimated {
		t.Fatalf("streamed payload = %d bytes, conservative bound = %d", payload.Len(), estimated)
	}
	graph.SnapshotBytes = estimated
	updated := CloneGraphStateShallow(graph)
	node := updated.Nodes.Get(100)
	updated.Nodes.CloneShardOnce(100)
	updated.Nodes.Set(100, &NodeRecord{ID: node.ID, Labels: node.Labels, Properties: map[string]any{"short": "x"}})
	updatedSize, err := ApplyDeltaSnapshotBytes(graph, updated, GraphDelta{UpsertNodes: []uint64{100}})
	if err != nil {
		t.Fatal(err)
	}
	payload.Reset()
	if err := writePersistedStateJSON(&payload, updated, 101, 100, 1000); err != nil {
		t.Fatal(err)
	}
	if uint64(payload.Len()) > updatedSize {
		t.Fatalf("updated payload = %d bytes, conservative bound = %d", payload.Len(), updatedSize)
	}
}

func TestSnapshotEstimatorRandomizedDeltasNeverUndercount(t *testing.T) {
	random := rand.New(rand.NewSource(1))
	graph := NewGraphState()
	graph.DatabaseID = "0123456789abcdef0123456789abcdef"
	for step := 0; step < 2000; step++ {
		estimate, err := EstimateSnapshotBytes(graph)
		if err != nil {
			t.Fatal(err)
		}
		graph.SnapshotBytes = estimate
		next := CloneGraphStateShallow(graph)
		id := uint64(random.Intn(128) + 1)
		delta := GraphDelta{}
		switch random.Intn(6) {
		case 0, 1:
			next.Nodes.CloneShardOnce(id)
			next.Nodes.Set(id, &NodeRecord{ID: id, Labels: []string{"문서", fmt.Sprintf("l%d", step)}, Properties: map[string]any{"text": fmt.Sprintf("quoted\\\"\n%d한글", random.Uint64()), "nested": map[string]any{"items": []any{int64(step), true, []byte{0, byte(step)}}}, "vector": []float32{float32(step), float32(step) / 3}}})
			delta.UpsertNodes = []uint64{id}
		case 2:
			next.Nodes.CloneShardOnce(id)
			next.Nodes.Delete(id)
			delta.DeleteNodes = []uint64{id}
		case 3:
			next.FTS.CloneShardOnce(id)
			next.FTS.Set(id, &FTSRecord{Text: fmt.Sprintf("token %d 한글", random.Uint64())})
			delta.UpsertFTS = []uint64{id}
		case 4:
			next.FTS.CloneShardOnce(id)
			next.FTS.Delete(id)
			delta.DeleteFTS = []uint64{id}
		case 5:
			next.Edges.CloneShardOnce(id)
			next.Edges.Set(id, &EdgeRecord{ID: id, SourceID: id, TargetID: id + 1, Type: "연결", Properties: map[string]any{"n": int64(step)}})
			delta.UpsertEdges = []uint64{id}
		}
		updated, err := ApplyDeltaSnapshotBytes(graph, next, delta)
		if err != nil {
			t.Fatal(err)
		}
		var payload bytes.Buffer
		if err := writePersistedStateJSON(&payload, next, 129, 129, uint64(step)); err != nil {
			t.Fatal(err)
		}
		if uint64(payload.Len()) > updated {
			t.Fatalf("step %d payload=%d estimate=%d", step, payload.Len(), updated)
		}
		next.SnapshotBytes = updated
		graph = next
	}
}

func TestLoadGraphStateUsesWALWhenCheckpointIsCorrupt(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "corrupt-checkpoint.ltdb")
	graph := NewGraphState()
	graph.Nodes.Set(1, &NodeRecord{ID: 1, Labels: []string{"Person"}, Properties: map[string]any{}})
	if err := AppendWALCommit(dbPath, graph, 2, 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stateFilePath(dbPath), []byte("not json"), 0o644); err != nil {
		t.Fatal(err)
	}

	recovered, nextNodeID, _, commitID, err := LoadGraphState(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Nodes.Get(1) == nil || nextNodeID != 2 || commitID != 1 {
		t.Fatalf("unexpected recovered state: nodes=%v nextNodeID=%d commitID=%d", recovered.Nodes, nextNodeID, commitID)
	}
}

func TestLoadGraphStateRepairsStaleIDCounters(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "stale-ids.ltdb")
	graph := NewGraphState()
	graph.Nodes.Set(9, &NodeRecord{ID: 9, Labels: []string{"Person"}, Properties: map[string]any{}})
	if err := CheckpointGraphState(dbPath, graph, 0, 0, 1); err != nil {
		t.Fatal(err)
	}

	_, nextNodeID, nextEdgeID, _, err := LoadGraphState(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if nextNodeID != 10 || nextEdgeID != 1 {
		t.Fatalf("unexpected repaired counters: node=%d edge=%d", nextNodeID, nextEdgeID)
	}
}

func TestLoadGraphStateRejectsDanglingEdge(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "dangling-edge.ltdb")
	graph := NewGraphState()
	graph.Edges.Set(1, &EdgeRecord{ID: 1, SourceID: 1, TargetID: 2, Type: "KNOWS", Properties: map[string]any{}})
	if err := CheckpointGraphState(dbPath, graph, 1, 2, 1); err != nil {
		t.Fatal(err)
	}

	if _, _, _, _, err := LoadGraphState(dbPath); err == nil {
		t.Fatal("expected dangling edge to be rejected")
	}
}

func TestLoadGraphStateIgnoresIncompleteWALTail(t *testing.T) {
	dbPath := t.TempDir()
	graph := NewGraphState()
	graph.Nodes.Set(1, &NodeRecord{ID: 1, Properties: map[string]any{}})
	if err := AppendWALCommit(dbPath, graph, 2, 1, 1); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(walFilePath(dbPath), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString(`{"commit_id":2`); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	loaded, _, _, commitID, err := LoadGraphState(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if commitID != 1 || loaded.Nodes.Get(1) == nil {
		t.Fatalf("recovered commit %d with nodes %#v", commitID, loaded.Nodes)
	}
}

func TestLoadGraphStateRejectsInteriorWALCorruption(t *testing.T) {
	dbPath := t.TempDir()
	if err := os.WriteFile(walFilePath(dbPath), []byte("not-json\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := LoadGraphState(dbPath); err == nil {
		t.Fatal("expected corrupt WAL to fail")
	}
}

func TestWALV2TruncationAndCorruption(t *testing.T) {
	graph := NewGraphState()
	graph.Nodes.Set(1, &NodeRecord{ID: 1, Properties: map[string]any{}})
	snapshot, err := buildPersistedState(graph, 2, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	record, err := encodeWALRecord(snapshot, payload)
	if err != nil {
		t.Fatal(err)
	}

	for offset := 0; offset < len(record); offset++ {
		dbPath := t.TempDir()
		if err := os.WriteFile(walFilePath(dbPath), record[:offset], 0o600); err != nil {
			t.Fatal(err)
		}
		if _, _, _, _, err := LoadGraphState(dbPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("truncate at %d: error = %v, want os.ErrNotExist", offset, err)
		}
	}

	corrupt := bytes.Clone(record)
	corrupt[len(corrupt)-1] ^= 1
	dbPath := t.TempDir()
	if err := os.WriteFile(walFilePath(dbPath), corrupt, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := LoadGraphState(dbPath); err == nil {
		t.Fatal("expected checksum corruption to fail")
	}
}

func TestValidCheckpointDoesNotHideCorruptWAL(t *testing.T) {
	path := t.TempDir()
	graph := NewGraphState()
	if err := CheckpointGraphState(path, graph, 1, 1, 0); err != nil {
		t.Fatal(err)
	}
	if err := AppendWALCommit(path, graph, 1, 1, 0); err != nil {
		t.Fatal(err)
	}
	wal, err := os.ReadFile(walFilePath(path))
	if err != nil {
		t.Fatal(err)
	}
	wal[len(wal)-1] ^= 1
	if err := os.WriteFile(walFilePath(path), wal, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := LoadGraphState(path); err == nil {
		t.Fatal("expected corrupt WAL to fail despite valid checkpoint")
	}
}

func TestWALRejectsOversizedDeclaredLengthWithoutAllocation(t *testing.T) {
	path := t.TempDir()
	header := make([]byte, walHeaderSize)
	copy(header, walMagic[:])
	binary.BigEndian.PutUint16(header[8:10], storageVersion)
	binary.BigEndian.PutUint16(header[10:12], walHeaderSize)
	binary.BigEndian.PutUint64(header[20:28], maxWALFrameBytes+1)
	if err := os.WriteFile(walFilePath(path), header, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := LoadGraphState(path); err == nil {
		t.Fatal("expected oversized WAL frame to fail")
	}
}

func TestWALV2RejectsCommitRegressionAndDatabaseMismatch(t *testing.T) {
	graph := NewGraphState()
	first, err := buildPersistedState(graph, 1, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	encode := func(snapshot persistedState) []byte {
		payload, err := json.Marshal(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		record, err := encodeWALRecord(snapshot, payload)
		if err != nil {
			t.Fatal(err)
		}
		return record
	}
	other := NewGraphState()
	mismatch, err := buildPersistedState(other, 1, 1, 2)
	if err != nil {
		t.Fatal(err)
	}

	for name, second := range map[string]persistedState{
		"commit regression": first,
		"database mismatch": mismatch,
	} {
		t.Run(name, func(t *testing.T) {
			dbPath := t.TempDir()
			data := append(encode(first), encode(second)...)
			if err := os.WriteFile(walFilePath(dbPath), data, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, _, _, _, err := LoadGraphState(dbPath); err == nil {
				t.Fatal("expected invalid WAL history to fail")
			}
		})
	}
}

func TestWALV2ReplaysDeltasWithoutCheckpoint(t *testing.T) {
	dbPath := t.TempDir()
	graph := NewGraphState()
	if err := CheckpointGraphState(dbPath, graph, 1, 1, 0); err != nil {
		t.Fatal(err)
	}
	if err := AppendWALCommit(dbPath, graph, 1, 1, 0); err != nil {
		t.Fatal(err)
	}
	graph.Nodes.Set(1, &NodeRecord{ID: 1, Properties: map[string]any{"value": int64(1)}})
	if err := AppendWALDelta(dbPath, graph, 2, 1, 1, GraphDelta{UpsertNodes: []uint64{1}}); err != nil {
		t.Fatal(err)
	}
	graph.Nodes.Set(1, &NodeRecord{ID: 1, Properties: map[string]any{"value": int64(2)}})
	if err := AppendWALDelta(dbPath, graph, 2, 1, 2, GraphDelta{UpsertNodes: []uint64{1}}); err != nil {
		t.Fatal(err)
	}
	if err := SimulateCrash(dbPath); err != nil {
		t.Fatal(err)
	}
	loaded, nextNodeID, _, commitID, err := LoadGraphState(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if commitID != 2 || nextNodeID != 2 || loaded.Nodes.Get(1).Properties["value"] != int64(2) {
		t.Fatalf("recovered commit=%d next=%d graph=%#v", commitID, nextNodeID, loaded)
	}
}

func TestWALV2RejectsSemanticallyInvalidDelta(t *testing.T) {
	base := NewGraphState()
	base.Nodes.Set(1, &NodeRecord{ID: 1, Properties: map[string]any{}})
	base.Nodes.Set(2, &NodeRecord{ID: 2, Properties: map[string]any{}})
	base.Edges.Set(1, &EdgeRecord{ID: 1, SourceID: 1, TargetID: 2, Properties: map[string]any{}})
	base.FTS.Set(1, &FTSRecord{Text: "one"})
	snapshot, err := buildPersistedState(base, 3, 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	encode := func(payload walPayload, databaseID string, commitID uint64) []byte {
		data, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		header, err := encodeWALHeader(databaseID, commitID, data)
		if err != nil {
			t.Fatal(err)
		}
		return append(header[:], data...)
	}
	baseRecord := encode(walPayload{Kind: "snapshot", Snapshot: &snapshot}, snapshot.DatabaseID, 1)
	emptyNode := persistedNode{ID: 3, Properties: map[string]persistedValue{}}
	for name, delta := range map[string]persistedDelta{
		"duplicate operation": {
			DatabaseID: snapshot.DatabaseID, CommitID: 2, NextNodeID: 4, NextEdgeID: 2,
			UpsertNodes: []persistedNode{emptyNode, emptyNode},
		},
		"node with incident edge": {
			DatabaseID: snapshot.DatabaseID, CommitID: 2, NextNodeID: 3, NextEdgeID: 2,
			DeleteNodes: []uint64{1}, DeleteFTS: []uint64{1},
		},
		"orphan FTS": {
			DatabaseID: snapshot.DatabaseID, CommitID: 2, NextNodeID: 3, NextEdgeID: 2,
			UpsertFTS: []persistedFTS{{NodeID: 9, Text: "missing"}},
		},
		"ID high-water regression": {
			DatabaseID: snapshot.DatabaseID, CommitID: 2, NextNodeID: 2, NextEdgeID: 2,
		},
	} {
		t.Run(name, func(t *testing.T) {
			dbPath := t.TempDir()
			data := append(bytes.Clone(baseRecord), encode(walPayload{Kind: "delta", Delta: &delta}, snapshot.DatabaseID, 2)...)
			if err := os.WriteFile(walFilePath(dbPath), data, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, _, _, _, err := LoadGraphState(dbPath); err == nil {
				t.Fatal("expected invalid delta to fail")
			}
		})
	}
}

func TestCompactionCrashMatrixPreservesAcknowledgedCommit(t *testing.T) {
	buildPath := t.TempDir()
	oldGraph := NewGraphState()
	oldGraph.Nodes.Set(1, &NodeRecord{ID: 1, Properties: map[string]any{"version": int64(1)}})
	if err := CheckpointGraphState(buildPath, oldGraph, 2, 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := AppendWALCommit(buildPath, oldGraph, 2, 1, 1); err != nil {
		t.Fatal(err)
	}
	newGraph := CloneGraphState(oldGraph)
	newGraph.Nodes.Get(1).Properties["version"] = int64(2)
	if err := AppendWALDelta(buildPath, newGraph, 2, 1, 2, GraphDelta{UpsertNodes: []uint64{1}}); err != nil {
		t.Fatal(err)
	}
	oldCheckpoint, err := os.ReadFile(stateFilePath(buildPath))
	if err != nil {
		t.Fatal(err)
	}
	oldWAL, err := os.ReadFile(walFilePath(buildPath))
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckpointGraphState(buildPath, newGraph, 2, 1, 2); err != nil {
		t.Fatal(err)
	}
	newCheckpoint, err := os.ReadFile(stateFilePath(buildPath))
	if err != nil {
		t.Fatal(err)
	}
	base, err := buildPersistedState(newGraph, 2, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	basePayload, err := json.Marshal(walPayload{Kind: "snapshot", Snapshot: &base})
	if err != nil {
		t.Fatal(err)
	}
	baseHeader, err := encodeWALHeader(base.DatabaseID, 2, basePayload)
	if err != nil {
		t.Fatal(err)
	}
	partialBase := append(baseHeader[:], basePayload[:len(basePayload)/2]...)
	if err := CheckpointGraphStateAndWAL(buildPath, newGraph, 2, 1, 2); err != nil {
		t.Fatal(err)
	}
	newWAL, err := os.ReadFile(walFilePath(buildPath))
	if err != nil {
		t.Fatal(err)
	}
	partialCheckpoint := newCheckpoint[:len(newCheckpoint)/2]

	for name, files := range map[string]struct{ checkpoint, wal []byte }{
		"before checkpoint publish":         {oldCheckpoint, oldWAL},
		"new checkpoint old WAL":            {newCheckpoint, oldWAL},
		"old checkpoint new WAL":            {oldCheckpoint, newWAL},
		"partial checkpoint old WAL":        {partialCheckpoint, oldWAL},
		"partial checkpoint new WAL":        {partialCheckpoint, newWAL},
		"new checkpoint after WAL truncate": {newCheckpoint, nil},
		"new checkpoint partial WAL":        {newCheckpoint, partialBase},
	} {
		t.Run(name, func(t *testing.T) {
			path := t.TempDir()
			if err := os.WriteFile(stateFilePath(path), files.checkpoint, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(walFilePath(path), files.wal, 0o600); err != nil {
				t.Fatal(err)
			}
			graph, _, _, commitID, err := LoadGraphState(path)
			if err != nil {
				t.Fatal(err)
			}
			if commitID != 2 || graph.Nodes.Get(1).Properties["version"] != int64(2) {
				t.Fatalf("recovered commit %d graph %#v", commitID, graph)
			}
		})
	}
}

func TestPersistentFileSizeLimitsAreCheckedBeforeRead(t *testing.T) {
	path := t.TempDir()
	state, err := os.Create(stateFilePath(path))
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Truncate(maxStateFileBytes + 1); err != nil {
		t.Fatal(err)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := loadCheckpointSnapshot(path); err == nil {
		t.Fatal("oversized state file was accepted")
	}

	ids, err := os.Create(idsFilePath(path))
	if err != nil {
		t.Fatal(err)
	}
	if err := ids.Truncate(maxIDsFileBytes + 1); err != nil {
		t.Fatal(err)
	}
	if err := ids.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := LoadIDReservation(path, "00000000000000000000000000000000"); err == nil {
		t.Fatal("oversized ID reservation was accepted")
	}
}

func TestWALWriterUsesReaderFrameLimit(t *testing.T) {
	if err := validateWALPayloadSize(maxWALFrameBytes); err != nil {
		t.Fatalf("reader limit rejected: %v", err)
	}
	if err := validateWALPayloadSize(maxWALFrameBytes + 1); err == nil {
		t.Fatal("writer accepted a frame above the reader limit")
	}
}

func TestIDReservationEnvelopeRejectsCorruptionAndDatabaseMismatch(t *testing.T) {
	path := t.TempDir()
	graph := NewGraphState()
	if err := CheckpointGraphState(path, graph, 1, 1, 0); err != nil {
		t.Fatal(err)
	}
	if err := ReserveIDs(path, graph.DatabaseID, 100, 200); err != nil {
		t.Fatal(err)
	}
	if node, edge, err := LoadIDReservation(path, graph.DatabaseID); err != nil || node != 100 || edge != 200 {
		t.Fatalf("reservation node=%d edge=%d err=%v", node, edge, err)
	}
	data, err := os.ReadFile(idsFilePath(path))
	if err != nil {
		t.Fatal(err)
	}
	var ids persistedIDs
	if err := json.Unmarshal(data, &ids); err != nil {
		t.Fatal(err)
	}
	ids.DatabaseID = "00000000000000000000000000000000"
	ids.Checksum = checksumIDs(ids.DatabaseID, ids.NextNodeID, ids.NextEdgeID)
	data, err = json.Marshal(ids)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(idsFilePath(path), data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := LoadIDReservation(path, graph.DatabaseID); err == nil {
		t.Fatal("expected database ID mismatch")
	}
	ids.DatabaseID = graph.DatabaseID
	ids.Checksum++
	data, err = json.Marshal(ids)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(idsFilePath(path), data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := LoadIDReservation(path, graph.DatabaseID); err == nil {
		t.Fatal("expected reservation checksum mismatch")
	}
}
