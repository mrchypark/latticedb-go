package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
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

func TestWALAccumulatorCanonicalizesWithoutDerivedIndexes(t *testing.T) {
	snapshot := persistedState{DatabaseID: "00000000000000000000000000000001", NextNodeID: 1001}
	for id := uint64(1); id <= 1000; id++ {
		snapshot.Nodes = append(snapshot.Nodes, persistedNode{ID: id})
		snapshot.FTS = append(snapshot.FTS, persistedFTS{NodeID: id, Text: "unique token text"})
	}
	if _, err := newWALAccumulator(context.Background(), snapshot); err != nil {
		t.Fatalf("canonical WAL accumulator: %v", err)
	}
	if _, _, _, _, err := decodePersistedStateContext(context.Background(), snapshot, ^uint64(0), 1); !errors.Is(err, ErrDerivedIndexResourceLimit) {
		t.Fatalf("final derived-index decode = %v", err)
	}
}

func TestCleanupDatabaseTempFilesScopesFlatDatabases(t *testing.T) {
	dir := t.TempDir()
	flat := FlatDatabaseFiles(filepath.Join(dir, "one.ltdb"))
	scoped, err := os.CreateTemp(dir, databaseTempPattern(flat, "state"))
	if err != nil {
		t.Fatal(err)
	}
	if err := scoped.Close(); err != nil {
		t.Fatal(err)
	}
	legacy := filepath.Join(dir, ".state-other.tmp")
	if err := os.WriteFile(legacy, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := CleanupDatabaseTempFiles(flat, false); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(scoped.Name()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("scoped temp still exists: %v", err)
	}
	if _, err := os.Stat(legacy); err != nil {
		t.Fatalf("flat cleanup removed another database temp: %v", err)
	}
	colliding := FlatDatabaseFiles(filepath.Join(dir, "one.ltdb-state-other"))
	other, err := os.CreateTemp(dir, databaseTempPattern(colliding, "state"))
	if err != nil {
		t.Fatal(err)
	}
	if err := other.Close(); err != nil {
		t.Fatal(err)
	}
	if err := CleanupDatabaseTempFiles(flat, false); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(other.Name()); err != nil {
		t.Fatalf("flat cleanup removed prefix-colliding database temp: %v", err)
	}

	directory := DirectoryDatabaseFiles(filepath.Join(dir, "directory"))
	if err := os.MkdirAll(directory.Directory, 0o700); err != nil {
		t.Fatal(err)
	}
	legacy = filepath.Join(directory.Directory, ".wal-abandoned.tmp")
	if err := os.WriteFile(legacy, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := CleanupDatabaseTempFiles(directory, true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(legacy); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("directory legacy temp still exists: %v", err)
	}
}

func TestCreateCheckpointGraphStateFilesPublishesOnce(t *testing.T) {
	files := FlatDatabaseFiles(filepath.Join(t.TempDir(), "backup.ltdb"))
	first, second := NewGraphState(), NewGraphState()
	first.Nodes.Set(1, &NodeRecord{ID: 1, Properties: map[string]any{"source": "first"}})
	second.Nodes.Set(1, &NodeRecord{ID: 1, Properties: map[string]any{"source": "second"}})
	start := make(chan struct{})
	results := make(chan error, 2)
	for index, graph := range []*GraphState{first, second} {
		go func() {
			<-start
			results <- CreateCheckpointGraphStateFiles(files, graph, 2, 1, uint64(index+1))
		}()
	}
	close(start)
	succeeded, existed := 0, 0
	for range 2 {
		err := <-results
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, os.ErrExist):
			existed++
		default:
			t.Fatalf("publish error = %v", err)
		}
	}
	if succeeded != 1 || existed != 1 {
		t.Fatalf("publish results: success=%d exists=%d", succeeded, existed)
	}
	graph, _, _, _, err := LoadGraphStateFilesContext(context.Background(), files, ^uint64(0), ^uint64(0), ^uint64(0))
	if err != nil || graph.Nodes.Get(1) == nil {
		t.Fatalf("published backup is unreadable: graph=%#v err=%v", graph, err)
	}
}

func TestPreparedCheckpointPublishFaultMatrix(t *testing.T) {
	stages := []string{
		"wal-snapshot-rename", "wal-snapshot-dir-sync",
		"state-rename", "state-dir-sync",
		"wal-marker-rename", "wal-marker-dir-sync",
		"wal-base-remove", "wal-base-dir-sync",
	}
	for _, flat := range []bool{false, true} {
		for _, stage := range stages {
			for _, after := range []bool{false, true} {
				name := fmt.Sprintf("flat=%v/%s/after=%v", flat, stage, after)
				t.Run(name, func(t *testing.T) {
					dir := t.TempDir()
					var files DatabaseFiles
					if flat {
						files = FlatDatabaseFiles(filepath.Join(dir, "database.ltdb"))
					} else {
						files = DirectoryDatabaseFiles(filepath.Join(dir, "database.ltdb"))
					}
					first := NewGraphState()
					first.DatabaseID = "00000000000000000000000000000001"
					first.Nodes.Set(1, &NodeRecord{ID: 1, Properties: map[string]any{"revision": int64(1)}})
					second := CloneGraphState(first)
					second.Nodes.Set(1, &NodeRecord{ID: 1, Properties: map[string]any{"revision": int64(2)}})
					if err := CheckpointGraphStateAndCompactWALFiles(files, first, 2, 1, 1, 1); err != nil {
						t.Fatal(err)
					}
					prepared, err := PrepareCheckpointFiles(files, second, 2, 1, 2)
					if err != nil {
						t.Fatal(err)
					}
					injected := errors.New("injected prepared publish fault")
					fault := func(gotStage string, gotAfter bool) error {
						if gotStage == stage && gotAfter == after {
							return injected
						}
						return nil
					}
					if err := prepared.PublishCheckpointFilesWithFault(files, fault); !errors.Is(err, injected) {
						t.Fatalf("publish fault = %v, want %v", err, injected)
					}
					if err := prepared.Cleanup(); err != nil {
						t.Fatal(err)
					}
					graph, _, _, commitID, err := LoadGraphStateFilesContext(context.Background(), files, maxStateFileBytes, ^uint64(0), ^uint64(0))
					if err != nil {
						t.Fatalf("reopen after %s after=%v: %v", stage, after, err)
					}
					expectedCommit := uint64(2)
					if stage == "wal-snapshot-rename" && !after {
						expectedCommit = 1
					}
					if commitID != expectedCommit {
						t.Fatalf("recovered commit ID = %d, want %d", commitID, expectedCommit)
					}
					if got := graph.Nodes.Get(1).Properties["revision"]; got != int64(expectedCommit) {
						t.Fatalf("recovered revision = %v, want %d", got, expectedCommit)
					}
				})
			}
		}
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

func TestLoadGraphStateRecoveryBudgetsAreCumulative(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recovery-budget.ltdb")
	empty := NewGraphState()
	if err := CheckpointGraphState(path, empty, 1, 1, 0); err != nil {
		t.Fatal(err)
	}
	for commit := uint64(1); commit <= 2; commit++ {
		graph := NewGraphState()
		graph.DatabaseID = empty.DatabaseID
		graph.Nodes.Set(commit, &NodeRecord{ID: commit})
		if err := AppendWALCommit(path, graph, commit+1, 1, commit); err != nil {
			t.Fatal(err)
		}
	}
	files := DirectoryDatabaseFiles(path)
	if _, _, _, _, err := LoadGraphStateFilesContextWithRecoveryLimits(context.Background(), files, ^uint64(0), ^uint64(0), ^uint64(0), RecoveryLimits{MaxFrames: 2, MaxWork: 10}); err != nil {
		t.Fatalf("boundary recovery load: %v", err)
	}
	for name, limits := range map[string]RecoveryLimits{
		"frames": {MaxFrames: 1, MaxWork: 10},
		"work":   {MaxFrames: 2, MaxWork: 1},
		"bytes":  {MaxFrames: 2, MaxWork: 10, MaxDecodedBytes: 1},
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, _, _, err := LoadGraphStateFilesContextWithRecoveryLimits(context.Background(), files, ^uint64(0), ^uint64(0), ^uint64(0), limits); !errors.Is(err, ErrLoadResourceLimit) {
				t.Fatalf("recovery error = %v, want ErrLoadResourceLimit", err)
			}
		})
	}
}

func TestLoadGraphStateUsesWALBaseWhenCurrentWALIsMissing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal-base-only.ltdb")
	base := NewGraphState()
	if err := EnsureDatabaseID(base); err != nil {
		t.Fatal(err)
	}
	if err := CheckpointGraphState(path, base, 1, 1, 0); err != nil {
		t.Fatal(err)
	}
	committed := NewGraphState()
	committed.DatabaseID = base.DatabaseID
	committed.Nodes.Set(1, &NodeRecord{ID: 1})
	if err := AppendWALCommit(path, committed, 2, 1, 1); err != nil {
		t.Fatal(err)
	}
	files := DirectoryDatabaseFiles(path)
	if err := os.Rename(files.WAL, files.WALBase); err != nil {
		t.Fatal(err)
	}

	graph, _, _, commitID, err := LoadGraphStateFilesContext(context.Background(), files, ^uint64(0), ^uint64(0), ^uint64(0))
	if err != nil {
		t.Fatal(err)
	}
	if commitID != 1 || graph.Nodes.Get(1) == nil {
		t.Fatalf("recovered WAL base state = commit %d, node=%v", commitID, graph.Nodes.Get(1))
	}
}

func TestLoadGraphStateReplaysWALBaseChain(t *testing.T) {
	const databaseID = "00000000000000000000000000000001"
	const stateCommit, baseCommit, activeCommit = uint64(3), uint64(5), uint64(6)

	writeChain := func(t *testing.T, active bool) (DatabaseFiles, uint64, uint64) {
		t.Helper()
		path := t.TempDir()
		files := DirectoryDatabaseFiles(path)
		state := NewGraphState()
		state.DatabaseID = databaseID
		stateData, err := SerializeGraphState(state, 1, 1, stateCommit)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(files.State, stateData, 0o600); err != nil {
			t.Fatal(err)
		}
		delta := func(commitID, nodeID uint64) []byte {
			return persistedWALTestRecord(t, databaseID, commitID, walPayload{Kind: "delta", Delta: &persistedDelta{
				DatabaseID: databaseID, CommitID: commitID, NextNodeID: nodeID + 1, NextEdgeID: 1,
				UpsertNodes: []persistedNode{{ID: nodeID, Properties: map[string]persistedValue{}}},
			}})
		}
		base := persistedWALTestRecord(t, databaseID, stateCommit, walPayload{Kind: "checkpoint"})
		base = append(base, delta(4, 1)...)
		base = append(base, delta(baseCommit, 2)...)
		if err := os.WriteFile(files.WALBase, base, 0o600); err != nil {
			t.Fatal(err)
		}
		activeData := persistedWALTestRecord(t, databaseID, baseCommit, walPayload{Kind: "checkpoint"})
		activeData = append(activeData, delta(activeCommit, 3)...)
		if active {
			if err := os.WriteFile(files.WAL, activeData, 0o600); err != nil {
				t.Fatal(err)
			}
		}
		decodedBytes := uint64(len(stateData) - stateHeaderSize)
		decodedBytes += uint64(len(base) - 3*walHeaderSize)
		decodedBytes += uint64(len(activeData) - 2*walHeaderSize)
		return files, decodedBytes, 8
	}

	t.Run("repeated chain", func(t *testing.T) {
		files, _, _ := writeChain(t, true)
		graph, nextNodeID, _, commitID, err := LoadGraphStateFilesContext(context.Background(), files, ^uint64(0), ^uint64(0), ^uint64(0))
		if err != nil {
			t.Fatal(err)
		}
		if commitID != activeCommit || nextNodeID != 4 {
			t.Fatalf("recovered commit=%d next node=%d", commitID, nextNodeID)
		}
		for nodeID := uint64(1); nodeID <= 3; nodeID++ {
			if graph.Nodes.Get(nodeID) == nil {
				t.Fatalf("missing recovered node %d", nodeID)
			}
		}
	})

	t.Run("active WAL missing after rotation", func(t *testing.T) {
		files, _, _ := writeChain(t, false)
		graph, _, _, commitID, err := LoadGraphStateFilesContext(context.Background(), files, ^uint64(0), ^uint64(0), ^uint64(0))
		if err != nil {
			t.Fatal(err)
		}
		if commitID != baseCommit || graph.Nodes.Get(1) == nil || graph.Nodes.Get(2) == nil || graph.Nodes.Get(3) != nil {
			t.Fatalf("recovered rotated base commit=%d nodes=%v,%v,%v", commitID, graph.Nodes.Get(1), graph.Nodes.Get(2), graph.Nodes.Get(3))
		}
	})

	for name, limits := range map[string]RecoveryLimits{
		"frames below boundary": {MaxFrames: 4},
		"work below boundary":   {MaxWork: 7},
		"bytes below boundary":  {MaxDecodedBytes: 1},
	} {
		t.Run(name, func(t *testing.T) {
			files, _, _ := writeChain(t, true)
			if _, _, _, _, err := LoadGraphStateFilesContextWithRecoveryLimits(context.Background(), files, ^uint64(0), ^uint64(0), ^uint64(0), limits); !errors.Is(err, ErrLoadResourceLimit) {
				t.Fatalf("recovery error=%v, want ErrLoadResourceLimit", err)
			}
		})
	}
	files, decodedBytes, work := writeChain(t, true)
	if _, _, _, _, err := LoadGraphStateFilesContextWithRecoveryLimits(context.Background(), files, ^uint64(0), ^uint64(0), ^uint64(0), RecoveryLimits{MaxDecodedBytes: decodedBytes, MaxFrames: 5, MaxWork: work}); err != nil {
		t.Fatalf("recovery boundary load: %v", err)
	}
}

func TestLoadGraphStateRejectsMixedRecoveryDatabaseIDs(t *testing.T) {
	const databaseA = "00000000000000000000000000000001"
	const databaseB = "00000000000000000000000000000002"
	writeState := func(t *testing.T, files DatabaseFiles, databaseID string, commitID uint64) {
		t.Helper()
		graph := NewGraphState()
		graph.DatabaseID = databaseID
		data, err := SerializeGraphState(graph, 1, 1, commitID)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(files.State, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeWALSnapshot := func(t *testing.T, path, databaseID string, commitID uint64) {
		t.Helper()
		snapshot := persistedState{DatabaseID: databaseID, CommitID: commitID, NextNodeID: 1, NextEdgeID: 1}
		if err := os.WriteFile(path, persistedWALTestRecord(t, databaseID, commitID, walPayload{Kind: "snapshot", Snapshot: &snapshot}), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("checkpoint and stale base", func(t *testing.T) {
		files := DirectoryDatabaseFiles(t.TempDir())
		writeState(t, files, databaseA, 3)
		writeWALSnapshot(t, files.WALBase, databaseB, 2)
		if _, _, _, _, err := LoadGraphStateFilesContext(context.Background(), files, ^uint64(0), ^uint64(0), ^uint64(0)); err == nil {
			t.Fatal("mixed checkpoint and wal.base IDs were accepted")
		}
	})

	t.Run("WAL candidates without checkpoint", func(t *testing.T) {
		files := DirectoryDatabaseFiles(t.TempDir())
		writeWALSnapshot(t, files.WALBase, databaseA, 2)
		writeWALSnapshot(t, files.WAL, databaseB, 3)
		if _, _, _, _, err := LoadGraphStateFilesContext(context.Background(), files, ^uint64(0), ^uint64(0), ^uint64(0)); err == nil {
			t.Fatal("mixed WAL IDs without checkpoint were accepted")
		}
	})
}

func TestRecoveryBudgetCountsDistinctSameCommitStates(t *testing.T) {
	const databaseID = "00000000000000000000000000000001"
	first := &persistedState{DatabaseID: databaseID, CommitID: 1, Nodes: []persistedNode{{ID: 1}}}
	second := &persistedState{DatabaseID: databaseID, CommitID: 1, Nodes: []persistedNode{{ID: 1}, {ID: 2}}}
	budget := &recoveryBudget{limits: RecoveryLimits{MaxWork: persistedStateWork(*first)}}
	if err := budget.replayState(first); err != nil {
		t.Fatal(err)
	}
	if err := budget.replayState(first); err != nil {
		t.Fatalf("same state was charged twice: %v", err)
	}
	if err := budget.replayState(second); !errors.Is(err, ErrLoadResourceLimit) {
		t.Fatalf("distinct same-commit state error = %v, want ErrLoadResourceLimit", err)
	}
}

func TestLoadGraphStateSkipsStaleWALBaseAfterRotation(t *testing.T) {
	const databaseID = "00000000000000000000000000000001"
	files := DirectoryDatabaseFiles(t.TempDir())
	state := NewGraphState()
	state.DatabaseID = databaseID
	state.Nodes.Set(1, &NodeRecord{ID: 1})
	data, err := SerializeGraphState(state, 2, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(files.State, data, 0o600); err != nil {
		t.Fatal(err)
	}
	staleBase := persistedWALTestRecord(t, databaseID, 0, walPayload{Kind: "checkpoint"})
	if err := os.WriteFile(files.WALBase, staleBase, 0o600); err != nil {
		t.Fatal(err)
	}
	delta := persistedDelta{DatabaseID: databaseID, CommitID: 2, NextNodeID: 3, NextEdgeID: 1,
		UpsertNodes: []persistedNode{{ID: 2, Properties: map[string]persistedValue{}}}}
	active := persistedWALTestRecord(t, databaseID, 1, walPayload{Kind: "checkpoint"})
	active = append(active, persistedWALTestRecord(t, databaseID, 2, walPayload{Kind: "delta", Delta: &delta})...)
	if err := os.WriteFile(files.WAL, active, 0o600); err != nil {
		t.Fatal(err)
	}

	graph, _, _, commitID, err := LoadGraphStateFilesContext(context.Background(), files, ^uint64(0), ^uint64(0), ^uint64(0))
	if err != nil {
		t.Fatal(err)
	}
	if commitID != 2 || graph.Nodes.Get(1) == nil || graph.Nodes.Get(2) == nil {
		t.Fatalf("recovered commit=%d nodes=%v,%v", commitID, graph.Nodes.Get(1), graph.Nodes.Get(2))
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
	binary.BigEndian.PutUint16(header[8:10], walVersion)
	binary.BigEndian.PutUint16(header[10:12], walHeaderSize)
	binary.BigEndian.PutUint64(header[20:28], maxWALFrameBytes+1)
	if err := os.WriteFile(walFilePath(path), header, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := LoadGraphState(path); err == nil {
		t.Fatal("expected oversized WAL frame to fail")
	}
}

func TestAppMetadataWALDeltaIsIncremental(t *testing.T) {
	path := t.TempDir()
	graph := NewGraphState()
	if err := EnsureDatabaseID(graph); err != nil {
		t.Fatal(err)
	}
	if err := CheckpointGraphStateAndWAL(path, graph, 1, 1, 0); err != nil {
		t.Fatal(err)
	}
	large := bytes.Repeat([]byte("x"), 1<<20)
	graph.AppMetadata.Set("large", large)
	if err := AppendWALDelta(path, graph, 1, 1, 1, GraphDelta{AppMetadata: []AppMetadataChange{{Key: []byte("large"), Value: large}}}); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(walFilePath(path))
	if err != nil {
		t.Fatal(err)
	}
	graph.AppMetadata.Set("small", []byte("value"))
	if err := AppendWALDelta(path, graph, 1, 1, 2, GraphDelta{AppMetadata: []AppMetadataChange{{Key: []byte("small"), Value: []byte("value")}}}); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(walFilePath(path))
	if err != nil {
		t.Fatal(err)
	}
	if growth := after.Size() - before.Size(); growth > 4096 {
		t.Fatalf("small metadata update grew WAL by %d bytes", growth)
	}
	recovered, _, _, commitID, err := LoadGraphState(path)
	if err != nil {
		t.Fatal(err)
	}
	largeValue, _ := recovered.AppMetadata.Get("large")
	smallValue, _ := recovered.AppMetadata.Get("small")
	if commitID != 2 || !bytes.Equal(largeValue, large) || string(smallValue) != "value" {
		t.Fatal("incremental metadata WAL did not recover both values")
	}
}

func TestLegacyStateAndWALHeadersRemainReadable(t *testing.T) {
	graph := NewGraphState()
	serialized, err := SerializeGraphState(graph, 1, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	copy(serialized[:8], legacyStateBinaryMagic[:])
	binary.BigEndian.PutUint16(serialized[8:10], legacyStateVersion)
	if _, _, _, _, err := DeserializeGraphState(serialized, maxStateFileBytes, ^uint64(0), ^uint64(0)); err != nil {
		t.Fatalf("legacy state header: %v", err)
	}

	path := t.TempDir()
	if err := AppendWALCommit(path, graph, 1, 1, 0); err != nil {
		t.Fatal(err)
	}
	wal, err := os.ReadFile(walFilePath(path))
	if err != nil {
		t.Fatal(err)
	}
	copy(wal[:8], legacyWALMagic[:])
	binary.BigEndian.PutUint16(wal[8:10], legacyWALVersion)
	if err := os.WriteFile(walFilePath(path), wal, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := LoadGraphState(path); err != nil {
		t.Fatalf("legacy WAL header: %v", err)
	}
}

func TestLoadBinaryCheckpointRejectsTrailingJSONValue(t *testing.T) {
	graph := NewGraphState()
	checkpoint, err := SerializeGraphState(graph, 1, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	payload := append(append([]byte(nil), checkpoint[stateHeaderSize:]...), []byte(`{}`)...)
	binary.BigEndian.PutUint64(checkpoint[20:28], uint64(len(payload)))
	binary.BigEndian.PutUint32(checkpoint[28:32], crc32.ChecksumIEEE(payload))
	checkpoint = append(checkpoint[:stateHeaderSize], payload...)

	path := t.TempDir()
	if err := os.WriteFile(stateFilePath(path), checkpoint, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadCheckpointSnapshot(path); err == nil {
		t.Fatal("expected binary checkpoint with trailing JSON value to be rejected")
	}
}

func TestStoredAppMetadataRejectsDuplicateKeys(t *testing.T) {
	graph := NewGraphState()
	if err := EnsureDatabaseID(graph); err != nil {
		t.Fatal(err)
	}
	snapshot, err := buildPersistedState(graph, 1, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.AppMetadata = []persistedAppMetadata{{Key: []byte("same")}, {Key: []byte("same")}}
	if _, _, _, _, err := decodePersistedState(snapshot); err == nil {
		t.Fatal("duplicate metadata keys were accepted")
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
	gap, err := buildPersistedState(graph, 1, 1, 3)
	if err != nil {
		t.Fatal(err)
	}

	for name, second := range map[string]persistedState{
		"commit regression": first,
		"commit gap":        gap,
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

func TestWALV2RejectsCommitGap(t *testing.T) {
	graph := NewGraphState()
	snapshot, err := buildPersistedState(graph, 1, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	delta := persistedDelta{DatabaseID: snapshot.DatabaseID, CommitID: 3, NextNodeID: 1, NextEdgeID: 1}
	encode := func(payload walPayload, commitID uint64) []byte {
		data, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		header, err := encodeWALHeader(snapshot.DatabaseID, commitID, data)
		if err != nil {
			t.Fatal(err)
		}
		return append(header[:], data...)
	}
	data := append(encode(walPayload{Kind: "snapshot", Snapshot: &snapshot}, 1), encode(walPayload{Kind: "delta", Delta: &delta}, 3)...)
	dbPath := t.TempDir()
	if err := os.WriteFile(walFilePath(dbPath), data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := LoadGraphState(dbPath); err == nil {
		t.Fatal("WAL commit gap was accepted")
	}
}

func TestWALV2RejectsSemanticallyInvalidDelta(t *testing.T) {
	base := NewGraphState()
	base.Nodes.Set(1, &NodeRecord{ID: 1, Properties: map[string]any{}})
	base.Nodes.Set(2, &NodeRecord{ID: 2, Properties: map[string]any{}})
	base.Edges.Set(1, &EdgeRecord{ID: 1, SourceID: 1, TargetID: 2, Type: "edge", Properties: map[string]any{}})
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

func deeplyNestedPersistedValue(depth int) persistedValue {
	value := persistedValue{Kind: "string", String: "ok"}
	for range depth {
		value = persistedValue{Kind: "map", Map: map[string]persistedValue{"next": value}}
	}
	return value
}

func serializedPersistedState(t *testing.T, snapshot persistedState) []byte {
	t.Helper()
	payload, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	header, err := encodeStateHeader(snapshot.DatabaseID, snapshot.CommitID, uint64(len(payload)), crc32.ChecksumIEEE(payload))
	if err != nil {
		t.Fatal(err)
	}
	return append(header[:], payload...)
}

func TestDeserializeRejectsInvalidPersistedSemantics(t *testing.T) {
	const databaseID = "0123456789abcdef0123456789abcdef"
	base := persistedState{DatabaseID: databaseID, CommitID: 1, NextNodeID: 2, NextEdgeID: 2,
		Nodes: []persistedNode{{ID: 1, Properties: map[string]persistedValue{}}},
		Edges: []persistedEdge{{ID: 1, SourceID: 1, TargetID: 1, Type: "edge", Properties: map[string]persistedValue{}}}}
	for name, mutate := range map[string]func(*persistedState){
		"empty label":     func(state *persistedState) { state.Nodes[0].Labels = []string{""} },
		"duplicate label": func(state *persistedState) { state.Nodes[0].Labels = []string{"tag", "tag"} },
		"empty edge type": func(state *persistedState) { state.Edges[0].Type = "" },
		"deep property": func(state *persistedState) {
			state.Nodes[0].Properties = map[string]persistedValue{"deep": deeplyNestedPersistedValue(maxValueDepth + 1)}
		},
	} {
		t.Run(name, func(t *testing.T) {
			snapshot := base
			snapshot.Nodes = slices.Clone(base.Nodes)
			snapshot.Edges = slices.Clone(base.Edges)
			mutate(&snapshot)
			if _, _, _, _, err := DeserializeGraphState(serializedPersistedState(t, snapshot), maxStateFileBytes, ^uint64(0), ^uint64(0)); err == nil {
				t.Fatal("invalid persisted state was accepted")
			}
		})
	}
}

func TestPersistenceWritersRejectInvalidSemantics(t *testing.T) {
	const databaseID = "0123456789abcdef0123456789abcdef"
	for name, graph := range map[string]*GraphState{
		"empty label": func() *GraphState {
			graph := NewGraphState()
			graph.DatabaseID = databaseID
			graph.Nodes.Set(1, &NodeRecord{ID: 1, Labels: []string{""}, Properties: map[string]any{}})
			return graph
		}(),
		"duplicate label": func() *GraphState {
			graph := NewGraphState()
			graph.DatabaseID = databaseID
			graph.Nodes.Set(1, &NodeRecord{ID: 1, Labels: []string{"tag", "tag"}, Properties: map[string]any{}})
			return graph
		}(),
		"empty edge type": func() *GraphState {
			graph := NewGraphState()
			graph.DatabaseID = databaseID
			graph.Nodes.Set(1, &NodeRecord{ID: 1, Properties: map[string]any{}})
			graph.Edges.Set(1, &EdgeRecord{ID: 1, SourceID: 1, TargetID: 1, Properties: map[string]any{}})
			return graph
		}(),
		"deep property": func() *GraphState {
			graph := NewGraphState()
			graph.DatabaseID = databaseID
			value := any("ok")
			for range maxValueDepth + 1 {
				value = map[string]any{"next": value}
			}
			graph.Nodes.Set(1, &NodeRecord{ID: 1, Properties: map[string]any{"deep": value}})
			return graph
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := SerializeGraphState(graph, 2, 2, 1); err == nil {
				t.Fatal("invalid graph was serialized")
			}
		})
	}
	graph := NewGraphState()
	graph.DatabaseID = databaseID
	graph.Nodes.Set(1, &NodeRecord{ID: 1, Labels: []string{""}, Properties: map[string]any{}})
	if _, err := buildPersistedDelta(graph, 2, 1, 1, GraphDelta{UpsertNodes: []uint64{1}}); err == nil {
		t.Fatal("invalid graph was encoded in a WAL delta")
	}
}

func TestPersistedFTSRejectsInvalidUTF8(t *testing.T) {
	const databaseID = "0123456789abcdef0123456789abcdef"
	invalid := string([]byte{0xff})
	graph := NewGraphState()
	graph.DatabaseID = databaseID
	graph.Nodes.Set(1, &NodeRecord{ID: 1, Properties: map[string]any{}})
	graph.FTS.Set(1, &FTSRecord{Text: invalid})
	if _, err := SerializeGraphState(graph, 2, 1, 1); err == nil {
		t.Fatal("invalid FTS text was serialized")
	}
	if _, err := buildPersistedDelta(graph, 2, 1, 1, GraphDelta{UpsertFTS: []uint64{1}}); err == nil {
		t.Fatal("invalid FTS text was encoded in a WAL delta")
	}

	snapshot := persistedState{DatabaseID: databaseID, CommitID: 1, NextNodeID: 2, NextEdgeID: 1,
		Nodes: []persistedNode{{ID: 1, Properties: map[string]persistedValue{}}},
		FTS:   []persistedFTS{{NodeID: 1, Text: invalid}}}
	if _, _, _, _, err := decodePersistedState(snapshot); err == nil {
		t.Fatal("invalid FTS text was decoded")
	}
	if _, err := newWALAccumulator(context.Background(), snapshot); err == nil {
		t.Fatal("invalid FTS text was accepted by WAL replay")
	}
	snapshot.FTS[0].Text = "valid"
	accumulator, err := newWALAccumulator(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := accumulator.apply(persistedDelta{CommitID: 2, DatabaseID: databaseID, NextNodeID: 2, NextEdgeID: 1,
		UpsertFTS: []persistedFTS{{NodeID: 1, Text: invalid}}}); err == nil {
		t.Fatal("invalid FTS text was accepted in a WAL delta")
	}
}

func persistedWALTestRecord(t *testing.T, databaseID string, commitID uint64, payload walPayload) []byte {
	t.Helper()
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

func TestWALDeltaRejectsInvalidPersistedSemantics(t *testing.T) {
	const databaseID = "0123456789abcdef0123456789abcdef"
	base := persistedState{DatabaseID: databaseID, CommitID: 1, NextNodeID: 2, NextEdgeID: 2,
		Nodes: []persistedNode{{ID: 1, Properties: map[string]persistedValue{}}},
		Edges: []persistedEdge{{ID: 1, SourceID: 1, TargetID: 1, Type: "edge", Properties: map[string]persistedValue{}}}}
	for name, delta := range map[string]persistedDelta{
		"empty label": {
			DatabaseID: databaseID, CommitID: 2, NextNodeID: 3, NextEdgeID: 2,
			UpsertNodes: []persistedNode{{ID: 2, Labels: []string{""}, Properties: map[string]persistedValue{}}},
		},
		"duplicate label": {
			DatabaseID: databaseID, CommitID: 2, NextNodeID: 3, NextEdgeID: 2,
			UpsertNodes: []persistedNode{{ID: 2, Labels: []string{"tag", "tag"}, Properties: map[string]persistedValue{}}},
		},
		"empty edge type": {
			DatabaseID: databaseID, CommitID: 2, NextNodeID: 2, NextEdgeID: 3,
			UpsertEdges: []persistedEdge{{ID: 2, SourceID: 1, TargetID: 1, Properties: map[string]persistedValue{}}},
		},
		"deep property": {
			DatabaseID: databaseID, CommitID: 2, NextNodeID: 3, NextEdgeID: 2,
			UpsertNodes: []persistedNode{{ID: 2, Properties: map[string]persistedValue{"deep": deeplyNestedPersistedValue(maxValueDepth + 1)}}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			path := t.TempDir()
			wal := persistedWALTestRecord(t, databaseID, 1, walPayload{Kind: "snapshot", Snapshot: &base})
			wal = append(wal, persistedWALTestRecord(t, databaseID, 2, walPayload{Kind: "delta", Delta: &delta})...)
			if err := os.WriteFile(walFilePath(path), wal, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, _, _, _, err := LoadGraphState(path); err == nil {
				t.Fatal("invalid WAL delta was accepted")
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
