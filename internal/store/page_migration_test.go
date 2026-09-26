package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/pagestore"
)

func TestImportPageCheckpointAndReopen(t *testing.T) {
	state := checkpointScanFixture()
	input := checkpointScanFixtureBytes(t, state)
	path := filepath.Join(t.TempDir(), "graph.pages")
	if err := ImportPageCheckpoint(context.Background(), bytes.NewReader(input), path); err != nil {
		t.Fatal(err)
	}
	db, err := pagestore.Open(path, pagestore.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	graph := &PageGraph{Tx: tx}
	catalog, err := graph.Catalog()
	if err != nil {
		t.Fatal(err)
	}
	if catalog.DatabaseID != state.DatabaseID || catalog.CommitID != state.CommitID || catalog.Nodes != 2 || catalog.Edges != 1 || catalog.SnapshotBytes < 4096 {
		t.Fatalf("catalog = %+v", catalog)
	}
	loaded, _, err := graph.LoadGraph(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if size, err := EstimateSnapshotBytes(loaded); err != nil || size != catalog.SnapshotBytes {
		t.Fatalf("snapshot size = %d, catalog = %d, error = %v", size, catalog.SnapshotBytes, err)
	}
	node, err := graph.GetNode(1)
	if err != nil {
		t.Fatal(err)
	}
	if node == nil || node.Properties.Get("name") != "first" {
		t.Fatalf("node = %#v", node)
	}
	edge, err := graph.GetEdge(1)
	if err != nil {
		t.Fatal(err)
	}
	if edge == nil || edge.SourceID != 1 || edge.TargetID != 2 {
		t.Fatalf("edge = %#v", edge)
	}
	streams, err := graph.LoadStreams(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := streams.Read("events", 1, 4); len(got) != 1 || got[0].Payload != int64(2) {
		t.Fatalf("stream = %#v", got)
	}
	_ = tx.Rollback()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMigrateToPagesReplaysPropertyDeltaAndPreservesSource(t *testing.T) {
	dir := t.TempDir()
	files := DirectoryDatabaseFiles(dir)
	checkpoint := checkpointScanFixtureBytes(t, checkpointScanFixture())
	if err := os.WriteFile(files.State, checkpoint, 0o600); err != nil {
		t.Fatal(err)
	}
	delta := persistedDelta{DatabaseID: "0123456789abcdef0123456789abcdef", CommitID: 18, NextNodeID: 3, NextEdgeID: 2,
		NodePropertyChanges: []persistedPropertyChange{{ID: 1, Set: map[string]persistedValue{"name": {Kind: "string", String: "updated"}}}},
		AppMetadata:         []persistedAppMetadataChange{{Key: []byte("raw\x00key"), Value: []byte{}}},
	}
	marker, err := encodeBinaryWALPayload(walPayload{Kind: "checkpoint"})
	if err != nil {
		t.Fatal(err)
	}
	markerHeader, err := encodeWALHeader(delta.DatabaseID, 17, marker)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(files.WALBase, append(markerHeader[:], marker...), 0o600); err != nil {
		t.Fatal(err)
	}
	payload, err := encodeBinaryWALPayload(walPayload{Kind: "property_delta", Delta: &delta})
	if err != nil {
		t.Fatal(err)
	}
	header, err := encodeWALHeader(delta.DatabaseID, delta.CommitID, payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(files.WAL, append(header[:], payload...), 0o600); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dir, "state.pages")
	if err := MigrateToPages(context.Background(), files, target); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(files.State)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, checkpoint) {
		t.Fatal("migration changed source checkpoint")
	}
	db, err := pagestore.Open(target, pagestore.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	graph := &PageGraph{Tx: tx}
	catalog, err := graph.Catalog()
	if err != nil {
		t.Fatal(err)
	}
	if catalog.CommitID != 18 {
		t.Fatalf("commit id=%d", catalog.CommitID)
	}
	node, err := graph.GetNode(1)
	if err != nil {
		t.Fatal(err)
	}
	if node.Properties.Get("name") != "updated" {
		t.Fatalf("name=%v", node.Properties.Get("name"))
	}
	indexes, err := graph.LoadPropertyIndexes(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	if !indexes.Has(PropertyIndexDefinition{Scope: "Item", Property: "name"}) {
		t.Fatal("node property index lost during update")
	}
	_ = tx.Rollback()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMigrateToPagesRecoveryLimitsLeaveNoPublishedFile(t *testing.T) {
	for _, test := range []struct {
		name   string
		limits RecoveryLimits
		wal    bool
	}{
		{name: "decoded checkpoint bytes", limits: RecoveryLimits{MaxDecodedBytes: 1}},
		{name: "checkpoint work", limits: RecoveryLimits{MaxWork: 1}},
		{name: "covered WAL frames", limits: RecoveryLimits{MaxFrames: 1}, wal: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			files := DirectoryDatabaseFiles(dir)
			state := checkpointScanFixture()
			checkpoint := checkpointScanFixtureBytes(t, state)
			if err := os.WriteFile(files.State, checkpoint, 0o600); err != nil {
				t.Fatal(err)
			}
			if test.wal {
				marker, err := encodeBinaryWALPayload(walPayload{Kind: "checkpoint"})
				if err != nil {
					t.Fatal(err)
				}
				header, err := encodeWALHeader(state.DatabaseID, state.CommitID, marker)
				if err != nil {
					t.Fatal(err)
				}
				frame := append(append([]byte(nil), header[:]...), marker...)
				frame = append(frame, header[:]...)
				frame = append(frame, marker...)
				if err := os.WriteFile(files.WAL, frame, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			target := filepath.Join(dir, "limited.pages")
			err := MigrateToPages(context.Background(), files, target, test.limits)
			if !errors.Is(err, ErrLoadResourceLimit) {
				t.Fatalf("migration error = %v, want resource limit", err)
			}
			if _, statErr := os.Stat(target); !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("limited migration published target: %v", statErr)
			}
		})
	}
}

func TestPageImportRejectsCorruptionAndNeverOverwrites(t *testing.T) {
	data := checkpointScanFixtureBytes(t, checkpointScanFixture())
	bad := bytes.Clone(data)
	bad[len(bad)-1] ^= 0xff
	path := filepath.Join(t.TempDir(), "bad.pages")
	if err := ImportPageCheckpoint(context.Background(), bytes.NewReader(bad), path); err == nil {
		t.Fatal("corrupt checkpoint imported")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed import published target: %v", err)
	}
	if err := os.WriteFile(path, []byte("existing"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ImportPageCheckpoint(context.Background(), bytes.NewReader(data), path); err == nil {
		t.Fatal("import overwrote existing target")
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "existing" {
		t.Fatal("existing target was modified")
	}
}

func TestRestorePageBackupEnforcesAggregateLimitAndStrictWALTail(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "base.checkpoint")
	data := checkpointScanFixtureBytes(t, checkpointScanFixture())
	if err := os.WriteFile(base, data, 0o600); err != nil {
		t.Fatal(err)
	}
	segment := filepath.Join(dir, "segment.wal")
	marker, err := encodeBinaryWALPayload(walPayload{Kind: "checkpoint"})
	if err != nil {
		t.Fatal(err)
	}
	markerHeader, err := encodeWALHeader("0123456789abcdef0123456789abcdef", 17, marker)
	if err != nil {
		t.Fatal(err)
	}
	malformedTail := append(append(markerHeader[:], marker...), 1, 2, 3)
	if err := os.WriteFile(segment, malformedTail, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := RestorePageBackup(context.Background(), base, []string{segment}, filepath.Join(dir, "limited.pages"), 17, uint64(len(data))); !errors.Is(err, ErrLoadResourceLimit) {
		t.Fatalf("limit error=%v", err)
	}
	if err := RestorePageBackup(context.Background(), base, []string{segment}, filepath.Join(dir, "truncated.pages"), 17, 0); err == nil {
		t.Fatal("restore accepted malformed WAL tail")
	}
	if err := MigrateToPages(context.Background(), DatabaseFiles{State: base, WAL: segment}, filepath.Join(dir, "native.pages")); err != nil {
		t.Fatalf("native migration did not ignore incomplete crash tail: %v", err)
	}
}

func TestWALSnapshotReplaysThroughStreamingCheckpointScanner(t *testing.T) {
	dir := t.TempDir()
	base := checkpointScanFixtureBytes(t, checkpointScanFixture())
	state := checkpointScanFixture()
	state.CommitID = 18
	state.Nodes[0].Properties["name"] = persistedValue{Kind: "string", String: "snapshot"}
	walPayload, err := encodeBinaryWALPayload(walPayload{Kind: "snapshot", Snapshot: &state})
	if err != nil {
		t.Fatal(err)
	}
	header, err := encodeWALHeader(state.DatabaseID, state.CommitID, walPayload)
	if err != nil {
		t.Fatal(err)
	}
	segment := filepath.Join(dir, "snapshot.wal")
	if err := os.WriteFile(segment, append(header[:], walPayload...), 0o600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "snapshot.pages")
	if err := ImportPageCheckpointWithWAL(context.Background(), bytes.NewReader(base), []string{segment}, path, 18, 0); err != nil {
		t.Fatal(err)
	}
	db, err := pagestore.Open(path, pagestore.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	node, err := (&PageGraph{Tx: tx}).GetNode(1)
	if err != nil {
		t.Fatal(err)
	}
	if node.Properties.Get("name") != "snapshot" {
		t.Fatalf("snapshot node property=%v", node.Properties.Get("name"))
	}
	_ = tx.Rollback()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRestorePageBackupRequiresRequestedCommit(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "base")
	if err := os.WriteFile(base, checkpointScanFixtureBytes(t, checkpointScanFixture()), 0o600); err != nil {
		t.Fatal(err)
	}
	err := RestorePageBackup(context.Background(), base, nil, filepath.Join(dir, "wrong-commit.pages"), 18, 0)
	if err == nil {
		t.Fatal("restore published wrong commit")
	}
}

func TestRestorePageBackupInstallsAndVerifiesSourceHistory(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "source.checkpoint")
	if err := os.WriteFile(base, checkpointScanFixtureBytes(t, checkpointScanFixture()), 0o600); err != nil {
		t.Fatal(err)
	}
	delta := persistedDelta{DatabaseID: "0123456789abcdef0123456789abcdef", CommitID: 18, NextNodeID: 3, NextEdgeID: 2, AppMetadata: []persistedAppMetadataChange{{Key: []byte("backup"), Value: []byte("restored")}}}
	payload, err := encodeBinaryWALPayload(walPayload{Kind: "delta", Delta: &delta})
	if err != nil {
		t.Fatal(err)
	}
	header, err := encodeWALHeader(delta.DatabaseID, delta.CommitID, payload)
	if err != nil {
		t.Fatal(err)
	}
	segment := filepath.Join(dir, "segment.wal")
	if err := os.WriteFile(segment, append(header[:], payload...), 0o600); err != nil {
		t.Fatal(err)
	}
	var sourceHistory [32]byte
	copy(sourceHistory[:], bytes.Repeat([]byte{0x5a}, 32))
	hash := sha256.New()
	_, _ = hash.Write(sourceHistory[:])
	_, _ = hash.Write(header[:])
	_, _ = hash.Write(payload)
	var finalHistory [32]byte
	copy(finalHistory[:], hash.Sum(nil))
	path := filepath.Join(dir, "restored.pages")
	if err := RestorePageBackup(context.Background(), base, []string{segment}, path, 18, 0, sourceHistory, finalHistory); err != nil {
		t.Fatal(err)
	}
	db, err := pagestore.Open(path, pagestore.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	graph := &PageGraph{Tx: tx}
	catalog, err := graph.Catalog()
	if err != nil {
		t.Fatal(err)
	}
	if catalog.History != finalHistory {
		t.Fatal("catalog history differs from archive source")
	}
	stored, err := tx.Get("commit-history", pageID(17))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stored, sourceHistory[:]) {
		t.Fatal("base source history was not installed")
	}
	stored, err = tx.Get("commit-history", pageID(18))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stored, finalHistory[:]) {
		t.Fatal("final source history was not recorded")
	}
	_ = tx.Rollback()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	wrongPath := filepath.Join(dir, "wrong-history.pages")
	if err := RestorePageBackup(context.Background(), base, []string{segment}, wrongPath, 18, 0, sourceHistory, sourceHistory); err == nil {
		t.Fatal("restore accepted incorrect final history")
	}
	if _, err := os.Stat(wrongPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed history verification published page DB: %v", err)
	}
	if err := RestorePageBackup(context.Background(), base, nil, filepath.Join(dir, "wrong-arity.pages"), 17, 0, sourceHistory); err == nil {
		t.Fatal("restore accepted one history value")
	}
}
