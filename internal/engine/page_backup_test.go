package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mrchypark/latticedb-go/internal/pagestore"
	"github.com/mrchypark/latticedb-go/internal/store"
)

type pageBackupFixture struct {
	db          *pagestore.DB
	pagePath    string
	sourcePath  string
	archivePath string
	databaseID  string
}

func newPageBackupFixture(t *testing.T) pageBackupFixture {
	t.Helper()
	root := t.TempDir()
	pagePath := filepath.Join(root, "source.pages")
	db, err := pagestore.Open(pagePath, pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	graph := store.NewGraphState()
	if err := store.EnsureDatabaseID(graph); err != nil {
		t.Fatal(err)
	}
	write, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	page := &store.PageGraph{Tx: write}
	history := sha256.Sum256([]byte(graph.DatabaseID))
	catalog := store.PageCatalog{DatabaseID: graph.DatabaseID, NextNodeID: 1, NextEdgeID: 1, History: history}
	if err := page.PutCatalog(catalog); err != nil {
		t.Fatal(err)
	}
	if err := write.Put("commit-history", pageCommitKey(0), history[:]); err != nil {
		t.Fatal(err)
	}
	if err := write.Commit(); err != nil {
		t.Fatal(err)
	}
	return pageBackupFixture{
		db: db, pagePath: pagePath, sourcePath: filepath.Join(root, "source"),
		archivePath: filepath.Join(root, "archive"), databaseID: graph.DatabaseID,
	}
}

func (fixture pageBackupFixture) commitNode(t *testing.T, id, commitID uint64) {
	t.Helper()
	write, err := fixture.db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	graph := store.NewGraphState()
	graph.DatabaseID = fixture.databaseID
	graph.Nodes.Set(id, &store.NodeRecord{ID: id, Labels: []string{"Item"}})
	_, _, err = (&store.PageGraph{Tx: write}).PageCommit(context.Background(), graph, id+1, 1, commitID, store.GraphDelta{UpsertNodes: []uint64{id}}, true)
	if err != nil {
		_ = write.Rollback()
		t.Fatal(err)
	}
	if err := write.Commit(); err != nil {
		t.Fatal(err)
	}
}

func (fixture pageBackupFixture) readGraph(t *testing.T) (*pagestore.Tx, *store.GraphState) {
	t.Helper()
	read, err := fixture.db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	graph, _, err := (&store.PageGraph{Tx: read}).LoadGraph(context.Background())
	if err != nil {
		_ = read.Rollback()
		t.Fatal(err)
	}
	return read, graph
}

func TestPageBackupResumesOutboxAndRepairsEntryBeforeHeadCrash(t *testing.T) {
	fixture := newPageBackupFixture(t)
	ctx := context.Background()
	read, graph := fixture.readGraph(t)
	archive, err := openBackupArchive(fixture.archivePath, fixture.sourcePath, fixture.databaseID)
	if err != nil {
		t.Fatal(err)
	}
	base, err := archive.capturePage(ctx, time.Unix(100, 0), graph, 1, 1, 0)
	if err != nil || base.CommitID != 0 {
		t.Fatalf("base capture = %+v, %v", base, err)
	}
	if err := archive.close(); err != nil {
		t.Fatal(err)
	}
	if err := read.Rollback(); err != nil {
		t.Fatal(err)
	}

	fixture.commitNode(t, 1, 1)
	read, graph = fixture.readGraph(t)
	archive, err = openBackupArchive(fixture.archivePath, fixture.sourcePath, fixture.databaseID)
	if err != nil {
		t.Fatal(err)
	}
	first, err := archive.capturePage(ctx, time.Unix(101, 0), graph, 2, 1, 1)
	if err != nil || first.CommitID != 1 || !archive.hasHeadSourceHistory {
		t.Fatalf("outbox resume = %+v, %v", first, err)
	}
	oldHead, err := os.ReadFile(filepath.Join(fixture.archivePath, backupHeadFile))
	if err != nil {
		t.Fatal(err)
	}
	if err := archive.close(); err != nil {
		t.Fatal(err)
	}
	if err := read.Rollback(); err != nil {
		t.Fatal(err)
	}

	fixture.commitNode(t, 2, 2)
	read, graph = fixture.readGraph(t)
	archive, err = openBackupArchive(fixture.archivePath, fixture.sourcePath, fixture.databaseID)
	if err != nil {
		t.Fatal(err)
	}
	page := graph.PageBase
	frame, err := pageOutboxFrame(page, 2)
	if err != nil || frame == nil {
		t.Fatalf("commit 2 outbox = %d bytes, %v", len(frame), err)
	}
	catalog, err := page.Catalog()
	if err != nil {
		t.Fatal(err)
	}
	stage, err := os.CreateTemp(fixture.archivePath, ".test-page-frame-*")
	if err != nil {
		t.Fatal(err)
	}
	stagePath := stage.Name()
	if _, err := stage.Write(frame); err != nil {
		t.Fatal(err)
	}
	if err := stage.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := stage.Close(); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(stagePath)
	identity := backupSourceHistory{databaseID: catalog.DatabaseID, history: catalog.History}
	metadata := BackupMetadata{CommitID: 2, CapturedAt: time.Unix(102, 0)}
	if _, err := publishStagedSegment(fixture.archivePath, stagePath, 2, metadata, archive.headDigest, identity); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fixture.archivePath, backupHeadFile), oldHead, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := archive.close(); err != nil {
		t.Fatal(err)
	}
	if err := read.Rollback(); err != nil {
		t.Fatal(err)
	}

	read, graph = fixture.readGraph(t)
	archive, err = openBackupArchive(fixture.archivePath, fixture.sourcePath, fixture.databaseID)
	if err != nil {
		t.Fatal(err)
	}
	defer archive.close()
	resumed, err := archive.capturePage(ctx, time.Unix(103, 0), graph, 3, 1, 2)
	if err != nil || resumed.CommitID != 2 {
		t.Fatalf("head repair = %+v, %v", resumed, err)
	}
	var head backupHead
	data, err := os.ReadFile(filepath.Join(fixture.archivePath, backupHeadFile))
	if err != nil || json.Unmarshal(data, &head) != nil || head.CommitID != 2 {
		t.Fatalf("repaired head = %s, %v", data, err)
	}
	limitedDestination := filepath.Join(t.TempDir(), "limited.pages")
	if _, err := RestoreBackup(ctx, fixture.archivePath, limitedDestination, BackupRestoreOptions{MaxDatabaseSnapshotBytes: 1}); err == nil {
		t.Fatal("page backup restore ignored the explicit input byte limit")
	}
	for _, suffix := range []string{".pages", ".pages.layout"} {
		occupied := filepath.Join(t.TempDir(), "occupied")
		if err := os.WriteFile(occupied+suffix, []byte("preserve"), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := RestoreBackup(ctx, fixture.archivePath, occupied, BackupRestoreOptions{}); err == nil {
			t.Fatalf("restore accepted existing sidecar %s", suffix)
		}
		data, err := os.ReadFile(occupied + suffix)
		if err != nil || string(data) != "preserve" {
			t.Fatalf("sidecar changed: %q, %v", data, err)
		}
		if _, err := os.Stat(occupied); !os.IsNotExist(err) {
			t.Fatalf("destination published: %v", err)
		}
	}
	destination := filepath.Join(t.TempDir(), "restored.pages")
	if restored, err := RestoreBackup(ctx, fixture.archivePath, destination, BackupRestoreOptions{}); err != nil || restored.CommitID != 2 {
		t.Fatalf("page restore = %+v, %v", restored, err)
	}
	entries, err := readArchiveEntries(ctx, fixture.archivePath, fixture.databaseID)
	if err != nil {
		t.Fatal(err)
	}
	var selectedHistory [sha256.Size]byte
	for _, entry := range entries {
		if entry.metadata.CommitID == 2 {
			selectedHistory = entry.sourceHistory
			break
		}
	}
	if selectedHistory == ([sha256.Size]byte{}) {
		t.Fatal("selected page archive entry has no source history proof")
	}
	pageDB, err := pagestore.Open(destination, pagestore.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	pageTx, err := pageDB.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	restoredCatalog, err := (&store.PageGraph{Tx: pageTx}).Catalog()
	if err != nil || restoredCatalog.CommitID != 2 || restoredCatalog.History != selectedHistory {
		t.Fatalf("restored page catalog = %+v, %v", restoredCatalog, err)
	}
	if err := pageTx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := pageDB.Close(); err != nil {
		t.Fatal(err)
	}
	_ = read.Rollback()
}

func TestPageBackupRestoreRejectsMismatchedSourceHistoryBeforePublish(t *testing.T) {
	fixture := newPageBackupFixture(t)
	ctx := context.Background()
	read, graph := fixture.readGraph(t)
	archive, err := openBackupArchive(fixture.archivePath, fixture.sourcePath, fixture.databaseID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := archive.capturePage(ctx, time.Unix(200, 0), graph, 1, 1, 0); err != nil {
		t.Fatal(err)
	}
	if err := archive.close(); err != nil {
		t.Fatal(err)
	}
	if err := read.Rollback(); err != nil {
		t.Fatal(err)
	}

	fixture.commitNode(t, 1, 1)
	read, graph = fixture.readGraph(t)
	archive, err = openBackupArchive(fixture.archivePath, fixture.sourcePath, fixture.databaseID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := archive.capturePage(ctx, time.Unix(201, 0), graph, 2, 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := archive.close(); err != nil {
		t.Fatal(err)
	}
	if err := read.Rollback(); err != nil {
		t.Fatal(err)
	}

	entries, err := readArchiveEntries(ctx, fixture.archivePath, fixture.databaseID)
	if err != nil {
		t.Fatal(err)
	}
	var selected backupEntry
	for _, entry := range entries {
		if entry.metadata.CommitID == 1 {
			selected = entry
			break
		}
	}
	if selected.path == "" || !selected.hasSourceHistory {
		t.Fatal("selected page archive entry has no source history proof")
	}
	proofPath := sourceHistoryProofPath(fixture.archivePath, filepath.Base(selected.path))
	proofData, err := os.ReadFile(proofPath)
	if err != nil {
		t.Fatal(err)
	}
	var proof backupSourceHistoryProof
	if err := json.Unmarshal(proofData, &proof); err != nil {
		t.Fatal(err)
	}
	wrongHistory := sha256.Sum256([]byte("different restored ancestry"))
	proof.History = hex.EncodeToString(wrongHistory[:])
	proofData, err = json.Marshal(proof)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(proofPath, proofData, 0o600); err != nil {
		t.Fatal(err)
	}

	destination := filepath.Join(t.TempDir(), "must-not-publish.pages")
	if _, err := RestoreBackup(ctx, fixture.archivePath, destination, BackupRestoreOptions{}); err == nil {
		t.Fatal("page backup restore accepted a mismatched source history proof")
	}
	if _, err := os.Lstat(destination); !os.IsNotExist(err) {
		t.Fatalf("failed page restore published destination: stat error = %v", err)
	}
}

func TestPageBackupRejectsEqualCommitWithDivergentHistory(t *testing.T) {
	fixture := newPageBackupFixture(t)
	read, graph := fixture.readGraph(t)
	archive, err := openBackupArchive(fixture.archivePath, fixture.sourcePath, fixture.databaseID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := archive.capturePage(context.Background(), time.Unix(100, 0), graph, 1, 1, 0); err != nil {
		t.Fatal(err)
	}
	if err := archive.close(); err != nil {
		t.Fatal(err)
	}
	if err := read.Rollback(); err != nil {
		t.Fatal(err)
	}
	write, err := fixture.db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	page := &store.PageGraph{Tx: write}
	catalog, err := page.Catalog()
	if err != nil {
		t.Fatal(err)
	}
	catalog.History = sha256.Sum256([]byte("same state, different ancestry"))
	catalog.ArchiveBasePending = true
	if err := page.PutCatalog(catalog); err != nil {
		t.Fatal(err)
	}
	if err := write.Put("commit-history", pageCommitKey(0), catalog.History[:]); err != nil {
		t.Fatal(err)
	}
	if err := write.Commit(); err != nil {
		t.Fatal(err)
	}

	read, graph = fixture.readGraph(t)
	defer read.Rollback()
	archive, err = openBackupArchive(fixture.archivePath, fixture.sourcePath, fixture.databaseID)
	if err != nil {
		t.Fatal(err)
	}
	defer archive.close()
	if _, err := archive.capturePage(context.Background(), time.Unix(101, 0), graph, 1, 1, 0); err == nil {
		t.Fatal("accepted the same database and commit with divergent page ancestry")
	}
}

func TestPageBackupRejectsCorruptHeadBaseOnReopen(t *testing.T) {
	fixture := newPageBackupFixture(t)
	read, graph := fixture.readGraph(t)
	archive, err := openBackupArchive(fixture.archivePath, fixture.sourcePath, fixture.databaseID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := archive.capturePage(context.Background(), time.Unix(100, 0), graph, 1, 1, 0); err != nil {
		t.Fatal(err)
	}
	basePath := archive.headPath
	if err := archive.close(); err != nil {
		t.Fatal(err)
	}
	if err := read.Rollback(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(basePath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	var changed [1]byte
	if _, err := file.ReadAt(changed[:], info.Size()-1); err != nil {
		t.Fatal(err)
	}
	changed[0] ^= 0xff
	if _, err := file.WriteAt(changed[:], info.Size()-1); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	read, graph = fixture.readGraph(t)
	defer read.Rollback()
	archive, err = openBackupArchive(fixture.archivePath, fixture.sourcePath, fixture.databaseID)
	if err != nil {
		t.Fatal(err)
	}
	defer archive.close()
	if _, err := archive.capturePage(context.Background(), time.Unix(101, 0), graph, 1, 1, 0); err == nil {
		t.Fatal("accepted a corrupted checkpoint at the page archive head")
	}
}
