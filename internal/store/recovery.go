package store

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"

	"github.com/mrchypark/latticedb-go/internal/search"
)

var ErrCommitOutcomeUnknown = errors.New("commit outcome is unknown")
var ErrLoadResourceLimit = errors.New("database load resource limit exceeded")
var ErrDerivedIndexResourceLimit = errors.New("derived index resource limit exceeded")

const (
	stateMagic             = "LATTICEDB"
	idsMagic               = "LATTICEIDS"
	storageVersion         = 2
	walHeaderSize          = 64
	walDatabaseIDAt        = 32
	maxWALFrameBytes       = 1 << 30
	maxStateFileBytes      = 1 << 30
	maxIDsFileBytes        = 64 << 10
	stateHeaderSize        = 64
	stateVersion           = 4
	legacyStateVersion     = 3
	walVersion             = 3
	legacyWALVersion       = 2
	maxAppMetadataKeyBytes = 1<<16 - 1
)

var walMagic = [8]byte{'L', 'D', 'B', 'W', 'A', 'L', '3', 0}
var legacyWALMagic = [8]byte{'L', 'D', 'B', 'W', 'A', 'L', '2', 0}
var stateBinaryMagic = [8]byte{'L', 'D', 'B', 'S', 'T', 'A', 'T', '4'}
var legacyStateBinaryMagic = [8]byte{'L', 'D', 'B', 'S', 'T', 'A', 'T', '3'}

type WALWriter struct {
	file          *os.File
	tailSize      int64
	fullSync      bool
	syncFn        func(*os.File) error
	writeFn       func(*os.File, []byte) (int, error)
	truncateFn    func(*os.File, int64) error
	cleanupSyncFn func(*os.File) error
}

func OpenWALWriter(dbPath string, fullSync bool, syncFn func(*os.File) error, writeFn func(*os.File, []byte) (int, error), truncateFn func(*os.File, int64) error, cleanupSyncFn func(*os.File) error) (*WALWriter, error) {
	return OpenWALWriterFiles(DirectoryDatabaseFiles(dbPath), fullSync, syncFn, writeFn, truncateFn, cleanupSyncFn)
}

func OpenWALWriterFiles(files DatabaseFiles, fullSync bool, syncFn func(*os.File) error, writeFn func(*os.File, []byte) (int, error), truncateFn func(*os.File, int64) error, cleanupSyncFn func(*os.File) error) (*WALWriter, error) {
	file, err := os.OpenFile(files.WAL, os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open WAL writer: %w", err)
	}
	var header [walHeaderSize]byte
	if _, err := file.ReadAt(header[:], 0); err != nil || !validCurrentWALHeader(header[:]) {
		_ = file.Close()
		return nil, errors.New("WAL writer requires current base snapshot")
	}
	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("seek WAL writer: %w", err)
	}
	baseSize := int64(walHeaderSize) + int64(binary.BigEndian.Uint64(header[20:28]))
	if baseSize > offset {
		_ = file.Close()
		return nil, errors.New("WAL base snapshot is incomplete")
	}
	return &WALWriter{file: file, tailSize: offset - baseSize, fullSync: fullSync, syncFn: syncFn, writeFn: writeFn, truncateFn: truncateFn, cleanupSyncFn: cleanupSyncFn}, nil
}

func (writer *WALWriter) AppendSnapshot(graph *GraphState, nextNodeID uint64, nextEdgeID uint64, commitID uint64) error {
	snapshot, err := buildPersistedState(graph, nextNodeID, nextEdgeID, commitID)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(walPayload{Kind: "snapshot", Snapshot: &snapshot})
	if err != nil {
		return fmt.Errorf("encode WAL snapshot: %w", err)
	}
	return writer.append(snapshot.DatabaseID, snapshot.CommitID, payload)
}

func (writer *WALWriter) AppendDelta(graph *GraphState, nextNodeID uint64, nextEdgeID uint64, commitID uint64, changes GraphDelta) error {
	delta, err := buildPersistedDelta(graph, nextNodeID, nextEdgeID, commitID, changes)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(walPayload{Kind: "delta", Delta: &delta})
	if err != nil {
		return fmt.Errorf("encode WAL delta: %w", err)
	}
	return writer.append(delta.DatabaseID, delta.CommitID, payload)
}

func (writer *WALWriter) append(databaseID string, commitID uint64, payload []byte) error {
	if writer == nil || writer.file == nil {
		return errors.New("WAL writer is closed")
	}
	header, err := encodeWALHeader(databaseID, commitID, payload)
	if err != nil {
		return err
	}
	offset, err := writer.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("locate WAL writer: %w", err)
	}
	if full, err := writer.write(header[:]); err != nil {
		if full {
			return fmt.Errorf("%w: write WAL header: %v", ErrCommitOutcomeUnknown, err)
		}
		return writer.writeFailure("header", err, offset)
	}
	if full, err := writer.write(payload); err != nil {
		if full {
			return fmt.Errorf("%w: write WAL payload: %v", ErrCommitOutcomeUnknown, err)
		}
		return writer.writeFailure("payload", err, offset)
	}
	if err := writer.sync(); err != nil {
		return fmt.Errorf("%w: sync WAL: %v", ErrCommitOutcomeUnknown, err)
	}
	writer.tailSize += int64(len(header) + len(payload))
	return nil
}

func (writer *WALWriter) write(data []byte) (bool, error) {
	var count int
	var err error
	if writer.writeFn != nil {
		count, err = writer.writeFn(writer.file, data)
	} else {
		count, err = writer.file.Write(data)
	}
	if err == nil && count != len(data) {
		err = io.ErrShortWrite
	}
	return count == len(data), err
}

func (writer *WALWriter) writeFailure(part string, writeErr error, offset int64) error {
	if cleanupErr := writer.truncateAndSync(offset); cleanupErr != nil {
		return fmt.Errorf("%w: write WAL %s: %v; cleanup: %v", ErrCommitOutcomeUnknown, part, writeErr, cleanupErr)
	}
	return fmt.Errorf("write WAL %s: %w", part, writeErr)
}

func (writer *WALWriter) truncateAndSync(offset int64) error {
	var err error
	if writer.truncateFn != nil {
		err = writer.truncateFn(writer.file, offset)
	} else {
		err = writer.file.Truncate(offset)
	}
	if err != nil {
		return fmt.Errorf("truncate failed WAL record: %w", err)
	}
	if _, err := writer.file.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("seek truncated WAL: %w", err)
	}
	if writer.cleanupSyncFn != nil {
		err = writer.cleanupSyncFn(writer.file)
	} else {
		err = writer.file.Sync()
	}
	if err != nil {
		return fmt.Errorf("sync truncated WAL: %w", err)
	}
	return nil
}

func (writer *WALWriter) sync() error {
	if writer.syncFn != nil {
		return writer.syncFn(writer.file)
	}
	if writer.fullSync {
		return writer.file.Sync()
	}
	return syncFileStandard(writer.file)
}

func (writer *WALWriter) Close() error {
	if writer == nil || writer.file == nil {
		return nil
	}
	err := writer.file.Close()
	writer.file = nil
	return err
}

func (writer *WALWriter) TailSize() (int64, error) {
	if writer == nil || writer.file == nil {
		return 0, errors.New("WAL writer is closed")
	}
	return writer.tailSize, nil
}

func (writer *WALWriter) MatchesPath(path string) (bool, error) {
	return writer.MatchesFiles(DirectoryDatabaseFiles(path))
}

func (writer *WALWriter) MatchesFiles(files DatabaseFiles) (bool, error) {
	if writer == nil || writer.file == nil {
		return false, errors.New("WAL writer is closed")
	}
	handle, err := writer.file.Stat()
	if err != nil {
		return false, err
	}
	current, err := os.Stat(files.WAL)
	if err != nil {
		return false, err
	}
	return os.SameFile(handle, current), nil
}

func LoadGraphState(dbPath string) (*GraphState, uint64, uint64, uint64, error) {
	return LoadGraphStateFilesContext(context.Background(), DirectoryDatabaseFiles(dbPath), maxStateFileBytes, ^uint64(0), ^uint64(0))
}

// SerializeGraphState returns the checkpoint representation used for persisted databases.
func SerializeGraphState(graph *GraphState, nextNodeID uint64, nextEdgeID uint64, commitID uint64) ([]byte, error) {
	if err := ensureDatabaseID(graph); err != nil {
		return nil, err
	}
	var payload bytes.Buffer
	checksum := crc32.NewIEEE()
	if err := writePersistedStateJSON(io.MultiWriter(&payload, checksum), graph, nextNodeID, nextEdgeID, commitID); err != nil {
		return nil, err
	}
	header, err := encodeStateHeader(graph.DatabaseID, commitID, uint64(payload.Len()), checksum.Sum32())
	if err != nil {
		return nil, err
	}
	return append(header[:], payload.Bytes()...), nil
}

// DeserializeGraphState decodes bytes returned by SerializeGraphState.
func DeserializeGraphState(data []byte, maxCanonicalBytes, maxDerivedWork, maxDerivedBytes uint64) (*GraphState, uint64, uint64, uint64, error) {
	if len(data) < stateHeaderSize {
		return nil, 0, 0, 0, errors.New("invalid state header")
	}
	header := data[:stateHeaderSize]
	if !validStateHeader(header) {
		return nil, 0, 0, 0, errors.New("invalid state header")
	}
	payloadLength := binary.BigEndian.Uint64(header[20:28])
	if payloadLength > maxStateFileBytes-stateHeaderSize || payloadLength > maxCanonicalBytes || payloadLength > uint64(^uint(0)>>1) {
		return nil, 0, 0, 0, fmt.Errorf("%w: state payload exceeds size limit", ErrLoadResourceLimit)
	}
	if payloadLength != uint64(len(data)-stateHeaderSize) {
		return nil, 0, 0, 0, errors.New("state payload length mismatch")
	}
	payload := data[stateHeaderSize:]
	if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(header[28:32]) {
		return nil, 0, 0, 0, errors.New("state checksum mismatch")
	}
	var snapshot persistedState
	if err := json.Unmarshal(payload, &snapshot); err != nil {
		return nil, 0, 0, 0, fmt.Errorf("decode state payload: %w", err)
	}
	if snapshot.DatabaseID != string(header[32:64]) || snapshot.CommitID != binary.BigEndian.Uint64(header[12:20]) {
		return nil, 0, 0, 0, errors.New("state header metadata mismatch")
	}
	return decodePersistedStateContext(context.Background(), snapshot, maxDerivedWork, maxDerivedBytes)
}

func LoadGraphStateContext(ctx context.Context, dbPath string, maxCanonicalBytes, maxDerivedWork, maxDerivedBytes uint64) (*GraphState, uint64, uint64, uint64, error) {
	return LoadGraphStateFilesContext(ctx, DirectoryDatabaseFiles(dbPath), maxCanonicalBytes, maxDerivedWork, maxDerivedBytes)
}

func LoadGraphStateFilesContext(ctx context.Context, files DatabaseFiles, maxCanonicalBytes, maxDerivedWork, maxDerivedBytes uint64) (*GraphState, uint64, uint64, uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	snapshot, snapshotErr := loadCheckpointSnapshotFilesContext(ctx, files, maxCanonicalBytes)
	base := snapshot
	if base == nil {
		base, _ = loadWALBaseSnapshotFilesContext(ctx, files, maxCanonicalBytes)
	}
	walSnapshot, walErr := loadLatestWALSnapshotFilesContextWithBase(ctx, files, maxCanonicalBytes, base)
	if walSnapshot == nil && walErr != nil && !errors.Is(walErr, os.ErrNotExist) {
		return nil, 0, 0, 0, walErr
	}

	var chosen *persistedState
	if snapshot != nil && walSnapshot != nil && snapshot.DatabaseID != "" && walSnapshot.DatabaseID != "" && snapshot.DatabaseID != walSnapshot.DatabaseID {
		return nil, 0, 0, 0, errors.New("checkpoint and WAL database IDs differ")
	}
	switch {
	case walSnapshot != nil && (snapshot == nil || walSnapshot.CommitID > snapshot.CommitID):
		chosen = walSnapshot
	case snapshot != nil:
		chosen = snapshot
	case base != nil:
		chosen = base
	case walSnapshot != nil:
		chosen = walSnapshot
	default:
		if !errors.Is(snapshotErr, os.ErrNotExist) || !errors.Is(walErr, os.ErrNotExist) {
			return nil, 0, 0, 0, errors.Join(snapshotErr, walErr)
		}
		return nil, 0, 0, 0, os.ErrNotExist
	}

	return decodePersistedStateContext(ctx, *chosen, maxDerivedWork, maxDerivedBytes)
}

func loadWALBaseSnapshotContext(ctx context.Context, dbPath string, maxCanonicalBytes uint64) (*persistedState, error) {
	return loadWALBaseSnapshotFilesContext(ctx, DirectoryDatabaseFiles(dbPath), maxCanonicalBytes)
}

func loadWALBaseSnapshotFilesContext(ctx context.Context, files DatabaseFiles, maxCanonicalBytes uint64) (*persistedState, error) {
	file, err := os.Open(files.WALBase)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return loadLatestWALV2Context(ctx, file, maxCanonicalBytes)
}

var databaseTempKinds = []string{"state-payload", "snapshot-payload", "state", "wal-payload", "wal", "ids"}

func databaseTempPattern(files DatabaseFiles, kind string) string {
	token := sha256.Sum256([]byte(filepath.Base(files.State)))
	return ".latticedb-" + hex.EncodeToString(token[:]) + "-" + kind + "-*.tmp"
}

// CleanupDatabaseTempFiles removes checkpoint files abandoned by a crashed writer.
// Legacy generic names are safe only for directory-backed databases, whose lock owns the directory.
func CleanupDatabaseTempFiles(files DatabaseFiles, includeLegacy bool) error {
	entries, err := os.ReadDir(files.Directory)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	prefixes := make([]string, 0, len(databaseTempKinds)*2)
	for _, kind := range databaseTempKinds {
		prefixes = append(prefixes, strings.TrimSuffix(databaseTempPattern(files, kind), "*.tmp"))
		if includeLegacy {
			prefixes = append(prefixes, "."+kind+"-")
		}
	}
	removed := false
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".tmp") ||
			!slices.ContainsFunc(prefixes, func(prefix string) bool { return strings.HasPrefix(entry.Name(), prefix) }) {
			continue
		}
		if err := os.Remove(filepath.Join(files.Directory, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		removed = true
	}
	if removed {
		return syncDirectory(files.Directory)
	}
	return nil
}

func CheckpointGraphState(dbPath string, graph *GraphState, nextNodeID uint64, nextEdgeID uint64, commitID uint64) error {
	return CheckpointGraphStateFiles(DirectoryDatabaseFiles(dbPath), graph, nextNodeID, nextEdgeID, commitID)
}

func CheckpointGraphStateFiles(files DatabaseFiles, graph *GraphState, nextNodeID uint64, nextEdgeID uint64, commitID uint64) error {
	return checkpointGraphStateFiles(files, graph, nextNodeID, nextEdgeID, commitID, false)
}

func CreateCheckpointGraphStateFiles(files DatabaseFiles, graph *GraphState, nextNodeID uint64, nextEdgeID uint64, commitID uint64) error {
	return checkpointGraphStateFiles(files, graph, nextNodeID, nextEdgeID, commitID, true)
}

func checkpointGraphStateFiles(files DatabaseFiles, graph *GraphState, nextNodeID uint64, nextEdgeID uint64, commitID uint64, noReplace bool) error {
	if err := os.MkdirAll(files.Directory, 0o700); err != nil {
		return fmt.Errorf("create db directory: %w", err)
	}

	if err := ensureDatabaseID(graph); err != nil {
		return err
	}
	payload, err := os.CreateTemp(files.Directory, databaseTempPattern(files, "state-payload"))
	if err != nil {
		return fmt.Errorf("create temp state payload: %w", err)
	}
	payloadPath := payload.Name()
	defer os.Remove(payloadPath)
	defer payload.Close()
	checksum := crc32.NewIEEE()
	if err := writePersistedStateJSON(io.MultiWriter(payload, checksum), graph, nextNodeID, nextEdgeID, commitID); err != nil {
		return err
	}
	if _, err := payload.Seek(0, io.SeekStart); err != nil {
		return err
	}

	temp, err := os.CreateTemp(files.Directory, databaseTempPattern(files, "state"))
	if err != nil {
		return fmt.Errorf("create temp state: %w", err)
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	info, err := payload.Stat()
	if err != nil {
		_ = temp.Close()
		return err
	}
	header, err := encodeStateHeader(graph.DatabaseID, commitID, uint64(info.Size()), checksum.Sum32())
	if err != nil {
		_ = temp.Close()
		return err
	}
	if _, err := temp.Write(header[:]); err != nil {
		_ = temp.Close()
		return err
	}
	if _, err := io.Copy(temp, payload); err != nil {
		_ = temp.Close()
		return err
	}
	if err := syncAndClose(temp); err != nil {
		return fmt.Errorf("write temp state: %w", err)
	}
	if noReplace {
		if err := os.Link(tempPath, files.State); err != nil {
			return fmt.Errorf("publish temp state: %w", err)
		}
		if err := syncDirectory(files.Directory); err != nil {
			return rollbackCreatedState(files, tempPath, err)
		}
		if err := os.Remove(tempPath); err != nil {
			return rollbackCreatedState(files, tempPath, fmt.Errorf("remove linked temp state: %w", err))
		}
		if err := syncDirectory(files.Directory); err != nil {
			return rollbackCreatedState(files, tempPath, err)
		}
		return nil
	} else if err := os.Rename(tempPath, files.State); err != nil {
		return fmt.Errorf("rename temp state: %w", err)
	}
	return syncDirectory(files.Directory)
}

func rollbackCreatedState(files DatabaseFiles, tempPath string, cause error) error {
	var cleanupErrs []error
	for _, path := range []string{files.State, tempPath} {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			cleanupErrs = append(cleanupErrs, err)
		}
	}
	cleanupErrs = append(cleanupErrs, syncDirectory(files.Directory))
	return errors.Join(append([]error{cause}, cleanupErrs...)...)
}

func CheckpointGraphStateAndWAL(dbPath string, graph *GraphState, nextNodeID uint64, nextEdgeID uint64, commitID uint64) error {
	return CheckpointGraphStateAndWALFiles(DirectoryDatabaseFiles(dbPath), graph, nextNodeID, nextEdgeID, commitID)
}

func CheckpointGraphStateAndWALFiles(files DatabaseFiles, graph *GraphState, nextNodeID uint64, nextEdgeID uint64, commitID uint64) error {
	return checkpointGraphStateAndWALFiles(files, graph, nextNodeID, nextEdgeID, commitID, 0, nil)
}

// CheckpointGraphStateAndCompactWAL avoids mirroring a checkpoint larger than
// maxWALBytes into wal.log; later deltas replay from the durable checkpoint.
func CheckpointGraphStateAndCompactWAL(dbPath string, graph *GraphState, nextNodeID uint64, nextEdgeID uint64, commitID uint64, maxWALBytes uint64) error {
	return CheckpointGraphStateAndCompactWALFiles(DirectoryDatabaseFiles(dbPath), graph, nextNodeID, nextEdgeID, commitID, maxWALBytes)
}

func CheckpointGraphStateAndCompactWALFiles(files DatabaseFiles, graph *GraphState, nextNodeID uint64, nextEdgeID uint64, commitID uint64, maxWALBytes uint64) error {
	return checkpointGraphStateAndWALFiles(files, graph, nextNodeID, nextEdgeID, commitID, maxWALBytes, nil)
}

type CheckpointFault func(stage string, afterSideEffect bool) error

func CheckpointGraphStateAndWALWithFault(dbPath string, graph *GraphState, nextNodeID uint64, nextEdgeID uint64, commitID uint64, fault CheckpointFault) error {
	return checkpointGraphStateAndWALFiles(DirectoryDatabaseFiles(dbPath), graph, nextNodeID, nextEdgeID, commitID, 0, fault)
}

func checkpointGraphStateAndWALFiles(files DatabaseFiles, graph *GraphState, nextNodeID uint64, nextEdgeID uint64, commitID uint64, maxWALBytes uint64, fault CheckpointFault) error {
	if err := os.MkdirAll(files.Directory, 0o700); err != nil {
		return err
	}
	if err := ensureDatabaseID(graph); err != nil {
		return err
	}
	payload, err := os.CreateTemp(files.Directory, databaseTempPattern(files, "snapshot-payload"))
	if err != nil {
		return err
	}
	payloadPath := payload.Name()
	defer os.Remove(payloadPath)
	defer payload.Close()
	checksum := crc32.NewIEEE()
	if err := writePersistedStateJSON(io.MultiWriter(payload, checksum), graph, nextNodeID, nextEdgeID, commitID); err != nil {
		return err
	}
	if err := publishStatePayload(files, payload, graph.DatabaseID, commitID, checksum.Sum32(), fault); err != nil {
		return err
	}
	if info, err := payload.Stat(); err != nil {
		return err
	} else if maxWALBytes != 0 && uint64(info.Size()) > maxWALBytes {
		if err := rewriteWALStatePayload(files, payload, graph.DatabaseID, commitID, fault, files.WALBase); err != nil {
			return err
		}
		return rewriteWALStatePayload(files, nil, graph.DatabaseID, commitID, fault, files.WAL)
	}
	if err := rewriteWALStatePayload(files, payload, graph.DatabaseID, commitID, fault, files.WAL); err != nil {
		return err
	}
	if err := os.Remove(files.WALBase); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func publishStatePayload(files DatabaseFiles, payload *os.File, databaseID string, commitID uint64, checksum uint32, fault CheckpointFault) error {
	info, err := payload.Stat()
	if err != nil {
		return err
	}
	header, err := encodeStateHeader(databaseID, commitID, uint64(info.Size()), checksum)
	if err != nil {
		return err
	}
	if _, err := payload.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err := runCheckpointFault(fault, "state-create", false); err != nil {
		return err
	}
	temp, err := os.CreateTemp(files.Directory, databaseTempPattern(files, "state"))
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := runCheckpointFault(fault, "state-create", true); err != nil {
		_ = temp.Close()
		return err
	}
	if err := runCheckpointFault(fault, "state-write", false); err != nil {
		_ = temp.Close()
		return err
	}
	if _, err := temp.Write(header[:]); err != nil {
		_ = temp.Close()
		return err
	}
	if _, err := io.Copy(temp, payload); err != nil {
		_ = temp.Close()
		return err
	}
	if err := runCheckpointFault(fault, "state-write", true); err != nil {
		_ = temp.Close()
		return err
	}
	if err := syncCloseCheckpointFile(temp, "state", fault); err != nil {
		return err
	}
	if err := runCheckpointFault(fault, "state-rename", false); err != nil {
		return err
	}
	if err := os.Rename(tempPath, files.State); err != nil {
		return err
	}
	if err := runCheckpointFault(fault, "state-rename", true); err != nil {
		return err
	}
	return syncCheckpointDirectory(files.Directory, "state", fault)
}

func rewriteWALStatePayload(files DatabaseFiles, statePayload *os.File, databaseID string, commitID uint64, fault CheckpointFault, targetPath string) error {
	payload, err := os.CreateTemp(files.Directory, databaseTempPattern(files, "wal-payload"))
	if err != nil {
		return err
	}
	payloadPath := payload.Name()
	defer os.Remove(payloadPath)
	defer payload.Close()
	checksum := crc32.NewIEEE()
	output := io.MultiWriter(payload, checksum)
	if statePayload == nil {
		if _, err := io.WriteString(output, `{"kind":"checkpoint"}`); err != nil {
			return err
		}
	} else {
		if _, err := statePayload.Seek(0, io.SeekStart); err != nil {
			return err
		}
		if _, err := io.WriteString(output, `{"kind":"snapshot","snapshot":`); err != nil {
			return err
		}
		if _, err := io.Copy(output, statePayload); err != nil {
			return err
		}
		if _, err := io.WriteString(output, "}"); err != nil {
			return err
		}
	}
	info, err := payload.Stat()
	if err != nil {
		return err
	}
	header, err := encodeWALHeaderFields(databaseID, commitID, uint64(info.Size()), checksum.Sum32())
	if err != nil {
		return err
	}
	if _, err := payload.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if err := runCheckpointFault(fault, "wal-create", false); err != nil {
		return err
	}
	temp, err := os.CreateTemp(files.Directory, databaseTempPattern(files, "wal"))
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := runCheckpointFault(fault, "wal-create", true); err != nil {
		_ = temp.Close()
		return err
	}
	if err := runCheckpointFault(fault, "wal-write", false); err != nil {
		_ = temp.Close()
		return err
	}
	if _, err := temp.Write(header[:]); err != nil {
		_ = temp.Close()
		return err
	}
	if _, err := io.Copy(temp, payload); err != nil {
		_ = temp.Close()
		return err
	}
	if err := runCheckpointFault(fault, "wal-write", true); err != nil {
		_ = temp.Close()
		return err
	}
	if err := syncCloseCheckpointFile(temp, "wal", fault); err != nil {
		return err
	}
	if err := runCheckpointFault(fault, "wal-rename", false); err != nil {
		return err
	}
	if err := os.Rename(tempPath, targetPath); err != nil {
		return err
	}
	if err := runCheckpointFault(fault, "wal-rename", true); err != nil {
		return err
	}
	return syncCheckpointDirectory(files.Directory, "wal", fault)
}

func runCheckpointFault(fault CheckpointFault, stage string, after bool) error {
	if fault == nil {
		return nil
	}
	return fault(stage, after)
}

func syncCloseCheckpointFile(file *os.File, prefix string, fault CheckpointFault) error {
	if err := runCheckpointFault(fault, prefix+"-sync", false); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := runCheckpointFault(fault, prefix+"-sync", true); err != nil {
		_ = file.Close()
		return err
	}
	if err := runCheckpointFault(fault, prefix+"-close", false); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return runCheckpointFault(fault, prefix+"-close", true)
}

func syncCheckpointDirectory(dbPath, prefix string, fault CheckpointFault) error {
	if err := runCheckpointFault(fault, prefix+"-dir-sync", false); err != nil {
		return err
	}
	if err := syncDirectory(dbPath); err != nil {
		return err
	}
	return runCheckpointFault(fault, prefix+"-dir-sync", true)
}

func writePersistedStateJSON(output io.Writer, graph *GraphState, nextNodeID uint64, nextEdgeID uint64, commitID uint64) error {
	metadata, err := buildPersistedAppMetadata(graph.AppMetadata)
	if err != nil {
		return err
	}
	metadataData, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(output, `{"database_id":%q,"vector_dimensions":%d,"commit_id":%d,"next_node_id":%d,"next_edge_id":%d,"app_metadata":`, graph.DatabaseID, graph.VectorDimensions, commitID, nextNodeID, nextEdgeID); err != nil {
		return err
	}
	if _, err := output.Write(metadataData); err != nil {
		return err
	}
	if _, err := io.WriteString(output, `,"nodes":[`); err != nil {
		return err
	}
	for index, nodeID := range SortedNodeIDs(graph) {
		if index > 0 {
			if _, err := io.WriteString(output, ","); err != nil {
				return err
			}
		}
		node := graph.Nodes.Get(nodeID)
		properties, err := encodePropertyMap(node.Properties)
		if err != nil {
			return err
		}
		data, err := json.Marshal(persistedNode{ID: node.ID, Labels: CloneStrings(node.Labels), Properties: properties})
		if err != nil {
			return err
		}
		if _, err := output.Write(data); err != nil {
			return err
		}
	}
	if _, err := io.WriteString(output, `],"edges":[`); err != nil {
		return err
	}
	for index, edgeID := range SortedEdgeIDs(graph) {
		if index > 0 {
			if _, err := io.WriteString(output, ","); err != nil {
				return err
			}
		}
		edge := graph.Edges.Get(edgeID)
		properties, err := encodePropertyMap(edge.Properties)
		if err != nil {
			return err
		}
		data, err := json.Marshal(persistedEdge{ID: edge.ID, SourceID: edge.SourceID, TargetID: edge.TargetID, Type: edge.Type, Properties: properties})
		if err != nil {
			return err
		}
		if _, err := output.Write(data); err != nil {
			return err
		}
	}
	if _, err := io.WriteString(output, `],"fts":[`); err != nil {
		return err
	}
	first := true
	for _, nodeID := range SortedNodeIDs(graph) {
		record := graph.FTS.Get(nodeID)
		if record == nil {
			continue
		}
		if !first {
			if _, err := io.WriteString(output, ","); err != nil {
				return err
			}
		}
		first = false
		data, err := json.Marshal(persistedFTS{NodeID: nodeID, Text: record.Text})
		if err != nil {
			return err
		}
		if _, err := output.Write(data); err != nil {
			return err
		}
	}
	nodeIndexes, err := json.Marshal(persistedPropertyIndexes(graph.NodeProperties))
	if err != nil {
		return err
	}
	edgeIndexes, err := json.Marshal(persistedPropertyIndexes(graph.EdgeProperties))
	if err != nil {
		return err
	}
	streams, err := buildPersistedStreams(graph.Streams)
	if err != nil {
		return err
	}
	streamData, err := json.Marshal(streams)
	if err != nil {
		return err
	}
	if _, err := io.WriteString(output, `],"node_property_indexes":`); err != nil {
		return err
	}
	if _, err := output.Write(nodeIndexes); err != nil {
		return err
	}
	if _, err := io.WriteString(output, `,"edge_property_indexes":`); err != nil {
		return err
	}
	if _, err := output.Write(edgeIndexes); err != nil {
		return err
	}
	if _, err := io.WriteString(output, `,"streams":`); err != nil {
		return err
	}
	if _, err := output.Write(streamData); err != nil {
		return err
	}
	_, err = io.WriteString(output, "}")
	return err
}

func AppendWALCommit(dbPath string, graph *GraphState, nextNodeID uint64, nextEdgeID uint64, commitID uint64) error {
	return AppendWALCommitFiles(DirectoryDatabaseFiles(dbPath), graph, nextNodeID, nextEdgeID, commitID)
}

func AppendWALCommitFiles(files DatabaseFiles, graph *GraphState, nextNodeID uint64, nextEdgeID uint64, commitID uint64) error {
	snapshot, err := buildPersistedState(graph, nextNodeID, nextEdgeID, commitID)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(walPayload{Kind: "snapshot", Snapshot: &snapshot})
	if err != nil {
		return fmt.Errorf("encode wal entry: %w", err)
	}
	return appendWALRecord(files, snapshot.DatabaseID, snapshot.CommitID, payload)
}

func RewriteWALSnapshot(dbPath string, graph *GraphState, nextNodeID uint64, nextEdgeID uint64, commitID uint64) error {
	return RewriteWALSnapshotFiles(DirectoryDatabaseFiles(dbPath), graph, nextNodeID, nextEdgeID, commitID)
}

func RewriteWALSnapshotFiles(files DatabaseFiles, graph *GraphState, nextNodeID uint64, nextEdgeID uint64, commitID uint64) error {
	if err := ensureDatabaseID(graph); err != nil {
		return err
	}
	payload, err := os.CreateTemp(files.Directory, databaseTempPattern(files, "wal-payload"))
	if err != nil {
		return err
	}
	payloadPath := payload.Name()
	defer os.Remove(payloadPath)
	defer payload.Close()
	checksum := crc32.NewIEEE()
	output := io.MultiWriter(payload, checksum)
	if _, err := io.WriteString(output, `{"kind":"snapshot","snapshot":`); err != nil {
		return err
	}
	if err := writePersistedStateJSON(output, graph, nextNodeID, nextEdgeID, commitID); err != nil {
		return err
	}
	if _, err := io.WriteString(output, "}"); err != nil {
		return err
	}
	info, err := payload.Stat()
	if err != nil {
		return err
	}
	if info.Size() > maxWALFrameBytes {
		return errors.New("WAL frame exceeds size limit")
	}
	header, err := encodeWALHeaderFields(graph.DatabaseID, commitID, uint64(info.Size()), checksum.Sum32())
	if err != nil {
		return err
	}
	if _, err := payload.Seek(0, io.SeekStart); err != nil {
		return err
	}
	temp, err := os.CreateTemp(files.Directory, databaseTempPattern(files, "wal"))
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if _, err := temp.Write(header[:]); err != nil {
		_ = temp.Close()
		return err
	}
	if _, err := io.Copy(temp, payload); err != nil {
		_ = temp.Close()
		return err
	}
	if err := syncAndClose(temp); err != nil {
		return err
	}
	if err := os.Rename(tempPath, files.WAL); err != nil {
		return err
	}
	return syncDirectory(files.Directory)
}

func AppendWALDelta(dbPath string, graph *GraphState, nextNodeID uint64, nextEdgeID uint64, commitID uint64, changes GraphDelta) error {
	return AppendWALDeltaFiles(DirectoryDatabaseFiles(dbPath), graph, nextNodeID, nextEdgeID, commitID, changes)
}

func AppendWALDeltaFiles(files DatabaseFiles, graph *GraphState, nextNodeID uint64, nextEdgeID uint64, commitID uint64, changes GraphDelta) error {
	delta, err := buildPersistedDelta(graph, nextNodeID, nextEdgeID, commitID, changes)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(walPayload{Kind: "delta", Delta: &delta})
	if err != nil {
		return fmt.Errorf("encode WAL delta: %w", err)
	}
	return appendWALRecord(files, delta.DatabaseID, delta.CommitID, payload)
}

func appendWALRecord(files DatabaseFiles, databaseID string, commitID uint64, payload []byte) error {
	if err := os.MkdirAll(files.Directory, 0o700); err != nil {
		return fmt.Errorf("create db directory: %w", err)
	}
	header, err := encodeWALHeader(databaseID, commitID, payload)
	if err != nil {
		return err
	}

	walPath := files.WAL
	_, statErr := os.Stat(walPath)
	created := errors.Is(statErr, os.ErrNotExist)
	file, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open wal: %w", err)
	}
	offset, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		_ = file.Close()
		return fmt.Errorf("seek wal: %w", err)
	}
	if offset > 0 {
		var existingHeader [walHeaderSize]byte
		if _, err := file.ReadAt(existingHeader[:], 0); err != nil {
			_ = file.Close()
			return fmt.Errorf("read wal header: %w", err)
		}
		if !validCurrentWALHeader(existingHeader[:]) {
			_ = file.Close()
			record := append(header[:], payload...)
			return rewriteWAL(files, record)
		}
	}
	if _, err := file.Write(header[:]); err != nil {
		cleanupErr := truncateAndSync(file, offset)
		_ = file.Close()
		return errors.Join(fmt.Errorf("write wal header: %w", err), cleanupErr)
	}
	if _, err := file.Write(payload); err != nil {
		cleanupErr := truncateAndSync(file, offset)
		_ = file.Close()
		return errors.Join(fmt.Errorf("write wal: %w", err), cleanupErr)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("%w: sync wal: %v", ErrCommitOutcomeUnknown, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("%w: close wal: %v", ErrCommitOutcomeUnknown, err)
	}
	if created {
		if err := syncDirectory(files.Directory); err != nil {
			return fmt.Errorf("%w: %v", ErrCommitOutcomeUnknown, err)
		}
	}
	return nil
}

func ResetWAL(dbPath string) error {
	return ResetWALFiles(DirectoryDatabaseFiles(dbPath))
}

func ResetWALFiles(files DatabaseFiles) error {
	file, err := os.OpenFile(files.WAL, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("truncate wal: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync truncated wal: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close truncated wal: %w", err)
	}
	return syncDirectory(files.Directory)
}

func LoadIDReservation(dbPath string, databaseID string) (uint64, uint64, error) {
	return LoadIDReservationFiles(DirectoryDatabaseFiles(dbPath), databaseID)
}

func LoadIDReservationFiles(files DatabaseFiles, databaseID string) (uint64, uint64, error) {
	if err := rejectOversizedFile(files.IDs, maxIDsFileBytes); err != nil && !errors.Is(err, os.ErrNotExist) {
		return 0, 0, fmt.Errorf("read id reservation: %w", err)
	}
	data, err := os.ReadFile(files.IDs)
	if errors.Is(err, os.ErrNotExist) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, fmt.Errorf("read id reservation: %w", err)
	}
	var ids persistedIDs
	if err := json.Unmarshal(data, &ids); err != nil {
		return 0, 0, fmt.Errorf("decode id reservation: %w", err)
	}
	if ids.Magic != "" {
		if ids.Magic != idsMagic || ids.Version != storageVersion {
			return 0, 0, fmt.Errorf("unsupported ID reservation format %q version %d", ids.Magic, ids.Version)
		}
		if ids.DatabaseID != databaseID {
			return 0, 0, errors.New("ID reservation database ID mismatch")
		}
		if ids.Checksum != checksumIDs(ids.DatabaseID, ids.NextNodeID, ids.NextEdgeID) {
			return 0, 0, errors.New("ID reservation checksum mismatch")
		}
	}
	return ids.NextNodeID, ids.NextEdgeID, nil
}

func ReserveIDs(dbPath string, databaseID string, nextNodeID uint64, nextEdgeID uint64) error {
	return ReserveIDsFiles(DirectoryDatabaseFiles(dbPath), databaseID, nextNodeID, nextEdgeID)
}

func ReserveIDsFiles(files DatabaseFiles, databaseID string, nextNodeID uint64, nextEdgeID uint64) error {
	ids := persistedIDs{Magic: idsMagic, Version: storageVersion, DatabaseID: databaseID, NextNodeID: nextNodeID, NextEdgeID: nextEdgeID}
	ids.Checksum = checksumIDs(databaseID, nextNodeID, nextEdgeID)
	data, err := json.Marshal(ids)
	if err != nil {
		return fmt.Errorf("encode id reservation: %w", err)
	}
	temp, err := os.CreateTemp(files.Directory, databaseTempPattern(files, "ids"))
	if err != nil {
		return fmt.Errorf("create temp id reservation: %w", err)
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := writeFileDurably(temp, data); err != nil {
		return fmt.Errorf("write id reservation: %w", err)
	}
	if err := os.Rename(tempPath, files.IDs); err != nil {
		return fmt.Errorf("rename id reservation: %w", err)
	}
	return syncDirectory(files.Directory)
}

func checksumIDs(databaseID string, nextNodeID uint64, nextEdgeID uint64) uint32 {
	data := make([]byte, len(databaseID)+16)
	copy(data, databaseID)
	binary.BigEndian.PutUint64(data[len(databaseID):], nextNodeID)
	binary.BigEndian.PutUint64(data[len(databaseID)+8:], nextEdgeID)
	return crc32.ChecksumIEEE(data)
}

func writeFileDurably(file *os.File, data []byte) error {
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func syncAndClose(file *os.File) error {
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func truncateAndSync(file *os.File, offset int64) error {
	if err := file.Truncate(offset); err != nil {
		return fmt.Errorf("truncate failed wal record: %w", err)
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("seek truncated wal: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync truncated wal: %w", err)
	}
	return nil
}

func syncDirectory(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	directory, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open database directory: %w", err)
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		return fmt.Errorf("sync database directory: %w", err)
	}
	if err := directory.Close(); err != nil {
		return fmt.Errorf("close database directory: %w", err)
	}
	return nil
}

func SimulateCrash(dbPath string) error {
	files := DirectoryDatabaseFiles(dbPath)
	if err := os.Remove(files.State); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove state checkpoint: %w", err)
	}
	if err := os.Remove(filepath.Join(dbPath, ".state.tmp")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove temp checkpoint: %w", err)
	}
	return nil
}

func loadCheckpointSnapshot(dbPath string) (*persistedState, error) {
	return loadCheckpointSnapshotFilesContext(context.Background(), DirectoryDatabaseFiles(dbPath), maxStateFileBytes)
}

func loadCheckpointSnapshotContext(ctx context.Context, dbPath string, maxCanonicalBytes uint64) (*persistedState, error) {
	return loadCheckpointSnapshotFilesContext(ctx, DirectoryDatabaseFiles(dbPath), maxCanonicalBytes)
}

func loadCheckpointSnapshotFilesContext(ctx context.Context, files DatabaseFiles, maxCanonicalBytes uint64) (*persistedState, error) {
	path := files.State
	fileLimit := min(uint64(maxStateFileBytes), multiplySaturated(maxCanonicalBytes, 2))
	if fileLimit <= maxStateFileBytes-stateHeaderSize {
		fileLimit += stateHeaderSize
	}
	if err := rejectOversizedFile(path, int64(fileLimit)); err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var magic [8]byte
	if _, err := io.ReadFull(file, magic[:]); err != nil {
		return nil, err
	}
	if magic == stateBinaryMagic || magic == legacyStateBinaryMagic {
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		return loadBinaryCheckpointContext(ctx, file, maxCanonicalBytes)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(&contextReader{ctx: ctx, reader: io.LimitReader(file, int64(fileLimit)+1)})
	if err != nil {
		return nil, err
	}

	var envelope persistedEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, fmt.Errorf("decode state: %w", err)
	}
	if envelope.Magic == "" {
		var snapshot persistedState
		if err := json.Unmarshal(data, &snapshot); err != nil {
			return nil, fmt.Errorf("decode legacy state: %w", err)
		}
		return &snapshot, nil
	}
	if envelope.Magic != stateMagic || envelope.Version != storageVersion {
		return nil, fmt.Errorf("unsupported state format %q version %d", envelope.Magic, envelope.Version)
	}
	if crc32.ChecksumIEEE(envelope.Payload) != envelope.Checksum {
		return nil, errors.New("state checksum mismatch")
	}
	var snapshot persistedState
	if err := json.Unmarshal(envelope.Payload, &snapshot); err != nil {
		return nil, fmt.Errorf("decode state payload: %w", err)
	}
	if snapshot.DatabaseID != envelope.DatabaseID || snapshot.CommitID != envelope.CommitID {
		return nil, errors.New("state envelope metadata mismatch")
	}
	return &snapshot, nil
}

func loadBinaryCheckpoint(file *os.File) (*persistedState, error) {
	return loadBinaryCheckpointContext(context.Background(), file, maxStateFileBytes)
}

func loadBinaryCheckpointContext(ctx context.Context, file *os.File, maxCanonicalBytes uint64) (*persistedState, error) {
	var header [stateHeaderSize]byte
	if _, err := io.ReadFull(file, header[:]); err != nil {
		return nil, err
	}
	if !validStateHeader(header[:]) {
		return nil, errors.New("invalid state header")
	}
	payloadLength := binary.BigEndian.Uint64(header[20:28])
	if payloadLength > maxStateFileBytes-stateHeaderSize || payloadLength > maxCanonicalBytes || payloadLength > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("%w: state payload exceeds size limit", ErrLoadResourceLimit)
	}
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if payloadLength != uint64(info.Size()-stateHeaderSize) {
		return nil, errors.New("state payload length mismatch")
	}
	checksum := crc32.NewIEEE()
	decoder := json.NewDecoder(io.TeeReader(&contextReader{ctx: ctx, reader: io.LimitReader(file, int64(payloadLength))}, checksum))
	var snapshot persistedState
	if err := decoder.Decode(&snapshot); err != nil {
		return nil, fmt.Errorf("decode state payload: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("state payload has trailing data")
	}
	if checksum.Sum32() != binary.BigEndian.Uint32(header[28:32]) {
		return nil, errors.New("state checksum mismatch")
	}
	databaseID := string(header[32:64])
	commitID := binary.BigEndian.Uint64(header[12:20])
	if snapshot.DatabaseID != databaseID || snapshot.CommitID != commitID {
		return nil, errors.New("state header metadata mismatch")
	}
	return &snapshot, nil
}

func encodeStateHeader(databaseID string, commitID uint64, payloadLength uint64, checksum uint32) ([stateHeaderSize]byte, error) {
	if err := validateDatabaseID(databaseID); err != nil {
		return [stateHeaderSize]byte{}, err
	}
	if payloadLength > maxStateFileBytes-stateHeaderSize {
		return [stateHeaderSize]byte{}, errors.New("state payload exceeds size limit")
	}
	var header [stateHeaderSize]byte
	copy(header[:8], stateBinaryMagic[:])
	binary.BigEndian.PutUint16(header[8:10], stateVersion)
	binary.BigEndian.PutUint16(header[10:12], stateHeaderSize)
	binary.BigEndian.PutUint64(header[12:20], commitID)
	binary.BigEndian.PutUint64(header[20:28], payloadLength)
	binary.BigEndian.PutUint32(header[28:32], checksum)
	copy(header[32:], databaseID)
	return header, nil
}

func validStateHeader(header []byte) bool {
	if len(header) < stateHeaderSize || binary.BigEndian.Uint16(header[10:12]) != stateHeaderSize {
		return false
	}
	magic := string(header[:8])
	version := binary.BigEndian.Uint16(header[8:10])
	return magic == string(stateBinaryMagic[:]) && version == stateVersion || magic == string(legacyStateBinaryMagic[:]) && version == legacyStateVersion
}

func loadLatestWALSnapshot(dbPath string) (*persistedState, error) {
	return loadLatestWALSnapshotFilesContext(context.Background(), DirectoryDatabaseFiles(dbPath), maxWALFrameBytes)
}

func loadLatestWALSnapshotContext(ctx context.Context, dbPath string, maxCanonicalBytes uint64) (*persistedState, error) {
	return loadLatestWALSnapshotFilesContext(ctx, DirectoryDatabaseFiles(dbPath), maxCanonicalBytes)
}

func loadLatestWALSnapshotContextWithBase(ctx context.Context, dbPath string, maxCanonicalBytes uint64, base *persistedState) (*persistedState, error) {
	return loadLatestWALSnapshotFilesContextWithBase(ctx, DirectoryDatabaseFiles(dbPath), maxCanonicalBytes, base)
}

func loadLatestWALSnapshotFilesContext(ctx context.Context, files DatabaseFiles, maxCanonicalBytes uint64) (*persistedState, error) {
	return loadLatestWALSnapshotFilesContextWithBase(ctx, files, maxCanonicalBytes, nil)
}

func loadLatestWALSnapshotFilesContextWithBase(ctx context.Context, files DatabaseFiles, maxCanonicalBytes uint64, base *persistedState) (*persistedState, error) {
	file, err := os.Open(files.WAL)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var magic [8]byte
	if _, err := io.ReadFull(file, magic[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, os.ErrNotExist
		}
		return nil, fmt.Errorf("read wal magic: %w", err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("rewind wal: %w", err)
	}
	if magic == walMagic || magic == legacyWALMagic {
		return loadLatestWALV2ContextWithBase(ctx, file, maxCanonicalBytes, base)
	}
	return loadLatestLegacyWALContext(ctx, file, maxCanonicalBytes)
}

func WALReadyForAppend(dbPath string) bool {
	return WALFilesReadyForAppend(DirectoryDatabaseFiles(dbPath))
}

func WALFilesReadyForAppend(files DatabaseFiles) bool {
	file, err := os.Open(files.WAL)
	if err != nil {
		return false
	}
	defer file.Close()
	var header [walHeaderSize]byte
	if _, err := io.ReadFull(file, header[:]); err != nil || !validCurrentWALHeader(header[:]) {
		return false
	}
	payloadLength := binary.BigEndian.Uint64(header[20:28])
	if payloadLength > maxWALFrameBytes || payloadLength > uint64(^uint(0)>>1) {
		return false
	}
	payload := make([]byte, int(payloadLength))
	if _, err := io.ReadFull(file, payload); err != nil || crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(header[28:32]) {
		return false
	}
	if !walFramesComplete(file) {
		return false
	}
	var wrapper walPayload
	if json.Unmarshal(payload, &wrapper) == nil && wrapper.Kind == "checkpoint" {
		return true
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return false
	}
	_, err = loadLatestWALV2(file)
	return err == nil
}

func walFramesComplete(file *os.File) bool {
	info, err := file.Stat()
	if err != nil {
		return false
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return false
	}
	defer func() { _, _ = file.Seek(0, io.SeekStart) }()
	for offset := int64(0); offset < info.Size(); {
		if info.Size()-offset < walHeaderSize {
			return false
		}
		var header [walHeaderSize]byte
		if _, err := io.ReadFull(file, header[:]); err != nil || !validCurrentWALHeader(header[:]) {
			return false
		}
		payloadLength := binary.BigEndian.Uint64(header[20:28])
		if payloadLength > maxWALFrameBytes || payloadLength > uint64(info.Size()-offset-walHeaderSize) {
			return false
		}
		if _, err := file.Seek(int64(payloadLength), io.SeekCurrent); err != nil {
			return false
		}
		offset += walHeaderSize + int64(payloadLength)
	}
	return true
}

func loadLatestLegacyWAL(file *os.File) (*persistedState, error) {
	return loadLatestLegacyWALContext(context.Background(), file, maxWALFrameBytes)
}

func loadLatestLegacyWALContext(ctx context.Context, file *os.File, maxCanonicalBytes uint64) (*persistedState, error) {
	reader := bufio.NewReader(&contextReader{ctx: ctx, reader: file})
	var latest *persistedState
	for {
		line, err := reader.ReadBytes('\n')
		if uint64(len(line)) > maxCanonicalBytes {
			return nil, ErrLoadResourceLimit
		}
		if len(line) > 0 && err == nil {
			var entry persistedState
			if decodeErr := json.Unmarshal(trimTrailingNewline(line), &entry); decodeErr != nil {
				return nil, fmt.Errorf("decode wal: %w", decodeErr)
			}
			if latest != nil && (latest.CommitID == ^uint64(0) || entry.CommitID != latest.CommitID+1) {
				return nil, fmt.Errorf("wal commit id %d does not follow %d", entry.CommitID, latest.CommitID)
			}
			entryCopy := entry
			latest = &entryCopy
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read wal: %w", err)
		}
	}
	if latest == nil {
		return nil, os.ErrNotExist
	}
	return latest, nil
}

func loadLatestWALV2(file *os.File) (*persistedState, error) {
	return loadLatestWALV2Context(context.Background(), file, maxWALFrameBytes)
}

func loadLatestWALV2Context(ctx context.Context, file *os.File, maxCanonicalBytes uint64) (*persistedState, error) {
	return loadLatestWALV2ContextWithBase(ctx, file, maxCanonicalBytes, nil)
}

func loadLatestWALV2ContextWithBase(ctx context.Context, file *os.File, maxCanonicalBytes uint64, base *persistedState) (*persistedState, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat WAL: %w", err)
	}
	var accumulator *walAccumulator
	var header [walHeaderSize]byte
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, err := io.ReadFull(file, header[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return nil, fmt.Errorf("read wal header: %w", err)
		}
		if !validWALHeader(header[:]) {
			return nil, errors.New("invalid WAL frame header")
		}
		payloadLength := binary.BigEndian.Uint64(header[20:28])
		if payloadLength > maxWALFrameBytes || payloadLength > maxCanonicalBytes || payloadLength > uint64(^uint(0)>>1) {
			return nil, fmt.Errorf("%w: WAL frame exceeds size limit", ErrLoadResourceLimit)
		}
		offset, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, fmt.Errorf("locate WAL payload: %w", err)
		}
		if payloadLength > uint64(max(int64(0), info.Size()-offset)) {
			break
		}
		payload := make([]byte, int(payloadLength))
		if _, err := io.ReadFull(&contextReader{ctx: ctx, reader: file}, payload); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return nil, fmt.Errorf("read wal payload: %w", err)
		}
		if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(header[28:32]) {
			return nil, errors.New("WAL checksum mismatch")
		}
		var wrapper walPayload
		if err := unmarshalContext(ctx, payload, &wrapper); err != nil {
			return nil, fmt.Errorf("decode WAL payload: %w", err)
		}
		if wrapper.Kind == "" {
			var snapshot persistedState
			if err := unmarshalContext(ctx, payload, &snapshot); err != nil {
				return nil, fmt.Errorf("decode legacy v2 WAL payload: %w", err)
			}
			wrapper = walPayload{Kind: "snapshot", Snapshot: &snapshot}
		}
		commitID := binary.BigEndian.Uint64(header[12:20])
		databaseID := string(header[walDatabaseIDAt:walHeaderSize])
		switch wrapper.Kind {
		case "snapshot":
			if wrapper.Snapshot == nil || wrapper.Snapshot.CommitID != commitID || wrapper.Snapshot.DatabaseID != databaseID {
				return nil, errors.New("WAL snapshot metadata mismatch")
			}
			if accumulator != nil && (databaseID != accumulator.state.DatabaseID || accumulator.state.CommitID == ^uint64(0) || commitID != accumulator.state.CommitID+1) {
				return nil, errors.New("WAL snapshot history regression")
			}
			var err error
			accumulator, err = newWALAccumulator(ctx, *wrapper.Snapshot)
			if err != nil {
				return nil, fmt.Errorf("invalid WAL snapshot: %w", err)
			}
		case "delta":
			if wrapper.Delta == nil || wrapper.Delta.CommitID != commitID || wrapper.Delta.DatabaseID != databaseID {
				return nil, errors.New("WAL delta metadata mismatch")
			}
			if accumulator == nil {
				return nil, errors.New("WAL delta has no base snapshot")
			}
			if databaseID != accumulator.state.DatabaseID || accumulator.state.CommitID == ^uint64(0) || commitID != accumulator.state.CommitID+1 {
				return nil, errors.New("WAL delta history regression")
			}
			if err := accumulator.apply(*wrapper.Delta); err != nil {
				return nil, fmt.Errorf("invalid WAL delta: %w", err)
			}
		case "checkpoint":
			if accumulator != nil || base == nil || base.DatabaseID != databaseID || base.CommitID != commitID {
				return nil, errors.New("WAL checkpoint base mismatch")
			}
			var err error
			accumulator, err = newWALAccumulator(ctx, *base)
			if err != nil {
				return nil, fmt.Errorf("invalid WAL checkpoint base: %w", err)
			}
		default:
			return nil, fmt.Errorf("unknown WAL payload kind %q", wrapper.Kind)
		}
	}
	if accumulator == nil {
		return nil, os.ErrNotExist
	}
	state := accumulator.persistedState()
	return &state, nil
}

type walAccumulator struct {
	state    persistedState
	streams  StreamStore
	metadata map[string]persistedAppMetadata
	nodes    map[uint64]persistedNode
	edges    map[uint64]persistedEdge
	fts      map[uint64]persistedFTS
	incident map[uint64]uint64
}

func newWALAccumulator(ctx context.Context, state persistedState) (*walAccumulator, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if state.DatabaseID != "" {
		if err := validateDatabaseID(state.DatabaseID); err != nil {
			return nil, err
		}
	}
	if state.CommitID == ^uint64(0) {
		return nil, errors.New("commit ID space exhausted")
	}
	accumulator := &walAccumulator{
		state:    state,
		metadata: make(map[string]persistedAppMetadata, len(state.AppMetadata)),
		nodes:    make(map[uint64]persistedNode, len(state.Nodes)),
		edges:    make(map[uint64]persistedEdge, len(state.Edges)),
		fts:      make(map[uint64]persistedFTS, len(state.FTS)),
		incident: make(map[uint64]uint64),
	}
	for _, entry := range state.AppMetadata {
		if len(entry.Key) == 0 || len(entry.Key) > maxAppMetadataKeyBytes {
			return nil, errors.New("invalid stored application metadata key length")
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key := string(entry.Key)
		if _, exists := accumulator.metadata[key]; exists {
			return nil, errors.New("duplicate stored application metadata key")
		}
		accumulator.metadata[string(entry.Key)] = persistedAppMetadata{Key: slices.Clone(entry.Key), Value: slices.Clone(entry.Value)}
	}
	var maxNodeID uint64
	for index, node := range state.Nodes {
		if index&255 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		if node.ID == 0 {
			return nil, errors.New("stored node id must be non-zero")
		}
		if _, exists := accumulator.nodes[node.ID]; exists {
			return nil, fmt.Errorf("duplicate stored node id %d", node.ID)
		}
		if _, err := decodePropertyMap(node.Properties); err != nil {
			return nil, fmt.Errorf("decode node %d properties: %w", node.ID, err)
		}
		accumulator.nodes[node.ID] = node
		maxNodeID = max(maxNodeID, node.ID)
	}
	var maxEdgeID uint64
	for index, edge := range state.Edges {
		if index&255 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		if edge.ID == 0 {
			return nil, errors.New("stored edge id must be non-zero")
		}
		if _, exists := accumulator.edges[edge.ID]; exists {
			return nil, fmt.Errorf("duplicate stored edge id %d", edge.ID)
		}
		if _, ok := accumulator.nodes[edge.SourceID]; !ok {
			return nil, fmt.Errorf("stored edge %d references missing node", edge.ID)
		}
		if _, ok := accumulator.nodes[edge.TargetID]; !ok {
			return nil, fmt.Errorf("stored edge %d references missing node", edge.ID)
		}
		if _, err := decodePropertyMap(edge.Properties); err != nil {
			return nil, fmt.Errorf("decode edge %d properties: %w", edge.ID, err)
		}
		accumulator.edges[edge.ID] = edge
		accumulator.incident[edge.SourceID]++
		accumulator.incident[edge.TargetID]++
		maxEdgeID = max(maxEdgeID, edge.ID)
	}
	if err := validatePersistedPropertyIndexes(ctx, state.NodeIndexes, "node"); err != nil {
		return nil, err
	}
	if err := validatePersistedPropertyIndexes(ctx, state.EdgeIndexes, "edge"); err != nil {
		return nil, err
	}
	for index, record := range state.FTS {
		if index&255 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		if _, ok := accumulator.nodes[record.NodeID]; !ok {
			return nil, fmt.Errorf("stored FTS record references missing node %d", record.NodeID)
		}
		if _, exists := accumulator.fts[record.NodeID]; exists {
			return nil, fmt.Errorf("duplicate stored FTS node id %d", record.NodeID)
		}
		accumulator.fts[record.NodeID] = record
	}
	streams, err := decodePersistedStreams(state.Streams)
	if err != nil {
		return nil, fmt.Errorf("decode streams: %w", err)
	}
	accumulator.streams = streams
	if state.NextNodeID <= maxNodeID {
		if maxNodeID == ^uint64(0) {
			return nil, errors.New("node id space exhausted")
		}
		state.NextNodeID = maxNodeID + 1
	}
	if state.NextEdgeID <= maxEdgeID {
		if maxEdgeID == ^uint64(0) {
			return nil, errors.New("edge id space exhausted")
		}
		state.NextEdgeID = maxEdgeID + 1
	}
	accumulator.state.NextNodeID = state.NextNodeID
	accumulator.state.NextEdgeID = state.NextEdgeID
	return accumulator, nil
}

func validatePersistedPropertyIndexes(ctx context.Context, indexes []persistedPropertyIndexDefinition, kind string) error {
	seen := make(map[PropertyIndexDefinition]struct{}, len(indexes))
	for _, stored := range indexes {
		if err := ctx.Err(); err != nil {
			return err
		}
		definition := PropertyIndexDefinition{Scope: stored.Scope, Property: stored.Property}
		if definition.Scope == "" || definition.Property == "" {
			return fmt.Errorf("stored %s property index has an empty definition", kind)
		}
		if _, exists := seen[definition]; exists {
			return fmt.Errorf("duplicate stored %s property index", kind)
		}
		seen[definition] = struct{}{}
	}
	return nil
}

func (accumulator *walAccumulator) apply(delta persistedDelta) error {
	if delta.NextNodeID < accumulator.state.NextNodeID || delta.NextEdgeID < accumulator.state.NextEdgeID {
		return errors.New("WAL ID high-water mark regressed")
	}
	if err := validateUniqueDeltaIDs(delta.DeleteNodes, delta.UpsertNodes, func(node persistedNode) uint64 { return node.ID }); err != nil {
		return fmt.Errorf("node operations: %w", err)
	}
	if err := validateUniqueDeltaIDs(delta.DeleteEdges, delta.UpsertEdges, func(edge persistedEdge) uint64 { return edge.ID }); err != nil {
		return fmt.Errorf("edge operations: %w", err)
	}
	if err := validateUniqueDeltaIDs(delta.DeleteFTS, delta.UpsertFTS, func(record persistedFTS) uint64 { return record.NodeID }); err != nil {
		return fmt.Errorf("FTS operations: %w", err)
	}
	if delta.Streams != nil && len(delta.StreamOperations) != 0 {
		return errors.New("WAL delta mixes full and incremental streams")
	}
	seenMetadata := make(map[string]struct{}, len(delta.AppMetadata))
	for _, change := range delta.AppMetadata {
		if len(change.Key) == 0 || len(change.Key) > maxAppMetadataKeyBytes || change.Delete && len(change.Value) != 0 {
			return errors.New("invalid WAL application metadata change")
		}
		key := string(change.Key)
		if _, exists := seenMetadata[key]; exists {
			return errors.New("duplicate WAL application metadata change")
		}
		seenMetadata[key] = struct{}{}
	}
	var streams StreamStore
	if delta.Streams != nil {
		if _, err := decodePersistedStreams(*delta.Streams); err != nil {
			return fmt.Errorf("decode streams: %w", err)
		}
	} else if len(delta.StreamOperations) != 0 {
		var err error
		streams, err = ApplyPersistedStreamOperations(accumulator.streams, delta.StreamOperations)
		if err != nil {
			return fmt.Errorf("apply stream operations: %w", err)
		}
	}
	for _, change := range delta.AppMetadata {
		key := string(change.Key)
		if change.Delete {
			delete(accumulator.metadata, key)
		} else {
			accumulator.metadata[key] = persistedAppMetadata{Key: slices.Clone(change.Key), Value: slices.Clone(change.Value)}
		}
	}
	if err := applyPropertyIndexDelta(&accumulator.state.NodeIndexes, delta.CreateNodeIndexes, delta.DropNodeIndexes); err != nil {
		return fmt.Errorf("node property index operations: %w", err)
	}
	if err := applyPropertyIndexDelta(&accumulator.state.EdgeIndexes, delta.CreateEdgeIndexes, delta.DropEdgeIndexes); err != nil {
		return fmt.Errorf("edge property index operations: %w", err)
	}
	for _, node := range delta.UpsertNodes {
		if _, err := decodePropertyMap(node.Properties); err != nil {
			return fmt.Errorf("decode node %d properties: %w", node.ID, err)
		}
	}
	for _, edge := range delta.UpsertEdges {
		if _, err := decodePropertyMap(edge.Properties); err != nil {
			return fmt.Errorf("decode edge %d properties: %w", edge.ID, err)
		}
	}
	for _, id := range delta.DeleteEdges {
		edge, ok := accumulator.edges[id]
		if !ok {
			return fmt.Errorf("delete missing edge %d", id)
		}
		accumulator.incident[edge.SourceID]--
		accumulator.incident[edge.TargetID]--
		delete(accumulator.edges, id)
	}
	for _, node := range delta.UpsertNodes {
		accumulator.nodes[node.ID] = node
	}
	for _, edge := range delta.UpsertEdges {
		if old, ok := accumulator.edges[edge.ID]; ok {
			accumulator.incident[old.SourceID]--
			accumulator.incident[old.TargetID]--
		}
		if accumulator.nodes[edge.SourceID].ID == 0 || accumulator.nodes[edge.TargetID].ID == 0 {
			return fmt.Errorf("edge %d references missing node", edge.ID)
		}
		accumulator.edges[edge.ID] = edge
		accumulator.incident[edge.SourceID]++
		accumulator.incident[edge.TargetID]++
	}
	for _, id := range delta.DeleteNodes {
		if _, ok := accumulator.nodes[id]; !ok {
			return fmt.Errorf("delete missing node %d", id)
		}
		if accumulator.incident[id] != 0 {
			return fmt.Errorf("delete node %d with incident edges", id)
		}
		delete(accumulator.nodes, id)
	}
	for _, id := range delta.DeleteFTS {
		if _, ok := accumulator.fts[id]; !ok {
			return fmt.Errorf("delete missing FTS record %d", id)
		}
		delete(accumulator.fts, id)
	}
	for _, record := range delta.UpsertFTS {
		if _, ok := accumulator.nodes[record.NodeID]; !ok {
			return fmt.Errorf("FTS record references missing node %d", record.NodeID)
		}
		accumulator.fts[record.NodeID] = record
	}
	for _, id := range delta.DeleteNodes {
		if _, exists := accumulator.fts[id]; exists {
			return fmt.Errorf("delete node %d with FTS record", id)
		}
	}
	if delta.Streams != nil {
		accumulator.streams, _ = decodePersistedStreams(*delta.Streams)
	} else if len(delta.StreamOperations) != 0 {
		accumulator.streams = streams
	}
	for _, node := range delta.UpsertNodes {
		if node.ID >= delta.NextNodeID {
			return fmt.Errorf("node id %d reaches high-water mark %d", node.ID, delta.NextNodeID)
		}
	}
	for _, edge := range delta.UpsertEdges {
		if edge.ID >= delta.NextEdgeID {
			return fmt.Errorf("edge id %d reaches high-water mark %d", edge.ID, delta.NextEdgeID)
		}
	}
	accumulator.state.CommitID = delta.CommitID
	accumulator.state.NextNodeID = delta.NextNodeID
	accumulator.state.NextEdgeID = delta.NextEdgeID
	return nil
}

func applyPropertyIndexDelta(definitions *[]persistedPropertyIndexDefinition, creates, drops []persistedPropertyIndexDefinition) error {
	active := make(map[PropertyIndexDefinition]struct{}, len(*definitions))
	for _, persisted := range *definitions {
		definition := PropertyIndexDefinition{Scope: persisted.Scope, Property: persisted.Property}
		active[definition] = struct{}{}
	}
	for _, persisted := range drops {
		definition := PropertyIndexDefinition{Scope: persisted.Scope, Property: persisted.Property}
		if definition.Scope == "" || definition.Property == "" {
			return errors.New("empty definition")
		}
		if _, ok := active[definition]; !ok {
			return fmt.Errorf("drop missing index %q.%q", definition.Scope, definition.Property)
		}
		delete(active, definition)
	}
	for _, persisted := range creates {
		definition := PropertyIndexDefinition{Scope: persisted.Scope, Property: persisted.Property}
		if definition.Scope == "" || definition.Property == "" {
			return errors.New("empty definition")
		}
		if _, ok := active[definition]; ok {
			return fmt.Errorf("create existing index %q.%q", definition.Scope, definition.Property)
		}
		active[definition] = struct{}{}
	}
	*definitions = (*definitions)[:0]
	for definition := range active {
		*definitions = append(*definitions, persistedPropertyIndexDefinition{Scope: definition.Scope, Property: definition.Property})
	}
	slices.SortFunc(*definitions, func(left, right persistedPropertyIndexDefinition) int {
		if order := strings.Compare(left.Scope, right.Scope); order != 0 {
			return order
		}
		return strings.Compare(left.Property, right.Property)
	})
	return nil
}

func validateUniqueDeltaIDs[T any](deletes []uint64, upserts []T, id func(T) uint64) error {
	seen := make(map[uint64]struct{}, len(deletes)+len(upserts))
	for _, value := range deletes {
		if value == 0 {
			return errors.New("zero ID")
		}
		if _, exists := seen[value]; exists {
			return fmt.Errorf("duplicate ID %d", value)
		}
		seen[value] = struct{}{}
	}
	for _, value := range upserts {
		valueID := id(value)
		if valueID == 0 {
			return errors.New("zero ID")
		}
		if _, exists := seen[valueID]; exists {
			return fmt.Errorf("duplicate ID %d", valueID)
		}
		seen[valueID] = struct{}{}
	}
	return nil
}

func (accumulator *walAccumulator) persistedState() persistedState {
	state := accumulator.state
	state.AppMetadata = make([]persistedAppMetadata, 0, len(accumulator.metadata))
	metadataKeys := make([]string, 0, len(accumulator.metadata))
	for key := range accumulator.metadata {
		metadataKeys = append(metadataKeys, key)
	}
	slices.Sort(metadataKeys)
	for _, key := range metadataKeys {
		entry := accumulator.metadata[key]
		state.AppMetadata = append(state.AppMetadata, persistedAppMetadata{Key: slices.Clone(entry.Key), Value: slices.Clone(entry.Value)})
	}
	state.Streams, _ = buildPersistedStreams(accumulator.streams)
	state.Nodes = state.Nodes[:0]
	state.Edges = state.Edges[:0]
	state.FTS = state.FTS[:0]
	for _, id := range sortedMapKeys(accumulator.nodes) {
		state.Nodes = append(state.Nodes, accumulator.nodes[id])
	}
	for _, id := range sortedMapKeys(accumulator.edges) {
		state.Edges = append(state.Edges, accumulator.edges[id])
	}
	for _, id := range sortedMapKeys(accumulator.fts) {
		state.FTS = append(state.FTS, accumulator.fts[id])
	}
	return state
}

func sortedMapKeys[T any](values map[uint64]T) []uint64 {
	ids := make([]uint64, 0, len(values))
	for id := range values {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func buildPersistedState(graph *GraphState, nextNodeID uint64, nextEdgeID uint64, commitID uint64) (persistedState, error) {
	if err := ensureDatabaseID(graph); err != nil {
		return persistedState{}, err
	}
	snapshot := persistedState{
		DatabaseID:       graph.DatabaseID,
		VectorDimensions: graph.VectorDimensions,
		CommitID:         commitID,
		NextNodeID:       nextNodeID,
		NextEdgeID:       nextEdgeID,
		AppMetadata:      nil,
		Nodes:            make([]persistedNode, 0, graph.Nodes.Len()),
		Edges:            make([]persistedEdge, 0, graph.Edges.Len()),
		FTS:              make([]persistedFTS, 0, graph.FTS.Len()),
		NodeIndexes:      persistedPropertyIndexes(graph.NodeProperties),
		EdgeIndexes:      persistedPropertyIndexes(graph.EdgeProperties),
	}
	var err error
	snapshot.AppMetadata, err = buildPersistedAppMetadata(graph.AppMetadata)
	if err != nil {
		return persistedState{}, err
	}
	streams, err := buildPersistedStreams(graph.Streams)
	if err != nil {
		return persistedState{}, fmt.Errorf("encode streams: %w", err)
	}
	snapshot.Streams = streams

	for _, nodeID := range SortedNodeIDs(graph) {
		node := graph.Nodes.Get(nodeID)
		props, err := encodePropertyMap(node.Properties)
		if err != nil {
			return persistedState{}, fmt.Errorf("encode node %d properties: %w", nodeID, err)
		}
		snapshot.Nodes = append(snapshot.Nodes, persistedNode{
			ID:         node.ID,
			Labels:     CloneStrings(node.Labels),
			Properties: props,
		})
	}
	for _, edgeID := range SortedEdgeIDs(graph) {
		edge := graph.Edges.Get(edgeID)
		props, err := encodePropertyMap(edge.Properties)
		if err != nil {
			return persistedState{}, fmt.Errorf("encode edge %d properties: %w", edgeID, err)
		}
		snapshot.Edges = append(snapshot.Edges, persistedEdge{
			ID:         edge.ID,
			SourceID:   edge.SourceID,
			TargetID:   edge.TargetID,
			Type:       edge.Type,
			Properties: props,
		})
	}
	for _, nodeID := range SortedNodeIDs(graph) {
		record := graph.FTS.Get(nodeID)
		if record == nil {
			continue
		}
		snapshot.FTS = append(snapshot.FTS, persistedFTS{NodeID: nodeID, Text: record.Text})
	}
	return snapshot, nil
}

func buildPersistedAppMetadata(metadata map[string][]byte) ([]persistedAppMetadata, error) {
	keys := make([]string, 0, len(metadata))
	for key := range metadata {
		if len(key) == 0 || len(key) > maxAppMetadataKeyBytes {
			return nil, errors.New("invalid application metadata key length")
		}
		keys = append(keys, key)
	}
	slices.Sort(keys)
	persisted := make([]persistedAppMetadata, 0, len(keys))
	for _, key := range keys {
		persisted = append(persisted, persistedAppMetadata{Key: []byte(key), Value: slices.Clone(metadata[key])})
	}
	return persisted, nil
}

func persistedPropertyIndexes(indexes PropertyIndexes) []persistedPropertyIndexDefinition {
	definitions := make([]persistedPropertyIndexDefinition, 0)
	for definition := range indexes.Definitions() {
		definitions = append(definitions, persistedPropertyIndexDefinition{Scope: definition.Scope, Property: definition.Property})
	}
	slices.SortFunc(definitions, func(left, right persistedPropertyIndexDefinition) int {
		if order := strings.Compare(left.Scope, right.Scope); order != 0 {
			return order
		}
		return strings.Compare(left.Property, right.Property)
	})
	return definitions
}

func buildPersistedDelta(graph *GraphState, nextNodeID uint64, nextEdgeID uint64, commitID uint64, changes GraphDelta) (persistedDelta, error) {
	if err := ensureDatabaseID(graph); err != nil {
		return persistedDelta{}, err
	}
	delta := persistedDelta{
		DatabaseID:  graph.DatabaseID,
		CommitID:    commitID,
		NextNodeID:  nextNodeID,
		NextEdgeID:  nextEdgeID,
		DeleteNodes: slices.Clone(changes.DeleteNodes),
		DeleteEdges: slices.Clone(changes.DeleteEdges),
		DeleteFTS:   slices.Clone(changes.DeleteFTS),
	}
	seenMetadata := make(map[string]struct{}, len(changes.AppMetadata))
	for _, change := range changes.AppMetadata {
		if len(change.Key) == 0 || len(change.Key) > maxAppMetadataKeyBytes {
			return persistedDelta{}, errors.New("invalid application metadata key length")
		}
		key := string(change.Key)
		if _, exists := seenMetadata[key]; exists {
			return persistedDelta{}, errors.New("duplicate application metadata change")
		}
		seenMetadata[key] = struct{}{}
		delta.AppMetadata = append(delta.AppMetadata, persistedAppMetadataChange{Key: slices.Clone(change.Key), Value: slices.Clone(change.Value), Delete: change.Delete})
	}
	slices.SortFunc(delta.AppMetadata, func(left, right persistedAppMetadataChange) int {
		return bytes.Compare(left.Key, right.Key)
	})
	slices.Sort(delta.DeleteNodes)
	slices.Sort(delta.DeleteEdges)
	slices.Sort(delta.DeleteFTS)
	delta.CreateNodeIndexes = persistedPropertyIndexDefinitions(changes.CreateNodeIndexes)
	delta.DropNodeIndexes = persistedPropertyIndexDefinitions(changes.DropNodeIndexes)
	delta.CreateEdgeIndexes = persistedPropertyIndexDefinitions(changes.CreateEdgeIndexes)
	delta.DropEdgeIndexes = persistedPropertyIndexDefinitions(changes.DropEdgeIndexes)
	if len(changes.StreamOperations) != 0 {
		operations, err := BuildPersistedStreamOperations(changes.StreamOperations)
		if err != nil {
			return persistedDelta{}, fmt.Errorf("encode stream operations: %w", err)
		}
		delta.StreamOperations = operations
	} else if changes.StreamsChanged {
		streams, err := buildPersistedStreams(graph.Streams)
		if err != nil {
			return persistedDelta{}, fmt.Errorf("encode streams: %w", err)
		}
		delta.Streams = &streams
	}
	for _, nodeID := range sortedIDs(changes.UpsertNodes) {
		node := graph.Nodes.Get(nodeID)
		if node == nil {
			return persistedDelta{}, fmt.Errorf("WAL delta node %d does not exist", nodeID)
		}
		props, err := encodePropertyMap(node.Properties)
		if err != nil {
			return persistedDelta{}, fmt.Errorf("encode node %d properties: %w", nodeID, err)
		}
		delta.UpsertNodes = append(delta.UpsertNodes, persistedNode{ID: node.ID, Labels: CloneStrings(node.Labels), Properties: props})
	}
	for _, edgeID := range sortedIDs(changes.UpsertEdges) {
		edge := graph.Edges.Get(edgeID)
		if edge == nil {
			return persistedDelta{}, fmt.Errorf("WAL delta edge %d does not exist", edgeID)
		}
		props, err := encodePropertyMap(edge.Properties)
		if err != nil {
			return persistedDelta{}, fmt.Errorf("encode edge %d properties: %w", edgeID, err)
		}
		delta.UpsertEdges = append(delta.UpsertEdges, persistedEdge{ID: edge.ID, SourceID: edge.SourceID, TargetID: edge.TargetID, Type: edge.Type, Properties: props})
	}
	for _, nodeID := range sortedIDs(changes.UpsertFTS) {
		record := graph.FTS.Get(nodeID)
		if record == nil {
			return persistedDelta{}, fmt.Errorf("WAL delta FTS node %d does not exist", nodeID)
		}
		delta.UpsertFTS = append(delta.UpsertFTS, persistedFTS{NodeID: nodeID, Text: record.Text})
	}
	return delta, nil
}

func persistedPropertyIndexDefinitions(definitions []PropertyIndexDefinition) []persistedPropertyIndexDefinition {
	persisted := make([]persistedPropertyIndexDefinition, len(definitions))
	for index, definition := range definitions {
		persisted[index] = persistedPropertyIndexDefinition{Scope: definition.Scope, Property: definition.Property}
	}
	slices.SortFunc(persisted, func(left, right persistedPropertyIndexDefinition) int {
		if order := strings.Compare(left.Scope, right.Scope); order != 0 {
			return order
		}
		return strings.Compare(left.Property, right.Property)
	})
	return persisted
}

func EstimateSnapshotBytes(graph *GraphState) (uint64, error) {
	size := uint64(4096)
	for key, value := range graph.AppMetadata {
		size = snapshotAdd(size, appMetadataSnapshotBytes(key, value))
	}
	for _, node := range graph.Nodes.All() {
		recordSize, err := nodeSnapshotBytes(node)
		if err != nil {
			return 0, err
		}
		size = snapshotAdd(size, recordSize)
	}
	for _, edge := range graph.Edges.All() {
		recordSize, err := edgeSnapshotBytes(edge)
		if err != nil {
			return 0, err
		}
		size = snapshotAdd(size, recordSize)
	}
	for nodeID, record := range graph.FTS.All() {
		size = snapshotAdd(size, ftsSnapshotBytes(nodeID, record.Text))
	}
	size = snapshotAdd(size, streamStoreBytes(graph.Streams))
	return size, nil
}

func ApplyDeltaSnapshotBytes(base *GraphState, graph *GraphState, changes GraphDelta) (uint64, error) {
	size := base.SnapshotBytes
	adjust := func(oldSize uint64, newSize uint64) error {
		if oldSize > size {
			return errors.New("snapshot size accounting underflow")
		}
		size = snapshotAdd(size-oldSize, newSize)
		return nil
	}
	for _, change := range changes.AppMetadata {
		key := string(change.Key)
		oldSize := uint64(0)
		if old, exists := base.AppMetadata[key]; exists {
			oldSize = appMetadataSnapshotBytes(key, old)
		}
		newSize := uint64(0)
		if !change.Delete {
			value, exists := graph.AppMetadata[key]
			if !exists {
				return 0, errors.New("application metadata delta value is missing")
			}
			newSize = appMetadataSnapshotBytes(key, value)
		}
		if err := adjust(oldSize, newSize); err != nil {
			return 0, err
		}
	}
	for _, id := range changes.UpsertNodes {
		oldSize, newSize := uint64(0), uint64(0)
		var err error
		if old := base.Nodes.Get(id); old != nil {
			oldSize, err = nodeSnapshotBytes(old)
		}
		if err == nil {
			newSize, err = nodeSnapshotBytes(graph.Nodes.Get(id))
		}
		if err != nil {
			return 0, err
		}
		if err := adjust(oldSize, newSize); err != nil {
			return 0, err
		}
	}
	for _, id := range changes.DeleteNodes {
		if old := base.Nodes.Get(id); old != nil {
			oldSize, err := nodeSnapshotBytes(old)
			if err != nil {
				return 0, err
			}
			if err := adjust(oldSize, 0); err != nil {
				return 0, err
			}
		}
	}
	for _, id := range changes.UpsertEdges {
		oldSize, newSize := uint64(0), uint64(0)
		var err error
		if old := base.Edges.Get(id); old != nil {
			oldSize, err = edgeSnapshotBytes(old)
		}
		if err == nil {
			newSize, err = edgeSnapshotBytes(graph.Edges.Get(id))
		}
		if err != nil {
			return 0, err
		}
		if err := adjust(oldSize, newSize); err != nil {
			return 0, err
		}
	}
	for _, id := range changes.DeleteEdges {
		if old := base.Edges.Get(id); old != nil {
			oldSize, err := edgeSnapshotBytes(old)
			if err != nil {
				return 0, err
			}
			if err := adjust(oldSize, 0); err != nil {
				return 0, err
			}
		}
	}
	for _, id := range changes.UpsertFTS {
		oldSize, newSize := uint64(0), uint64(0)
		if old := base.FTS.Get(id); old != nil {
			oldSize = ftsSnapshotBytes(id, old.Text)
		}
		if record := graph.FTS.Get(id); record != nil {
			newSize = ftsSnapshotBytes(id, record.Text)
		}
		if err := adjust(oldSize, newSize); err != nil {
			return 0, err
		}
	}
	for _, id := range changes.DeleteFTS {
		if old := base.FTS.Get(id); old != nil {
			oldSize := ftsSnapshotBytes(id, old.Text)
			if err := adjust(oldSize, 0); err != nil {
				return 0, err
			}
		}
	}
	if changes.StreamsChanged {
		if err := adjust(streamStoreBytes(base.Streams), streamStoreBytes(graph.Streams)); err != nil {
			return 0, err
		}
	}
	return size, nil
}

func appMetadataSnapshotBytes(key string, value []byte) uint64 {
	return snapshotAdd(snapshotAdd(64, uint64(len(key))), uint64(len(value)))
}

func nodeSnapshotBytes(node *NodeRecord) (uint64, error) {
	size := snapshotAdd(128, estimatePropertyMapBytes(node.Properties))
	for _, label := range node.Labels {
		size = snapshotAdd(size, snapshotAdd(snapshotMul(uint64(len(label)), 6), 8))
	}
	return size, nil
}

func edgeSnapshotBytes(edge *EdgeRecord) (uint64, error) {
	return snapshotAdd(snapshotAdd(192, snapshotMul(uint64(len(edge.Type)), 6)), estimatePropertyMapBytes(edge.Properties)), nil
}

func estimatePropertyMapBytes(properties map[string]any) uint64 {
	size := uint64(64)
	for key, value := range properties {
		size = snapshotAdd(size, snapshotAdd(snapshotAdd(snapshotMul(uint64(len(key)), 6), 64), estimateValueBytes(value)))
	}
	return size
}

func estimateValueBytes(value any) uint64 {
	switch value := value.(type) {
	case nil, bool, int64, float64:
		return 64
	case string:
		return snapshotAdd(snapshotMul(uint64(len(value)), 6), 64)
	case []byte:
		return snapshotAdd(snapshotMul(uint64(len(value)), 2), 64)
	case []float32:
		return snapshotAdd(snapshotMul(uint64(len(value)), 32), 64)
	case []any:
		size := uint64(64)
		for _, item := range value {
			size = snapshotAdd(size, snapshotAdd(estimateValueBytes(item), 8))
		}
		return size
	case map[string]any:
		return estimatePropertyMapBytes(value)
	default:
		return 256
	}
}

func ftsSnapshotBytes(nodeID uint64, text string) uint64 {
	return snapshotAdd(snapshotAdd(snapshotMul(uint64(len(text)), 6), 64), uint64(len(strconv.FormatUint(nodeID, 10))))
}

func snapshotAdd(left, right uint64) uint64 {
	if right > ^uint64(0)-left {
		return ^uint64(0)
	}
	return left + right
}

func snapshotMul(left, right uint64) uint64 {
	if left != 0 && right > ^uint64(0)/left {
		return ^uint64(0)
	}
	return left * right
}

func sortedIDs(ids []uint64) []uint64 {
	ids = slices.Clone(ids)
	slices.Sort(ids)
	return ids
}

func decodePersistedState(snapshot persistedState) (*GraphState, uint64, uint64, uint64, error) {
	return decodePersistedStateContext(context.Background(), snapshot, ^uint64(0), ^uint64(0))
}

func decodePersistedStateContext(ctx context.Context, snapshot persistedState, maxDerivedWork, maxDerivedBytes uint64) (*GraphState, uint64, uint64, uint64, error) {
	budget := derivedIndexBudget{ctx: ctx, maxWork: maxDerivedWork, maxBytes: maxDerivedBytes}
	graph := NewGraphState()
	if snapshot.DatabaseID == "" {
		if err := ensureDatabaseID(graph); err != nil {
			return nil, 0, 0, 0, err
		}
	} else {
		if err := validateDatabaseID(snapshot.DatabaseID); err != nil {
			return nil, 0, 0, 0, err
		}
		graph.DatabaseID = snapshot.DatabaseID
	}
	graph.VectorDimensions = snapshot.VectorDimensions
	for _, entry := range snapshot.AppMetadata {
		if len(entry.Key) == 0 || len(entry.Key) > maxAppMetadataKeyBytes {
			return nil, 0, 0, 0, errors.New("invalid stored application metadata key length")
		}
		key := string(entry.Key)
		if _, exists := graph.AppMetadata[key]; exists {
			return nil, 0, 0, 0, errors.New("duplicate stored application metadata key")
		}
		graph.AppMetadata[key] = slices.Clone(entry.Value)
	}
	var maxNodeID uint64
	for index, storedNode := range snapshot.Nodes {
		if index&255 == 0 {
			if err := budget.check(); err != nil {
				return nil, 0, 0, 0, err
			}
		}
		if storedNode.ID == 0 {
			return nil, 0, 0, 0, errors.New("stored node id must be non-zero")
		}
		if graph.Nodes.Get(storedNode.ID) != nil {
			return nil, 0, 0, 0, fmt.Errorf("duplicate stored node id %d", storedNode.ID)
		}
		props, err := decodePropertyMap(storedNode.Properties)
		if err != nil {
			return nil, 0, 0, 0, fmt.Errorf("decode node %d properties: %w", storedNode.ID, err)
		}
		graph.Nodes.Set(storedNode.ID, &NodeRecord{
			ID:         storedNode.ID,
			Labels:     CloneStrings(storedNode.Labels),
			Properties: props,
		})
		for _, label := range storedNode.Labels {
			if err := budget.add(1, uint64(len(label))+128); err != nil {
				return nil, 0, 0, 0, err
			}
			graph.Labels.Add(label, storedNode.ID)
		}
		maxNodeID = max(maxNodeID, storedNode.ID)
	}
	var maxEdgeID uint64
	for index, storedEdge := range snapshot.Edges {
		if index&255 == 0 {
			if err := budget.check(); err != nil {
				return nil, 0, 0, 0, err
			}
		}
		if storedEdge.ID == 0 {
			return nil, 0, 0, 0, errors.New("stored edge id must be non-zero")
		}
		if graph.Edges.Get(storedEdge.ID) != nil {
			return nil, 0, 0, 0, fmt.Errorf("duplicate stored edge id %d", storedEdge.ID)
		}
		if graph.Nodes.Get(storedEdge.SourceID) == nil || graph.Nodes.Get(storedEdge.TargetID) == nil {
			return nil, 0, 0, 0, fmt.Errorf("stored edge %d references missing node", storedEdge.ID)
		}
		props, err := decodePropertyMap(storedEdge.Properties)
		if err != nil {
			return nil, 0, 0, 0, fmt.Errorf("decode edge %d properties: %w", storedEdge.ID, err)
		}
		graph.Edges.Set(storedEdge.ID, &EdgeRecord{
			ID:         storedEdge.ID,
			SourceID:   storedEdge.SourceID,
			TargetID:   storedEdge.TargetID,
			Type:       storedEdge.Type,
			Properties: props,
		})
		if err := budget.add(3, uint64(len(storedEdge.Type))+160); err != nil {
			return nil, 0, 0, 0, err
		}
		graph.EdgeTypes.Add(storedEdge.Type, storedEdge.ID)
		graph.Outgoing.Set(storedEdge.SourceID, graph.Outgoing.Get(storedEdge.SourceID).Append(storedEdge.ID))
		graph.Incoming.Set(storedEdge.TargetID, graph.Incoming.Get(storedEdge.TargetID).Append(storedEdge.ID))
		maxEdgeID = max(maxEdgeID, storedEdge.ID)
	}
	for _, stored := range snapshot.NodeIndexes {
		definition := PropertyIndexDefinition{Scope: stored.Scope, Property: stored.Property}
		if definition.Scope == "" || definition.Property == "" {
			return nil, 0, 0, 0, errors.New("stored node property index has an empty definition")
		}
		if !graph.NodeProperties.Create(definition) {
			return nil, 0, 0, 0, errors.New("duplicate stored node property index")
		}
		if err := budget.add(1, uint64(len(definition.Scope)+len(definition.Property))+192); err != nil {
			return nil, 0, 0, 0, err
		}
		for nodeID := range graph.Labels.All(definition.Scope) {
			if err := budget.add(1, 0); err != nil {
				return nil, 0, 0, 0, err
			}
			node := graph.Nodes.Get(nodeID)
			value, ok := node.Properties[definition.Property]
			if !ok {
				continue
			}
			valueBytes := estimateValueBytes(value)
			if err := budget.add(max(uint64(1), valueBytes), snapshotAdd(valueBytes, 192)); err != nil {
				return nil, 0, 0, 0, err
			}
			if err := graph.NodeProperties.Add(definition, value, nodeID); err != nil {
				return nil, 0, 0, 0, err
			}
		}
	}
	for _, stored := range snapshot.EdgeIndexes {
		definition := PropertyIndexDefinition{Scope: stored.Scope, Property: stored.Property}
		if definition.Scope == "" || definition.Property == "" {
			return nil, 0, 0, 0, errors.New("stored edge property index has an empty definition")
		}
		if !graph.EdgeProperties.Create(definition) {
			return nil, 0, 0, 0, errors.New("duplicate stored edge property index")
		}
		if err := budget.add(1, uint64(len(definition.Scope)+len(definition.Property))+192); err != nil {
			return nil, 0, 0, 0, err
		}
		for edgeID := range graph.EdgeTypes.All(definition.Scope) {
			if err := budget.add(1, 0); err != nil {
				return nil, 0, 0, 0, err
			}
			edge := graph.Edges.Get(edgeID)
			value, ok := edge.Properties[definition.Property]
			if !ok {
				continue
			}
			valueBytes := estimateValueBytes(value)
			if err := budget.add(max(uint64(1), valueBytes), snapshotAdd(valueBytes, 192)); err != nil {
				return nil, 0, 0, 0, err
			}
			if err := graph.EdgeProperties.Add(definition, value, edgeID); err != nil {
				return nil, 0, 0, 0, err
			}
		}
	}
	for index, storedFTS := range snapshot.FTS {
		if index&255 == 0 {
			if err := budget.check(); err != nil {
				return nil, 0, 0, 0, err
			}
		}
		if graph.Nodes.Get(storedFTS.NodeID) == nil {
			return nil, 0, 0, 0, fmt.Errorf("stored FTS record references missing node %d", storedFTS.NodeID)
		}
		if graph.FTS.Get(storedFTS.NodeID) != nil {
			return nil, 0, 0, 0, fmt.Errorf("duplicate stored FTS node id %d", storedFTS.NodeID)
		}
		estimatedWork := multiplySaturated(uint64(len(storedFTS.Text)), 2)
		if budget.work > budget.maxWork || estimatedWork > budget.maxWork-budget.work {
			return nil, 0, 0, 0, ErrDerivedIndexResourceLimit
		}
		remainingBytes := budget.maxBytes - min(budget.bytes, budget.maxBytes)
		tokens, err := search.TokenizeContextWithLimit(ctx, storedFTS.Text, remainingBytes)
		if err != nil {
			if errors.Is(err, search.ErrTokenizationLimit) {
				return nil, 0, 0, 0, ErrDerivedIndexResourceLimit
			}
			return nil, 0, 0, 0, err
		}
		var tokenBytes uint64
		for _, token := range tokens {
			tokenBytes = addSaturated(tokenBytes, uint64(len(token))+128)
		}
		if err := budget.add(uint64(len(storedFTS.Text))+uint64(len(tokens)), addSaturated(tokenBytes, multiplySaturated(uint64(len(tokens)), 16))); err != nil {
			return nil, 0, 0, 0, err
		}
		graph.FTS.Set(storedFTS.NodeID, &FTSRecord{Text: storedFTS.Text, Tokens: tokens})
		for _, token := range tokens {
			graph.FTSTokens.Add(token, storedFTS.NodeID)
		}
	}
	streams, err := decodePersistedStreams(snapshot.Streams)
	if err != nil {
		return nil, 0, 0, 0, fmt.Errorf("decode streams: %w", err)
	}
	graph.Streams = streams

	nextNodeID := snapshot.NextNodeID
	if nextNodeID <= maxNodeID {
		if maxNodeID == ^uint64(0) {
			return nil, 0, 0, 0, errors.New("node id space exhausted")
		}
		nextNodeID = maxNodeID + 1
	}
	nextEdgeID := snapshot.NextEdgeID
	if nextEdgeID <= maxEdgeID {
		if maxEdgeID == ^uint64(0) {
			return nil, 0, 0, 0, errors.New("edge id space exhausted")
		}
		nextEdgeID = maxEdgeID + 1
	}
	snapshotBytes, err := EstimateSnapshotBytes(graph)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	graph.SnapshotBytes = snapshotBytes
	graph.DerivedIndexWork, graph.DerivedIndexLogicalBytes = budget.work, budget.bytes
	return graph, nextNodeID, nextEdgeID, snapshot.CommitID, nil
}

func propertyIndexBudget(graph *GraphState) (uint64, uint64) {
	var work, bytes uint64
	for def := range graph.NodeProperties.Definitions() {
		work, bytes = addSaturated(work, 1), addSaturated(bytes, uint64(len(def.Scope)+len(def.Property))+192)
		for id := range graph.Labels.All(def.Scope) {
			work = addSaturated(work, 1)
			if value, ok := graph.Nodes.Get(id).Properties[def.Property]; ok {
				v := estimateValueBytes(value)
				work = addSaturated(work, max(uint64(1), v))
				bytes = addSaturated(bytes, v+192)
			}
		}
	}
	for def := range graph.EdgeProperties.Definitions() {
		work, bytes = addSaturated(work, 1), addSaturated(bytes, uint64(len(def.Scope)+len(def.Property))+192)
		for id := range graph.EdgeTypes.All(def.Scope) {
			work = addSaturated(work, 1)
			if value, ok := graph.Edges.Get(id).Properties[def.Property]; ok {
				v := estimateValueBytes(value)
				work = addSaturated(work, max(uint64(1), v))
				bytes = addSaturated(bytes, v+192)
			}
		}
	}
	return work, bytes
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (reader *contextReader) Read(buffer []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	return reader.reader.Read(buffer)
}

func unmarshalContext(ctx context.Context, data []byte, value any) error {
	return json.NewDecoder(&contextReader{ctx: ctx, reader: bytes.NewReader(data)}).Decode(value)
}

type derivedIndexBudget struct {
	ctx               context.Context
	maxWork, maxBytes uint64
	work, bytes       uint64
}

func (budget *derivedIndexBudget) check() error { return budget.ctx.Err() }

func (budget *derivedIndexBudget) add(work, bytes uint64) error {
	budget.work = addSaturated(budget.work, work)
	budget.bytes = addSaturated(budget.bytes, bytes)
	if budget.work > budget.maxWork || budget.bytes > budget.maxBytes {
		return ErrDerivedIndexResourceLimit
	}
	return budget.check()
}

func addSaturated(left, right uint64) uint64 {
	if right > ^uint64(0)-left {
		return ^uint64(0)
	}
	return left + right
}

func multiplySaturated(left, right uint64) uint64 {
	if left != 0 && right > ^uint64(0)/left {
		return ^uint64(0)
	}
	return left * right
}

func ensureDatabaseID(graph *GraphState) error {
	if graph.DatabaseID != "" {
		return validateDatabaseID(graph.DatabaseID)
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return fmt.Errorf("generate database ID: %w", err)
	}
	graph.DatabaseID = hex.EncodeToString(id[:])
	return nil
}

// EnsureDatabaseID assigns the durable identity required before publishing a
// new database layout owner.
func EnsureDatabaseID(graph *GraphState) error {
	return ensureDatabaseID(graph)
}

func validateDatabaseID(id string) error {
	if len(id) != 32 {
		return fmt.Errorf("invalid database ID %q", id)
	}
	for index := range len(id) {
		char := id[index]
		if char < '0' || char > '9' {
			if char < 'a' || char > 'f' {
				return fmt.Errorf("invalid database ID %q", id)
			}
		}
	}
	return nil
}

func encodeWALRecord(snapshot persistedState, payload []byte) ([]byte, error) {
	header, err := encodeWALHeader(snapshot.DatabaseID, snapshot.CommitID, payload)
	if err != nil {
		return nil, err
	}
	record := make([]byte, walHeaderSize+len(payload))
	copy(record, header[:])
	copy(record[walHeaderSize:], payload)
	return record, nil
}

func encodeWALHeader(databaseID string, commitID uint64, payload []byte) ([walHeaderSize]byte, error) {
	if err := validateWALPayloadSize(len(payload)); err != nil {
		return [walHeaderSize]byte{}, err
	}
	return encodeWALHeaderFields(databaseID, commitID, uint64(len(payload)), crc32.ChecksumIEEE(payload))
}

func encodeWALHeaderFields(databaseID string, commitID uint64, payloadLength uint64, checksum uint32) ([walHeaderSize]byte, error) {
	if err := validateDatabaseID(databaseID); err != nil {
		return [walHeaderSize]byte{}, err
	}
	if payloadLength > maxWALFrameBytes {
		return [walHeaderSize]byte{}, errors.New("WAL frame exceeds size limit")
	}
	var header [walHeaderSize]byte
	copy(header[:8], walMagic[:])
	binary.BigEndian.PutUint16(header[8:10], walVersion)
	binary.BigEndian.PutUint16(header[10:12], walHeaderSize)
	binary.BigEndian.PutUint64(header[12:20], commitID)
	binary.BigEndian.PutUint64(header[20:28], payloadLength)
	binary.BigEndian.PutUint32(header[28:32], checksum)
	copy(header[walDatabaseIDAt:walHeaderSize], databaseID)
	return header, nil
}

func validCurrentWALHeader(header []byte) bool {
	return len(header) >= walHeaderSize && string(header[:8]) == string(walMagic[:]) && binary.BigEndian.Uint16(header[8:10]) == walVersion && binary.BigEndian.Uint16(header[10:12]) == walHeaderSize
}

func validWALHeader(header []byte) bool {
	if len(header) < walHeaderSize || binary.BigEndian.Uint16(header[10:12]) != walHeaderSize {
		return false
	}
	magic := string(header[:8])
	version := binary.BigEndian.Uint16(header[8:10])
	return magic == string(walMagic[:]) && version == walVersion || magic == string(legacyWALMagic[:]) && version == legacyWALVersion
}

func validateWALPayloadSize(size int) error {
	if size > maxWALFrameBytes {
		return errors.New("WAL frame exceeds size limit")
	}
	return nil
}

func rejectOversizedFile(path string, limit int64) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.Size() > limit {
		return fmt.Errorf("%w: file exceeds %d byte size limit", ErrLoadResourceLimit, limit)
	}
	return nil
}

func rewriteWAL(files DatabaseFiles, record []byte) error {
	temp, err := os.CreateTemp(files.Directory, databaseTempPattern(files, "wal"))
	if err != nil {
		return fmt.Errorf("create temp WAL: %w", err)
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := writeFileDurably(temp, record); err != nil {
		return fmt.Errorf("write temp WAL: %w", err)
	}
	if err := os.Rename(tempPath, files.WAL); err != nil {
		return fmt.Errorf("rename temp WAL: %w", err)
	}
	return syncDirectory(files.Directory)
}

func trimTrailingNewline(line []byte) []byte {
	for len(line) > 0 {
		last := line[len(line)-1]
		if last != '\n' && last != '\r' {
			break
		}
		line = line[:len(line)-1]
	}
	return line
}

func CloneStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}
