package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/pagestore"
)

func graphConstraintFormatGraph(t *testing.T, valid bool) *GraphState {
	t.Helper()
	graph := NewGraphState()
	if err := EnsureDatabaseID(graph); err != nil {
		t.Fatal(err)
	}
	if err := PutGraphConstraint(graph, GraphConstraint{
		Name: "required-name", Label: "Person", Property: "name", Kind: ConstraintKindRequired,
	}); err != nil {
		t.Fatal(err)
	}
	properties := Properties{}
	if valid {
		properties = PropertiesFromMap(map[string]any{"name": "Ada"})
	}
	graph.Nodes.Set(1, &NodeRecord{ID: 1, Labels: []string{"Person"}, Properties: properties})
	return graph
}

func graphConstraintCheckpoint(t *testing.T, graph *GraphState) []byte {
	t.Helper()
	var payload bytes.Buffer
	if err := writePersistedStateBinary(&payload, graph, 2, 1, 1); err != nil {
		t.Fatal(err)
	}
	header, err := encodeStateHeaderForGraph(graph, 1, uint64(payload.Len()), crc32.ChecksumIEEE(payload.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	return append(header[:], payload.Bytes()...)
}

func TestGraphConstraintPageCatalogFormat(t *testing.T) {
	db, err := pagestore.Open(filepath.Join(t.TempDir(), "pages"), pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const databaseID = "0123456789abcdef0123456789abcdef"
	write, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	page := &PageGraph{Tx: write}
	if err := page.PutCatalog(PageCatalog{DatabaseID: databaseID, NextNodeID: 1, NextEdgeID: 1}); err != nil {
		t.Fatal(err)
	}
	if err := write.Commit(); err != nil {
		t.Fatal(err)
	}

	write, err = db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	page = &PageGraph{Tx: write}
	if err := page.PutMetadata([]byte(GraphConstraintsFeatureKey), []byte("1"), false); err != nil {
		t.Fatal(err)
	}
	catalog := PageCatalog{DatabaseID: databaseID, NextNodeID: 1, NextEdgeID: 1, graphConstraints: true}
	if err := page.PutCatalog(catalog); err != nil {
		t.Fatal(err)
	}
	if err := write.Commit(); err != nil {
		t.Fatal(err)
	}
	read, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Rollback()
	page = &PageGraph{Tx: read}
	data, err := read.Get(pageCatalogBucket, []byte("state"))
	if err != nil || len(data) < 2 || data[1] != pageCatalogGraphConstraintsRecord {
		t.Fatalf("constrained catalog record = %v, %v", data, err)
	}
	got, err := page.Catalog()
	if err != nil || !got.graphConstraints {
		t.Fatalf("constrained catalog = %#v, %v", got, err)
	}
}

func TestGraphConstraintPageCatalogRejectsTruncatedRecord(t *testing.T) {
	db, err := pagestore.Open(filepath.Join(t.TempDir(), "pages"), pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	write, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := write.Put(pageCatalogBucket, []byte("state"), []byte{pageRecordVersion}); err != nil {
		t.Fatal(err)
	}
	if err := write.Commit(); err != nil {
		t.Fatal(err)
	}
	read, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Rollback()
	if _, err := (&PageGraph{Tx: read}).Catalog(); err == nil {
		t.Fatal("truncated page catalog was accepted")
	}
}

func TestGraphConstraintCheckpointRefusalBeforePublication(t *testing.T) {
	ctx := context.Background()
	invalid := graphConstraintFormatGraph(t, false)
	checkpoint := graphConstraintCheckpoint(t, invalid)
	if version := binary.BigEndian.Uint16(checkpoint[8:10]); version != constrainedStateVersion {
		t.Fatalf("checkpoint version = %d", version)
	}
	if _, _, _, _, err := DeserializeGraphState(checkpoint, maxStateFileBytes, ^uint64(0), ^uint64(0)); !errors.Is(err, ErrConstraintViolation) {
		t.Fatalf("deserialize error = %v", err)
	}
	target := filepath.Join(t.TempDir(), "import.pages")
	if err := ImportPageCheckpoint(ctx, bytes.NewReader(checkpoint), target); !errors.Is(err, ErrConstraintViolation) {
		t.Fatalf("page import error = %v", err)
	}
	if _, err := os.Stat(target); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid page import was published: %v", err)
	}

	base := FlatDatabaseFiles(filepath.Join(t.TempDir(), "invalid-base"))
	if err := os.WriteFile(base.State, checkpoint, 0o600); err != nil {
		t.Fatal(err)
	}
	destination := FlatDatabaseFiles(filepath.Join(t.TempDir(), "restored"))
	if err := CreateBackupRecoveryFilesFrom(ctx, destination, base.State, nil, 1<<20, 1); !errors.Is(err, ErrConstraintViolation) {
		t.Fatalf("flat restore error = %v", err)
	}
	if _, err := os.Stat(destination.State); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid flat restore was published: %v", err)
	}
}

func TestGraphConstraintCheckpointMarkerVersionMismatch(t *testing.T) {
	checkpoint := graphConstraintCheckpoint(t, graphConstraintFormatGraph(t, true))
	for name, version := range map[string]uint16{"v5-marked": stateVersion, "v6-unmarked": constrainedStateVersion} {
		t.Run(name, func(t *testing.T) {
			data := bytes.Clone(checkpoint)
			binary.BigEndian.PutUint16(data[8:10], version)
			if name == "v6-unmarked" {
				got := graphConstraintFormatGraph(t, true)
				got.AppMetadata.Delete(GraphConstraintsFeatureKey)
				data = graphConstraintCheckpoint(t, got)
			}
			if _, _, _, _, err := DeserializeGraphState(data, maxStateFileBytes, ^uint64(0), ^uint64(0)); err == nil {
				t.Fatal("checkpoint version/marker mismatch was accepted")
			}
		})
	}
	malformed := graphConstraintFormatGraph(t, true)
	malformed.AppMetadata.Set(GraphConstraintsFeatureKey, []byte("yes"))
	data := graphConstraintCheckpoint(t, malformed)
	if _, _, _, _, err := DeserializeGraphState(data, maxStateFileBytes, ^uint64(0), ^uint64(0)); err == nil {
		t.Fatal("malformed feature marker was accepted")
	}
}

func TestGraphConstraintWALDefinitionDeltaVersion(t *testing.T) {
	value, _ := graphConstraintFormatGraph(t, true).AppMetadata.Get(GraphConstraintPrefix + "required-name")
	marker, _ := graphConstraintFormatGraph(t, true).AppMetadata.Get(GraphConstraintsFeatureKey)
	changes := GraphDelta{AppMetadata: []AppMetadataChange{
		{Key: []byte(GraphConstraintsFeatureKey), Value: marker},
		{Key: []byte(GraphConstraintPrefix + "required-name"), Value: value},
	}}
	graph := graphConstraintFormatGraph(t, true)
	delta, err := buildPersistedDelta(graph, 2, 1, 1, changes)
	if err != nil {
		t.Fatal(err)
	}
	payloadValue := walPayload{Kind: "delta", Delta: &delta}
	payload, err := encodeBinaryWALPayload(payloadValue)
	if err != nil {
		t.Fatal(err)
	}
	constrainedHeader, err := encodeWALHeaderVersion(delta.DatabaseID, delta.CommitID, payload, constrainedWALVersion)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeBinaryWALPayloadForHeader(constrainedHeader[:], bytes.NewReader(payload), uint64(len(payload)), maxWALFrameBytes); err != nil {
		t.Fatal(err)
	}
	unmarkedHeader, err := encodeWALHeader(delta.DatabaseID, delta.CommitID, payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeBinaryWALPayloadForHeader(unmarkedHeader[:], bytes.NewReader(payload), uint64(len(payload)), maxWALFrameBytes); err == nil {
		t.Fatal("unmarked constrained WAL delta was accepted")
	}
	if !v0122RejectsWALHeader(constrainedHeader[:]) {
		t.Fatal("v0.12.2 WAL header rules accepted a constrained frame")
	}
	for name, headerVersion := range map[string]struct {
		header [walHeaderSize]byte
		valid  bool
	}{
		"unmarked": {header: unmarkedHeader},
		"marked":   {header: constrainedHeader, valid: true},
	} {
		t.Run("strict-"+name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "segment.wal")
			frame := append(headerVersion.header[:], payload...)
			if err := os.WriteFile(path, frame, 0o600); err != nil {
				t.Fatal(err)
			}
			err := ValidateBackupWALSegmentFile(path, delta.DatabaseID, 0, delta.CommitID)
			if (err == nil) != headerVersion.valid {
				t.Fatalf("strict archive validation error = %v", err)
			}
		})
	}

	files := FlatDatabaseFiles(filepath.Join(t.TempDir(), "archive"))
	base := NewGraphState()
	if err := EnsureDatabaseID(base); err != nil {
		t.Fatal(err)
	}
	base.DatabaseID = graph.DatabaseID
	if err := AppendWALCommitFiles(files, base, 1, 1, 0); err != nil {
		t.Fatal(err)
	}
	if err := AppendWALDeltaFiles(files, graph, 2, 1, 1, changes); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(files.WAL)
	if err != nil {
		t.Fatal(err)
	}
	firstLength := int(binary.BigEndian.Uint64(data[20:28]))
	second := data[walHeaderSize+firstLength:]
	if binary.BigEndian.Uint16(second[8:10]) != constrainedWALVersion {
		t.Fatalf("archived delta version = %d", binary.BigEndian.Uint16(second[8:10]))
	}
	if !v0122RejectsWALHeader(second[:walHeaderSize]) {
		t.Fatal("v0.12.2 rules accepted archived constrained delta")
	}
}

// These rules are copied from v0.12.2 recovery. Its header reader falls back
// to 64 bytes on an unknown version, then validWALHeader rejects that header.
func v0122RejectsWALHeader(header []byte) bool {
	if len(header) < walHeaderPrefixSize {
		return true
	}
	version := binary.BigEndian.Uint16(header[8:10])
	headerSize := 0
	if string(header[:8]) == string(walMagic[:]) && version == walVersion && binary.BigEndian.Uint16(header[10:12]) == walHeaderSize {
		headerSize = walHeaderSize
	}
	if headerSize == 0 {
		headerSize = legacyWALHeaderSize
	}
	if len(header) < headerSize {
		return true
	}
	header = header[:headerSize]
	validCurrent := len(header) >= walHeaderSize && string(header[:8]) == string(walMagic[:]) && version == walVersion && binary.BigEndian.Uint16(header[10:12]) == walHeaderSize && binary.BigEndian.Uint32(header[walHeaderChecksumAt:walHeaderSize]) == crc32.ChecksumIEEE(header[:walHeaderChecksumAt])
	validLegacy := len(header) >= legacyWALHeaderSize && binary.BigEndian.Uint16(header[10:12]) == legacyWALHeaderSize && (string(header[:8]) == string(binaryWALMagic[:]) && version == binaryWALVersion || string(header[:8]) == string(jsonWALMagic[:]) && version == jsonWALVersion || string(header[:8]) == string(legacyWALMagic[:]) && version == legacyWALVersion)
	return !validCurrent && !validLegacy
}
