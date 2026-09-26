package store

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"testing"
)

func jsonStateTestBytes(t *testing.T, graph *GraphState, nextNode, nextEdge, commit uint64, version uint16) []byte {
	t.Helper()
	if err := ensureDatabaseID(graph); err != nil {
		t.Fatal(err)
	}
	var payload bytes.Buffer
	if err := writePersistedStateJSON(&payload, graph, nextNode, nextEdge, commit); err != nil {
		t.Fatal(err)
	}
	header, err := encodeStateHeader(graph.DatabaseID, commit, uint64(payload.Len()), crc32.ChecksumIEEE(payload.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	switch version {
	case jsonStateVersion:
		copy(header[:8], jsonStateBinaryMagic[:])
	case legacyStateVersion:
		copy(header[:8], legacyStateBinaryMagic[:])
	default:
		t.Fatal("invalid JSON state fixture version")
	}
	binary.BigEndian.PutUint16(header[8:10], version)
	return append(header[:], payload.Bytes()...)
}

func jsonWALTestRecord(t *testing.T, databaseID string, commit uint64, value walPayload, version uint16) []byte {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateDatabaseID(databaseID); err != nil {
		t.Fatal(err)
	}
	header := make([]byte, legacyWALHeaderSize)
	switch version {
	case jsonWALVersion:
		copy(header[:8], jsonWALMagic[:])
	case legacyWALVersion:
		copy(header[:8], legacyWALMagic[:])
	default:
		t.Fatal("invalid JSON WAL fixture version")
	}
	binary.BigEndian.PutUint16(header[8:10], version)
	binary.BigEndian.PutUint16(header[10:12], legacyWALHeaderSize)
	binary.BigEndian.PutUint64(header[12:20], commit)
	binary.BigEndian.PutUint64(header[20:28], uint64(len(payload)))
	binary.BigEndian.PutUint32(header[28:32], crc32.ChecksumIEEE(payload))
	copy(header[walDatabaseIDAt:legacyWALHeaderSize], databaseID)
	return append(header, payload...)
}
