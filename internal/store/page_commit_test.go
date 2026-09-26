package store

import (
	"crypto/sha256"
	"testing"
)

func TestPageVectorConfigurationHistoryBindsAllocatorFrame(t *testing.T) {
	const databaseID = "00000000000000000000000000000001"
	makeFrame := func(nextNodeID uint64) []byte {
		t.Helper()
		payload, err := encodeBinaryWALPayload(walPayload{
			Kind: "delta",
			Delta: &persistedDelta{
				DatabaseID: databaseID,
				CommitID:   7,
				NextNodeID: nextNodeID,
				NextEdgeID: 1,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		header, err := encodeWALHeader(databaseID, 7, payload)
		if err != nil {
			t.Fatal(err)
		}
		return append(header[:], payload...)
	}

	previous := sha256.Sum256([]byte("previous page history"))
	first := pageVectorConfigurationHistory(previous, 7, 0, 2, makeFrame(1))
	second := pageVectorConfigurationHistory(previous, 7, 0, 2, makeFrame(9))
	if first == second {
		t.Fatal("configuration history omitted allocator high-water marks from the commit frame")
	}
}
