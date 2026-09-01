package engine

import (
	"testing"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestReadStreamImmediateDoesNotAllocateTimer(t *testing.T) {
	db := &DB{graph: store.NewGraphState(), streamNotify: make(chan struct{})}
	allocs := testing.AllocsPerRun(100, func() {
		records, err := db.ReadStream("events", 0, 1, 0)
		if err != nil || len(records) != 0 {
			t.Fatalf("read = %#v, %v", records, err)
		}
	})
	if allocs != 0 {
		t.Fatalf("immediate stream read allocations = %f", allocs)
	}
}
