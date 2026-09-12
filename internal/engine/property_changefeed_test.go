package engine

import (
	"reflect"
	"testing"

	"github.com/mrchypark/latticedb-go/internal/store"
)

func TestTrackedPropertyChangefeedMatchesFullDiff(t *testing.T) {
	before := map[string]any{"keep": []any{int64(1), "nested"}, "remove": int64(3), "revert": "same", "set": int64(1)}
	after := map[string]any{"keep": before["keep"], "revert": "same", "set": int64(2), "null": nil}
	keys := map[string]struct{}{"remove": {}, "revert": {}, "set": {}, "null": {}, "missing": {}}
	for _, entity := range []string{"node", "edge"} {
		t.Run(entity, func(t *testing.T) {
			emit := func(tracked map[string]struct{}) ([]store.StreamRecord, uint64) {
				changes := newTxChanges(0)
				if entity == "node" {
					changes.nodePropertyKeys = map[uint64]map[string]struct{}{1: tracked}
				} else {
					changes.edgePropertyKeys = map[uint64]map[string]struct{}{1: tracked}
				}
				tx := &Tx{db: &DB{changefeedMaxBytes: 1 << 20}, graph: store.NewGraphState(), changes: changes}
				count := tx.countPropertyChanges(before, after, tracked)
				tx.appendPropertyChanges(entity, 1, before, after)
				return tx.graph.Streams.Read(changeStreamName, 0, 100), count
			}
			want, wantCount := emit(nil)
			got, count := emit(keys)
			if count != 3 || count != wantCount || uint64(len(got)) != count || !reflect.DeepEqual(got, want) {
				t.Fatalf("tracked changefeed count=%d records=%v, full count=%d records=%v", count, got, wantCount, want)
			}
		})
	}
}
