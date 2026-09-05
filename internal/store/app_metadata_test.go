package store

import (
	"fmt"
	"maps"
	"slices"
	"testing"
)

func TestAppMetadataForkIsolationWithShardCollisions(t *testing.T) {
	groups := make(map[uint8][]string)
	var keys []string
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key/%d", i)
		shard := uint8(hashString(key))
		groups[shard] = append(groups[shard], key)
		if len(groups[shard]) == 3 {
			keys = groups[shard]
			break
		}
	}
	if len(keys) != 3 {
		t.Fatal("did not find three keys sharing a shard")
	}
	neighbor := "neighbor"
	for uint8(hashString(neighbor)) == uint8(hashString(keys[0])) {
		neighbor += "x"
	}
	keys = append(keys, neighbor)
	var base AppMetadata
	for _, key := range keys {
		base.Set(key, []byte("old"))
	}
	left, right := base.Fork(), base.Fork()
	left.Set(keys[0], []byte("new"))
	left.Delete(keys[1])
	right.Delete(keys[0])
	right.Set(keys[1], nil)
	for _, key := range keys {
		if got, ok := base.Get(key); !ok || string(got) != "old" {
			t.Fatalf("base changed: %q = %q, %v", key, got, ok)
		}
	}
	if got, ok := right.Get(keys[1]); !ok || got != nil {
		t.Fatalf("nil value lost: %q, %v", got, ok)
	}
	if _, ok := right.Get(keys[0]); ok {
		t.Fatal("deleted key remains visible")
	}
	if got := maps.Collect(left.All()); len(got) != 3 || string(got[keys[0]]) != "new" || string(got[keys[2]]) != "old" || string(got[neighbor]) != "old" || left.Len() != 3 {
		t.Fatalf("left values = %v, length = %d", got, left.Len())
	}
	cleared := left.Fork()
	for _, key := range keys {
		cleared.Delete(key)
	}
	if cleared.Len() != 0 || len(maps.Collect(cleared.All())) != 0 {
		t.Fatal("deleted shard still has entries")
	}
	cleared.Set(keys[0], []byte("reinserted"))
	if got, _ := left.Get(keys[0]); string(got) != "new" || left.Len() != 3 || cleared.Len() != 1 {
		t.Fatal("reinsertion changed an older generation")
	}
	visits := 0
	for range base.All() {
		visits++
		break
	}
	if visits != 1 {
		t.Fatalf("early iterator stop = %d visits", visits)
	}
}

func TestCloneGraphStateDeepClonesAppMetadata(t *testing.T) {
	base := NewGraphState()
	base.AppMetadata.Set("key", []byte("old"))
	cloned := CloneGraphState(base)
	value, _ := cloned.AppMetadata.Get("key")
	value[0] = 'X'
	if got, _ := base.AppMetadata.Get("key"); !slices.Equal(got, []byte("old")) {
		t.Fatalf("deep clone shared metadata bytes: %q", got)
	}
}
