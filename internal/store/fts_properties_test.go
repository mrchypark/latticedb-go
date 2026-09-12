package store

import "testing"

func ftsPropertiesFixture() *GraphState {
	graph := NewGraphState()
	postings := NewStringPostings()
	postings.Add("alice", 1)
	postings.Add("bob", 2)
	graph.FTSProperties = map[string]StringPostings{"name": postings}
	return graph
}

func TestFTSPropertiesShallowCloneForksPostings(t *testing.T) {
	base := ftsPropertiesFixture()
	clone := CloneGraphStateShallow(base)
	postings := clone.FTSProperties["name"]
	postings.Add("carol", 3)
	clone.FTSProperties["name"] = postings

	if base.FTSProperties["name"].Len("carol") != 0 {
		t.Fatal("shallow FTS property clone mutated the old generation")
	}
	if clone.FTSProperties["name"].Len("carol") != 1 {
		t.Fatal("shallow FTS property clone lost its mutation")
	}
}

func TestFTSPropertiesDeepCloneCopiesPostings(t *testing.T) {
	base := ftsPropertiesFixture()
	clone := CloneGraphState(base)
	postings := clone.FTSProperties["name"]
	postings.Add("carol", 3)

	if base.FTSProperties["name"].Len("carol") != 0 {
		t.Fatal("deep FTS property clone mutated the old generation")
	}
	if clone.FTSProperties["name"].Len("carol") != 1 {
		t.Fatal("deep FTS property clone lost its mutation")
	}
}

func TestFTSPropertiesEmptyMapRemainsNil(t *testing.T) {
	base := NewGraphState()
	shallow := CloneGraphStateShallow(base)
	deep := CloneGraphState(base)
	if base.FTSProperties != nil || shallow.FTSProperties != nil || deep.FTSProperties != nil {
		t.Fatal("empty FTS property map was allocated")
	}
}
