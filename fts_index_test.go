package latticedb

import "testing"

func TestFTSIndexDefinitionSurvivesSerializeDeserialize(t *testing.T) {
	db, err := Open(":memory:", OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.CreateFTSIndex(FTSIndexDefinition{Name: "articles", Kind: FTSIndexNode, Scope: "Article", Property: "body"}); err != nil {
		t.Fatal(err)
	}
	data, err := db.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := Deserialize(data, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if _, err := restored.FTSSearchIndex("articles", "term", FTSSearchOptions{}); err != nil {
		t.Fatalf("serialized definition unavailable: %v", err)
	}
	if err := restored.DropFTSIndex("articles"); err != nil {
		t.Fatal(err)
	}
	data, err = restored.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	restored, err = Deserialize(data, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if _, err := restored.FTSSearchIndex("articles", "term", FTSSearchOptions{}); err == nil {
		t.Fatal("dropped definition survived serialize/deserialize")
	}
}
