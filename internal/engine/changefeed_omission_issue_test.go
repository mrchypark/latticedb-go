package engine

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func smallChangefeedDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "changefeed_test"), OpenOptions{
		Create:             true,
		ChangefeedMaxBytes: 16 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestChangefeedOmissionFlagsExplicit(t *testing.T) {
	db := smallChangefeedDB(t)
	var nodeID uint64
	if err := db.Update(func(tx *Tx) error {
		n, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"test"}})
		if err != nil {
			return err
		}
		nodeID = n.ID
		return tx.SetProperty(n.ID, "color", "red")
	}); err != nil {
		t.Fatal(err)
	}
	bigVal := strings.Repeat("x", 5<<10)
	if err := db.Update(func(tx *Tx) error {
		return tx.SetProperty(nodeID, "data", bigVal)
	}); err != nil {
		t.Fatal(err)
	}
	records, err := db.Changes(0, 64, 0)
	if err != nil {
		t.Fatal(err)
	}
	var colorSet, dataSet map[string]any
	for _, rec := range records {
		p := rec.Payload.(map[string]any)
		if p["key"] == "color" {
			colorSet = p
		}
		if p["key"] == "data" {
			dataSet = p
		}
	}
	if colorSet == nil {
		t.Fatal("no property_set record for color")
	}
	if v, ok := colorSet["new_value_omitted"]; !ok {
		t.Fatal("color property_set missing new_value_omitted flag")
	} else if v != false {
		t.Fatalf("color new_value_omitted = %v, want false", v)
	}
	if dataSet == nil {
		t.Fatal("no property_set record for data")
	}
	if v, ok := dataSet["new_value_omitted"]; !ok {
		t.Fatal("data property_set missing new_value_omitted flag")
	} else if v != true {
		t.Fatalf("data new_value_omitted = %v, want true", v)
	}
	env, ok := dataSet["new_value"].(map[string]any)
	if !ok {
		t.Fatalf("data new_value is %T, want omission envelope map", dataSet["new_value"])
	}
	if env["__lattice_value_omitted"] != true {
		t.Fatal("omission envelope missing __lattice_value_omitted")
	}
}

func TestChangefeedOrdinaryMapNotConfusedWithEnvelope(t *testing.T) {
	db := smallChangefeedDB(t)
	envelopeLike := map[string]any{
		"__lattice_value_omitted": true,
		"type":                    "string",
		"encoded_bytes":           int64(999),
	}
	if err := db.Update(func(tx *Tx) error {
		n, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"test"}})
		if err != nil {
			return err
		}
		return tx.SetProperty(n.ID, "meta", envelopeLike)
	}); err != nil {
		t.Fatal(err)
	}
	records, err := db.Changes(0, 64, 0)
	if err != nil {
		t.Fatal(err)
	}
	var metaSet map[string]any
	for _, rec := range records {
		p := rec.Payload.(map[string]any)
		if p["key"] == "meta" {
			metaSet = p
		}
	}
	if metaSet == nil {
		t.Fatal("no property_set record for meta")
	}
	if v, ok := metaSet["new_value_omitted"]; !ok {
		t.Fatal("meta property_set missing new_value_omitted flag")
	} else if v != false {
		t.Fatalf("meta new_value_omitted = %v, want false", v)
	}
	got := metaSet["new_value"].(map[string]any)
	if got["__lattice_value_omitted"] != true {
		t.Fatal("value content changed")
	}
	serialized, err := db.Serialize()
	if err != nil {
		t.Fatal(err)
	}
	copyDB, err := Deserialize(serialized, OpenOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer copyDB.Close()
	copyRecords, err := copyDB.Changes(0, 64, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, rec := range copyRecords {
		payload, ok := rec.Payload.(map[string]any)
		if ok && payload["key"] == "meta" {
			if payload["new_value_omitted"] != false || !reflect.DeepEqual(payload["new_value"], envelopeLike) {
				t.Fatalf("round-tripped user map = %#v", payload)
			}
			return
		}
	}
	t.Fatal("no round-tripped property_set record for meta")
}
