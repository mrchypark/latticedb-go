package conformance

import (
	"path/filepath"
	"reflect"
	"slices"
	"testing"
)

type Value = any

type OpenOptions struct {
	Create           bool
	ReadOnly         bool
	CacheSizeMB      uint32
	PageSize         uint32
	EnableVector     bool
	VectorDimensions uint16
}

type CreateNodeOptions struct {
	Labels     []string
	Properties map[string]Value
}

type CreateEdgeOptions struct {
	Properties map[string]Value
}

type Node struct {
	ID         uint64
	Labels     []string
	Properties map[string]Value
}

type Edge struct {
	ID         uint64
	SourceID   uint64
	TargetID   uint64
	Type       string
	Properties map[string]Value
}

type QueryResult struct {
	Columns []string
	Rows    []map[string]Value
}

type VectorSearchOptions struct {
	K        uint32
	EfSearch uint16
}

type FTSSearchOptions struct {
	Limit         uint32
	MaxDistance   uint32
	MinTermLength uint32
}

type QueryCacheStats struct {
	Entries uint32
	Hits    uint64
	Misses  uint64
}

type VectorSearchResult struct {
	NodeID   uint64
	Distance float32
}

type FTSSearchResult struct {
	NodeID uint64
	Score  float32
}

type Driver interface {
	Open(path string, opts OpenOptions) (Database, error)
}

type Database interface {
	Close() error
	Begin(readOnly bool) (Tx, error)
	View(func(Tx) error) error
	Update(func(Tx) error) error
	Query(cypher string, params map[string]Value) (QueryResult, error)
	VectorSearch(vector []float32, opts VectorSearchOptions) ([]VectorSearchResult, error)
	FTSSearch(query string, opts FTSSearchOptions) ([]FTSSearchResult, error)
	CacheClear() error
	CacheStats() (QueryCacheStats, error)
}

type Tx interface {
	Commit() error
	Rollback() error
	CreateNode(opts CreateNodeOptions) (Node, error)
	DeleteNode(nodeID uint64) error
	NodeExists(nodeID uint64) (bool, error)
	GetNode(nodeID uint64) (*Node, error)
	SetProperty(nodeID uint64, key string, value Value) error
	GetProperty(nodeID uint64, key string) (Value, bool, error)
	SetVector(nodeID uint64, key string, vector []float32) error
	FTSIndex(nodeID uint64, text string) error
	CreateEdge(sourceID, targetID uint64, edgeType string, opts CreateEdgeOptions) (Edge, error)
	GetEdgeProperty(edgeID uint64, key string) (Value, bool, error)
	SetEdgeProperty(edgeID uint64, key string, value Value) error
	RemoveEdgeProperty(edgeID uint64, key string) error
	GetOutgoingEdges(nodeID uint64) ([]Edge, error)
}

var testDriver Driver

func TestConformancePersistenceAndStableEdgeIdentity(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "persist.ltdb")

	db := openDB(t, dbPath, OpenOptions{Create: true})

	var aliceID uint64
	var bobID uint64
	var edge1ID uint64
	var rolledBackEdgeID uint64
	var edge2ID uint64

	err := db.Update(func(tx Tx) error {
		alice, err := tx.CreateNode(CreateNodeOptions{
			Labels: []string{"Person", "Employee"},
			Properties: map[string]Value{
				"name":    "Alice",
				"payload": []byte{1, 2, 3},
				"vector":  []float32{1.0, 2.5, 3.0},
				"note":    nil,
				"meta": map[string]Value{
					"team": "graph",
				},
			},
		})
		if err != nil {
			return err
		}
		bob, err := tx.CreateNode(CreateNodeOptions{
			Labels:     []string{"Person"},
			Properties: map[string]Value{"name": "Bob"},
		})
		if err != nil {
			return err
		}
		edge1, err := tx.CreateEdge(alice.ID, bob.ID, "KNOWS", CreateEdgeOptions{
			Properties: map[string]Value{
				"since":   int64(2024),
				"payload": []byte{9, 9},
				"vector":  []float32{0.25, 0.75},
				"note":    nil,
			},
		})
		if err != nil {
			return err
		}

		aliceID = alice.ID
		bobID = bob.ID
		edge1ID = edge1.ID
		return nil
	})
	if err != nil {
		t.Fatalf("seed graph: %v", err)
	}

	rollbackTx, err := db.Begin(false)
	if err != nil {
		t.Fatalf("begin rollback tx: %v", err)
	}
	rolledBackEdge, err := rollbackTx.CreateEdge(aliceID, bobID, "KNOWS", CreateEdgeOptions{
		Properties: map[string]Value{"since": int64(2025)},
	})
	if err != nil {
		t.Fatalf("create rolled-back edge: %v", err)
	}
	rolledBackEdgeID = rolledBackEdge.ID
	if err := rollbackTx.Rollback(); err != nil {
		t.Fatalf("rollback tx: %v", err)
	}

	err = db.Update(func(tx Tx) error {
		edge2, err := tx.CreateEdge(aliceID, bobID, "KNOWS", CreateEdgeOptions{
			Properties: map[string]Value{"since": int64(2026)},
		})
		if err != nil {
			return err
		}
		edge2ID = edge2.ID
		return nil
	})
	if err != nil {
		t.Fatalf("create committed edge after rollback: %v", err)
	}

	if edge2ID <= rolledBackEdgeID {
		t.Fatalf("expected committed edge id %d to be greater than rolled-back edge id %d", edge2ID, rolledBackEdgeID)
	}

	closeDB(t, db)
	db = openDB(t, dbPath, OpenOptions{})
	defer closeDB(t, db)

	err = db.View(func(tx Tx) error {
		alice, err := tx.GetNode(aliceID)
		if err != nil {
			return err
		}
		if alice == nil {
			t.Fatalf("expected alice to exist")
		}
		if !reflect.DeepEqual(alice.Labels, []string{"Person", "Employee"}) {
			t.Fatalf("unexpected labels after reopen: %#v", alice.Labels)
		}

		meta, ok, err := tx.GetProperty(aliceID, "meta")
		if err != nil {
			return err
		}
		if !ok {
			t.Fatalf("expected meta property after reopen")
		}
		expectedMeta := map[string]Value{"team": "graph"}
		if !reflect.DeepEqual(meta, expectedMeta) {
			t.Fatalf("unexpected meta property: %#v", meta)
		}

		payload, ok, err := tx.GetProperty(aliceID, "payload")
		if err != nil {
			return err
		}
		if !ok || !reflect.DeepEqual(payload, []byte{1, 2, 3}) {
			t.Fatalf("unexpected bytes property after reopen: ok=%v value=%#v", ok, payload)
		}

		vector, ok, err := tx.GetProperty(aliceID, "vector")
		if err != nil {
			return err
		}
		if !ok || !reflect.DeepEqual(vector, []float32{1.0, 2.5, 3.0}) {
			t.Fatalf("unexpected vector property after reopen: ok=%v value=%#v", ok, vector)
		}

		note, ok, err := tx.GetProperty(aliceID, "note")
		if err != nil {
			return err
		}
		if !ok || note != nil {
			t.Fatalf("unexpected null property after reopen: ok=%v value=%#v", ok, note)
		}

		edge1Since, ok, err := tx.GetEdgeProperty(edge1ID, "since")
		if err != nil {
			return err
		}
		if !ok || edge1Since != int64(2024) {
			t.Fatalf("unexpected edge1 property: ok=%v value=%#v", ok, edge1Since)
		}

		edge1Payload, ok, err := tx.GetEdgeProperty(edge1ID, "payload")
		if err != nil {
			return err
		}
		if !ok || !reflect.DeepEqual(edge1Payload, []byte{9, 9}) {
			t.Fatalf("unexpected edge1 bytes property: ok=%v value=%#v", ok, edge1Payload)
		}

		edge1Vector, ok, err := tx.GetEdgeProperty(edge1ID, "vector")
		if err != nil {
			return err
		}
		if !ok || !reflect.DeepEqual(edge1Vector, []float32{0.25, 0.75}) {
			t.Fatalf("unexpected edge1 vector property: ok=%v value=%#v", ok, edge1Vector)
		}

		edge1Note, ok, err := tx.GetEdgeProperty(edge1ID, "note")
		if err != nil {
			return err
		}
		if !ok || edge1Note != nil {
			t.Fatalf("unexpected edge1 null property: ok=%v value=%#v", ok, edge1Note)
		}

		edge2Since, ok, err := tx.GetEdgeProperty(edge2ID, "since")
		if err != nil {
			return err
		}
		if !ok || edge2Since != int64(2026) {
			t.Fatalf("unexpected edge2 property: ok=%v value=%#v", ok, edge2Since)
		}

		outgoing, err := tx.GetOutgoingEdges(aliceID)
		if err != nil {
			return err
		}
		if len(outgoing) != 2 {
			t.Fatalf("expected 2 persisted outgoing edges, got %d", len(outgoing))
		}
		ids := []uint64{outgoing[0].ID, outgoing[1].ID}
		slices.Sort(ids)
		if !reflect.DeepEqual(ids, []uint64{edge1ID, edge2ID}) {
			t.Fatalf("unexpected outgoing edge ids: %#v", ids)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("validate reopen state: %v", err)
	}

	var edge3ID uint64
	err = db.Update(func(tx Tx) error {
		edge3, err := tx.CreateEdge(aliceID, bobID, "KNOWS", CreateEdgeOptions{})
		if err != nil {
			return err
		}
		edge3ID = edge3.ID
		return nil
	})
	if err != nil {
		t.Fatalf("create edge after reopen: %v", err)
	}

	if edge3ID <= edge2ID {
		t.Fatalf("expected edge id after reopen %d to be greater than prior committed id %d", edge3ID, edge2ID)
	}
}

func TestConformanceLabelOrderAndUnlabeledNodes(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "labels.ltdb")
	db := openDB(t, dbPath, OpenOptions{Create: true})
	defer closeDB(t, db)

	var unlabeledID uint64
	err := db.Update(func(tx Tx) error {
		if _, err := tx.CreateNode(CreateNodeOptions{
			Labels:     []string{"Person", "Employee"},
			Properties: map[string]Value{"name": "Alice"},
		}); err != nil {
			return err
		}
		unlabeled, err := tx.CreateNode(CreateNodeOptions{
			Properties: map[string]Value{"name": "NoLabel"},
		})
		if err != nil {
			return err
		}
		unlabeledID = unlabeled.ID
		return nil
	})
	if err != nil {
		t.Fatalf("seed label-order graph: %v", err)
	}

	err = db.View(func(tx Tx) error {
		node, err := tx.GetNode(unlabeledID)
		if err != nil {
			return err
		}
		if node == nil {
			t.Fatalf("expected unlabeled node to exist")
		}
		if len(node.Labels) != 0 {
			t.Fatalf("expected unlabeled node to round-trip without labels, got %#v", node.Labels)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("validate unlabeled node direct api: %v", err)
	}

	result, err := db.Query(`MATCH (n:Person:Employee {name: "Alice"}) RETURN count(n) AS count`, nil)
	if err != nil {
		t.Fatalf("query multi-label conjunction in insertion order: %v", err)
	}
	requireSingleIntResult(t, result, "count", 1)

	result, err = db.Query(`MATCH (n:Employee:Person {name: "Alice"}) RETURN count(n) AS count`, nil)
	if err != nil {
		t.Fatalf("query multi-label conjunction in reversed order: %v", err)
	}
	requireSingleIntResult(t, result, "count", 1)

	result, err = db.Query(`MATCH (n:Person) RETURN count(n) AS count`, nil)
	if err != nil {
		t.Fatalf("query single-label match without duplicating multi-label nodes: %v", err)
	}
	requireSingleIntResult(t, result, "count", 1)

	result, err = db.Query(`MATCH (n) RETURN count(n) AS count`, nil)
	if err != nil {
		t.Fatalf("query all nodes without duplicating multi-label nodes: %v", err)
	}
	requireSingleIntResult(t, result, "count", 2)

	result, err = db.Query(`MATCH (n {name: "NoLabel"}) RETURN count(n) AS count`, nil)
	if err != nil {
		t.Fatalf("query unlabeled node by property: %v", err)
	}
	requireSingleIntResult(t, result, "count", 1)

	result, err = db.Query(`MATCH (n:Person {name: "NoLabel"}) RETURN count(n) AS count`, nil)
	if err != nil {
		t.Fatalf("query unlabeled node with missing label constraint: %v", err)
	}
	requireSingleIntResult(t, result, "count", 0)
}

func TestConformanceMissingVsNullAndNestedRoundTrip(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "values.ltdb")
	db := openDB(t, dbPath, OpenOptions{Create: true})

	var nodeID uint64
	err := db.Update(func(tx Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{
			Labels: []string{"Profile"},
			Properties: map[string]Value{
				"name":    "Alice",
				"note":    nil,
				"payload": []byte{1, 2, 3},
				"vector":  []float32{1.0, 2.5, 3.0},
				"profile": map[string]Value{
					"active": true,
					"tags":   []Value{"graph", int64(7)},
				},
			},
		})
		if err != nil {
			return err
		}
		nodeID = node.ID
		return nil
	})
	if err != nil {
		t.Fatalf("seed nested values: %v", err)
	}

	closeDB(t, db)
	db = openDB(t, dbPath, OpenOptions{})
	defer closeDB(t, db)

	err = db.View(func(tx Tx) error {
		note, ok, err := tx.GetProperty(nodeID, "note")
		if err != nil {
			return err
		}
		if !ok {
			t.Fatalf("expected stored null property")
		}
		if note != nil {
			t.Fatalf("expected stored null, got %#v", note)
		}

		missing, ok, err := tx.GetProperty(nodeID, "missing")
		if err != nil {
			return err
		}
		if ok {
			t.Fatalf("expected missing property, got %#v", missing)
		}

		profile, ok, err := tx.GetProperty(nodeID, "profile")
		if err != nil {
			return err
		}
		if !ok {
			t.Fatalf("expected nested profile property")
		}
		expectedProfile := map[string]Value{
			"active": true,
			"tags":   []Value{"graph", int64(7)},
		}
		if !reflect.DeepEqual(profile, expectedProfile) {
			t.Fatalf("unexpected profile value: %#v", profile)
		}

		payload, ok, err := tx.GetProperty(nodeID, "payload")
		if err != nil {
			return err
		}
		if !ok || !reflect.DeepEqual(payload, []byte{1, 2, 3}) {
			t.Fatalf("unexpected bytes payload: ok=%v value=%#v", ok, payload)
		}

		vector, ok, err := tx.GetProperty(nodeID, "vector")
		if err != nil {
			return err
		}
		if !ok || !reflect.DeepEqual(vector, []float32{1.0, 2.5, 3.0}) {
			t.Fatalf("unexpected vector value: ok=%v value=%#v", ok, vector)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("validate nested values: %v", err)
	}

	result, err := db.Query("MATCH (n:Profile) RETURN n.payload AS payload, n.vector AS vector, n.profile AS profile", nil)
	if err != nil {
		t.Fatalf("query projected nested values: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 projected nested-value row, got %d", len(result.Rows))
	}
	if !reflect.DeepEqual(result.Rows[0]["payload"], []byte{1, 2, 3}) {
		t.Fatalf("unexpected projected bytes payload: %#v", result.Rows[0]["payload"])
	}
	if !reflect.DeepEqual(result.Rows[0]["vector"], []float32{1.0, 2.5, 3.0}) {
		t.Fatalf("unexpected projected vector: %#v", result.Rows[0]["vector"])
	}
	expectedProjectedProfile := map[string]Value{
		"active": true,
		"tags":   []Value{"graph", int64(7)},
	}
	if !reflect.DeepEqual(result.Rows[0]["profile"], expectedProjectedProfile) {
		t.Fatalf("unexpected projected nested map: %#v", result.Rows[0]["profile"])
	}

	unwindRows := []Value{
		map[string]Value{
			"payload": []byte{9, 8},
			"vector":  []float32{4.0, 5.0},
			"profile": map[string]Value{
				"active": true,
				"tags":   []Value{"go", int64(9)},
			},
		},
		map[string]Value{
			"payload": []byte{7},
			"vector":  []float32{6.0, 7.0},
			"profile": map[string]Value{
				"active": false,
				"tags":   []Value{"db"},
			},
		},
	}
	result, err = db.Query(
		"UNWIND $rows AS row RETURN row.payload AS payload, row.vector AS vector, row.profile AS profile",
		map[string]Value{"rows": unwindRows},
	)
	if err != nil {
		t.Fatalf("query unwind nested values: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 unwind rows, got %d", len(result.Rows))
	}
	if !reflect.DeepEqual(result.Rows[0]["payload"], []byte{9, 8}) {
		t.Fatalf("unexpected unwind bytes payload row 0: %#v", result.Rows[0]["payload"])
	}
	if !reflect.DeepEqual(result.Rows[0]["vector"], []float32{4.0, 5.0}) {
		t.Fatalf("unexpected unwind vector row 0: %#v", result.Rows[0]["vector"])
	}
	if !reflect.DeepEqual(result.Rows[0]["profile"], map[string]Value{
		"active": true,
		"tags":   []Value{"go", int64(9)},
	}) {
		t.Fatalf("unexpected unwind nested map row 0: %#v", result.Rows[0]["profile"])
	}
	if !reflect.DeepEqual(result.Rows[1]["payload"], []byte{7}) {
		t.Fatalf("unexpected unwind bytes payload row 1: %#v", result.Rows[1]["payload"])
	}
	if !reflect.DeepEqual(result.Rows[1]["vector"], []float32{6.0, 7.0}) {
		t.Fatalf("unexpected unwind vector row 1: %#v", result.Rows[1]["vector"])
	}
	if !reflect.DeepEqual(result.Rows[1]["profile"], map[string]Value{
		"active": false,
		"tags":   []Value{"db"},
	}) {
		t.Fatalf("unexpected unwind nested map row 1: %#v", result.Rows[1]["profile"])
	}

	directUnwindRows := []Value{
		[]byte{9, 8},
		[]float32{4.0, 5.0},
		map[string]Value{
			"active": true,
			"tags":   []Value{"go", int64(9)},
		},
		[]Value{int64(2), "two", nil},
		nil,
	}
	result, err = db.Query(
		"UNWIND $rows AS row RETURN row AS value",
		map[string]Value{"rows": directUnwindRows},
	)
	if err != nil {
		t.Fatalf("query unwind direct values: %v", err)
	}
	if len(result.Rows) != 5 {
		t.Fatalf("expected 5 direct unwind rows, got %d", len(result.Rows))
	}
	if !reflect.DeepEqual(result.Rows[0]["value"], []byte{9, 8}) {
		t.Fatalf("unexpected direct unwind bytes row: %#v", result.Rows[0]["value"])
	}
	if !reflect.DeepEqual(result.Rows[1]["value"], []float32{4.0, 5.0}) {
		t.Fatalf("unexpected direct unwind vector row: %#v", result.Rows[1]["value"])
	}
	if !reflect.DeepEqual(result.Rows[2]["value"], map[string]Value{
		"active": true,
		"tags":   []Value{"go", int64(9)},
	}) {
		t.Fatalf("unexpected direct unwind map row: %#v", result.Rows[2]["value"])
	}
	if !reflect.DeepEqual(result.Rows[3]["value"], []Value{int64(2), "two", nil}) {
		t.Fatalf("unexpected direct unwind list row: %#v", result.Rows[3]["value"])
	}
	if result.Rows[4]["value"] != nil {
		t.Fatalf("unexpected direct unwind null row: %#v", result.Rows[4]["value"])
	}

	result, err = db.Query("MATCH (n:Profile) WHERE n.note IS NULL RETURN count(n) AS count", nil)
	if err != nil {
		t.Fatalf("query stored null with IS NULL: %v", err)
	}
	requireSingleIntResult(t, result, "count", 1)

	result, err = db.Query("MATCH (n:Profile) WHERE n.missing IS NULL RETURN count(n) AS count", nil)
	if err != nil {
		t.Fatalf("query missing property with IS NULL: %v", err)
	}
	requireSingleIntResult(t, result, "count", 1)

	result, err = db.Query("MATCH (n:Profile) WHERE n.name IS NOT NULL RETURN count(n) AS count", nil)
	if err != nil {
		t.Fatalf("query present property with IS NOT NULL: %v", err)
	}
	requireSingleIntResult(t, result, "count", 1)
}

func TestConformanceDirectionalTraversalAndUnknownRelationshipTypes(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "directionality.ltdb")
	db := openDB(t, dbPath, OpenOptions{Create: true})
	defer closeDB(t, db)

	err := db.Update(func(tx Tx) error {
		alice, err := tx.CreateNode(CreateNodeOptions{
			Labels:     []string{"Person"},
			Properties: map[string]Value{"name": "Alice"},
		})
		if err != nil {
			return err
		}
		bob, err := tx.CreateNode(CreateNodeOptions{
			Labels:     []string{"Person"},
			Properties: map[string]Value{"name": "Bob"},
		})
		if err != nil {
			return err
		}
		_, err = tx.CreateEdge(alice.ID, bob.ID, "KNOWS", CreateEdgeOptions{})
		return err
	})
	if err != nil {
		t.Fatalf("seed directionality graph: %v", err)
	}

	result, err := db.Query(
		"MATCH (a:Person {name: \"Alice\"})-[:KNOWS]->(b:Person {name: \"Bob\"}) RETURN count(b) AS count",
		nil,
	)
	if err != nil {
		t.Fatalf("query forward directional edge: %v", err)
	}
	requireSingleIntResult(t, result, "count", 1)

	result, err = db.Query(
		"MATCH (:Person {name: $name})-[:KNOWS]->(b:Person) RETURN count(b) AS count",
		map[string]Value{"name": "Alice"},
	)
	if err != nil {
		t.Fatalf("query anonymous parameterized start node: %v", err)
	}
	requireSingleIntResult(t, result, "count", 1)

	result, err = db.Query(
		"MATCH (a:Person {name: \"Bob\"})-[:KNOWS]->(b:Person {name: \"Alice\"}) RETURN count(b) AS count",
		nil,
	)
	if err != nil {
		t.Fatalf("query reverse directional edge: %v", err)
	}
	requireSingleIntResult(t, result, "count", 0)

	result, err = db.Query(
		"MATCH (a:Person)-[:UNKNOWN]->(b:Person) RETURN count(b) AS count",
		nil,
	)
	if err != nil {
		t.Fatalf("query unknown relationship type: %v", err)
	}
	requireSingleIntResult(t, result, "count", 0)
}

func TestConformanceQueryCreateNodeWithLabelsAndProperties(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "query_create.ltdb")
	db := openDB(t, dbPath, OpenOptions{Create: true})
	defer closeDB(t, db)

	result, err := db.Query(
		"CREATE (n:Profile:Employee {name: $name, payload: $payload, vector: $vector, meta: $meta}) RETURN id(n) AS id, n.name AS name",
		map[string]Value{
			"name":    "Carol",
			"payload": []byte{1, 2, 3},
			"vector":  []float32{1.0, 2.0, 3.0},
			"meta": map[string]Value{
				"team": "graph",
			},
		},
	)
	if err != nil {
		t.Fatalf("query create node: %v", err)
	}
	if !reflect.DeepEqual(result.Columns, []string{"id", "name"}) {
		t.Fatalf("unexpected create result columns: %#v", result.Columns)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 created row, got %d", len(result.Rows))
	}
	if result.Rows[0]["name"] != "Carol" {
		t.Fatalf("unexpected created node name: %#v", result.Rows[0]["name"])
	}

	idValue, ok := result.Rows[0]["id"].(int64)
	if !ok || idValue <= 0 {
		t.Fatalf("unexpected created node id: %#v", result.Rows[0]["id"])
	}
	createdID := uint64(idValue)

	err = db.View(func(tx Tx) error {
		node, err := tx.GetNode(createdID)
		if err != nil {
			return err
		}
		if node == nil {
			t.Fatalf("expected created node %d to exist", createdID)
		}
		if !reflect.DeepEqual(node.Labels, []string{"Profile", "Employee"}) {
			t.Fatalf("unexpected created node labels: %#v", node.Labels)
		}

		payload, ok, err := tx.GetProperty(createdID, "payload")
		if err != nil {
			return err
		}
		if !ok || !reflect.DeepEqual(payload, []byte{1, 2, 3}) {
			t.Fatalf("unexpected created payload: ok=%v value=%#v", ok, payload)
		}

		vector, ok, err := tx.GetProperty(createdID, "vector")
		if err != nil {
			return err
		}
		if !ok || !reflect.DeepEqual(vector, []float32{1.0, 2.0, 3.0}) {
			t.Fatalf("unexpected created vector: ok=%v value=%#v", ok, vector)
		}

		meta, ok, err := tx.GetProperty(createdID, "meta")
		if err != nil {
			return err
		}
		if !ok || !reflect.DeepEqual(meta, map[string]Value{"team": "graph"}) {
			t.Fatalf("unexpected created meta property: ok=%v value=%#v", ok, meta)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("validate created node: %v", err)
	}

	unsupported := "CREATE (a:Profile {name: $a})-[:REL]->(b:Profile {name: $b})"
	if _, err := db.Query(unsupported, map[string]Value{"a": "A", "b": "B"}); err == nil {
		t.Fatal("unsupported compound CREATE unexpectedly succeeded")
	}
	result, err = db.Query("MATCH (n) RETURN count(n) AS count", nil)
	if err != nil {
		t.Fatal(err)
	}
	requireSingleIntResult(t, result, "count", 1)
}

func TestConformanceQueryLimitZeroAndNegativeValues(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "query_limit.ltdb")
	db := openDB(t, dbPath, OpenOptions{Create: true})
	defer closeDB(t, db)

	err := db.Update(func(tx Tx) error {
		if _, err := tx.CreateNode(CreateNodeOptions{
			Labels:     []string{"Person"},
			Properties: map[string]Value{"name": "Alice"},
		}); err != nil {
			return err
		}
		if _, err := tx.CreateNode(CreateNodeOptions{
			Labels:     []string{"Person"},
			Properties: map[string]Value{"name": "Bob"},
		}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed limit graph: %v", err)
	}

	matchResult, err := db.Query("MATCH (n:Person) RETURN n.name AS name LIMIT 0", nil)
	if err != nil {
		t.Fatalf("match query with LIMIT 0: %v", err)
	}
	if !reflect.DeepEqual(matchResult.Columns, []string{"name"}) {
		t.Fatalf("unexpected match LIMIT 0 columns: %#v", matchResult.Columns)
	}
	if len(matchResult.Rows) != 0 {
		t.Fatalf("expected LIMIT 0 to return no MATCH rows, got %#v", matchResult.Rows)
	}

	unwindResult, err := db.Query(
		"UNWIND $rows AS row RETURN row AS value LIMIT 0",
		map[string]Value{"rows": []Value{"alpha", "beta"}},
	)
	if err != nil {
		t.Fatalf("unwind query with LIMIT 0: %v", err)
	}
	if !reflect.DeepEqual(unwindResult.Columns, []string{"value"}) {
		t.Fatalf("unexpected unwind LIMIT 0 columns: %#v", unwindResult.Columns)
	}
	if len(unwindResult.Rows) != 0 {
		t.Fatalf("expected LIMIT 0 to return no UNWIND rows, got %#v", unwindResult.Rows)
	}

	createResult, err := db.Query(
		"CREATE (n:Temp {name: \"CreatedWithZeroLimit\"}) RETURN id(n) AS id LIMIT 0",
		nil,
	)
	if err != nil {
		t.Fatalf("create query with LIMIT 0: %v", err)
	}
	if !reflect.DeepEqual(createResult.Columns, []string{"id"}) {
		t.Fatalf("unexpected create LIMIT 0 columns: %#v", createResult.Columns)
	}
	if len(createResult.Rows) != 0 {
		t.Fatalf("expected LIMIT 0 to return no CREATE rows, got %#v", createResult.Rows)
	}

	createdCount, err := db.Query(
		"MATCH (n:Temp {name: \"CreatedWithZeroLimit\"}) RETURN count(n) AS count",
		nil,
	)
	if err != nil {
		t.Fatalf("query node created by LIMIT 0 create: %v", err)
	}
	requireSingleIntResult(t, createdCount, "count", 1)

	if _, err := db.Query("MATCH (n:Person) RETURN n.name AS name LIMIT -1", nil); err == nil {
		t.Fatalf("expected negative LIMIT on MATCH to fail")
	}

	if _, err := db.Query(
		"CREATE (n:Temp {name: \"RejectedNegativeLimit\"}) RETURN id(n) AS id LIMIT -1",
		nil,
	); err == nil {
		t.Fatalf("expected negative LIMIT on CREATE to fail")
	}

	rejectedCount, err := db.Query(
		"MATCH (n:Temp {name: \"RejectedNegativeLimit\"}) RETURN count(n) AS count",
		nil,
	)
	if err != nil {
		t.Fatalf("query node rejected by negative LIMIT create: %v", err)
	}
	requireSingleIntResult(t, rejectedCount, "count", 0)
}

func TestConformanceTransactionOwnWritesCommitAndRollback(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "mvcc.ltdb")
	db := openDB(t, dbPath, OpenOptions{Create: true})
	defer closeDB(t, db)

	var aliceID uint64
	err := db.Update(func(tx Tx) error {
		alice, err := tx.CreateNode(CreateNodeOptions{
			Labels:     []string{"Person"},
			Properties: map[string]Value{"name": "Alice"},
		})
		if err != nil {
			return err
		}
		aliceID = alice.ID
		return nil
	})
	if err != nil {
		t.Fatalf("seed alice: %v", err)
	}

	readOnlyTx := beginTx(t, db, true)
	if _, err := readOnlyTx.CreateNode(CreateNodeOptions{Labels: []string{"ShouldFail"}}); err == nil {
		t.Fatalf("expected read-only transaction to reject writes")
	}
	rollbackTx(t, readOnlyTx)

	writer := beginTx(t, db, false)
	var tempID uint64
	tempNode, err := writer.CreateNode(CreateNodeOptions{
		Labels:     []string{"Temp"},
		Properties: map[string]Value{"state": "draft"},
	})
	if err != nil {
		t.Fatalf("create temp node in writer: %v", err)
	}
	tempID = tempNode.ID
	if err := writer.SetProperty(aliceID, "name", "Bob"); err != nil {
		t.Fatalf("writer set property: %v", err)
	}

	nameWithinWriter := requireStringProperty(t, writer, aliceID, "name")
	if nameWithinWriter != "Bob" {
		t.Fatalf("writer should see own change, got %q", nameWithinWriter)
	}
	requireNodeExists(t, writer, tempID, true)

	if err := writer.Commit(); err != nil {
		t.Fatalf("commit writer: %v", err)
	}

	reader3 := beginTx(t, db, true)
	defer rollbackTx(t, reader3)
	nameAfterCommit := requireStringProperty(t, reader3, aliceID, "name")
	if nameAfterCommit != "Bob" {
		t.Fatalf("new snapshot should see committed value, got %q", nameAfterCommit)
	}
	requireNodeExists(t, reader3, tempID, true)

	rollbackWriter := beginTx(t, db, false)
	rolledBackNode, err := rollbackWriter.CreateNode(CreateNodeOptions{
		Labels:     []string{"Temp"},
		Properties: map[string]Value{"state": "rollback"},
	})
	if err != nil {
		t.Fatalf("create rolled back node: %v", err)
	}
	requireNodeExists(t, rollbackWriter, rolledBackNode.ID, true)
	if err := rollbackWriter.Rollback(); err != nil {
		t.Fatalf("rollback writer: %v", err)
	}

	reader4 := beginTx(t, db, true)
	defer rollbackTx(t, reader4)
	requireNodeExists(t, reader4, rolledBackNode.ID, false)
}

func TestConformanceQueryMutationAtomicityAndParallelEdgeTargeting(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "query.ltdb")
	db := openDB(t, dbPath, OpenOptions{Create: true})
	defer closeDB(t, db)

	var aliceID uint64
	var bobID uint64
	var edge1ID uint64
	var edge2ID uint64

	err := db.Update(func(tx Tx) error {
		alice, err := tx.CreateNode(CreateNodeOptions{
			Labels:     []string{"Person", "Employee"},
			Properties: map[string]Value{"name": "Alice", "temp": "remove"},
		})
		if err != nil {
			return err
		}
		bob, err := tx.CreateNode(CreateNodeOptions{
			Labels:     []string{"Person"},
			Properties: map[string]Value{"name": "Bob"},
		})
		if err != nil {
			return err
		}
		edge1, err := tx.CreateEdge(alice.ID, bob.ID, "REL", CreateEdgeOptions{
			Properties: map[string]Value{"w": int64(1)},
		})
		if err != nil {
			return err
		}
		edge2, err := tx.CreateEdge(alice.ID, bob.ID, "REL", CreateEdgeOptions{
			Properties: map[string]Value{"w": int64(2)},
		})
		if err != nil {
			return err
		}

		aliceID = alice.ID
		bobID = bob.ID
		edge1ID = edge1.ID
		edge2ID = edge2.ID
		return nil
	})
	if err != nil {
		t.Fatalf("seed query graph: %v", err)
	}

	if _, err := db.Query(
		"MATCH (a:Person {name: \"Alice\"}), (b:Person {name: \"Bob\"}) CREATE (a)-[:GOOD {since: $since, payload: $payload, meta: $meta}]->(b)",
		map[string]Value{
			"since":   int64(2030),
			"payload": []byte{4, 5, 6},
			"meta": map[string]Value{
				"team": "graph",
			},
		},
	); err != nil {
		t.Fatalf("successful edge create query: %v", err)
	}

	err = db.View(func(tx Tx) error {
		outgoing, err := tx.GetOutgoingEdges(aliceID)
		if err != nil {
			return err
		}
		found := false
		for _, edge := range outgoing {
			if edge.Type != "GOOD" {
				continue
			}
			found = true
			since, ok, err := tx.GetEdgeProperty(edge.ID, "since")
			if err != nil {
				return err
			}
			if !ok || since != int64(2030) {
				t.Fatalf("unexpected GOOD edge since property: ok=%v value=%#v", ok, since)
			}

			payload, ok, err := tx.GetEdgeProperty(edge.ID, "payload")
			if err != nil {
				return err
			}
			if !ok || !reflect.DeepEqual(payload, []byte{4, 5, 6}) {
				t.Fatalf("unexpected GOOD edge payload: ok=%v value=%#v", ok, payload)
			}

			meta, ok, err := tx.GetEdgeProperty(edge.ID, "meta")
			if err != nil {
				return err
			}
			if !ok || !reflect.DeepEqual(meta, map[string]Value{"team": "graph"}) {
				t.Fatalf("unexpected GOOD edge meta property: ok=%v value=%#v", ok, meta)
			}
		}
		if !found {
			t.Fatalf("expected successful query CREATE to add GOOD edge")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("validate successful edge create query: %v", err)
	}

	if _, err := db.Query(
		"MATCH (a:Person {name: \"Alice\"}), (b:Person {name: \"Bob\"}) CREATE (a)-[:BAD {owner: a}]->(b)",
		nil,
	); err == nil {
		t.Fatalf("expected invalid mutation query to fail")
	}

	result, err := db.Query("MATCH (:Person)-[r:BAD]->(:Person) RETURN count(r) AS count", nil)
	if err != nil {
		t.Fatalf("query bad edge count: %v", err)
	}
	requireSingleIntResult(t, result, "count", 0)

	if _, err := db.Query(
		"MATCH (a:Person {name: \"Alice\"}), (b:Person {name: \"Bob\"}) CREATE (a)-[:BAD {w: 1, w: 2}]->(b)",
		nil,
	); err == nil {
		t.Fatalf("expected duplicate-key mutation query to fail")
	}

	result, err = db.Query("MATCH (:Person)-[r:BAD]->(:Person) RETURN count(r) AS count", nil)
	if err != nil {
		t.Fatalf("query duplicate-key edge count: %v", err)
	}
	requireSingleIntResult(t, result, "count", 0)

	if _, err := db.Query(
		"MATCH (n:Person), (m:Person {name: \"Alice\"}) SET n.bad = m",
		nil,
	); err == nil {
		t.Fatalf("expected invalid SET mutation query to fail")
	}

	err = db.View(func(tx Tx) error {
		aliceBad, ok, err := tx.GetProperty(aliceID, "bad")
		if err != nil {
			return err
		}
		if ok {
			t.Fatalf("expected failed SET query to leave alice without bad property, got %#v", aliceBad)
		}

		bob, err := tx.GetNode(bobID)
		if err != nil {
			return err
		}
		if bob == nil {
			t.Fatalf("expected bob after failed SET query")
		}
		bobBad, ok, err := tx.GetProperty(bob.ID, "bad")
		if err != nil {
			return err
		}
		if ok {
			t.Fatalf("expected failed SET query to leave bob without bad property, got %#v", bobBad)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("validate failed SET query atomicity: %v", err)
	}

	if _, err := db.Query("MATCH (a)-[r:REL]->(b) WHERE r.w = 1 SET r.tag = \"selected\"", nil); err != nil {
		t.Fatalf("selective edge mutation query: %v", err)
	}

	if _, err := db.Query(
		"MATCH (a)-[r:REL]->(b) WHERE id(r) = $edgeID SET r += {note: \"keep\", temp: \"remove\"}",
		map[string]Value{"edgeID": edge1ID},
	); err != nil {
		t.Fatalf("edge property-map merge query: %v", err)
	}

	if _, err := db.Query(
		"MATCH (a)-[r:REL]->(b) WHERE id(r) = $edgeID REMOVE r.note",
		map[string]Value{"edgeID": edge1ID},
	); err != nil {
		t.Fatalf("edge property remove query: %v", err)
	}

	if _, err := db.Query(
		"MATCH (a)-[r:REL]->(b) WHERE id(r) = $edgeID SET r.temp = null",
		map[string]Value{"edgeID": edge1ID},
	); err != nil {
		t.Fatalf("edge null-removal query: %v", err)
	}

	if _, err := db.Query(
		"MATCH (a)-[r:REL]->(b) WHERE id(r) = $edgeID SET r = {w: 22, channel: \"secondary\"}",
		map[string]Value{"edgeID": edge2ID},
	); err != nil {
		t.Fatalf("edge property-map replacement query: %v", err)
	}

	err = db.View(func(tx Tx) error {
		tag1, ok, err := tx.GetEdgeProperty(edge1ID, "tag")
		if err != nil {
			return err
		}
		if !ok || tag1 != "selected" {
			t.Fatalf("expected first parallel edge to be tagged, got ok=%v value=%#v", ok, tag1)
		}

		tag2, ok, err := tx.GetEdgeProperty(edge2ID, "tag")
		if err != nil {
			return err
		}
		if ok {
			t.Fatalf("expected second parallel edge to remain untagged, got %#v", tag2)
		}

		w1, ok, err := tx.GetEdgeProperty(edge1ID, "w")
		if err != nil {
			return err
		}
		if !ok || w1 != int64(1) {
			t.Fatalf("unexpected first edge weight after edge mutations: ok=%v value=%#v", ok, w1)
		}

		_, ok, err = tx.GetEdgeProperty(edge1ID, "note")
		if err != nil {
			return err
		}
		if ok {
			t.Fatalf("expected edge REMOVE r.note to delete property")
		}

		_, ok, err = tx.GetEdgeProperty(edge1ID, "temp")
		if err != nil {
			return err
		}
		if ok {
			t.Fatalf("expected edge SET ... = null to remove property")
		}

		w2, ok, err := tx.GetEdgeProperty(edge2ID, "w")
		if err != nil {
			return err
		}
		if !ok || w2 != int64(22) {
			t.Fatalf("unexpected second edge weight after replacement: ok=%v value=%#v", ok, w2)
		}

		channel, ok, err := tx.GetEdgeProperty(edge2ID, "channel")
		if err != nil {
			return err
		}
		if !ok || channel != "secondary" {
			t.Fatalf("unexpected second edge channel after replacement: ok=%v value=%#v", ok, channel)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("validate parallel edge targeting: %v", err)
	}

	if _, err := db.Query("MATCH (n:Person:Employee {name: \"Alice\"}) SET n = {name: \"Alice\", role: \"lead\"}", nil); err != nil {
		t.Fatalf("query property-map replacement: %v", err)
	}

	if _, err := db.Query("MATCH (n:Person {name: \"Alice\"}) SET n += {team: \"graph\", role: \"staff\"}", nil); err != nil {
		t.Fatalf("query property-map merge: %v", err)
	}

	if _, err := db.Query("MATCH (n:Person {name: \"Alice\"}) REMOVE n.team", nil); err != nil {
		t.Fatalf("query property remove: %v", err)
	}

	if _, err := db.Query("MATCH (n:Person {name: \"Alice\"}) REMOVE n:Employee", nil); err != nil {
		t.Fatalf("query label remove: %v", err)
	}

	if _, err := db.Query("MATCH (n:Person {name: \"Alice\"}) SET n.temp = \"remove\"", nil); err != nil {
		t.Fatalf("query property set before null removal: %v", err)
	}

	if _, err := db.Query("MATCH (n:Person {name: \"Alice\"}) SET n.temp = null", nil); err != nil {
		t.Fatalf("query null removal: %v", err)
	}

	err = db.View(func(tx Tx) error {
		alice, err := tx.GetNode(aliceID)
		if err != nil {
			return err
		}
		if alice == nil {
			t.Fatalf("expected alice after query mutations")
		}
		if !reflect.DeepEqual(alice.Labels, []string{"Person"}) {
			t.Fatalf("unexpected labels after query mutations: %#v", alice.Labels)
		}

		_, ok, err := tx.GetProperty(aliceID, "temp")
		if err != nil {
			return err
		}
		if ok {
			t.Fatalf("expected query SET ... = null to remove property")
		}

		name, ok, err := tx.GetProperty(aliceID, "name")
		if err != nil {
			return err
		}
		if !ok || name != "Alice" {
			t.Fatalf("unexpected name after query mutations: ok=%v value=%#v", ok, name)
		}

		role, ok, err := tx.GetProperty(aliceID, "role")
		if err != nil {
			return err
		}
		if !ok || role != "staff" {
			t.Fatalf("unexpected role after query mutations: ok=%v value=%#v", ok, role)
		}

		_, ok, err = tx.GetProperty(aliceID, "team")
		if err != nil {
			return err
		}
		if ok {
			t.Fatalf("expected REMOVE n.team to delete property")
		}

		outgoing, err := tx.GetOutgoingEdges(aliceID)
		if err != nil {
			return err
		}
		relCount := 0
		goodCount := 0
		for _, edge := range outgoing {
			switch edge.Type {
			case "REL":
				relCount++
			case "GOOD":
				goodCount++
			}
		}
		if relCount != 2 {
			t.Fatalf("expected both parallel REL edges to remain, got %d in %#v", relCount, outgoing)
		}
		if goodCount != 1 {
			t.Fatalf("expected successful query CREATE edge to remain, got %d in %#v", goodCount, outgoing)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("validate query mutation semantics: %v", err)
	}

	if _, err := db.Query(
		"MATCH (:Person {name: \"Alice\"})-[r:REL]->(:Person {name: \"Bob\"}) WHERE id(r) = $edgeID DELETE r",
		map[string]Value{"edgeID": edge1ID},
	); err != nil {
		t.Fatalf("delete one parallel edge by stable id: %v", err)
	}

	err = db.View(func(tx Tx) error {
		outgoing, err := tx.GetOutgoingEdges(aliceID)
		if err != nil {
			return err
		}
		relCount := 0
		goodCount := 0
		for _, edge := range outgoing {
			switch edge.Type {
			case "REL":
				relCount++
				if edge.ID != edge2ID {
					t.Fatalf("expected surviving REL edge id %d, got %d", edge2ID, edge.ID)
				}
			case "GOOD":
				goodCount++
			}
		}
		if relCount != 1 {
			t.Fatalf("expected exactly 1 surviving REL edge after targeted delete, got %d in %#v", relCount, outgoing)
		}
		if goodCount != 1 {
			t.Fatalf("expected GOOD edge to remain after targeted delete, got %d in %#v", goodCount, outgoing)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("validate targeted parallel edge delete: %v", err)
	}
}

func TestConformanceSearchSemanticsAndQueryCache(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "search.ltdb")
	db := openDB(t, dbPath, OpenOptions{
		Create:           true,
		EnableVector:     true,
		VectorDimensions: 4,
	})
	defer closeDB(t, db)

	if err := db.CacheClear(); err != nil {
		t.Fatalf("clear cache before queries: %v", err)
	}
	initialStats, err := db.CacheStats()
	if err != nil {
		t.Fatalf("initial cache stats: %v", err)
	}
	if initialStats.Entries != 0 || initialStats.Hits != 0 || initialStats.Misses != 0 {
		t.Fatalf("unexpected initial cache stats: %#v", initialStats)
	}

	var nearDocID uint64
	var miscGraphDocID uint64
	var farDocID uint64
	err = db.Update(func(tx Tx) error {
		categoryDB, err := tx.CreateNode(CreateNodeOptions{
			Labels:     []string{"Category"},
			Properties: map[string]Value{"name": "Databases"},
		})
		if err != nil {
			return err
		}
		categoryMisc, err := tx.CreateNode(CreateNodeOptions{
			Labels:     []string{"Category"},
			Properties: map[string]Value{"name": "Misc"},
		})
		if err != nil {
			return err
		}

		docNear, err := tx.CreateNode(CreateNodeOptions{
			Labels:     []string{"Document"},
			Properties: map[string]Value{"name": "Doc Candidate", "text": "graph databases and traversal"},
		})
		if err != nil {
			return err
		}
		if err := tx.SetVector(docNear.ID, "embedding", []float32{1.0, 0.0, 0.0, 0.0}); err != nil {
			return err
		}
		if err := tx.FTSIndex(docNear.ID, "graph databases and traversal"); err != nil {
			return err
		}
		nearDocID = docNear.ID

		docMiscGraph, err := tx.CreateNode(CreateNodeOptions{
			Labels:     []string{"Document"},
			Properties: map[string]Value{"name": "Doc Misc Graph", "text": "graph notes and references"},
		})
		if err != nil {
			return err
		}
		if err := tx.SetVector(docMiscGraph.ID, "embedding", []float32{1.0, 0.1, 0.0, 0.0}); err != nil {
			return err
		}
		if err := tx.FTSIndex(docMiscGraph.ID, "graph notes and references"); err != nil {
			return err
		}
		miscGraphDocID = docMiscGraph.ID

		docFar, err := tx.CreateNode(CreateNodeOptions{
			Labels:     []string{"Document"},
			Properties: map[string]Value{"name": "Doc Far", "text": "cooking recipes and ingredients"},
		})
		if err != nil {
			return err
		}
		if err := tx.SetVector(docFar.ID, "embedding", []float32{0.0, 1.0, 0.0, 0.0}); err != nil {
			return err
		}
		if err := tx.FTSIndex(docFar.ID, "cooking recipes and ingredients"); err != nil {
			return err
		}
		farDocID = docFar.ID

		if _, err := tx.CreateEdge(docNear.ID, categoryDB.ID, "TAGGED", CreateEdgeOptions{}); err != nil {
			return err
		}
		if _, err := tx.CreateEdge(docNear.ID, categoryDB.ID, "TAGGED", CreateEdgeOptions{}); err != nil {
			return err
		}
		if _, err := tx.CreateEdge(docMiscGraph.ID, categoryMisc.ID, "TAGGED", CreateEdgeOptions{}); err != nil {
			return err
		}
		if _, err := tx.CreateEdge(docFar.ID, categoryMisc.ID, "TAGGED", CreateEdgeOptions{}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed search graph: %v", err)
	}

	vectorResults, err := db.VectorSearch([]float32{1.0, 0.0, 0.0, 0.0}, VectorSearchOptions{K: 3})
	if err != nil {
		t.Fatalf("direct vector search: %v", err)
	}
	if len(vectorResults) < 3 {
		t.Fatalf("expected at least 3 vector results, got %d", len(vectorResults))
	}
	if vectorResults[0].NodeID != nearDocID {
		t.Fatalf("expected nearest vector result %d first, got %d", nearDocID, vectorResults[0].NodeID)
	}
	if vectorResults[1].NodeID != miscGraphDocID {
		t.Fatalf("expected filtered misc graph result %d second, got %d", miscGraphDocID, vectorResults[1].NodeID)
	}
	if vectorResults[2].NodeID != farDocID {
		t.Fatalf("expected farther vector result %d third, got %d", farDocID, vectorResults[2].NodeID)
	}

	vectorQuery, err := db.Query(
		"MATCH (d:Document)-[:TAGGED]->(c:Category) WHERE d.embedding <=> $query RETURN c.name AS category, d.name AS document LIMIT 1",
		map[string]Value{"query": []float32{1.0, 0.0, 0.0, 0.0}},
	)
	if err != nil {
		t.Fatalf("vector query: %v", err)
	}
	requireSingleStringResult(t, vectorQuery, "category", "Databases")
	requireSingleStringResult(t, vectorQuery, "document", "Doc Candidate")

	filteredVectorQuery, err := db.Query(
		"MATCH (d:Document)-[:TAGGED]->(c:Category) WHERE d.embedding <=> $query AND c.name = \"Misc\" RETURN c.name AS category, d.name AS document LIMIT 1",
		map[string]Value{"query": []float32{1.0, 0.0, 0.0, 0.0}},
	)
	if err != nil {
		t.Fatalf("filtered vector query: %v", err)
	}
	requireSingleStringResult(t, filteredVectorQuery, "category", "Misc")
	requireSingleStringResult(t, filteredVectorQuery, "document", "Doc Misc Graph")

	ftsResults, err := db.FTSSearch("graph databases", FTSSearchOptions{Limit: 5})
	if err != nil {
		t.Fatalf("direct fts search: %v", err)
	}
	if len(ftsResults) == 0 {
		t.Fatalf("expected direct fts results")
	}
	if ftsResults[0].NodeID != nearDocID {
		t.Fatalf("expected graph-focused document %d to rank first, got %d", nearDocID, ftsResults[0].NodeID)
	}
	if len(ftsResults) < 2 {
		t.Fatalf("expected at least 2 direct fts results, got %d", len(ftsResults))
	}
	if ftsResults[1].NodeID != miscGraphDocID {
		t.Fatalf("expected weaker graph match %d second, got %d", miscGraphDocID, ftsResults[1].NodeID)
	}

	strictFTSResults, err := db.FTSSearch("grahp databases", FTSSearchOptions{
		Limit:         5,
		MaxDistance:   0,
		MinTermLength: 1,
	})
	if err != nil {
		t.Fatalf("strict direct fts search: %v", err)
	}
	if len(strictFTSResults) != 1 || strictFTSResults[0].NodeID != nearDocID {
		t.Fatalf("unexpected strict fts results: %#v", strictFTSResults)
	}

	looseFTSResults, err := db.FTSSearch("grahp databases", FTSSearchOptions{
		Limit:         5,
		MaxDistance:   2,
		MinTermLength: 1,
	})
	if err != nil {
		t.Fatalf("loose direct fts search: %v", err)
	}
	if len(looseFTSResults) < len(strictFTSResults) {
		t.Fatalf("expected loose fts results %#v to be at least as permissive as strict %#v", looseFTSResults, strictFTSResults)
	}
	if looseFTSResults[0].NodeID != nearDocID {
		t.Fatalf("expected fuzzy match to keep best document %d first, got %#v", nearDocID, looseFTSResults)
	}
	if !ftsResultsContain(looseFTSResults, miscGraphDocID) {
		t.Fatalf("expected loose fts results to include weaker fuzzy match %d, got %#v", miscGraphDocID, looseFTSResults)
	}

	ftsQuery, err := db.Query(
		"MATCH (d:Document)-[:TAGGED]->(c:Category) WHERE d.text @@ \"graph\" RETURN c.name AS category, d.name AS document LIMIT 1",
		nil,
	)
	if err != nil {
		t.Fatalf("fts query: %v", err)
	}
	requireSingleStringResult(t, ftsQuery, "category", "Databases")
	requireSingleStringResult(t, ftsQuery, "document", "Doc Candidate")

	filteredFTSQuery, err := db.Query(
		"MATCH (d:Document)-[:TAGGED]->(c:Category) WHERE d.text @@ \"graph\" AND c.name = \"Misc\" RETURN c.name AS category, d.name AS document",
		nil,
	)
	if err != nil {
		t.Fatalf("filtered fts query: %v", err)
	}
	requireSingleStringResult(t, filteredFTSQuery, "category", "Misc")
	requireSingleStringResult(t, filteredFTSQuery, "document", "Doc Misc Graph")

	vectorMultiplicityQuery, err := db.Query(
		"MATCH (d:Document)-[:TAGGED]->(c:Category) WHERE d.embedding <=> $query AND c.name = \"Databases\" RETURN count(d) AS count",
		map[string]Value{"query": []float32{1.0, 0.0, 0.0, 0.0}},
	)
	if err != nil {
		t.Fatalf("vector multiplicity query: %v", err)
	}
	requireSingleIntResult(t, vectorMultiplicityQuery, "count", 2)

	ftsMultiplicityQuery, err := db.Query(
		"MATCH (d:Document)-[:TAGGED]->(c:Category) WHERE d.text @@ \"graph\" AND c.name = \"Databases\" RETURN count(d) AS count",
		nil,
	)
	if err != nil {
		t.Fatalf("fts multiplicity query: %v", err)
	}
	requireSingleIntResult(t, ftsMultiplicityQuery, "count", 2)

	statsAfterFirst, err := db.CacheStats()
	if err != nil {
		t.Fatalf("cache stats after first query batch: %v", err)
	}
	if statsAfterFirst.Misses < 1 {
		t.Fatalf("expected cache misses after first queries, got %#v", statsAfterFirst)
	}

	if _, err := db.Query(
		"MATCH (d:Document)-[:TAGGED]->(c:Category) WHERE d.embedding <=> $query RETURN c.name AS category, d.name AS document LIMIT 1",
		map[string]Value{"query": []float32{1.0, 0.0, 0.0, 0.0}},
	); err != nil {
		t.Fatalf("repeat vector query: %v", err)
	}

	statsAfterSecond, err := db.CacheStats()
	if err != nil {
		t.Fatalf("cache stats after repeat query: %v", err)
	}
	if statsAfterSecond.Hits <= statsAfterFirst.Hits {
		t.Fatalf("expected cache hits to increase, got %#v then %#v", statsAfterFirst, statsAfterSecond)
	}

	if err := db.CacheClear(); err != nil {
		t.Fatalf("cache clear after queries: %v", err)
	}
	statsAfterClear, err := db.CacheStats()
	if err != nil {
		t.Fatalf("cache stats after clear: %v", err)
	}
	if statsAfterClear.Entries != 0 {
		t.Fatalf("expected cache entries to reset to zero, got %#v", statsAfterClear)
	}
}

func openDB(t *testing.T, path string, opts OpenOptions) Database {
	t.Helper()
	driver := currentDriver(t)
	db, err := driver.Open(path, opts)
	if err != nil {
		t.Fatalf("open db %s: %v", path, err)
	}
	return db
}

func currentDriver(t *testing.T) Driver {
	t.Helper()
	if testDriver == nil {
		t.Fatal("conformance driver adapter not configured")
	}
	return testDriver
}

func closeDB(t *testing.T, db Database) {
	t.Helper()
	if db == nil {
		return
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}
}

func beginTx(t *testing.T, db Database, readOnly bool) Tx {
	t.Helper()
	tx, err := db.Begin(readOnly)
	if err != nil {
		t.Fatalf("begin tx (readOnly=%v): %v", readOnly, err)
	}
	return tx
}

func rollbackTx(t *testing.T, tx Tx) {
	t.Helper()
	if tx == nil {
		return
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback tx: %v", err)
	}
}

func requireNodeExists(t *testing.T, tx Tx, nodeID uint64, want bool) {
	t.Helper()
	exists, err := tx.NodeExists(nodeID)
	if err != nil {
		t.Fatalf("node exists %d: %v", nodeID, err)
	}
	if exists != want {
		t.Fatalf("node %d exists=%v, want %v", nodeID, exists, want)
	}
}

func requireStringProperty(t *testing.T, tx Tx, nodeID uint64, key string) string {
	t.Helper()
	value, ok, err := tx.GetProperty(nodeID, key)
	if err != nil {
		t.Fatalf("get property %q on node %d: %v", key, nodeID, err)
	}
	if !ok {
		t.Fatalf("missing property %q on node %d", key, nodeID)
	}
	text, ok := value.(string)
	if !ok {
		t.Fatalf("property %q on node %d is %T, want string", key, nodeID, value)
	}
	return text
}

func requireSingleIntResult(t *testing.T, result QueryResult, column string, want int64) {
	t.Helper()
	value := requireSingleValue(t, result, column)
	got, ok := value.(int64)
	if !ok {
		t.Fatalf("column %q is %T, want int64", column, value)
	}
	if got != want {
		t.Fatalf("column %q = %d, want %d", column, got, want)
	}
}

func requireSingleStringResult(t *testing.T, result QueryResult, column string, want string) {
	t.Helper()
	value := requireSingleValue(t, result, column)
	got, ok := value.(string)
	if !ok {
		t.Fatalf("column %q is %T, want string", column, value)
	}
	if got != want {
		t.Fatalf("column %q = %q, want %q", column, got, want)
	}
}

func requireSingleValue(t *testing.T, result QueryResult, column string) Value {
	t.Helper()
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	value, ok := result.Rows[0][column]
	if !ok {
		t.Fatalf("missing column %q in row %#v", column, result.Rows[0])
	}
	return value
}

func ftsResultsContain(results []FTSSearchResult, nodeID uint64) bool {
	for _, result := range results {
		if result.NodeID == nodeID {
			return true
		}
	}
	return false
}

func cloneValueMap(values map[string]Value) map[string]Value {
	if values == nil {
		return nil
	}
	out := make(map[string]Value, len(values))
	for key, value := range values {
		out[key] = cloneValue(value)
	}
	return out
}

func cloneValue(value Value) Value {
	switch v := value.(type) {
	case nil, bool, int64, float64, string:
		return v
	case []byte:
		return append([]byte(nil), v...)
	case []float32:
		return append([]float32(nil), v...)
	case []any:
		out := make([]Value, len(v))
		for i, item := range v {
			out[i] = cloneValue(item)
		}
		return out
	case map[string]any:
		out := make(map[string]Value, len(v))
		for key, item := range v {
			out[key] = cloneValue(item)
		}
		return out
	default:
		return v
	}
}
