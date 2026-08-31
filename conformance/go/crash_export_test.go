package conformance

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestConformanceProcessDeathRecoversCommittedWAL(t *testing.T) {
	if dbPath := os.Getenv("LATTICEDB_CRASH_DB"); dbPath != "" {
		db := openDB(t, dbPath, OpenOptions{Create: true})
		var committedID uint64
		if err := db.Update(func(tx Tx) error {
			node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Committed"}})
			if err == nil {
				committedID = node.ID
			}
			return err
		}); err != nil {
			t.Fatal(err)
		}
		uncommitted := beginTx(t, db, false)
		ghost, err := uncommitted.CreateNode(CreateNodeOptions{Labels: []string{"Ghost"}})
		if err != nil {
			t.Fatal(err)
		}
		ids, err := json.Marshal([2]uint64{committedID, ghost.ID})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(os.Getenv("LATTICEDB_CRASH_IDS"), ids, 0o600); err != nil {
			t.Fatal(err)
		}
		os.Exit(0) // Deliberately bypass transaction rollback and database Close.
	}

	base := t.TempDir()
	dbPath := filepath.Join(base, "process-death.ltdb")
	idsPath := filepath.Join(base, "ids.json")
	command := exec.Command(os.Args[0], "-test.run=^TestConformanceProcessDeathRecoversCommittedWAL$")
	command.Env = append(os.Environ(), "LATTICEDB_CRASH_DB="+dbPath, "LATTICEDB_CRASH_IDS="+idsPath)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("crash helper: %v\n%s", err, output)
	}
	data, err := os.ReadFile(idsPath)
	if err != nil {
		t.Fatal(err)
	}
	var ids [2]uint64
	if err := json.Unmarshal(data, &ids); err != nil {
		t.Fatal(err)
	}
	db := openDB(t, dbPath, OpenOptions{})
	defer closeDB(t, db)
	if err := db.View(func(tx Tx) error {
		requireNodeExists(t, tx, ids[0], true)
		requireNodeExists(t, tx, ids[1], false)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestConformanceCrashRecoveryCommittedGraphAndRolledBackTransaction(t *testing.T) {
	recovery := currentRecoveryHarness(t)

	dbPath := filepath.Join(t.TempDir(), "crash_graph.ltdb")
	db := openDB(t, dbPath, OpenOptions{Create: true})

	var aliceID uint64
	var bobID uint64
	var edge1ID uint64
	var edge2ID uint64
	var rolledBackNodeID uint64
	var rolledBackEdgeID uint64

	err := db.Update(func(tx Tx) error {
		alice, err := tx.CreateNode(CreateNodeOptions{
			Labels: []string{"Person"},
			Properties: map[string]Value{
				"name": "Alice",
				"team": "graph",
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
		edge1, err := tx.CreateEdge(alice.ID, bob.ID, "KNOWS", CreateEdgeOptions{})
		if err != nil {
			return err
		}
		edge2, err := tx.CreateEdge(alice.ID, bob.ID, "KNOWS", CreateEdgeOptions{})
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
		t.Fatalf("seed crash graph: %v", err)
	}

	rollback := beginTx(t, db, false)
	ghost, err := rollback.CreateNode(CreateNodeOptions{
		Labels:     []string{"Ghost"},
		Properties: map[string]Value{"name": "Transient"},
	})
	if err != nil {
		t.Fatalf("create rolled-back node: %v", err)
	}
	ghostEdge, err := rollback.CreateEdge(aliceID, bobID, "KNOWS", CreateEdgeOptions{})
	if err != nil {
		t.Fatalf("create rolled-back edge: %v", err)
	}
	rolledBackNodeID = ghost.ID
	rolledBackEdgeID = ghostEdge.ID
	rollbackTx(t, rollback)

	closeDB(t, db)

	if err := recovery.SimulateCrash(dbPath); err != nil {
		t.Fatalf("simulate crash: %v", err)
	}

	db = openDB(t, dbPath, OpenOptions{})
	defer closeDB(t, db)

	err = db.View(func(tx Tx) error {
		alice, err := tx.GetNode(aliceID)
		if err != nil {
			return err
		}
		if alice == nil {
			t.Fatalf("expected recovered alice node")
		}
		if !reflect.DeepEqual(alice.Labels, []string{"Person"}) {
			t.Fatalf("unexpected recovered labels: %#v", alice.Labels)
		}

		team, ok, err := tx.GetProperty(aliceID, "team")
		if err != nil {
			return err
		}
		if !ok || team != "graph" {
			t.Fatalf("unexpected recovered direct property: ok=%v value=%#v", ok, team)
		}

		requireNodeExists(t, tx, aliceID, true)
		requireNodeExists(t, tx, bobID, true)
		requireNodeExists(t, tx, rolledBackNodeID, false)

		outgoing, err := tx.GetOutgoingEdges(aliceID)
		if err != nil {
			return err
		}
		if len(outgoing) != 2 {
			t.Fatalf("expected 2 recovered committed outgoing edges, got %d", len(outgoing))
		}

		recoveredIDs := make([]uint64, 0, len(outgoing))
		for _, edge := range outgoing {
			recoveredIDs = append(recoveredIDs, edge.ID)
			if edge.ID == rolledBackEdgeID {
				t.Fatalf("unexpected rolled-back edge %d after recovery", rolledBackEdgeID)
			}
		}
		slices.Sort(recoveredIDs)

		expectedIDs := []uint64{edge1ID, edge2ID}
		slices.Sort(expectedIDs)
		if !reflect.DeepEqual(recoveredIDs, expectedIDs) {
			t.Fatalf("unexpected recovered edge ids: got %#v want %#v", recoveredIDs, expectedIDs)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("validate recovered direct APIs: %v", err)
	}

	var newEdgeID uint64
	err = db.Update(func(tx Tx) error {
		edge, err := tx.CreateEdge(
			aliceID,
			bobID,
			"KNOWS",
			CreateEdgeOptions{Properties: map[string]Value{"since": int64(2026)}},
		)
		if err != nil {
			return err
		}
		newEdgeID = edge.ID
		return nil
	})
	if err != nil {
		t.Fatalf("create new edge after recovery: %v", err)
	}

	if newEdgeID <= edge2ID {
		t.Fatalf(
			"expected post-recovery edge id %d to be greater than highest committed id %d",
			newEdgeID,
			edge2ID,
		)
	}
}

func TestConformanceCrashRecoveryCommittedNodePropertyUpdateWinsOverRolledBackUpdate(t *testing.T) {
	recovery := currentRecoveryHarness(t)

	dbPath := filepath.Join(t.TempDir(), "crash_node_property_update.ltdb")
	db := openDB(t, dbPath, OpenOptions{Create: true})

	var nodeID uint64

	err := db.Update(func(tx Tx) error {
		node, err := tx.CreateNode(CreateNodeOptions{
			Labels: []string{"Metric"},
			Properties: map[string]Value{
				"score":   int64(1),
				"payload": []byte{1, 1},
				"vector":  []float32{1.0, 1.0},
				"note":    nil,
			},
		})
		if err != nil {
			return err
		}
		nodeID = node.ID
		return nil
	})
	if err != nil {
		t.Fatalf("seed node property graph: %v", err)
	}

	err = db.Update(func(tx Tx) error {
		if err := tx.SetProperty(nodeID, "score", int64(7)); err != nil {
			return err
		}
		if err := tx.SetProperty(nodeID, "payload", []byte{7, 8, 9}); err != nil {
			return err
		}
		if err := tx.SetProperty(nodeID, "vector", []float32{7.0, 8.0}); err != nil {
			return err
		}
		return tx.SetProperty(nodeID, "note", nil)
	})
	if err != nil {
		t.Fatalf("commit node property update: %v", err)
	}

	rollback := beginTx(t, db, false)
	if err := rollback.SetProperty(nodeID, "score", int64(9)); err != nil {
		t.Fatalf("set rolled-back node property update: %v", err)
	}
	if err := rollback.SetProperty(nodeID, "payload", []byte{9, 9, 9}); err != nil {
		t.Fatalf("set rolled-back node bytes update: %v", err)
	}
	if err := rollback.SetProperty(nodeID, "vector", []float32{9.0, 9.0}); err != nil {
		t.Fatalf("set rolled-back node vector update: %v", err)
	}
	rollbackTx(t, rollback)

	closeDB(t, db)

	if err := recovery.SimulateCrash(dbPath); err != nil {
		t.Fatalf("simulate crash: %v", err)
	}

	db = openDB(t, dbPath, OpenOptions{})
	defer closeDB(t, db)

	err = db.View(func(tx Tx) error {
		requireNodeExists(t, tx, nodeID, true)

		score, ok, err := tx.GetProperty(nodeID, "score")
		if err != nil {
			return err
		}
		if !ok || score != int64(7) {
			t.Fatalf("unexpected recovered node property: ok=%v value=%#v", ok, score)
		}

		payload, ok, err := tx.GetProperty(nodeID, "payload")
		if err != nil {
			return err
		}
		if !ok || !reflect.DeepEqual(payload, []byte{7, 8, 9}) {
			t.Fatalf("unexpected recovered node bytes property: ok=%v value=%#v", ok, payload)
		}

		vector, ok, err := tx.GetProperty(nodeID, "vector")
		if err != nil {
			return err
		}
		if !ok || !reflect.DeepEqual(vector, []float32{7.0, 8.0}) {
			t.Fatalf("unexpected recovered node vector property: ok=%v value=%#v", ok, vector)
		}

		note, ok, err := tx.GetProperty(nodeID, "note")
		if err != nil {
			return err
		}
		if !ok || note != nil {
			t.Fatalf("unexpected recovered node null property: ok=%v value=%#v", ok, note)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("validate recovered node property: %v", err)
	}
}

func TestConformanceCrashRecoverySecondaryLabelsAndEdgeProperties(t *testing.T) {
	recovery := currentRecoveryHarness(t)

	dbPath := filepath.Join(t.TempDir(), "crash_secondary_labels_edge_props.ltdb")
	db := openDB(t, dbPath, OpenOptions{Create: true})

	var aliceID uint64
	var bobID uint64
	var edgeID uint64

	err := db.Update(func(tx Tx) error {
		alice, err := tx.CreateNode(CreateNodeOptions{
			Labels: []string{"Person", "Employee"},
			Properties: map[string]Value{
				"name": "Alice",
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
		edge, err := tx.CreateEdge(alice.ID, bob.ID, "KNOWS", CreateEdgeOptions{
			Properties: map[string]Value{
				"since":    int64(2026),
				"note":     "stable",
				"payload":  []byte{5, 6, 7},
				"vector":   []float32{2.0, 4.0},
				"nullable": nil,
			},
		})
		if err != nil {
			return err
		}

		aliceID = alice.ID
		bobID = bob.ID
		edgeID = edge.ID
		return nil
	})
	if err != nil {
		t.Fatalf("seed crash graph with secondary labels and edge properties: %v", err)
	}
	closeDB(t, db)

	if err := recovery.SimulateCrash(dbPath); err != nil {
		t.Fatalf("simulate crash: %v", err)
	}

	db = openDB(t, dbPath, OpenOptions{})
	defer closeDB(t, db)

	err = db.View(func(tx Tx) error {
		requireNodeExists(t, tx, aliceID, true)
		requireNodeExists(t, tx, bobID, true)

		alice, err := tx.GetNode(aliceID)
		if err != nil {
			return err
		}
		if alice == nil {
			t.Fatalf("expected recovered alice node")
		}
		if !reflect.DeepEqual(alice.Labels, []string{"Person", "Employee"}) {
			t.Fatalf("unexpected recovered alice labels: %#v", alice.Labels)
		}

		since, ok, err := tx.GetEdgeProperty(edgeID, "since")
		if err != nil {
			return err
		}
		if !ok || since != int64(2026) {
			t.Fatalf("unexpected recovered edge property since: ok=%v value=%#v", ok, since)
		}

		note, ok, err := tx.GetEdgeProperty(edgeID, "note")
		if err != nil {
			return err
		}
		if !ok || note != "stable" {
			t.Fatalf("unexpected recovered edge property note: ok=%v value=%#v", ok, note)
		}

		payload, ok, err := tx.GetEdgeProperty(edgeID, "payload")
		if err != nil {
			return err
		}
		if !ok || !reflect.DeepEqual(payload, []byte{5, 6, 7}) {
			t.Fatalf("unexpected recovered edge property payload: ok=%v value=%#v", ok, payload)
		}

		vector, ok, err := tx.GetEdgeProperty(edgeID, "vector")
		if err != nil {
			return err
		}
		if !ok || !reflect.DeepEqual(vector, []float32{2.0, 4.0}) {
			t.Fatalf("unexpected recovered edge property vector: ok=%v value=%#v", ok, vector)
		}

		nullable, ok, err := tx.GetEdgeProperty(edgeID, "nullable")
		if err != nil {
			return err
		}
		if !ok || nullable != nil {
			t.Fatalf("unexpected recovered edge property nullable: ok=%v value=%#v", ok, nullable)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("validate recovered secondary labels and edge properties: %v", err)
	}

	labelQuery, err := db.Query(
		"MATCH (n:Employee) RETURN id(n) AS id",
		nil,
	)
	if err != nil {
		t.Fatalf("query recovered secondary label: %v", err)
	}
	if len(labelQuery.Rows) != 1 {
		t.Fatalf("expected 1 recovered Employee row, got %d", len(labelQuery.Rows))
	}
	if labelQuery.Rows[0]["id"] != int64(aliceID) {
		t.Fatalf("unexpected recovered Employee id: %#v", labelQuery.Rows[0]["id"])
	}

	edgeQuery, err := db.Query(
		"MATCH (:Person)-[r:KNOWS]->(:Person) RETURN r.since AS since, r.note AS note, r.payload AS payload, r.vector AS vector, r.nullable AS nullable",
		nil,
	)
	if err != nil {
		t.Fatalf("query recovered edge properties: %v", err)
	}
	if len(edgeQuery.Rows) != 1 {
		t.Fatalf("expected 1 recovered edge row, got %d", len(edgeQuery.Rows))
	}
	if edgeQuery.Rows[0]["since"] != int64(2026) || edgeQuery.Rows[0]["note"] != "stable" {
		t.Fatalf("unexpected recovered edge query row: %#v", edgeQuery.Rows[0])
	}
	if !reflect.DeepEqual(edgeQuery.Rows[0]["payload"], []byte{5, 6, 7}) {
		t.Fatalf("unexpected recovered edge query payload: %#v", edgeQuery.Rows[0]["payload"])
	}
	if !reflect.DeepEqual(edgeQuery.Rows[0]["vector"], []float32{2.0, 4.0}) {
		t.Fatalf("unexpected recovered edge query vector: %#v", edgeQuery.Rows[0]["vector"])
	}
	if edgeQuery.Rows[0]["nullable"] != nil {
		t.Fatalf("unexpected recovered edge query row: %#v", edgeQuery.Rows[0])
	}
}

func TestConformanceExportAndDumpInvariants(t *testing.T) {
	exporter := currentExporter(t)

	dbPath := filepath.Join(t.TempDir(), "export.ltdb")
	db := openDB(t, dbPath, OpenOptions{Create: true})

	var aliceID uint64
	var bobID uint64
	err := db.Update(func(tx Tx) error {
		alice, err := tx.CreateNode(CreateNodeOptions{
			Labels: []string{"Person", "Employee"},
			Properties: map[string]Value{
				"name": "Alice",
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
		if _, err := tx.CreateEdge(alice.ID, bob.ID, "REL", CreateEdgeOptions{
			Properties: map[string]Value{
				"since":    int64(2020),
				"status":   "active",
				"list":     []Value{int64(3), "two", nil},
				"nested":   map[string]Value{"beta": int64(2), "alpha": int64(1)},
				"nullable": nil,
				"vector":   []float32{1.5, 2.5},
			},
		}); err != nil {
			return err
		}
		if _, err := tx.CreateEdge(alice.ID, bob.ID, "REL", CreateEdgeOptions{
			Properties: map[string]Value{"since": int64(2021)},
		}); err != nil {
			return err
		}

		aliceID = alice.ID
		bobID = bob.ID
		return nil
	})
	if err != nil {
		t.Fatalf("seed export graph: %v", err)
	}
	closeDB(t, db)

	jsonPath := filepath.Join(t.TempDir(), "graph.json")
	if _, err := exporter.Export(dbPath, ExportFormatJSON, jsonPath); err != nil {
		t.Fatalf("export json: %v", err)
	}
	jsonGraph := readJSONGraphFromFile(t, jsonPath)
	requireGraphCounts(t, jsonGraph, 2, 2)
	requireExportEdgeProperties(t, jsonGraph)
	requireSingleNodeID(t, jsonGraph, fmt.Sprintf("%d", aliceID))
	requireSingleNodeID(t, jsonGraph, fmt.Sprintf("%d", bobID))

	jsonlPath := filepath.Join(t.TempDir(), "graph.jsonl")
	if _, err := exporter.Export(dbPath, ExportFormatJSONL, jsonlPath); err != nil {
		t.Fatalf("export jsonl: %v", err)
	}
	validateJSONLExport(t, jsonlPath)

	csvPath := filepath.Join(t.TempDir(), "graph.csv")
	manifest, err := exporter.Export(dbPath, ExportFormatCSV, csvPath)
	if err != nil {
		t.Fatalf("export csv: %v", err)
	}
	nodesCSV, edgesCSV := csvManifestPaths(t, csvPath, manifest)
	if lines := countNonEmptyLinesFile(t, nodesCSV); lines != 3 {
		t.Fatalf("unexpected node csv line count %d", lines)
	}
	if lines := countNonEmptyLinesFile(t, edgesCSV); lines != 3 {
		t.Fatalf("unexpected edge csv line count %d", lines)
	}

	dotPath := filepath.Join(t.TempDir(), "graph.dot")
	if _, err := exporter.Export(dbPath, ExportFormatDOT, dotPath); err != nil {
		t.Fatalf("export dot: %v", err)
	}
	validateDOTExport(t, dotPath)

	dumpBytes := mustDump(t, exporter, dbPath)
	dumpGraph := readJSONGraphBytes(t, dumpBytes)
	requireGraphCounts(t, dumpGraph, 2, 2)
	requireExportEdgeProperties(t, dumpGraph)
	requireCanonicalDump(t, dumpGraph, aliceID, bobID)

	secondDump := mustDump(t, exporter, dbPath)
	if string(dumpBytes) != string(secondDump) {
		t.Fatalf("expected canonical dump output to be byte-stable across repeated runs")
	}
}

func TestConformanceCanonicalDumpOrderingAndUnlabeledNodes(t *testing.T) {
	exporter := currentExporter(t)

	dbPath := filepath.Join(t.TempDir(), "canonical_dump.ltdb")
	db := openDB(t, dbPath, OpenOptions{Create: true})

	var alphaID uint64
	var betaID uint64
	var unlabeledID uint64
	var edgeBetaToAlpha uint64
	var edgeAlphaToBetaZeta uint64
	var edgeAlphaToBetaAlpha1 uint64
	var edgeAlphaToBetaAlpha2 uint64

	err := db.Update(func(tx Tx) error {
		alpha, err := tx.CreateNode(CreateNodeOptions{
			Labels: []string{"Person", "Employee"},
			Properties: map[string]Value{
				"zeta":     "last",
				"nullable": nil,
				"nested": map[string]Value{
					"beta":  int64(2),
					"alpha": int64(1),
				},
				"list":  []Value{int64(3), "two", nil},
				"alpha": "first",
				"name":  "Alpha",
			},
		})
		if err != nil {
			return err
		}
		beta, err := tx.CreateNode(CreateNodeOptions{
			Labels:     []string{"Person"},
			Properties: map[string]Value{"name": "Beta"},
		})
		if err != nil {
			return err
		}
		unlabeled, err := tx.CreateNode(CreateNodeOptions{
			Properties: map[string]Value{"name": "NoLabel"},
		})
		if err != nil {
			return err
		}

		edge1, err := tx.CreateEdge(beta.ID, alpha.ID, "BETA", CreateEdgeOptions{})
		if err != nil {
			return err
		}
		edge2, err := tx.CreateEdge(alpha.ID, beta.ID, "ZETA", CreateEdgeOptions{})
		if err != nil {
			return err
		}
		edge3, err := tx.CreateEdge(alpha.ID, beta.ID, "ALPHA", CreateEdgeOptions{
			Properties: map[string]Value{
				"zeta":     "last",
				"nullable": nil,
				"status":   "active",
				"nested": map[string]Value{
					"beta":  int64(2),
					"alpha": int64(1),
				},
				"list":   []Value{int64(3), "two", nil},
				"alpha":  "first",
				"vector": []float32{1.5, 2.5},
			},
		})
		if err != nil {
			return err
		}
		edge4, err := tx.CreateEdge(alpha.ID, beta.ID, "ALPHA", CreateEdgeOptions{})
		if err != nil {
			return err
		}

		alphaID = alpha.ID
		betaID = beta.ID
		unlabeledID = unlabeled.ID
		edgeBetaToAlpha = edge1.ID
		edgeAlphaToBetaZeta = edge2.ID
		edgeAlphaToBetaAlpha1 = edge3.ID
		edgeAlphaToBetaAlpha2 = edge4.ID
		return nil
	})
	if err != nil {
		t.Fatalf("seed canonical dump graph: %v", err)
	}
	closeDB(t, db)

	dumpBytes := mustDump(t, exporter, dbPath)
	dumpGraph := readJSONGraphBytes(t, dumpBytes)
	requireGraphCounts(t, dumpGraph, 3, 4)
	requireSingleNodeID(t, dumpGraph, fmt.Sprintf("%d", alphaID))
	requireSingleNodeID(t, dumpGraph, fmt.Sprintf("%d", betaID))
	requireSingleNodeID(t, dumpGraph, fmt.Sprintf("%d", unlabeledID))
	requireCanonicalNodeOrder(t, dumpGraph, []uint64{alphaID, betaID, unlabeledID})
	requireUnlabeledNodePresent(t, dumpGraph, unlabeledID)
	requireCanonicalEdgeOrder(t, dumpGraph, []uint64{
		edgeAlphaToBetaAlpha1,
		edgeAlphaToBetaAlpha2,
		edgeAlphaToBetaZeta,
		edgeBetaToAlpha,
	})
	requireCanonicalDumpListAndNull(t, dumpGraph, alphaID)
	requireCanonicalDumpEdgeValues(t, dumpGraph, edgeAlphaToBetaAlpha1)
	requireRawPropertyKeyOrder(t, dumpBytes, alphaID, []string{"alpha", "list", "name", "nested", "nullable", "zeta"}, "nested", []string{"alpha", "beta"})
	requireRawEdgePropertyKeyOrder(t, dumpBytes, edgeAlphaToBetaAlpha1, []string{"alpha", "list", "nested", "nullable", "status", "vector", "zeta"}, "nested", []string{"alpha", "beta"})

	secondDump := mustDump(t, exporter, dbPath)
	if string(dumpBytes) != string(secondDump) {
		t.Fatalf("expected canonical dump output to be byte-stable across repeated runs")
	}
}

func TestConformanceTypedExportPreservesLogicalValueKinds(t *testing.T) {
	exporter := currentExporter(t)

	dbPath := filepath.Join(t.TempDir(), "typed_export.ltdb")
	db := openDB(t, dbPath, OpenOptions{Create: true})

	err := db.Update(func(tx Tx) error {
		_, err := tx.CreateNode(CreateNodeOptions{
			Labels: []string{"Typed"},
			Properties: map[string]Value{
				"bytes_value":  []byte{1, 2, 3},
				"string_value": "AQID",
				"int_value":    int64(1),
				"float_value":  float64(1),
				"vector_value": []float32{1, 2},
				"list_value":   []Value{float64(1), float64(2)},
				"nested_value": map[string]Value{
					"inner_bytes": []byte{9},
					"inner_float": float64(2),
					"inner_list":  []Value{int64(3), nil},
				},
			},
		})
		return err
	})
	if err != nil {
		t.Fatalf("seed typed export graph: %v", err)
	}
	closeDB(t, db)

	jsonPath := filepath.Join(t.TempDir(), "typed.json")
	if _, err := exporter.Export(dbPath, ExportFormatJSON, jsonPath); err != nil {
		t.Fatalf("export typed json: %v", err)
	}
	jsonGraph := readJSONGraphFromFile(t, jsonPath)
	requireGraphCounts(t, jsonGraph, 1, 0)
	requireTypedValueKinds(t, jsonGraph.Nodes[0].Properties)

	dumpGraph := readJSONGraphBytes(t, mustDump(t, exporter, dbPath))
	requireGraphCounts(t, dumpGraph, 1, 0)
	requireTypedValueKinds(t, dumpGraph.Nodes[0].Properties)

	jsonlPath := filepath.Join(t.TempDir(), "typed.jsonl")
	if _, err := exporter.Export(dbPath, ExportFormatJSONL, jsonlPath); err != nil {
		t.Fatalf("export typed jsonl: %v", err)
	}
	requireTypedValueKinds(t, readSingleJSONLNodeProperties(t, jsonlPath))

	csvPath := filepath.Join(t.TempDir(), "typed.csv")
	manifest, err := exporter.Export(dbPath, ExportFormatCSV, csvPath)
	if err != nil {
		t.Fatalf("export typed csv: %v", err)
	}
	nodesCSV, _ := csvManifestPaths(t, csvPath, manifest)
	requireTypedValueKinds(t, readSingleCSVNodeProperties(t, nodesCSV))
}

func csvManifestPaths(t *testing.T, outputPath string, data []byte) (string, string) {
	t.Helper()
	var manifest struct {
		Nodes string `json:"nodes"`
		Edges string `json:"edges"`
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatalf("unmarshal CSV manifest: %v", err)
	}
	base := filepath.Dir(outputPath)
	return filepath.Join(base, manifest.Nodes), filepath.Join(base, manifest.Edges)
}

type exportedGraph struct {
	Nodes []exportedNode `json:"nodes"`
	Edges []exportedEdge `json:"edges"`
}

type exportedNode struct {
	ID         string                   `json:"id"`
	Labels     []string                 `json:"labels"`
	Properties map[string]exportedValue `json:"properties"`
}

type exportedEdge struct {
	ID         string                   `json:"id"`
	Source     string                   `json:"source"`
	Target     string                   `json:"target"`
	Type       string                   `json:"type"`
	Properties map[string]exportedValue `json:"properties"`
}

type exportedValue struct {
	Kind   string                   `json:"kind"`
	Bool   bool                     `json:"bool,omitempty"`
	Int    int64                    `json:"int,omitempty"`
	Float  float64                  `json:"float,omitempty"`
	String string                   `json:"string,omitempty"`
	Bytes  []byte                   `json:"bytes,omitempty"`
	Vector []float32                `json:"vector,omitempty"`
	List   []exportedValue          `json:"list,omitempty"`
	Map    map[string]exportedValue `json:"map,omitempty"`
}

func mustDump(t *testing.T, exporter Exporter, dbPath string) []byte {
	t.Helper()
	output, err := exporter.Dump(dbPath)
	if err != nil {
		t.Fatalf("dump database: %v", err)
	}
	return output
}

func readJSONGraphFromFile(t *testing.T, path string) exportedGraph {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read json export %s: %v", path, err)
	}
	return readJSONGraphBytes(t, data)
}

func readJSONGraphBytes(t *testing.T, data []byte) exportedGraph {
	t.Helper()
	var graph exportedGraph
	if err := json.Unmarshal(data, &graph); err != nil {
		t.Fatalf("unmarshal graph export: %v\n%s", err, data)
	}
	return graph
}

type jsonlExportRecord struct {
	Kind       string                   `json:"kind"`
	ID         string                   `json:"id"`
	Labels     []string                 `json:"labels"`
	Source     string                   `json:"source"`
	Target     string                   `json:"target"`
	Type       string                   `json:"type"`
	Properties map[string]exportedValue `json:"properties"`
}

func validateJSONLExport(t *testing.T, path string) {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open jsonl export %s: %v", path, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	nodeIDs := map[string]struct{}{}
	nodeCount := 0
	edgeCount := 0
	found2020 := false
	found2021 := false

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var record jsonlExportRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("unmarshal jsonl line %q: %v", line, err)
		}

		switch record.Kind {
		case "node":
			nodeCount++
			nodeIDs[record.ID] = struct{}{}
		case "edge":
			edgeCount++
			props := decodeExportPropertyMap(t, record.Properties)
			switch jsonIntValue(t, props["since"]) {
			case 2020:
				found2020 = true
				requireRichEdgeProperties(t, record.Properties)
			case 2021:
				found2021 = true
			}
		default:
			t.Fatalf("unexpected jsonl record kind: %#v", record.Kind)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan jsonl export %s: %v", path, err)
	}

	if nodeCount != 2 || len(nodeIDs) != 2 {
		t.Fatalf("expected 2 unique jsonl node records, got count=%d unique=%d", nodeCount, len(nodeIDs))
	}
	if edgeCount != 2 {
		t.Fatalf("expected 2 jsonl edge records, got %d", edgeCount)
	}
	if !found2020 || !found2021 {
		t.Fatalf("expected jsonl export to preserve both parallel edges")
	}
}

func readSingleJSONLNodeProperties(t *testing.T, path string) map[string]exportedValue {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open jsonl export %s: %v", path, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var record jsonlExportRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("unmarshal jsonl line %q: %v", line, err)
		}
		if record.Kind == "node" {
			return record.Properties
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan jsonl export %s: %v", path, err)
	}
	t.Fatalf("expected at least one node in jsonl export %s", path)
	return nil
}

func readSingleCSVNodeProperties(t *testing.T, path string) map[string]exportedValue {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open csv export %s: %v", path, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	rows, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("read csv export %s: %v", path, err)
	}
	if len(rows) < 2 {
		t.Fatalf("expected csv export %s to include one data row, got %d rows", path, len(rows))
	}
	if len(rows[1]) != 3 {
		t.Fatalf("unexpected csv node row shape in %s: %#v", path, rows[1])
	}

	var props map[string]exportedValue
	if err := json.Unmarshal([]byte(rows[1][2]), &props); err != nil {
		t.Fatalf("unmarshal csv node properties from %s: %v", path, err)
	}
	return props
}

func validateDOTExport(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read dot export %s: %v", path, err)
	}

	output := string(data)
	if !strings.HasPrefix(output, "digraph G {\n") {
		t.Fatalf("dot export missing digraph header:\n%s", output)
	}
	if !strings.HasSuffix(output, "}\n") {
		t.Fatalf("dot export missing closing brace:\n%s", output)
	}

	nodeLines := 0
	edgeLines := 0
	for _, line := range strings.Split(output, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if strings.Contains(trimmed, " -> ") {
			edgeLines++
			continue
		}
		if strings.HasPrefix(trimmed, "n") && strings.HasSuffix(trimmed, "];") {
			nodeLines++
		}
	}
	if nodeLines != 2 {
		t.Fatalf("expected 2 dot node lines, got %d", nodeLines)
	}
	if edgeLines != 2 {
		t.Fatalf("expected 2 dot edge lines, got %d", edgeLines)
	}
}

func countNonEmptyLinesFile(t *testing.T, path string) int {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read csv export %s: %v", path, err)
	}

	count := 0
	for _, line := range strings.Split(string(data), "\n") {
		if strings.TrimSpace(line) != "" {
			count++
		}
	}
	return count
}

func requireGraphCounts(t *testing.T, graph exportedGraph, wantNodes int, wantEdges int) {
	t.Helper()
	if len(graph.Nodes) != wantNodes {
		t.Fatalf("expected %d exported nodes, got %d", wantNodes, len(graph.Nodes))
	}
	if len(graph.Edges) != wantEdges {
		t.Fatalf("expected %d exported edges, got %d", wantEdges, len(graph.Edges))
	}
}

func decodeExportPropertyMap(t *testing.T, props map[string]exportedValue) map[string]any {
	t.Helper()
	if len(props) == 0 {
		return map[string]any{}
	}
	decoded := make(map[string]any, len(props))
	for key, value := range props {
		decoded[key] = decodeExportValue(t, value)
	}
	return decoded
}

func decodeExportValue(t *testing.T, value exportedValue) any {
	t.Helper()
	switch value.Kind {
	case "null":
		return nil
	case "bool":
		return value.Bool
	case "int":
		return value.Int
	case "float":
		return value.Float
	case "string":
		return value.String
	case "bytes":
		return append([]byte(nil), value.Bytes...)
	case "vector":
		return append([]float32(nil), value.Vector...)
	case "list":
		out := make([]any, len(value.List))
		for i, item := range value.List {
			out[i] = decodeExportValue(t, item)
		}
		return out
	case "map":
		return decodeExportPropertyMap(t, value.Map)
	default:
		t.Fatalf("unexpected exported value kind %q", value.Kind)
		return nil
	}
}

func requireExportValueKind(t *testing.T, props map[string]exportedValue, key string, want string) exportedValue {
	t.Helper()
	value, ok := props[key]
	if !ok {
		t.Fatalf("missing exported property %q", key)
	}
	if value.Kind != want {
		t.Fatalf("unexpected exported kind for %q: got %q want %q", key, value.Kind, want)
	}
	return value
}

func requireExportEdgeProperties(t *testing.T, graph exportedGraph) {
	t.Helper()

	found2020 := false
	found2021 := false
	for _, edge := range graph.Edges {
		props := decodeExportPropertyMap(t, edge.Properties)
		switch jsonIntValue(t, props["since"]) {
		case 2020:
			found2020 = true
			requireExportValueKind(t, edge.Properties, "since", "int")
			requireRichEdgeProperties(t, edge.Properties)
		case 2021:
			found2021 = true
		}
	}
	if !found2020 || !found2021 {
		t.Fatalf("expected export to preserve both parallel edges, got %#v", graph.Edges)
	}
}

func requireRichEdgeProperties(t *testing.T, props map[string]exportedValue) {
	t.Helper()

	requireExportValueKind(t, props, "status", "string")
	requireExportValueKind(t, props, "list", "list")
	requireExportValueKind(t, props, "nested", "map")
	requireExportValueKind(t, props, "nullable", "null")
	requireExportValueKind(t, props, "vector", "vector")

	decoded := decodeExportPropertyMap(t, props)

	if status := fmt.Sprint(decoded["status"]); status != "active" {
		t.Fatalf("expected rich edge status active, got %#v", decoded["status"])
	}

	listValue, ok := decoded["list"].([]any)
	if !ok {
		t.Fatalf("expected rich edge list property, got %#v", decoded["list"])
	}
	if !reflect.DeepEqual(listValue, []any{int64(3), "two", nil}) {
		t.Fatalf("unexpected rich edge list ordering: %#v", listValue)
	}

	nestedValue, ok := decoded["nested"].(map[string]any)
	if !ok {
		t.Fatalf("expected rich edge nested property, got %#v", decoded["nested"])
	}
	if jsonIntValue(t, nestedValue["alpha"]) != 1 || jsonIntValue(t, nestedValue["beta"]) != 2 {
		t.Fatalf("unexpected rich edge nested property values: %#v", nestedValue)
	}

	nullableValue, ok := decoded["nullable"]
	if !ok {
		t.Fatalf("missing rich edge nullable property")
	}
	if nullableValue != nil {
		t.Fatalf("expected rich edge nullable property to round-trip as null, got %#v", nullableValue)
	}

	vectorValue, ok := decoded["vector"].([]float32)
	if !ok {
		t.Fatalf("expected rich edge vector property, got %#v", decoded["vector"])
	}
	if !reflect.DeepEqual(vectorValue, []float32{1.5, 2.5}) {
		t.Fatalf("unexpected rich edge vector property: %#v", vectorValue)
	}
}

func requireTypedValueKinds(t *testing.T, props map[string]exportedValue) {
	t.Helper()

	requireExportValueKind(t, props, "bytes_value", "bytes")
	requireExportValueKind(t, props, "string_value", "string")
	requireExportValueKind(t, props, "int_value", "int")
	requireExportValueKind(t, props, "float_value", "float")
	requireExportValueKind(t, props, "vector_value", "vector")
	requireExportValueKind(t, props, "list_value", "list")
	nested := requireExportValueKind(t, props, "nested_value", "map")

	decoded := decodeExportPropertyMap(t, props)
	if !reflect.DeepEqual(decoded["bytes_value"], []byte{1, 2, 3}) {
		t.Fatalf("unexpected exported bytes value: %#v", decoded["bytes_value"])
	}
	if decoded["string_value"] != "AQID" {
		t.Fatalf("unexpected exported string value: %#v", decoded["string_value"])
	}
	if decoded["int_value"] != int64(1) {
		t.Fatalf("unexpected exported int value: %#v", decoded["int_value"])
	}
	if decoded["float_value"] != float64(1) {
		t.Fatalf("unexpected exported float value: %#v", decoded["float_value"])
	}
	if !reflect.DeepEqual(decoded["vector_value"], []float32{1, 2}) {
		t.Fatalf("unexpected exported vector value: %#v", decoded["vector_value"])
	}
	if !reflect.DeepEqual(decoded["list_value"], []any{float64(1), float64(2)}) {
		t.Fatalf("unexpected exported list value: %#v", decoded["list_value"])
	}

	requireExportValueKind(t, nested.Map, "inner_bytes", "bytes")
	requireExportValueKind(t, nested.Map, "inner_float", "float")
	requireExportValueKind(t, nested.Map, "inner_list", "list")

	nestedDecoded, ok := decoded["nested_value"].(map[string]any)
	if !ok {
		t.Fatalf("unexpected exported nested map value: %#v", decoded["nested_value"])
	}
	if !reflect.DeepEqual(nestedDecoded["inner_bytes"], []byte{9}) {
		t.Fatalf("unexpected exported nested bytes value: %#v", nestedDecoded["inner_bytes"])
	}
	if nestedDecoded["inner_float"] != float64(2) {
		t.Fatalf("unexpected exported nested float value: %#v", nestedDecoded["inner_float"])
	}
	if !reflect.DeepEqual(nestedDecoded["inner_list"], []any{int64(3), nil}) {
		t.Fatalf("unexpected exported nested list value: %#v", nestedDecoded["inner_list"])
	}
}

func requireSingleNodeID(t *testing.T, graph exportedGraph, wantID string) {
	t.Helper()
	count := 0
	for _, node := range graph.Nodes {
		if node.ID == wantID {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected node id %s exactly once in export, got %d matches", wantID, count)
	}
}

func requireCanonicalNodeOrder(t *testing.T, graph exportedGraph, wantIDs []uint64) {
	t.Helper()
	if len(graph.Nodes) != len(wantIDs) {
		t.Fatalf("expected %d canonical dump nodes, got %d", len(wantIDs), len(graph.Nodes))
	}
	for i, wantID := range wantIDs {
		if graph.Nodes[i].ID != fmt.Sprintf("%d", wantID) {
			t.Fatalf("expected node %d at canonical position %d, got %#v", wantID, i, graph.Nodes)
		}
	}
}

func requireUnlabeledNodePresent(t *testing.T, graph exportedGraph, nodeID uint64) {
	t.Helper()
	wantID := fmt.Sprintf("%d", nodeID)
	for _, node := range graph.Nodes {
		if node.ID == wantID {
			if len(node.Labels) != 0 {
				t.Fatalf("expected unlabeled node %s to round-trip without labels, got %#v", wantID, node.Labels)
			}
			return
		}
	}
	t.Fatalf("missing unlabeled node %s in canonical dump", wantID)
}

func requireCanonicalEdgeOrder(t *testing.T, graph exportedGraph, wantIDs []uint64) {
	t.Helper()
	if len(graph.Edges) != len(wantIDs) {
		t.Fatalf("expected %d canonical dump edges, got %d", len(wantIDs), len(graph.Edges))
	}
	for i, wantID := range wantIDs {
		if graph.Edges[i].ID != fmt.Sprintf("%d", wantID) {
			t.Fatalf("expected edge %d at canonical position %d, got %#v", wantID, i, graph.Edges)
		}
	}
}

func requireCanonicalDumpListAndNull(t *testing.T, graph exportedGraph, nodeID uint64) {
	t.Helper()

	wantID := fmt.Sprintf("%d", nodeID)
	for _, node := range graph.Nodes {
		if node.ID != wantID {
			continue
		}
		requireExportValueKind(t, node.Properties, "list", "list")
		requireExportValueKind(t, node.Properties, "nullable", "null")
		decoded := decodeExportPropertyMap(t, node.Properties)
		listValue, ok := decoded["list"].([]any)
		if !ok {
			t.Fatalf("expected canonical dump list property on node %s, got %#v", wantID, decoded["list"])
		}
		if !reflect.DeepEqual(listValue, []any{int64(3), "two", nil}) {
			t.Fatalf("unexpected canonical dump list ordering on node %s: %#v", wantID, listValue)
		}
		nullableValue, ok := decoded["nullable"]
		if !ok {
			t.Fatalf("missing canonical dump nullable property on node %s", wantID)
		}
		if nullableValue != nil {
			t.Fatalf("expected canonical dump nullable property on node %s to round-trip as null, got %#v", wantID, nullableValue)
		}
		return
	}

	t.Fatalf("missing node %s when validating canonical dump list/null values", wantID)
}

func requireCanonicalDumpEdgeValues(t *testing.T, graph exportedGraph, edgeID uint64) {
	t.Helper()

	wantID := fmt.Sprintf("%d", edgeID)
	for _, edge := range graph.Edges {
		if edge.ID != wantID {
			continue
		}
		requireRichEdgeProperties(t, edge.Properties)
		return
	}

	t.Fatalf("missing edge %s when validating canonical dump edge values", wantID)
}

func requireRawPropertyKeyOrder(t *testing.T, dumpBytes []byte, nodeID uint64, wantKeys []string, nestedKey string, wantNestedKeys []string) {
	t.Helper()

	type rawExportedGraph struct {
		Nodes []json.RawMessage `json:"nodes"`
	}
	type rawExportedNode struct {
		ID         string          `json:"id"`
		Properties json.RawMessage `json:"properties"`
	}

	var graph rawExportedGraph
	if err := json.Unmarshal(dumpBytes, &graph); err != nil {
		t.Fatalf("unmarshal raw dump graph: %v", err)
	}

	wantID := fmt.Sprintf("%d", nodeID)
	for _, rawNode := range graph.Nodes {
		var node rawExportedNode
		if err := json.Unmarshal(rawNode, &node); err != nil {
			t.Fatalf("unmarshal raw dump node: %v", err)
		}
		if node.ID != wantID {
			continue
		}

		keys, values := orderedJSONObject(t, node.Properties)
		if !reflect.DeepEqual(keys, wantKeys) {
			t.Fatalf("unexpected canonical property key order for node %s: got %#v want %#v", wantID, keys, wantKeys)
		}

		nestedRaw, ok := values[nestedKey]
		if !ok {
			t.Fatalf("missing nested canonical property %q on node %s", nestedKey, wantID)
		}
		nestedKeys, _ := orderedExportMapKeys(t, nestedRaw)
		if !reflect.DeepEqual(nestedKeys, wantNestedKeys) {
			t.Fatalf("unexpected canonical nested key order for node %s property %q: got %#v want %#v", wantID, nestedKey, nestedKeys, wantNestedKeys)
		}
		return
	}

	t.Fatalf("missing node %s in raw canonical dump", wantID)
}

func requireRawEdgePropertyKeyOrder(t *testing.T, dumpBytes []byte, edgeID uint64, wantKeys []string, nestedKey string, wantNestedKeys []string) {
	t.Helper()

	type rawExportedGraph struct {
		Edges []json.RawMessage `json:"edges"`
	}
	type rawExportedEdge struct {
		ID         string          `json:"id"`
		Properties json.RawMessage `json:"properties"`
	}

	var graph rawExportedGraph
	if err := json.Unmarshal(dumpBytes, &graph); err != nil {
		t.Fatalf("unmarshal raw dump graph: %v", err)
	}

	wantID := fmt.Sprintf("%d", edgeID)
	for _, rawEdge := range graph.Edges {
		var edge rawExportedEdge
		if err := json.Unmarshal(rawEdge, &edge); err != nil {
			t.Fatalf("unmarshal raw dump edge: %v", err)
		}
		if edge.ID != wantID {
			continue
		}

		keys, values := orderedJSONObject(t, edge.Properties)
		if !reflect.DeepEqual(keys, wantKeys) {
			t.Fatalf("unexpected canonical edge property key order for edge %s: got %#v want %#v", wantID, keys, wantKeys)
		}

		nestedRaw, ok := values[nestedKey]
		if !ok {
			t.Fatalf("missing nested canonical edge property %q on edge %s", nestedKey, wantID)
		}
		nestedKeys, _ := orderedExportMapKeys(t, nestedRaw)
		if !reflect.DeepEqual(nestedKeys, wantNestedKeys) {
			t.Fatalf("unexpected canonical edge nested key order for edge %s property %q: got %#v want %#v", wantID, nestedKey, nestedKeys, wantNestedKeys)
		}
		return
	}

	t.Fatalf("missing edge %s in raw canonical dump", wantID)
}

func orderedJSONObject(t *testing.T, raw json.RawMessage) ([]string, map[string]json.RawMessage) {
	t.Helper()

	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil {
		t.Fatalf("read json object start: %v", err)
	}
	delim, ok := token.(json.Delim)
	if !ok || delim != '{' {
		t.Fatalf("expected json object, got %#v", token)
	}

	keys := []string{}
	values := map[string]json.RawMessage{}
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			t.Fatalf("read json object key: %v", err)
		}
		key, ok := keyToken.(string)
		if !ok {
			t.Fatalf("expected string json key, got %#v", keyToken)
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			t.Fatalf("decode raw json object value for key %q: %v", key, err)
		}
		keys = append(keys, key)
		values[key] = value
	}
	endToken, err := decoder.Token()
	if err != nil {
		t.Fatalf("read json object end: %v", err)
	}
	endDelim, ok := endToken.(json.Delim)
	if !ok || endDelim != '}' {
		t.Fatalf("expected json object end, got %#v", endToken)
	}
	return keys, values
}

func orderedExportMapKeys(t *testing.T, raw json.RawMessage) ([]string, map[string]json.RawMessage) {
	t.Helper()

	valueKeys, valueFields := orderedJSONObject(t, raw)
	if !reflect.DeepEqual(valueKeys, []string{"kind", "map"}) {
		t.Fatalf("expected typed map export wrapper, got keys %#v", valueKeys)
	}
	mapRaw, ok := valueFields["map"]
	if !ok {
		t.Fatalf("typed map export wrapper missing map payload")
	}
	return orderedJSONObject(t, mapRaw)
}

func requireCanonicalDump(t *testing.T, graph exportedGraph, aliceID, bobID uint64) {
	t.Helper()

	if len(graph.Nodes) != 2 || len(graph.Edges) != 2 {
		t.Fatalf("canonical dump requires 2 nodes and 2 edges, got nodes=%d edges=%d", len(graph.Nodes), len(graph.Edges))
	}

	if graph.Nodes[0].ID != fmt.Sprintf("%d", aliceID) || graph.Nodes[1].ID != fmt.Sprintf("%d", bobID) {
		t.Fatalf("expected canonical dump nodes sorted by id, got %#v", graph.Nodes)
	}

	if !reflect.DeepEqual(graph.Nodes[0].Labels, []string{"Employee", "Person"}) {
		t.Fatalf("expected canonical dump labels sorted lexicographically, got %#v", graph.Nodes[0].Labels)
	}

	if graph.Edges[0].ID == "" || graph.Edges[1].ID == "" {
		t.Fatalf("expected canonical dump edges to include stable ids, got %#v", graph.Edges)
	}
	firstProps := decodeExportPropertyMap(t, graph.Edges[0].Properties)
	secondProps := decodeExportPropertyMap(t, graph.Edges[1].Properties)
	if jsonIntValue(t, firstProps["since"]) != 2020 || jsonIntValue(t, secondProps["since"]) != 2021 {
		t.Fatalf("expected canonical dump edges sorted deterministically, got %#v", graph.Edges)
	}
}

func jsonIntValue(t *testing.T, value any) int64 {
	t.Helper()
	switch v := value.(type) {
	case float64:
		return int64(v)
	case int64:
		return v
	case json.Number:
		n, err := v.Int64()
		if err != nil {
			t.Fatalf("parse json number %q: %v", v, err)
		}
		return n
	default:
		t.Fatalf("unexpected numeric json value type %T (%#v)", value, value)
		return 0
	}
}
