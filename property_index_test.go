package latticedb

import (
	"errors"
	"math"
	"path/filepath"
	"slices"
	"testing"
)

func TestExplicitPropertyIndexesPersistAndTrackTransactions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "property-indexes.ltdb")
	db, err := Open(path, OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	var aliceID, bobID, edgeID uint64
	if err := db.Update(func(tx *Tx) error {
		alice, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Person"}, Properties: map[string]Value{"email": "alice@example.com"}})
		if err != nil {
			return err
		}
		bob, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Person"}, Properties: map[string]Value{"email": "bob@example.com"}})
		if err != nil {
			return err
		}
		edge, err := tx.CreateEdge(alice.ID, bob.ID, "KNOWS", CreateEdgeOptions{Properties: map[string]Value{"since": int64(2024)}})
		aliceID, bobID, edgeID = alice.ID, bob.ID, edge.ID
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *Tx) error {
		_, err := tx.FindNodesByLabelProperty("Person", "email", "alice@example.com", 100)
		return err
	}); !errors.Is(err, ErrUnsupportedOption) {
		t.Fatalf("lookup without index = %v", err)
	}
	if err := db.CreateNodePropertyIndex("Person", "email"); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("Person", "email"); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("duplicate index creation = %v", err)
	}
	if err := db.CreateEdgePropertyIndex("KNOWS", "since"); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		if err := tx.SetProperty(bobID, "email", "alice@example.com"); err != nil {
			return err
		}
		ids, err := tx.FindNodesByLabelProperty("Person", "email", "alice@example.com", 100)
		if err != nil {
			return err
		}
		if !slices.Equal(ids, []uint64{aliceID, bobID}) {
			t.Fatalf("transactional lookup = %v", ids)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.View(func(tx *Tx) error {
		nodes, err := tx.FindNodesByLabelProperty("Person", "email", "alice@example.com", 1)
		if err != nil {
			return err
		}
		if !slices.Equal(nodes, []uint64{aliceID}) {
			t.Fatalf("limited node lookup = %v", nodes)
		}
		edges, err := tx.FindEdgesByTypeProperty("KNOWS", "since", int64(2024), 100)
		if err != nil {
			return err
		}
		if !slices.Equal(edges, []uint64{edgeID}) {
			t.Fatalf("edge lookup = %v", edges)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	nodeQuery, err := db.Query("MATCH (n:Person) WHERE n.email = $email RETURN id(n) AS id", map[string]Value{"email": "alice@example.com"})
	if err != nil || len(nodeQuery.Rows) != 2 {
		t.Fatalf("indexed node query = %#v, %v", nodeQuery.Rows, err)
	}
	edgeQuery, err := db.Query("MATCH (a)-[e:KNOWS]->(b) WHERE e.since = $since RETURN id(e) AS id", map[string]Value{"since": float64(2024)})
	if err != nil || len(edgeQuery.Rows) != 1 {
		t.Fatalf("indexed edge query = %#v, %v", edgeQuery.Rows, err)
	}
	if err := db.DropNodePropertyIndex("Person", "email"); err != nil {
		t.Fatal(err)
	}
	if err := db.DropEdgePropertyIndex("KNOWS", "since"); err != nil {
		t.Fatal(err)
	}
}

func TestPropertyIndexQueryLimitAppliesAfterFiltering(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "query-limit.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var matchingID uint64
	if err := db.Update(func(tx *Tx) error {
		for _, active := range []bool{false, true} {
			node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Person"}, Properties: map[string]Value{"email": "shared@example.com", "active": active}})
			if err != nil {
				return err
			}
			if active {
				matchingID = node.ID
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("Person", "email"); err != nil {
		t.Fatal(err)
	}

	for _, query := range []string{
		"MATCH (n:Person) WHERE n.email = $email RETURN id(n) AS id LIMIT 0",
		"MATCH (n:Person) WHERE n.active = $active RETURN id(n) AS id LIMIT 0",
	} {
		result, err := db.Query(query, map[string]Value{"email": "shared@example.com", "active": true})
		if err != nil || len(result.Rows) != 0 {
			t.Fatalf("LIMIT 0 query %q = %#v, %v", query, result.Rows, err)
		}
	}
	result, err := db.Query("MATCH (n:Person) WHERE n.email = $email AND n.active = $active RETURN id(n) AS id LIMIT 1", map[string]Value{"email": "shared@example.com", "active": true})
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["id"] != int64(matchingID) {
		t.Fatalf("post-filter LIMIT = %#v, %v; want node %d", result.Rows, err, matchingID)
	}
}

func TestPropertyIndexQueryLimitScansPastResidualFailures(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "query-residual-limit.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var matchingID uint64
	if err := db.Update(func(tx *Tx) error {
		for index := range 256 {
			node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Person"}, Properties: map[string]Value{"email": "common@example.com", "active": index == 255}})
			if err != nil {
				return err
			}
			if index == 255 {
				matchingID = node.ID
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("Person", "email"); err != nil {
		t.Fatal(err)
	}
	result, err := db.Query("MATCH (n:Person) WHERE n.email = $email AND n.active = $active RETURN id(n) AS id LIMIT 1", map[string]Value{"email": "common@example.com", "active": true})
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["id"] != int64(matchingID) {
		t.Fatalf("residual-filter LIMIT = %#v, %v; want node %d", result.Rows, err, matchingID)
	}
	allocs := testing.AllocsPerRun(20, func() {
		if _, err := db.Query("MATCH (n:Person) WHERE n.email = $email AND n.active = $active RETURN id(n) AS id LIMIT 1", map[string]Value{"email": "common@example.com", "active": true}); err != nil {
			t.Fatal(err)
		}
	})
	if allocs > 40 {
		t.Fatalf("residual-filter LIMIT allocations = %.0f, want bounded hot path", allocs)
	}
}

func TestPropertyIndexWriteMutationTypeChangeAndStaleEntries(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "write-mutations.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var firstID, secondID uint64
	if err := db.Update(func(tx *Tx) error {
		var err error
		first, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Metric"}, Properties: map[string]Value{"value": int64(7)}})
		if err != nil {
			return err
		}
		second, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Metric"}, Properties: map[string]Value{"value": int64(7)}})
		if err != nil {
			return err
		}
		firstID, secondID = first.ID, second.ID
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("Metric", "value"); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *Tx) error {
		if err := tx.SetProperty(firstID, "value", float64(8)); err != nil {
			return err
		}
		oldIDs, err := tx.FindNodesByLabelProperty("Metric", "value", int64(7), 1)
		if err != nil || !slices.Equal(oldIDs, []uint64{secondID}) {
			t.Fatalf("stale-first lookup = %v, %v", oldIDs, err)
		}
		newIDs, err := tx.FindNodesByLabelProperty("Metric", "value", float64(8), 10)
		if err != nil || !slices.Equal(newIDs, []uint64{firstID}) {
			t.Fatalf("type-change lookup = %v, %v", newIDs, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestPropertyIndexNumericBoundaryQueries(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "numeric-boundaries.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	values := []int64{math.MinInt64, -(1 << 53), (1 << 53) - 1, 1 << 53, (1 << 53) + 1, math.MaxInt64}
	wantIDs := make(map[int64]uint64, len(values))
	if err := db.Update(func(tx *Tx) error {
		for _, value := range values {
			node, err := tx.CreateNode(CreateNodeOptions{Labels: []string{"Number"}, Properties: map[string]Value{"value": value}})
			if err != nil {
				return err
			}
			wantIDs[value] = node.ID
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateNodePropertyIndex("Number", "value"); err != nil {
		t.Fatal(err)
	}
	for _, value := range values {
		result, err := db.Query("MATCH (n:Number) WHERE n.value = $value RETURN id(n) AS id", map[string]Value{"value": value})
		if err != nil || len(result.Rows) != 1 || result.Rows[0]["id"] != int64(wantIDs[value]) {
			t.Fatalf("numeric boundary %d = %#v, %v; want node %d", value, result.Rows, err, wantIDs[value])
		}
	}
	result, err := db.Query("MATCH (n:Number) WHERE n.value = $value RETURN id(n) AS id", map[string]Value{"value": math.Exp2(63)})
	if err != nil || len(result.Rows) != 0 {
		t.Fatalf("2^63 query = %#v, %v", result.Rows, err)
	}
}

func TestPropertyIndexTracksQueryLabelPropertyAndEntityRemoval(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "query-removals.ltdb"), OpenOptions{Create: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.CreateNodePropertyIndex("Tracked", "key"); err != nil {
		t.Fatal(err)
	}

	created, err := db.Query("CREATE (n:Tracked {key: \"label\"}) RETURN id(n) AS id", nil)
	if err != nil || len(created.Rows) != 1 {
		t.Fatalf("create labeled node = %#v, %v", created.Rows, err)
	}
	labelID := created.Rows[0]["id"].(int64)
	if err := db.View(func(tx *Tx) error {
		ids, err := tx.FindNodesByLabelProperty("Tracked", "key", "label", 1)
		if err != nil || !slices.Equal(ids, []uint64{uint64(labelID)}) {
			t.Fatalf("created label lookup = %v, %v", ids, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query("MATCH (n:Tracked) WHERE id(n) = $id REMOVE n:Tracked", map[string]Value{"id": labelID}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query("MATCH (n) WHERE id(n) = $id SET n:Tracked", map[string]Value{"id": labelID}); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *Tx) error {
		ids, err := tx.FindNodesByLabelProperty("Tracked", "key", "label", 1)
		if err != nil || !slices.Equal(ids, []uint64{uint64(labelID)}) {
			t.Fatalf("restored label lookup = %v, %v", ids, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query("MATCH (n:Tracked) WHERE id(n) = $id REMOVE n:Tracked", map[string]Value{"id": labelID}); err != nil {
		t.Fatal(err)
	}

	for _, mutation := range []string{
		"CREATE (n:Tracked {key: \"property\"}) RETURN id(n) AS id",
		"CREATE (n:Tracked {key: \"entity\"}) RETURN id(n) AS id",
	} {
		if _, err := db.Query(mutation, nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Query("MATCH (n:Tracked) WHERE n.key = \"property\" REMOVE n.key", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query("MATCH (n:Tracked) WHERE n.key = \"entity\" DELETE n", nil); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(tx *Tx) error {
		for _, key := range []string{"label", "property", "entity"} {
			ids, err := tx.FindNodesByLabelProperty("Tracked", "key", key, 10)
			if err != nil || len(ids) != 0 {
				t.Fatalf("removed %q lookup = %v, %v", key, ids, err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestPropertyIndexCreationChargesDefinitionBudget(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "definition-budget.ltdb"), OpenOptions{
		Create:                           true,
		DerivedIndexBuildMaxWork:         1,
		DerivedIndexBuildMaxLogicalBytes: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.CreateNodePropertyIndex("Unused", "value"); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("node index definition budget = %v", err)
	}
	if err := db.CreateEdgePropertyIndex("UNUSED", "value"); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("edge index definition budget = %v", err)
	}
}
