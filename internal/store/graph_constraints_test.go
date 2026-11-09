package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/mrchypark/latticedb-go/internal/pagestore"
)

type constraintTestBudget struct {
	work, bytes, peak, maxWork, maxBytes uint64
}

var errConstraintTestBudget = errors.New("constraint test budget rejected")

func (budget *constraintTestBudget) ChargeConstraintWork(work uint64) error {
	if work > budget.maxWork-budget.work {
		return errConstraintTestBudget
	}
	budget.work += work
	return nil
}

func (budget *constraintTestBudget) ReserveConstraintBytes(bytes uint64) error {
	if bytes > budget.maxBytes-budget.bytes {
		return errConstraintTestBudget
	}
	budget.bytes += bytes
	budget.peak = max(budget.peak, budget.bytes)
	return nil
}

func (budget *constraintTestBudget) ReleaseConstraintBytes(bytes uint64) {
	budget.bytes -= min(bytes, budget.bytes)
}

func TestGraphConstraintEncodingAndCatalog(t *testing.T) {
	required := GraphConstraint{Name: "z-email", Label: "Person", Property: "email", Kind: ConstraintKindRequired}
	typed := GraphConstraint{Name: "a-age", Label: "Person", Property: "age", Kind: ConstraintKindType, Type: ConstraintScalarInt64}
	encoded, err := EncodeGraphConstraint(typed)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeGraphConstraint([]byte(GraphConstraintPrefix+typed.Name), encoded)
	if err != nil || decoded != typed {
		t.Fatalf("round trip = %+v, %v", decoded, err)
	}
	if _, err := decodeGraphConstraint([]byte(GraphConstraintPrefix+"other"), encoded); err == nil {
		t.Fatal("mismatched key accepted")
	}
	if _, err := decodeGraphConstraint([]byte(GraphConstraintPrefix+typed.Name), append(encoded, 0)); err == nil {
		t.Fatal("trailing definition data accepted")
	}
	if _, err := EncodeGraphConstraint(GraphConstraint{Name: "bad", Label: "Person", Property: "age", Kind: ConstraintKindRequired, Type: ConstraintScalarInt64}); err == nil {
		t.Fatal("required constraint with a scalar type accepted")
	}

	graph := NewGraphState()
	if err := PutGraphConstraint(graph, required); err != nil {
		t.Fatal(err)
	}
	if err := PutGraphConstraint(graph, typed); err != nil {
		t.Fatal(err)
	}
	if !GraphConstraintsPresent(graph) {
		t.Fatal("feature marker was not written")
	}
	if marker, ok := graph.AppMetadata.Get(GraphConstraintsFeatureKey); !ok || string(marker) != "1" {
		t.Fatalf("marker = %q, %v", marker, ok)
	}
	got, err := ReadGraphConstraints(graph, nil)
	if err != nil {
		t.Fatal(err)
	}
	if want := []GraphConstraint{typed, required}; !slices.Equal(got, want) {
		t.Fatalf("definitions = %+v, want %+v", got, want)
	}
	if err := PutGraphConstraint(graph, required); err == nil {
		t.Fatal("duplicate constraint accepted")
	}
	if err := DropGraphConstraint(graph, required.Name); err != nil {
		t.Fatal(err)
	}
	if !GraphConstraintsPresent(graph) {
		t.Fatal("marker removed before the last definition")
	}
	if err := DropGraphConstraint(graph, typed.Name); err != nil {
		t.Fatal(err)
	}
	if GraphConstraintsPresent(graph) {
		t.Fatal("marker survived the last drop")
	}
	if err := DropGraphConstraint(graph, typed.Name); err == nil {
		t.Fatal("absent drop accepted")
	}
}

func TestReadGraphConstraintsRejectsMarkerCatalogMismatch(t *testing.T) {
	def := GraphConstraint{Name: "email", Label: "Person", Property: "email", Kind: ConstraintKindRequired}
	encoded, err := EncodeGraphConstraint(def)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		set  func(*AppMetadata)
	}{
		{"marker without catalog", func(metadata *AppMetadata) { metadata.Set(GraphConstraintsFeatureKey, []byte("1")) }},
		{"catalog without marker", func(metadata *AppMetadata) { metadata.Set(GraphConstraintPrefix+def.Name, encoded) }},
		{"malformed marker", func(metadata *AppMetadata) { metadata.Set(GraphConstraintsFeatureKey, []byte("true")) }},
		{"malformed definition", func(metadata *AppMetadata) {
			metadata.Set(GraphConstraintsFeatureKey, []byte("1"))
			metadata.Set(GraphConstraintPrefix+def.Name, []byte{1})
		}},
		{"mismatched definition key", func(metadata *AppMetadata) {
			metadata.Set(GraphConstraintsFeatureKey, []byte("1"))
			metadata.Set(GraphConstraintPrefix+"other", encoded)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			graph := NewGraphState()
			test.set(graph.AppMetadata)
			if _, err := ReadGraphConstraints(graph, nil); err == nil {
				t.Fatal("inconsistent raw constraint catalog accepted")
			}
		})
	}
}

func TestReadGraphConstraintsChargesCatalogAndRetainsDefinitions(t *testing.T) {
	graph := NewGraphState()
	for index := 0; index < 32; index++ {
		graph.AppMetadata.Set("unrelated/"+string(rune('a'+index%26)), []byte("value"))
	}
	putConstraint(t, graph, GraphConstraint{Name: "z", Label: "Item", Property: "z", Kind: ConstraintKindRequired})
	putConstraint(t, graph, GraphConstraint{Name: "a", Label: "Item", Property: "a", Kind: ConstraintKindRequired})
	budget := &constraintTestBudget{maxWork: 1 << 20, maxBytes: 1 << 20}
	definitions, retained, err := readGraphConstraints(context.Background(), graph, budget)
	if err != nil {
		t.Fatal(err)
	}
	if len(definitions) != 2 || retained == 0 || budget.bytes != retained {
		t.Fatalf("definitions=%d retained=%d budget=%+v", len(definitions), retained, budget)
	}
	if budget.work < uint64(graph.AppMetadata.Len()+1) {
		t.Fatalf("metadata scan work=%d, entries=%d", budget.work, graph.AppMetadata.Len())
	}
	budget.ReleaseConstraintBytes(retained)
	if budget.bytes != 0 {
		t.Fatalf("definition bytes retained after release: %d", budget.bytes)
	}

	limited := &constraintTestBudget{maxWork: 4, maxBytes: 1 << 20}
	if _, err := ReadGraphConstraints(graph, limited); !errors.Is(err, errConstraintTestBudget) {
		t.Fatalf("bounded catalog scan error = %v", err)
	}
}

func TestValidateGraphConstraintsSemantics(t *testing.T) {
	t.Run("required absent and null", func(t *testing.T) {
		for name := range map[string]any{"absent": nil, "null": nil} {
			t.Run(name, func(t *testing.T) {
				graph := NewGraphState()
				properties := map[string]any{"name": "alice"}
				if name == "null" {
					properties["email"] = nil
				}
				addNode(graph, 1, []string{"Person"}, properties)
				putConstraint(t, graph, GraphConstraint{Name: "email", Label: "Person", Property: "email", Kind: ConstraintKindRequired})
				err := ValidateGraphConstraints(context.Background(), graph, nil)
				assertConstraintViolation(t, err, "email", 1)
			})
		}
	})

	t.Run("type missing null and wrong type", func(t *testing.T) {
		graph := NewGraphState()
		addNode(graph, 1, []string{"Person"}, map[string]any{"name": "alice"})
		addNode(graph, 2, []string{"Person"}, nil)
		putConstraint(t, graph, GraphConstraint{Name: "name-type", Label: "Person", Property: "name", Kind: ConstraintKindType, Type: ConstraintScalarString})
		if err := ValidateGraphConstraints(context.Background(), graph, nil); err != nil {
			t.Fatal(err)
		}
		graph.Nodes.Get(1).Properties.Set("name", nil)
		assertConstraintViolation(t, ValidateGraphConstraints(context.Background(), graph, nil), "name-type", 1)
		graph.Nodes.Get(1).Properties.Set("name", int64(1))
		assertConstraintViolation(t, ValidateGraphConstraints(context.Background(), graph, nil), "name-type", 1)
	})

	t.Run("all scalar types", func(t *testing.T) {
		graph := NewGraphState()
		addNode(graph, 1, []string{"Item"}, map[string]any{"b": true, "i": int64(1), "f": float64(1), "s": "one"})
		for _, item := range []struct {
			name string
			kind ConstraintScalarType
		}{{"b", ConstraintScalarBool}, {"i", ConstraintScalarInt64}, {"f", ConstraintScalarFloat64}, {"s", ConstraintScalarString}} {
			putConstraint(t, graph, GraphConstraint{Name: item.name, Label: "Item", Property: item.name, Kind: ConstraintKindType, Type: item.kind})
		}
		if err := ValidateGraphConstraints(context.Background(), graph, nil); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("unique skips missing null and distinguishes scalar kinds", func(t *testing.T) {
		graph := NewGraphState()
		addNode(graph, 1, []string{"Item"}, map[string]any{"key": true})
		addNode(graph, 2, []string{"Item"}, map[string]any{"key": int64(1)})
		addNode(graph, 3, []string{"Item"}, map[string]any{"key": float64(1)})
		addNode(graph, 4, []string{"Item"}, map[string]any{"key": "1"})
		addNode(graph, 5, []string{"Item"}, map[string]any{"key": nil})
		addNode(graph, 6, []string{"Item"}, nil)
		putConstraint(t, graph, GraphConstraint{Name: "key", Label: "Item", Property: "key", Kind: ConstraintKindUnique})
		if err := ValidateGraphConstraints(context.Background(), graph, nil); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("unique duplicates and multilabel", func(t *testing.T) {
		graph := NewGraphState()
		addNode(graph, 20, []string{"Person", "Employee"}, map[string]any{"email": "same"})
		addNode(graph, 10, []string{"Person"}, map[string]any{"email": "same"})
		putConstraint(t, graph, GraphConstraint{Name: "email", Label: "Person", Property: "email", Kind: ConstraintKindUnique})
		for range 32 {
			assertConstraintViolation(t, ValidateGraphConstraints(context.Background(), graph, nil), "email", 20)
		}
	})

	t.Run("unique has no cross label scope", func(t *testing.T) {
		graph := NewGraphState()
		addNode(graph, 1, []string{"Person"}, map[string]any{"email": "same"})
		addNode(graph, 2, []string{"Company"}, map[string]any{"email": "same"})
		putConstraint(t, graph, GraphConstraint{Name: "person-email", Label: "Person", Property: "email", Kind: ConstraintKindUnique})
		if err := ValidateGraphConstraints(context.Background(), graph, nil); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("unique canonical signed zero", func(t *testing.T) {
		graph := NewGraphState()
		addNode(graph, 1, []string{"Item"}, map[string]any{"score": math.Copysign(0, -1)})
		addNode(graph, 2, []string{"Item"}, map[string]any{"score": float64(0)})
		putConstraint(t, graph, GraphConstraint{Name: "score", Label: "Item", Property: "score", Kind: ConstraintKindUnique})
		assertConstraintViolation(t, ValidateGraphConstraints(context.Background(), graph, nil), "score", 2)
	})

	t.Run("unique rejects collection values", func(t *testing.T) {
		graph := NewGraphState()
		addNode(graph, 1, []string{"Item"}, map[string]any{"key": []any{int64(1)}})
		putConstraint(t, graph, GraphConstraint{Name: "key", Label: "Item", Property: "key", Kind: ConstraintKindUnique})
		assertConstraintViolation(t, ValidateGraphConstraints(context.Background(), graph, nil), "key", 1)
	})

	t.Run("violation order uses constraint name then node id", func(t *testing.T) {
		graph := NewGraphState()
		addNode(graph, 1, []string{"Item"}, map[string]any{"z": "same"})
		addNode(graph, 2, []string{"Item"}, map[string]any{"z": "same"})
		putConstraint(t, graph, GraphConstraint{Name: "b-required", Label: "Item", Property: "y", Kind: ConstraintKindRequired})
		putConstraint(t, graph, GraphConstraint{Name: "a-unique", Label: "Item", Property: "z", Kind: ConstraintKindUnique})
		assertConstraintViolation(t, ValidateGraphConstraints(context.Background(), graph, nil), "a-unique", 2)
	})

	t.Run("budget and cancellation", func(t *testing.T) {
		graph := NewGraphState()
		addNode(graph, 1, []string{"Item"}, map[string]any{"key": "one"})
		putConstraint(t, graph, GraphConstraint{Name: "key", Label: "Item", Property: "key", Kind: ConstraintKindUnique})
		budget := &constraintTestBudget{maxWork: 3, maxBytes: 1 << 20}
		if err := ValidateGraphConstraints(context.Background(), graph, budget); !errors.Is(err, errConstraintTestBudget) {
			t.Fatalf("budget error = %v", err)
		}
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		if err := ValidateGraphConstraints(canceled, graph, nil); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation error = %v", err)
		}
	})

	t.Run("memory label index avoids unrelated node scan", func(t *testing.T) {
		graph := NewGraphState()
		for id := uint64(1); id <= 512; id++ {
			addNode(graph, id, []string{"Other"}, map[string]any{"value": int64(id)})
		}
		addNode(graph, 1000, []string{"Item"}, map[string]any{"key": "one"})
		putConstraint(t, graph, GraphConstraint{Name: "key", Label: "Item", Property: "key", Kind: ConstraintKindUnique})
		budget := &constraintTestBudget{maxWork: 20, maxBytes: 1024}
		if err := ValidateGraphConstraints(context.Background(), graph, budget); err != nil {
			t.Fatal(err)
		}
		if budget.bytes != 0 {
			t.Fatalf("label-index bytes retained: %d", budget.bytes)
		}
	})
}

func TestValidateGraphConstraintsPageLease(t *testing.T) {
	db, err := pagestore.Open(filepath.Join(t.TempDir(), "constraints.pages"), pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	write, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	page := &PageGraph{Tx: write}
	for _, node := range []*NodeRecord{
		{ID: 1, Labels: []string{"Person"}, Properties: PropertiesFromMap(map[string]any{"email": "one"})},
		{ID: 2, Labels: []string{"Person"}, Properties: PropertiesFromMap(map[string]any{"email": "two"})},
	} {
		if err := page.PutNode(node); err != nil {
			t.Fatal(err)
		}
	}
	if err := write.Commit(); err != nil {
		t.Fatal(err)
	}
	read, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Rollback()
	graph := NewGraphState()
	graph.PageBase = &PageGraph{Tx: read}
	putConstraint(t, graph, GraphConstraint{Name: "email", Label: "Person", Property: "email", Kind: ConstraintKindUnique})
	budget := &constraintTestBudget{maxWork: 1 << 20, maxBytes: 1 << 20}
	if err := ValidateGraphConstraints(context.Background(), graph, budget); err != nil {
		t.Fatal(err)
	}
	if budget.work == 0 || budget.peak == 0 || budget.bytes != 0 {
		t.Fatalf("page validation budget = %+v", budget)
	}
}

func TestGraphConstraintPageWorkUsesCallerBudget(t *testing.T) {
	db, err := pagestore.Open(filepath.Join(t.TempDir(), "constraints-work.pages"), pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	write, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if err := (&PageGraph{Tx: write}).PutNode(&NodeRecord{
		ID: 1, Labels: []string{"Item"},
		Properties: PropertiesFromMap(map[string]any{"active": true, "payload": strings.Repeat("x", 20<<20)}),
	}); err != nil {
		t.Fatal(err)
	}
	if err := write.Commit(); err != nil {
		t.Fatal(err)
	}
	read, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Rollback()
	graph := NewGraphState()
	graph.PageBase = &PageGraph{Tx: read}
	putConstraint(t, graph, GraphConstraint{Name: "active", Label: "Item", Property: "active", Kind: ConstraintKindRequired})

	writeBudget := &constraintTestBudget{maxWork: 16 << 20, maxBytes: 256 << 20}
	if err := ValidateGraphConstraints(t.Context(), graph, writeBudget); !errors.Is(err, errConstraintTestBudget) {
		t.Fatalf("write budget error = %v", err)
	}
	recovery := &recoveryBudget{limits: RecoveryLimits{MaxWork: defaultConstraintRecoveryWork, MaxDecodedBytes: defaultConstraintRecoveryBytes}}
	if err := ValidateGraphConstraints(t.Context(), graph, recovery); err != nil {
		t.Fatal(err)
	}
	if recovery.work <= 20<<20 {
		t.Fatalf("page decode work = %d, want more than payload bytes", recovery.work)
	}
	limited := &recoveryBudget{limits: RecoveryLimits{MaxWork: 16 << 20, MaxDecodedBytes: defaultConstraintRecoveryBytes}}
	if err := ValidateGraphConstraints(t.Context(), graph, limited); !errors.Is(err, ErrLoadResourceLimit) {
		t.Fatalf("limited recovery error = %v", err)
	}
}

func TestValidateGraphConstraintsPageOverlayLabelMerge(t *testing.T) {
	db, err := pagestore.Open(filepath.Join(t.TempDir(), "constraint-overlay.pages"), pagestore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	write, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	page := &PageGraph{Tx: write}
	for _, node := range []*NodeRecord{
		{ID: 1, Labels: []string{"Person"}, Properties: PropertiesFromMap(map[string]any{"email": "one"})},
		{ID: 2, Labels: []string{"Person"}, Properties: PropertiesFromMap(map[string]any{"email": "two"})},
	} {
		if err := page.PutNode(node); err != nil {
			t.Fatal(err)
		}
	}
	if err := write.Commit(); err != nil {
		t.Fatal(err)
	}
	read, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Rollback()
	graph := NewGraphState()
	graph.PageBase = &PageGraph{Tx: read}
	graph.Nodes.Set(2, &NodeRecord{ID: 2, Labels: []string{"Other"}, Properties: PropertiesFromMap(map[string]any{"email": "two"})})
	graph.Nodes.Set(3, &NodeRecord{ID: 3, Labels: []string{"Person"}, Properties: PropertiesFromMap(map[string]any{"email": "one"})})
	putConstraint(t, graph, GraphConstraint{Name: "email", Label: "Person", Property: "email", Kind: ConstraintKindUnique})
	budget := &constraintTestBudget{maxWork: 1 << 20, maxBytes: 1 << 20}
	assertConstraintViolation(t, ValidateGraphConstraints(context.Background(), graph, budget), "email", 3)
	if budget.bytes != 0 {
		t.Fatalf("overlay validation bytes retained: %d", budget.bytes)
	}
}

func addNode(graph *GraphState, id uint64, labels []string, properties map[string]any) {
	graph.Nodes.Set(id, &NodeRecord{ID: id, Labels: labels, Properties: PropertiesFromMap(properties)})
	for _, label := range labels {
		graph.Labels.Add(label, id)
	}
}

func putConstraint(t *testing.T, graph *GraphState, def GraphConstraint) {
	t.Helper()
	if err := PutGraphConstraint(graph, def); err != nil {
		t.Fatal(err)
	}
}

func assertConstraintViolation(t *testing.T, err error, name string, nodeID uint64) {
	t.Helper()
	if !errors.Is(err, ErrConstraintViolation) {
		t.Fatalf("error = %v, want ErrConstraintViolation", err)
	}
	var violation *ConstraintViolation
	if !errors.As(err, &violation) {
		t.Fatalf("error = %T, want *ConstraintViolation", err)
	}
	if violation.Name != name || violation.NodeID != nodeID || violation.Property == "" || violation.Reason == "" {
		t.Fatalf("violation = %+v", violation)
	}
	if strings.Contains(violation.Error(), "same") || strings.Contains(violation.Error(), "one") {
		t.Fatalf("violation leaked a property value: %s", violation)
	}
}

func (budget *constraintTestBudget) RemainingPageReadBytes() uint64 {
	return budget.maxBytes - budget.bytes
}

// PutGraphConstraint stores a definition and enables the feature marker.
func PutGraphConstraint(graph *GraphState, def GraphConstraint) error {
	if graph == nil {
		return errors.New("graph is nil")
	}
	encoded, err := EncodeGraphConstraint(def)
	if err != nil {
		return err
	}
	if graph.AppMetadata == nil {
		graph.AppMetadata = new(AppMetadata)
	}
	key := GraphConstraintPrefix + def.Name
	if _, exists := graph.AppMetadata.Get(key); exists {
		return fmt.Errorf("graph constraint %q already exists", def.Name)
	}
	graph.AppMetadata.Set(key, encoded)
	graph.AppMetadata.Set(GraphConstraintsFeatureKey, []byte("1"))
	return nil
}

// DropGraphConstraint removes a definition and clears the marker after the last
// definition. The caller's graph transaction owns publication of both changes.
func DropGraphConstraint(graph *GraphState, name string) error {
	if graph == nil {
		return errors.New("graph is nil")
	}
	if name == "" || !utf8.ValidString(name) || len(GraphConstraintPrefix)+len(name) > maxAppMetadataKeyBytes {
		return errors.New("invalid graph constraint name")
	}
	if graph.AppMetadata == nil {
		return fmt.Errorf("graph constraint %q not found", name)
	}
	key := GraphConstraintPrefix + name
	if _, exists := graph.AppMetadata.Get(key); !exists {
		return fmt.Errorf("graph constraint %q not found", name)
	}
	graph.AppMetadata.Delete(key)
	for existing := range graph.AppMetadata.All() {
		if strings.HasPrefix(existing, GraphConstraintPrefix) {
			return nil
		}
	}
	graph.AppMetadata.Delete(GraphConstraintsFeatureKey)
	return nil
}

func TestGraphConstraintCatalogDoesNotChargeUnrelatedValues(t *testing.T) {
	graph := NewGraphState()
	graph.AppMetadata.Set("app:large", make([]byte, 4096))
	budget := &constraintTestBudget{maxWork: 10, maxBytes: 16}
	if err := ValidateGraphConstraints(t.Context(), graph, budget); err != nil {
		t.Fatalf("unrelated resident metadata consumed validation memory: %v", err)
	}
}
