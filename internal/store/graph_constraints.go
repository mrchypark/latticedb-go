package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"slices"
	"strings"
	"unicode/utf8"
)

const (
	GraphConstraintPrefix      = "latticedb:graph-constraint:"
	GraphConstraintsFeatureKey = "latticedb:graph-constraints"

	graphConstraintEncodingVersion = 1
	graphConstraintHeaderBytes     = 15
	defaultConstraintWork          = uint64(1 << 24)
	defaultConstraintBytes         = uint64(256 << 20)
	defaultConstraintRecoveryWork  = uint64(1_000_000_000)
	defaultConstraintRecoveryBytes = uint64(4 << 30)
)

type ConstraintKind uint8

const (
	ConstraintKindRequired ConstraintKind = iota + 1
	ConstraintKindType
	ConstraintKindUnique
)

type ConstraintScalarType uint8

const (
	ConstraintScalarBool ConstraintScalarType = iota + 1
	ConstraintScalarInt64
	ConstraintScalarFloat64
	ConstraintScalarString
)

type GraphConstraint struct {
	Name     string
	Label    string
	Property string
	Kind     ConstraintKind
	Type     ConstraintScalarType
}

// ConstraintBudget accounts for one constraint validation pass. Work is
// cumulative; bytes are held only while a decoded value or unique key is used.
type ConstraintBudget interface {
	ChargeConstraintWork(uint64) error
	ReserveConstraintBytes(uint64) error
	ReleaseConstraintBytes(uint64)
	RemainingPageReadBytes() uint64
}

var ErrConstraintViolation = errors.New("graph constraint violation")

type ConstraintViolation struct {
	Name     string
	NodeID   uint64
	Property string
	Reason   string
}

func (violation *ConstraintViolation) Error() string {
	return fmt.Sprintf("graph constraint %q violated on node %d property %q: %s", violation.Name, violation.NodeID, violation.Property, violation.Reason)
}

func (violation *ConstraintViolation) Unwrap() error { return ErrConstraintViolation }

// EncodeGraphConstraint returns the canonical store representation of a
// definition. The metadata key supplies the name again so raw imports can
// detect a mismatched key and payload.
func EncodeGraphConstraint(def GraphConstraint) ([]byte, error) {
	if err := validateGraphConstraint(def); err != nil {
		return nil, err
	}
	if len(GraphConstraintPrefix)+len(def.Name) > maxAppMetadataKeyBytes {
		return nil, errors.New("graph constraint name exceeds metadata key limit")
	}
	data := make([]byte, graphConstraintHeaderBytes+len(def.Name)+len(def.Label)+len(def.Property))
	data[0] = graphConstraintEncodingVersion
	data[1] = byte(def.Kind)
	data[2] = byte(def.Type)
	binary.BigEndian.PutUint32(data[3:7], uint32(len(def.Name)))
	binary.BigEndian.PutUint32(data[7:11], uint32(len(def.Label)))
	binary.BigEndian.PutUint32(data[11:15], uint32(len(def.Property)))
	offset := graphConstraintHeaderBytes
	for _, value := range []string{def.Name, def.Label, def.Property} {
		copy(data[offset:], value)
		offset += len(value)
	}
	return data, nil
}

func validateGraphConstraint(def GraphConstraint) error {
	for _, field := range []struct {
		name, value string
	}{{"name", def.Name}, {"label", def.Label}, {"property", def.Property}} {
		if field.value == "" {
			return fmt.Errorf("graph constraint %s must not be empty", field.name)
		}
		if len(field.value) > maxAppMetadataKeyBytes {
			return fmt.Errorf("graph constraint %s exceeds metadata key limit", field.name)
		}
		if !utf8.ValidString(field.value) {
			return fmt.Errorf("graph constraint %s contains invalid UTF-8", field.name)
		}
	}
	switch def.Kind {
	case ConstraintKindRequired, ConstraintKindUnique:
		if def.Type != 0 {
			return errors.New("graph constraint type is only valid for type constraints")
		}
	case ConstraintKindType:
		if !validConstraintScalarType(def.Type) {
			return errors.New("graph constraint has an invalid scalar type")
		}
	default:
		return errors.New("graph constraint has an invalid kind")
	}
	return nil
}

func validConstraintScalarType(value ConstraintScalarType) bool {
	return value >= ConstraintScalarBool && value <= ConstraintScalarString
}

func decodeGraphConstraint(key, data []byte) (GraphConstraint, error) {
	var def GraphConstraint
	if len(data) < graphConstraintHeaderBytes {
		return def, errors.New("malformed graph constraint definition")
	}
	if data[0] != graphConstraintEncodingVersion {
		return def, errors.New("unsupported graph constraint definition version")
	}
	nameBytes := uint64(binary.BigEndian.Uint32(data[3:7]))
	labelBytes := uint64(binary.BigEndian.Uint32(data[7:11]))
	propertyBytes := uint64(binary.BigEndian.Uint32(data[11:15]))
	total := uint64(graphConstraintHeaderBytes) + nameBytes + labelBytes + propertyBytes
	if total != uint64(len(data)) {
		return def, errors.New("malformed graph constraint definition length")
	}
	offset := uint64(graphConstraintHeaderBytes)
	def.Name = string(data[offset : offset+nameBytes])
	offset += nameBytes
	def.Label = string(data[offset : offset+labelBytes])
	offset += labelBytes
	def.Property = string(data[offset : offset+propertyBytes])
	def.Kind = ConstraintKind(data[1])
	def.Type = ConstraintScalarType(data[2])
	if err := validateGraphConstraint(def); err != nil {
		return GraphConstraint{}, err
	}
	if !strings.HasPrefix(string(key), GraphConstraintPrefix) || string(key) != GraphConstraintPrefix+def.Name {
		return GraphConstraint{}, errors.New("graph constraint metadata key does not match definition")
	}
	return def, nil
}

// GraphConstraintsPresent probes the feature marker without scanning metadata.
// A malformed marker still returns true so validation can reject it.
func GraphConstraintsPresent(graph *GraphState) bool {
	if graph == nil || graph.AppMetadata == nil {
		return false
	}
	_, present := graph.AppMetadata.Get(GraphConstraintsFeatureKey)
	return present
}

// ReadGraphConstraints scans the bounded definition catalog. The scan also
// validates the marker/catalog pair for raw imports and restore paths.
func ReadGraphConstraints(graph *GraphState, budget ConstraintBudget) ([]GraphConstraint, error) {
	budget = constraintBudgetOrDefault(budget)
	definitions, retained, err := readGraphConstraints(context.Background(), graph, budget)
	if retained != 0 {
		budget.ReleaseConstraintBytes(retained)
	}
	return definitions, err
}

// readGraphConstraints keeps retained bytes charged for decoded definitions
// through sorting and, for validation, through the checks that consume them.
func readGraphConstraints(ctx context.Context, graph *GraphState, budget ConstraintBudget) ([]GraphConstraint, uint64, error) {
	if graph == nil {
		return nil, 0, errors.New("graph is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	if err := budget.ChargeConstraintWork(1); err != nil {
		return nil, 0, err
	}
	metadata := graph.AppMetadata
	marker, markerPresent := []byte(nil), false
	if metadata != nil {
		marker, markerPresent = metadata.Get(GraphConstraintsFeatureKey)
	}
	if markerPresent && !bytes.Equal(marker, []byte("1")) {
		return nil, 0, errors.New("invalid graph constraint feature marker")
	}
	definitions := make([]GraphConstraint, 0)
	var retained uint64
	if metadata != nil {
		for key, value := range metadata.All() {
			if err := ctx.Err(); err != nil {
				budget.ReleaseConstraintBytes(retained)
				return nil, 0, err
			}
			if err := budget.ChargeConstraintWork(1); err != nil {
				budget.ReleaseConstraintBytes(retained)
				return nil, 0, err
			}
			if !strings.HasPrefix(key, GraphConstraintPrefix) {
				continue
			}
			reserved := uint64(len(key) + len(value))
			if err := budget.ReserveConstraintBytes(reserved); err != nil {
				budget.ReleaseConstraintBytes(retained)
				return nil, 0, err
			}
			def, err := decodeGraphConstraint([]byte(key), value)
			budget.ReleaseConstraintBytes(reserved)
			if err != nil {
				budget.ReleaseConstraintBytes(retained)
				return nil, 0, fmt.Errorf("read graph constraint %q: %w", key, err)
			}
			definitionBytes := graphConstraintDefinitionBytes(def)
			if err := budget.ReserveConstraintBytes(definitionBytes); err != nil {
				budget.ReleaseConstraintBytes(retained)
				return nil, 0, err
			}
			retained += definitionBytes
			definitions = append(definitions, def)
		}
	}
	if markerPresent != (len(definitions) != 0) {
		budget.ReleaseConstraintBytes(retained)
		if markerPresent {
			return nil, 0, errors.New("graph constraint feature marker has no definitions")
		}
		return nil, 0, errors.New("graph constraint definitions have no feature marker")
	}
	if err := ctx.Err(); err != nil {
		budget.ReleaseConstraintBytes(retained)
		return nil, 0, err
	}
	slices.SortFunc(definitions, func(left, right GraphConstraint) int {
		return strings.Compare(left.Name, right.Name)
	})
	return definitions, retained, nil
}

// ValidateGraphConstraints validates the final graph state in constraint-name
// order and node-ID order. Page-backed values retain a page lease while checked.
func ValidateGraphConstraints(ctx context.Context, graph *GraphState, budget ConstraintBudget) error {
	if graph == nil {
		return errors.New("graph is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	budget = constraintBudgetOrDefault(budget)
	definitions, retained, err := readGraphConstraints(ctx, graph, budget)
	defer budget.ReleaseConstraintBytes(retained)
	if err != nil {
		return err
	}
	if len(definitions) == 0 {
		return ctx.Err()
	}
	ctx = WithPageReadBudget(ctx, constraintPageReadBudget{budget: budget})
	for _, def := range definitions {
		if err := validateGraphConstraintData(ctx, graph, def, budget); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// ValidateGraphConstraintsWithRecoveryLimits validates raw or staged state
// using the caller's recovery limits. Zero limits use the bounded constraint
// defaults rather than making restore unbounded.
func ValidateGraphConstraintsWithRecoveryLimits(ctx context.Context, graph *GraphState, limits RecoveryLimits) error {
	if limits.MaxDecodedBytes == 0 {
		limits.MaxDecodedBytes = defaultConstraintRecoveryBytes
	}
	if limits.MaxWork == 0 {
		limits.MaxWork = defaultConstraintRecoveryWork
	}
	return validateRecoveredGraphConstraints(ctx, graph, &recoveryBudget{limits: limits})
}

func defaultGraphConstraintRecoveryLimits() RecoveryLimits {
	return RecoveryLimits{
		MaxDecodedBytes: defaultConstraintRecoveryBytes,
		MaxWork:         defaultConstraintRecoveryWork,
	}
}

func validateRecoveredGraphConstraints(ctx context.Context, graph *GraphState, budget ConstraintBudget) error {
	if GraphConstraintsPresent(graph) {
		return ValidateGraphConstraints(ctx, graph, budget)
	}
	// A raw definition without its marker is corrupt and still needs the full
	// catalog check, but it must not consume recovery work for an unconstrained DB.
	return ValidateGraphConstraints(ctx, graph, nil)
}

func validateGraphConstraintData(ctx context.Context, graph *GraphState, def GraphConstraint, budget ConstraintBudget) error {
	unique := make(map[propertyValueKey]struct{})
	var retained uint64
	defer func() { budget.ReleaseConstraintBytes(retained) }()
	visit := func(id uint64) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := budget.ChargeConstraintWork(1); err != nil {
			return err
		}
		node, lease, err := graph.ReadNodeOwned(ctx, id)
		if err != nil {
			return err
		}
		if lease != nil {
			defer lease.Release()
		}
		if node == nil || !slices.Contains(node.Labels, def.Label) {
			return nil
		}
		value, present := node.Properties.Lookup(def.Property)
		if !present {
			if def.Kind == ConstraintKindRequired {
				return constraintViolation(def, id, "required property is absent")
			}
			return nil
		}
		if value == nil {
			if def.Kind == ConstraintKindRequired {
				return constraintViolation(def, id, "required property is null")
			}
			if def.Kind == ConstraintKindType {
				return constraintViolation(def, id, "type constraint rejects null")
			}
			return nil
		}
		if err := budget.ChargeConstraintWork(1); err != nil {
			return err
		}
		if lease == nil {
			decoded := constraintValueBytes(value)
			if err := budget.ReserveConstraintBytes(decoded); err != nil {
				return err
			}
			defer budget.ReleaseConstraintBytes(decoded)
		}
		switch def.Kind {
		case ConstraintKindRequired:
			return nil
		case ConstraintKindType:
			if !constraintScalarMatches(value, def.Type) {
				return constraintViolation(def, id, "property type is "+constraintScalarTypeName(def.Type))
			}
			return nil
		case ConstraintKindUnique:
			key, ok := constraintUniqueKey(value)
			if !ok {
				return constraintViolation(def, id, "unique constraint requires a supported scalar value")
			}
			if _, duplicate := unique[key]; duplicate {
				return constraintViolation(def, id, "value is not unique")
			}
			if err := budget.ChargeConstraintWork(1); err != nil {
				return err
			}
			bytes := constraintKeyBytes(key)
			if err := budget.ReserveConstraintBytes(bytes); err != nil {
				return err
			}
			retained += bytes
			unique[key] = struct{}{}
			return nil
		default:
			return errors.New("graph constraint has an invalid kind")
		}
	}
	return visitConstraintLabel(ctx, graph, def.Label, budget, visit)
}

// visitConstraintLabel uses the label posting for resident graphs. For a page
// overlay, it merges that sorted page posting with the resident overlay without
// materializing a second copy of every page-backed label ID.
func visitConstraintLabel(ctx context.Context, graph *GraphState, label string, budget ConstraintBudget, visit func(uint64) error) error {
	if graph.PageBase == nil {
		ids := make([]uint64, 0)
		var retained uint64
		defer func() { budget.ReleaseConstraintBytes(retained) }()
		for id := range graph.Labels.All(label) {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := budget.ChargeConstraintWork(1); err != nil {
				return err
			}
			if err := budget.ReserveConstraintBytes(16); err != nil {
				return err
			}
			retained += 16
			ids = append(ids, id)
		}
		if err := budget.ChargeConstraintWork(uint64(len(ids)) * uint64(bits.Len(uint(len(ids))))); err != nil {
			return err
		}
		slices.Sort(ids)
		for _, id := range ids {
			if err := ctx.Err(); err != nil {
				return err
			}
			if graph.DeletedNodes.Get(id) {
				continue
			}
			node := graph.Nodes.Get(id)
			if node == nil || !slices.Contains(node.Labels, label) {
				continue
			}
			if err := visit(id); err != nil {
				return err
			}
		}
		return ctx.Err()
	}

	overlayIDs := make([]uint64, 0)
	var retained uint64
	defer func() { budget.ReleaseConstraintBytes(retained) }()
	for id, node := range graph.Nodes.All() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := budget.ChargeConstraintWork(1); err != nil {
			return err
		}
		if graph.DeletedNodes.Get(id) || node == nil || !slices.Contains(node.Labels, label) {
			continue
		}
		if err := budget.ReserveConstraintBytes(16); err != nil {
			return err
		}
		retained += 16
		overlayIDs = append(overlayIDs, id)
	}
	if err := budget.ChargeConstraintWork(uint64(len(overlayIDs)) * uint64(bits.Len(uint(len(overlayIDs))))); err != nil {
		return err
	}
	slices.Sort(overlayIDs)
	overlayIndex := 0
	err := graph.PageBase.VisitLabel(ctx, label, func(baseID uint64) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := budget.ChargeConstraintWork(1); err != nil {
			return err
		}
		if err := budget.ReserveConstraintBytes(16); err != nil {
			return err
		}
		defer budget.ReleaseConstraintBytes(16)
		for overlayIndex < len(overlayIDs) && overlayIDs[overlayIndex] < baseID {
			if err := visit(overlayIDs[overlayIndex]); err != nil {
				return err
			}
			overlayIndex++
		}
		if overlayIndex < len(overlayIDs) && overlayIDs[overlayIndex] == baseID {
			overlayIndex++
			return visit(baseID)
		}
		if graph.DeletedNodes.Get(baseID) {
			return nil
		}
		if node := graph.Nodes.Get(baseID); node != nil && !slices.Contains(node.Labels, label) {
			return nil
		}
		return visit(baseID)
	})
	if err != nil {
		return err
	}
	for ; overlayIndex < len(overlayIDs); overlayIndex++ {
		if err := visit(overlayIDs[overlayIndex]); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func constraintViolation(def GraphConstraint, id uint64, reason string) error {
	return &ConstraintViolation{Name: def.Name, NodeID: id, Property: def.Property, Reason: reason}
}

func constraintScalarMatches(value any, want ConstraintScalarType) bool {
	switch value.(type) {
	case bool:
		return want == ConstraintScalarBool
	case int64:
		return want == ConstraintScalarInt64
	case float64:
		return want == ConstraintScalarFloat64
	case string:
		return want == ConstraintScalarString
	default:
		return false
	}
}

func constraintScalarTypeName(value ConstraintScalarType) string {
	switch value {
	case ConstraintScalarBool:
		return "bool"
	case ConstraintScalarInt64:
		return "int64"
	case ConstraintScalarFloat64:
		return "float64"
	case ConstraintScalarString:
		return "string"
	default:
		return "invalid"
	}
}

func constraintUniqueKey(value any) (propertyValueKey, bool) {
	switch value.(type) {
	case bool, int64, float64, string:
		key, err := makePropertyValueKey(value)
		return key, err == nil
	default:
		return propertyValueKey{}, false
	}
}

func constraintKeyBytes(key propertyValueKey) uint64 {
	return 64 + uint64(len(key.text))
}

func graphConstraintDefinitionBytes(def GraphConstraint) uint64 {
	return 64 + uint64(len(def.Name)+len(def.Label)+len(def.Property))
}

func constraintValueBytes(value any) uint64 {
	switch typed := value.(type) {
	case string:
		return 16 + uint64(len(typed))
	case []byte:
		return 24 + uint64(len(typed))
	case []float32:
		return 24 + uint64(len(typed))*4
	case []any:
		return 24 + uint64(len(typed))*32
	case map[string]any:
		return 24 + uint64(len(typed))*48
	default:
		return 16
	}
}

type constraintDefaultBudget struct {
	work, bytes, maxWork, maxBytes uint64
}

func constraintBudgetOrDefault(budget ConstraintBudget) ConstraintBudget {
	if budget != nil {
		return budget
	}
	return &constraintDefaultBudget{maxWork: defaultConstraintWork, maxBytes: defaultConstraintBytes}
}

func (budget *constraintDefaultBudget) ChargeConstraintWork(work uint64) error {
	if work > budget.maxWork-budget.work {
		return fmt.Errorf("%w: graph constraint work", ErrLoadResourceLimit)
	}
	budget.work += work
	return nil
}

func (budget *constraintDefaultBudget) ReserveConstraintBytes(bytes uint64) error {
	if bytes > budget.maxBytes-budget.bytes {
		return fmt.Errorf("%w: graph constraint bytes", ErrLoadResourceLimit)
	}
	budget.bytes += bytes
	return nil
}

func (budget *constraintDefaultBudget) ReleaseConstraintBytes(bytes uint64) {
	budget.bytes -= min(bytes, budget.bytes)
}

func (budget *constraintDefaultBudget) RemainingPageReadBytes() uint64 {
	return budget.maxBytes - budget.bytes
}

type constraintPageReadBudget struct{ budget ConstraintBudget }

func (budget constraintPageReadBudget) ReservePageRead(work, bytes uint64) error {
	if err := budget.budget.ChargeConstraintWork(work); err != nil {
		return err
	}
	return budget.budget.ReserveConstraintBytes(bytes)
}

func (budget constraintPageReadBudget) ReleasePageRead(bytes uint64) {
	budget.budget.ReleaseConstraintBytes(bytes)
}

func (budget constraintPageReadBudget) RemainingPageReadBytes() uint64 {
	return budget.budget.RemainingPageReadBytes()
}
