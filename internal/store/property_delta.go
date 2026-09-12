package store

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"unicode/utf8"
)

type persistedPropertyChange struct {
	ID     uint64                    `json:"id"`
	Set    map[string]persistedValue `json:"set,omitempty"`
	Remove []string                  `json:"remove,omitempty"`
}

type propertyValueTotals struct {
	elements int
	bytes    uint64
}
type propertyTotals struct {
	elements int
	bytes    uint64
	values   map[string]propertyValueTotals
}

func hasPropertyDelta(delta persistedDelta) bool {
	return len(delta.NodePropertyChanges) != 0 || len(delta.EdgePropertyChanges) != 0
}

func hasPropertyKeys(keys map[uint64][]string) bool {
	for _, values := range keys {
		if len(values) != 0 {
			return true
		}
	}
	return false
}

func propertyDeltaWork(ctx context.Context, accumulator *walAccumulator, delta persistedDelta) (uint64, error) {
	work := persistedDeltaWork(delta)
	for _, change := range delta.NodePropertyChanges {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		work = addSaturated(work, 1)
		node, ok := accumulator.nodes[change.ID]
		if !ok {
			continue
		}
		if _, ok := accumulator.nodePropertyTotals[change.ID]; !ok {
			// The first patch pays for deriving the old totals; later patches use
			// the cached totals and only charge the shallow map copy and changes.
			amount, err := accumulator.cachePropertyTotals(ctx, change.ID, node.Properties, true)
			if err != nil {
				return 0, err
			}
			work = addSaturated(work, amount)
		}
		work = addSaturated(work, uint64(len(node.Properties)))
		work = addSaturated(work, uint64(len(change.Set)))
		for _, value := range change.Set {
			amount, err := persistedValueStructureWork(ctx, value, 0)
			if err != nil {
				return 0, err
			}
			work = addSaturated(work, amount)
		}
		work = addSaturated(work, uint64(len(change.Remove)))
	}
	for _, change := range delta.EdgePropertyChanges {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		work = addSaturated(work, 1)
		edge, ok := accumulator.edges[change.ID]
		if !ok {
			continue
		}
		if _, ok := accumulator.edgePropertyTotals[change.ID]; !ok {
			amount, err := accumulator.cachePropertyTotals(ctx, change.ID, edge.Properties, false)
			if err != nil {
				return 0, err
			}
			work = addSaturated(work, amount)
		}
		work = addSaturated(work, uint64(len(edge.Properties)))
		work = addSaturated(work, uint64(len(change.Set)))
		for _, value := range change.Set {
			amount, err := persistedValueStructureWork(ctx, value, 0)
			if err != nil {
				return 0, err
			}
			work = addSaturated(work, amount)
		}
		work = addSaturated(work, uint64(len(change.Remove)))
	}
	return work, nil
}

func persistedValueStructureWork(ctx context.Context, value persistedValue, depth int) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if depth > maxValueDepth {
		return 0, fmt.Errorf("%w: nesting exceeds %d", ErrValueLimit, maxValueDepth)
	}
	switch value.Kind {
	case "map":
		work := uint64(len(value.Map))
		for _, nested := range value.Map {
			amount, err := persistedValueStructureWork(ctx, nested, depth+1)
			if err != nil {
				return 0, err
			}
			work = addSaturated(work, amount)
		}
		return work, nil
	case "list":
		work := uint64(len(value.List))
		for _, nested := range value.List {
			amount, err := persistedValueStructureWork(ctx, nested, depth+1)
			if err != nil {
				return 0, err
			}
			work = addSaturated(work, amount)
		}
		return work, nil
	default:
		return 0, nil
	}
}

func validatePropertyChanges(changes []persistedPropertyChange, entity func(uint64) (map[string]persistedValue, bool)) error {
	seen := make(map[uint64]struct{}, len(changes))
	for _, change := range changes {
		if err := ValidateEntityID(change.ID); err != nil {
			return err
		}
		if _, exists := seen[change.ID]; exists {
			return fmt.Errorf("duplicate property change ID %d", change.ID)
		}
		seen[change.ID] = struct{}{}
		if _, ok := entity(change.ID); !ok {
			return fmt.Errorf("property change references missing entity %d", change.ID)
		}
		keys := make(map[string]struct{}, len(change.Set)+len(change.Remove))
		for key := range change.Set {
			if err := ValidatePropertyKey(key); err != nil {
				return err
			}
			if _, exists := keys[key]; exists {
				return fmt.Errorf("duplicate property change key %q", key)
			}
			keys[key] = struct{}{}
		}
		for _, key := range change.Remove {
			if err := ValidatePropertyKey(key); err != nil {
				return err
			}
			if _, exists := keys[key]; exists {
				return fmt.Errorf("property %q appears in set and remove", key)
			}
			keys[key] = struct{}{}
		}
	}
	return nil
}

type preparedPropertyMap struct {
	nodeID     uint64
	edgeID     uint64
	properties map[string]persistedValue
	totals     propertyTotals
}

func derivePropertyTotals(ctx context.Context, values map[string]persistedValue) (propertyTotals, uint64, error) {
	totals := propertyTotals{elements: len(values), values: make(map[string]propertyValueTotals, len(values))}
	work := uint64(len(values))
	for key, value := range values {
		if err := ctx.Err(); err != nil {
			return propertyTotals{}, 0, err
		}
		amount, structure, err := derivePropertyValueTotals(ctx, value, 0)
		if err != nil {
			return propertyTotals{}, 0, err
		}
		totals.values[key] = amount
		totals.elements += amount.elements
		totals.bytes = addSaturated(totals.bytes, addSaturated(uint64(len(key)), amount.bytes))
		work = addSaturated(work, structure)
	}
	return totals, work, nil
}

func derivePropertyValueTotals(ctx context.Context, value persistedValue, depth int) (propertyValueTotals, uint64, error) {
	if err := ctx.Err(); err != nil {
		return propertyValueTotals{}, 0, err
	}
	if depth > maxValueDepth {
		return propertyValueTotals{}, 0, fmt.Errorf("%w: nesting exceeds %d", ErrValueLimit, maxValueDepth)
	}
	totals := propertyValueTotals{}
	switch value.Kind {
	case "string":
		totals.bytes = uint64(len(value.String))
		return totals, 0, nil
	case "bytes":
		totals.bytes = uint64(len(value.Bytes))
		return totals, 0, nil
	case "vector":
		totals.bytes = multiplySaturated(uint64(len(value.Vector)), 4)
		return totals, 0, nil
	case "map":
		totals.elements = len(value.Map)
		work := uint64(len(value.Map))
		for key, nested := range value.Map {
			totals.bytes = addSaturated(totals.bytes, uint64(len(key)))
			child, childWork, err := derivePropertyValueTotals(ctx, nested, depth+1)
			if err != nil {
				return propertyValueTotals{}, 0, err
			}
			totals.elements += child.elements
			totals.bytes = addSaturated(totals.bytes, child.bytes)
			work = addSaturated(work, childWork)
		}
		return totals, work, nil
	case "list":
		totals.elements = len(value.List)
		work := uint64(len(value.List))
		for _, nested := range value.List {
			child, childWork, err := derivePropertyValueTotals(ctx, nested, depth+1)
			if err != nil {
				return propertyValueTotals{}, 0, err
			}
			totals.elements += child.elements
			totals.bytes = addSaturated(totals.bytes, child.bytes)
			work = addSaturated(work, childWork)
		}
		return totals, work, nil
	}
	return totals, 0, nil
}

func (accumulator *walAccumulator) cachePropertyTotals(ctx context.Context, id uint64, values map[string]persistedValue, node bool) (uint64, error) {
	totals, work, err := derivePropertyTotals(ctx, values)
	if err != nil {
		return 0, err
	}
	if node {
		if accumulator.nodePropertyTotals == nil {
			accumulator.nodePropertyTotals = make(map[uint64]propertyTotals)
		}
		accumulator.nodePropertyTotals[id] = totals
	} else {
		if accumulator.edgePropertyTotals == nil {
			accumulator.edgePropertyTotals = make(map[uint64]propertyTotals)
		}
		accumulator.edgePropertyTotals[id] = totals
	}
	return work, nil
}

func adjustPropertyTotals(ctx context.Context, totals propertyTotals, change persistedPropertyChange) (propertyTotals, error) {
	if err := ctx.Err(); err != nil {
		return propertyTotals{}, err
	}
	if _, err := decodePropertyMap(change.Set); err != nil {
		return propertyTotals{}, err
	}
	if err := ctx.Err(); err != nil {
		return propertyTotals{}, err
	}
	updated := totals
	updated.values = maps.Clone(totals.values)
	if updated.values == nil {
		updated.values = make(map[string]propertyValueTotals)
	}
	for key, value := range change.Set {
		newTotals, _, err := derivePropertyValueTotals(ctx, value, 0)
		if err != nil {
			return propertyTotals{}, err
		}
		if old, ok := updated.values[key]; ok {
			updated.elements -= old.elements
			updated.bytes -= old.bytes
		} else {
			updated.elements++
			updated.bytes = addSaturated(updated.bytes, uint64(len(key)))
		}
		updated.values[key] = newTotals
		updated.elements += newTotals.elements
		updated.bytes = addSaturated(updated.bytes, newTotals.bytes)
	}
	for _, key := range change.Remove {
		if old, ok := updated.values[key]; ok {
			updated.elements -= 1 + old.elements
			updated.bytes -= addSaturated(uint64(len(key)), old.bytes)
			delete(updated.values, key)
		}
	}
	if updated.elements > maxValueElements || updated.bytes > maxValueBytes {
		return propertyTotals{}, fmt.Errorf("%w: property map exceeds limits", ErrValueLimit)
	}
	return updated, nil
}

func preparePropertyDelta(ctx context.Context, accumulator *walAccumulator, delta persistedDelta) ([]preparedPropertyMap, error) {
	if err := validatePropertyChanges(delta.NodePropertyChanges, func(id uint64) (map[string]persistedValue, bool) {
		node, ok := accumulator.nodes[id]
		if !ok {
			return nil, false
		}
		return node.Properties, true
	}); err != nil {
		return nil, fmt.Errorf("node property changes: %w", err)
	}
	if err := validatePropertyChanges(delta.EdgePropertyChanges, func(id uint64) (map[string]persistedValue, bool) {
		edge, ok := accumulator.edges[id]
		if !ok {
			return nil, false
		}
		return edge.Properties, true
	}); err != nil {
		return nil, fmt.Errorf("edge property changes: %w", err)
	}
	fullNodes := make(map[uint64]struct{}, len(delta.UpsertNodes))
	for _, node := range delta.UpsertNodes {
		fullNodes[node.ID] = struct{}{}
	}
	deletedNodes := make(map[uint64]struct{}, len(delta.DeleteNodes))
	for _, id := range delta.DeleteNodes {
		deletedNodes[id] = struct{}{}
	}
	fullEdges := make(map[uint64]struct{}, len(delta.UpsertEdges))
	for _, edge := range delta.UpsertEdges {
		fullEdges[edge.ID] = struct{}{}
	}
	deletedEdges := make(map[uint64]struct{}, len(delta.DeleteEdges))
	for _, id := range delta.DeleteEdges {
		deletedEdges[id] = struct{}{}
	}
	prepared := make([]preparedPropertyMap, 0, len(delta.NodePropertyChanges)+len(delta.EdgePropertyChanges))
	for _, change := range delta.NodePropertyChanges {
		if _, exists := deletedNodes[change.ID]; exists {
			return nil, fmt.Errorf("node %d has both delete and property change", change.ID)
		}
		if _, exists := fullNodes[change.ID]; exists {
			return nil, fmt.Errorf("node %d has full upsert and property change", change.ID)
		}
		node := accumulator.nodes[change.ID]
		totals, ok := accumulator.nodePropertyTotals[change.ID]
		if !ok {
			if _, err := accumulator.cachePropertyTotals(ctx, change.ID, node.Properties, true); err != nil {
				return nil, err
			}
			totals = accumulator.nodePropertyTotals[change.ID]
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		properties := maps.Clone(node.Properties)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if properties == nil {
			properties = make(map[string]persistedValue)
		}
		updatedTotals, err := adjustPropertyTotals(ctx, totals, change)
		if err != nil {
			return nil, fmt.Errorf("node %d properties: %w", change.ID, err)
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for key, value := range change.Set {
			properties[key] = value
		}
		for _, key := range change.Remove {
			delete(properties, key)
		}
		prepared = append(prepared, preparedPropertyMap{nodeID: node.ID, properties: properties, totals: updatedTotals})
	}
	for _, change := range delta.EdgePropertyChanges {
		if _, exists := deletedEdges[change.ID]; exists {
			return nil, fmt.Errorf("edge %d has both delete and property change", change.ID)
		}
		if _, exists := fullEdges[change.ID]; exists {
			return nil, fmt.Errorf("edge %d has full upsert and property change", change.ID)
		}
		edge := accumulator.edges[change.ID]
		totals, ok := accumulator.edgePropertyTotals[change.ID]
		if !ok {
			if _, err := accumulator.cachePropertyTotals(ctx, change.ID, edge.Properties, false); err != nil {
				return nil, err
			}
			totals = accumulator.edgePropertyTotals[change.ID]
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		properties := maps.Clone(edge.Properties)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if properties == nil {
			properties = make(map[string]persistedValue)
		}
		updatedTotals, err := adjustPropertyTotals(ctx, totals, change)
		if err != nil {
			return nil, fmt.Errorf("edge %d properties: %w", change.ID, err)
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for key, value := range change.Set {
			properties[key] = value
		}
		for _, key := range change.Remove {
			delete(properties, key)
		}
		prepared = append(prepared, preparedPropertyMap{edgeID: edge.ID, properties: properties, totals: updatedTotals})
	}
	return prepared, nil
}

// validateNormalizedPropertyMapLimits checks the aggregate limits without
// allocating a normalized copy. Graph properties have already crossed the
// normalization boundary; this repeats only the resource accounting needed
// before a property patch is emitted.
func validateNormalizedPropertyMapLimits(properties Properties) error {
	walk := &valueWalk{}
	if err := walk.add(properties.Len()); err != nil {
		return err
	}
	for key, value := range properties.All() {
		if !utf8.ValidString(key) {
			return fmt.Errorf("property %q: key contains invalid UTF-8", key)
		}
		if err := walk.addBytes(len(key)); err != nil {
			return fmt.Errorf("property %q: %w", key, err)
		}
		if err := validateNormalizedPropertyValueLimits(value, 0, walk); err != nil {
			return fmt.Errorf("property %q: %w", key, err)
		}
	}
	return nil
}

func validateNormalizedPropertyValueLimits(value any, depth int, walk *valueWalk) error {
	if depth > maxValueDepth {
		return fmt.Errorf("%w: nesting exceeds %d", ErrValueLimit, maxValueDepth)
	}
	switch value := value.(type) {
	case nil, bool, int64:
		return nil
	case float64:
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return errors.New("non-finite float64")
		}
		return nil
	case string:
		if !utf8.ValidString(value) {
			return errors.New("string contains invalid UTF-8")
		}
		return walk.addBytes(len(value))
	case []byte:
		return walk.addBytes(len(value))
	case []float32:
		if len(value) > maxValueBytes/4 {
			return fmt.Errorf("%w: byte count exceeds %d", ErrValueLimit, maxValueBytes)
		}
		for _, item := range value {
			if math.IsNaN(float64(item)) || math.IsInf(float64(item), 0) {
				return errors.New("non-finite float32")
			}
		}
		return walk.addBytes(len(value) * 4)
	case []any:
		if err := walk.add(len(value)); err != nil {
			return err
		}
		for _, nested := range value {
			if err := validateNormalizedPropertyValueLimits(nested, depth+1, walk); err != nil {
				return err
			}
		}
		return nil
	case map[string]any:
		if err := walk.add(len(value)); err != nil {
			return err
		}
		for key, nested := range value {
			if !utf8.ValidString(key) {
				return fmt.Errorf("map key contains invalid UTF-8")
			}
			if err := walk.addBytes(len(key)); err != nil {
				return err
			}
			if err := validateNormalizedPropertyValueLimits(nested, depth+1, walk); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported value type %T", value)
	}
}

func buildPersistedPropertyChange(id uint64, keys []string, properties Properties) (persistedPropertyChange, error) {
	if err := ValidateEntityID(id); err != nil {
		return persistedPropertyChange{}, err
	}
	if err := validateNormalizedPropertyMapLimits(properties); err != nil {
		return persistedPropertyChange{}, err
	}
	change := persistedPropertyChange{ID: id, Set: make(map[string]persistedValue)}
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if err := ValidatePropertyKey(key); err != nil {
			return persistedPropertyChange{}, err
		}
		if _, ok := seen[key]; ok {
			return persistedPropertyChange{}, fmt.Errorf("duplicate property key %q", key)
		}
		seen[key] = struct{}{}
		value, ok := properties.Lookup(key)
		if !ok {
			change.Remove = append(change.Remove, key)
			continue
		}
		encoded, err := encodeValue(value)
		if err != nil {
			return persistedPropertyChange{}, fmt.Errorf("property %q: %w", key, err)
		}
		change.Set[key] = encoded
	}
	slices.Sort(change.Remove)
	return change, nil
}

var errEmptyPropertyDelta = errors.New("property_delta payload has no property changes")
