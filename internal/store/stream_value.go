package store

import (
	"context"
	"fmt"
	"math"
	"unicode/utf8"
)

func estimateValueBytesContext(ctx context.Context, value any) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	size := uint64(64)
	switch value := value.(type) {
	case []any:
		for _, item := range value {
			bytes, err := estimateValueBytesContext(ctx, item)
			if err != nil {
				return 0, err
			}
			size = snapshotAdd(size, snapshotAdd(bytes, 8))
		}
	case map[string]any:
		for key, item := range value {
			bytes, err := estimateValueBytesContext(ctx, item)
			if err != nil {
				return 0, err
			}
			size = snapshotAdd(size, snapshotAdd(snapshotAdd(snapshotMul(uint64(len(key)), 6), 64), bytes))
		}
	default:
		return estimateValueBytes(value), nil
	}
	return size, nil
}

// Preflight the persisted tree before decodeValue allocates its recursive copy.
// One walk per payload matches NormalizeValue's live publication limits.
func decodeStreamValue(stream string, value persistedValue) (any, error) {
	if stream == "__lattice_changes" && value.Kind == "map" {
		if err := validateChangefeedEnvelope(value.Map); err != nil {
			return nil, err
		}
	} else if err := validateStreamValue(value, 0, &valueWalk{}); err != nil {
		return nil, err
	}
	return decodeValue(value)
}

func validateChangefeedEnvelope(envelope map[string]persistedValue) error {
	propertyKey, keyOK := envelope["key"]
	propertyKeyText, keyIsString := propertyKey.String, propertyKey.Kind == "string"
	propertyValues := make([]persistedValue, 0, 2)
	for _, field := range []string{"new_value", "old_value"} {
		if value, ok := envelope[field]; ok {
			propertyValues = append(propertyValues, value)
		}
	}
	propertyEvent := keyOK && keyIsString && len(propertyValues) != 0
	if !propertyEvent {
		return validateStreamValue(persistedValue{Kind: "map", Map: envelope}, -1, &valueWalk{})
	}
	if !utf8.ValidString(propertyKeyText) {
		return fmt.Errorf("property key contains invalid UTF-8")
	}

	metadata := &valueWalk{}
	if err := metadata.add(len(envelope)); err != nil {
		return err
	}
	for field, value := range envelope {
		if !utf8.ValidString(field) {
			return fmt.Errorf("map key contains invalid UTF-8")
		}
		if err := metadata.addBytes(len(field)); err != nil {
			return err
		}
		if field == "key" || field == "new_value" || field == "old_value" {
			continue
		}
		if err := validateStreamValue(value, 0, metadata); err != nil {
			return err
		}
	}
	for _, value := range propertyValues {
		property := &valueWalk{}
		if err := property.add(1); err != nil {
			return err
		}
		if err := property.addBytes(len(propertyKeyText)); err != nil {
			return err
		}
		if err := validateStreamValue(value, 0, property); err != nil {
			return err
		}
	}
	return nil
}

func validateStreamValue(value persistedValue, depth int, walk *valueWalk) error {
	if depth > maxValueDepth {
		return fmt.Errorf("%w: nesting exceeds %d", ErrValueLimit, maxValueDepth)
	}
	switch value.Kind {
	case "null", "bool", "int":
		return nil
	case "float":
		_, err := normalizeValue(value.Float, depth, walk)
		return err
	case "string":
		_, err := normalizeValue(value.String, depth, walk)
		return err
	case "bytes":
		return walk.addBytes(len(value.Bytes))
	case "vector":
		if len(value.Vector) > maxValueBytes/4 {
			return fmt.Errorf("%w: byte count exceeds %d", ErrValueLimit, maxValueBytes)
		}
		if err := walk.addBytes(len(value.Vector) * 4); err != nil {
			return err
		}
		for _, item := range value.Vector {
			if math.IsNaN(float64(item)) || math.IsInf(float64(item), 0) {
				return fmt.Errorf("vector contains non-finite value")
			}
		}
	case "list":
		if err := walk.add(len(value.List)); err != nil {
			return err
		}
		for _, item := range value.List {
			if err := validateStreamValue(item, depth+1, walk); err != nil {
				return err
			}
		}
	case "map":
		if err := walk.add(len(value.Map)); err != nil {
			return err
		}
		for key, item := range value.Map {
			if !utf8.ValidString(key) {
				return fmt.Errorf("map key contains invalid UTF-8")
			}
			if err := walk.addBytes(len(key)); err != nil {
				return err
			}
			if err := validateStreamValue(item, depth+1, walk); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unknown stored value kind %q", value.Kind)
	}
	return nil
}
