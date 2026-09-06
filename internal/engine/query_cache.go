package engine

import (
	"fmt"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
)

// canonicalQueryPlanKey encodes the parsed tree, including concrete expression
// and literal types. It is process-local, not a persisted format. Reflection
// includes every plan field so a new clause cannot silently alias an old plan.
// This runs only on source-cache misses; normal hits never walk the AST.
func canonicalQueryPlanKey(plan *queryPlan) (string, error) {
	var key strings.Builder
	var encode func(reflect.Value) error
	encode = func(value reflect.Value) error {
		if !value.IsValid() {
			key.WriteString("nil;")
			return nil
		}
		key.WriteString(strconv.Quote(value.Type().String()))
		key.WriteByte(':')
		switch value.Kind() {
		case reflect.Interface, reflect.Pointer:
			if value.IsNil() {
				key.WriteString("nil")
			} else if err := encode(value.Elem()); err != nil {
				return err
			}
		case reflect.Struct:
			for i := 0; i < value.NumField(); i++ {
				if err := encode(value.Field(i)); err != nil {
					return err
				}
			}
		case reflect.Map:
			if value.IsNil() {
				key.WriteString("nil;")
				return nil
			}
			if value.Type().Key().Kind() != reflect.String {
				return fmt.Errorf("unsupported query cache map key: %s", value.Type())
			}
			keys := value.MapKeys()
			slices.SortFunc(keys, func(a, b reflect.Value) int { return strings.Compare(a.String(), b.String()) })
			for _, field := range keys {
				if err := encode(field); err != nil {
					return err
				}
				if err := encode(value.MapIndex(field)); err != nil {
					return err
				}
			}
		case reflect.Slice, reflect.Array:
			if value.Kind() == reflect.Slice && value.IsNil() {
				key.WriteString("nil;")
				return nil
			}
			for i := 0; i < value.Len(); i++ {
				if err := encode(value.Index(i)); err != nil {
					return err
				}
			}
		case reflect.String:
			key.WriteString(strconv.Quote(value.String()))
		case reflect.Bool:
			key.WriteString(strconv.FormatBool(value.Bool()))
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			key.WriteString(strconv.FormatInt(value.Int(), 10))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			key.WriteString(strconv.FormatUint(value.Uint(), 10))
		case reflect.Float32, reflect.Float64:
			// Preserve numeric type and signed zero: write expressions can expose
			// literal representations even where query comparisons consider them equal.
			key.WriteString(strconv.FormatUint(math.Float64bits(value.Float()), 16))
		default:
			return fmt.Errorf("unsupported query cache value: %s", value.Type())
		}
		key.WriteByte(';')
		return nil
	}
	if err := encode(reflect.ValueOf(plan)); err != nil {
		return "", err
	}
	return key.String(), nil
}
