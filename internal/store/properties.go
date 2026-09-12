package store

import (
	"fmt"
	"iter"
	"slices"
	"strings"
	"unicode/utf8"
)

type propertyValueKind uint8

const (
	propertyNull propertyValueKind = iota
	propertyBool
	propertyInt
	propertyFloat
	propertyString
	propertyBytes
	propertyVector
	propertyList
	propertyMap
	propertyOther
)

type propertyValue struct {
	// ponytail: boxed payloads keep reads allocation-free; packing scalars needs typed query evaluation.
	raw  any
	kind propertyValueKind
}

type propertyEntry struct {
	key   string
	value propertyValue
}

// Properties stores entity properties contiguously rather than allocating a
// hash table per entity. Published values are immutable; Clone before mutation.
type Properties struct{ entries []propertyEntry }

func typedPropertyValue(value any) propertyValue {
	kind := propertyOther
	switch value.(type) {
	case nil:
		kind = propertyNull
	case bool:
		kind = propertyBool
	case int64:
		kind = propertyInt
	case float64:
		kind = propertyFloat
	case string:
		kind = propertyString
	case []byte:
		kind = propertyBytes
	case []float32:
		kind = propertyVector
	case []any:
		kind = propertyList
	case map[string]any:
		kind = propertyMap
	}
	return propertyValue{raw: value, kind: kind}
}

// PropertiesFromMap copies the container, borrowing already-owned keys and values.
// Ingress paths must use NormalizePropertyStorage for untrusted values.
func PropertiesFromMap(values map[string]any) Properties {
	var p Properties
	if len(values) == 0 {
		return p
	}
	p.entries = make([]propertyEntry, 0, len(values))
	for key, value := range values {
		p.entries = append(p.entries, propertyEntry{key, typedPropertyValue(value)})
	}
	p.sort()
	return p
}

func (p *Properties) sort() {
	slices.SortFunc(p.entries, func(a, b propertyEntry) int { return strings.Compare(a.key, b.key) })
}

// NormalizePropertyStorage validates and owns values without an intermediate map.
func NormalizePropertyStorage(values map[string]any) (Properties, error) {
	var p Properties
	if len(values) == 0 {
		return p, nil
	}
	walk := newValueWalk()
	if err := walk.add(len(values)); err != nil {
		return p, err
	}
	p.entries = make([]propertyEntry, 0, len(values))
	for key, value := range values {
		if !utf8.ValidString(key) {
			return Properties{}, fmt.Errorf("property %q: key contains invalid UTF-8", key)
		}
		if err := walk.addBytes(len(key)); err != nil {
			return Properties{}, fmt.Errorf("property %q: %w", key, err)
		}
		normalized, err := normalizeValue(value, 0, walk)
		if err != nil {
			return Properties{}, fmt.Errorf("property %q: %w", key, err)
		}
		p.entries = append(p.entries, propertyEntry{InternString(key), typedPropertyValue(normalized)})
	}
	p.sort()
	return p, nil
}

func (p Properties) Len() int { return len(p.entries) }

func (p Properties) find(key string) (int, bool) {
	if len(p.entries) <= 8 {
		for i, entry := range p.entries {
			if entry.key >= key {
				return i, entry.key == key
			}
		}
		return len(p.entries), false
	}
	return slices.BinarySearchFunc(p.entries, key, func(entry propertyEntry, key string) int { return strings.Compare(entry.key, key) })
}

func (p Properties) Lookup(key string) (any, bool) {
	i, ok := p.find(key)
	if !ok {
		return nil, false
	}
	return p.entries[i].value.raw, true
}
func (p Properties) Get(key string) any { value, _ := p.Lookup(key); return value }

func (p Properties) All() iter.Seq2[string, any] {
	return func(yield func(string, any) bool) {
		for _, entry := range p.entries {
			if !yield(entry.key, entry.value.raw) {
				return
			}
		}
	}
}

func (p *Properties) Set(key string, value any) {
	i, ok := p.find(key)
	if ok {
		p.entries[i].value = typedPropertyValue(value)
		return
	}
	p.entries = slices.Insert(p.entries, i, propertyEntry{InternString(key), typedPropertyValue(value)})
}
func (p *Properties) Delete(key string) {
	if i, ok := p.find(key); ok {
		p.entries = slices.Delete(p.entries, i, i+1)
	}
}
func (p Properties) Clone() Properties { return Properties{entries: slices.Clone(p.entries)} }
func (p Properties) CloneDeep() Properties {
	out := p.Clone()
	for i := range out.entries {
		out.entries[i].value.raw = CloneValue(out.entries[i].value.raw)
	}
	return out
}

// CloneMap is the public, independently mutable representation.
func (p Properties) CloneMap() map[string]any {
	out := make(map[string]any, p.Len())
	for _, entry := range p.entries {
		out[entry.key] = CloneValue(entry.value.raw)
	}
	return out
}
func (p Properties) Vector(key string) ([]float32, bool) {
	i, ok := p.find(key)
	if !ok || p.entries[i].value.kind != propertyVector {
		return nil, false
	}
	return p.entries[i].value.raw.([]float32), true
}
func (p Properties) FirstVector() ([]float32, bool) {
	for _, entry := range p.entries {
		if entry.value.kind == propertyVector {
			vector := entry.value.raw.([]float32)
			if vector != nil {
				return vector, true
			}
		}
	}
	return nil, false
}

// The wire representation remains the existing canonical property map.
func encodePropertyStorage(in Properties) (map[string]persistedValue, error) {
	walk := newValueWalk()
	if err := walk.add(in.Len()); err != nil {
		return nil, err
	}
	out := make(map[string]persistedValue, in.Len())
	for key, value := range in.All() {
		if !utf8.ValidString(key) {
			return nil, fmt.Errorf("property %q: key contains invalid UTF-8", key)
		}
		if err := walk.addBytes(len(key)); err != nil {
			return nil, fmt.Errorf("property %q: %w", key, err)
		}
		normalized, err := normalizeValue(value, 0, walk)
		if err != nil {
			return nil, fmt.Errorf("property %q: %w", key, err)
		}
		encoded, err := encodeValue(normalized)
		if err != nil {
			return nil, err
		}
		out[key] = encoded
	}
	return out, nil
}

func decodePropertyStorage(in map[string]persistedValue) (Properties, error) {
	var out Properties
	walk := newValueWalk()
	if err := walk.add(len(in)); err != nil {
		return out, err
	}
	if len(in) > 0 {
		out.entries = make([]propertyEntry, 0, len(in))
	}
	for key, value := range in {
		if !utf8.ValidString(key) {
			return Properties{}, fmt.Errorf("property %q: key contains invalid UTF-8", key)
		}
		if err := walk.addBytes(len(key)); err != nil {
			return Properties{}, fmt.Errorf("property %q: %w", key, err)
		}
		decoded, err := decodeValue(value)
		if err != nil {
			return Properties{}, err
		}
		normalized, err := normalizeValue(decoded, 0, walk)
		if err != nil {
			return Properties{}, fmt.Errorf("property %q: %w", key, err)
		}
		out.entries = append(out.entries, propertyEntry{InternString(key), typedPropertyValue(normalized)})
	}
	out.sort()
	return out, nil
}
