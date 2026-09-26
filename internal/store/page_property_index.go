package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"iter"
	"slices"
)

const (
	pageNodePropertyIndexes  = "node-property-indexes"
	pageEdgePropertyIndexes  = "edge-property-indexes"
	pageNodePropertyPostings = "node-property-postings"
	pageEdgePropertyPostings = "edge-property-postings"
)

type pagePropertyBackend struct {
	graph *PageGraph
	node  bool
}

func (graph *PageGraph) LoadPropertyIndexes(ctx context.Context, node bool) (PropertyIndexes, error) {
	indexes := NewPropertyIndexes()
	indexes.page = &pagePropertyBackend{graph: graph, node: node}
	bucket := pageEdgePropertyIndexes
	if node {
		bucket = pageNodePropertyIndexes
	}
	err := graph.Tx.Scan(ctx, bucket, nil, nil, func(key, value []byte) error {
		definition, err := decodePagePropertyDefinition(value)
		if err != nil {
			return err
		}
		if !bytes.Equal(key, pagePropertyDefinitionHash(definition)) {
			return errors.New("invalid page property index key")
		}
		indexes.Create(definition)
		return nil
	})
	return indexes, err
}

func (graph *PageGraph) CreatePropertyIndex(ctx context.Context, node bool, definition PropertyIndexDefinition) error {
	indexes, err := graph.LoadPropertyIndexes(ctx, node)
	if err != nil {
		return err
	}
	if indexes.Has(definition) {
		return errors.New("page property index already exists")
	}
	definitionBucket, _ := pagePropertyBuckets(node)
	definitionKey := pagePropertyDefinitionHash(definition)
	stored, err := graph.Tx.Get(definitionBucket, definitionKey)
	if err != nil {
		return err
	}
	if stored != nil {
		return errors.New("page property index definition hash collision")
	}
	if err := graph.Tx.Put(definitionBucket, definitionKey, encodePagePropertyDefinition(definition)); err != nil {
		return err
	}
	backend := pagePropertyBackend{graph: graph, node: node}
	visit := func(properties Properties, scopes []string, id uint64) error {
		if !propertyIndexMatches(definition, scopes, properties) {
			return nil
		}
		value, _ := properties.Lookup(definition.Property)
		return backend.put(definition, value, id)
	}
	if node {
		return graph.VisitNodes(ctx, func(record *NodeRecord) error { return visit(record.Properties, record.Labels, record.ID) })
	}
	return graph.VisitEdges(ctx, func(record *EdgeRecord) error { return visit(record.Properties, []string{record.Type}, record.ID) })
}

func (graph *PageGraph) DropPropertyIndex(ctx context.Context, node bool, definition PropertyIndexDefinition) error {
	definitionBucket, postingBucket := pagePropertyBuckets(node)
	definitionKey := pagePropertyDefinitionHash(definition)
	stored, err := graph.Tx.Get(definitionBucket, definitionKey)
	if err != nil {
		return err
	}
	if stored != nil {
		storedDefinition, err := decodePagePropertyDefinition(stored)
		if err != nil {
			return err
		}
		if storedDefinition != definition {
			return errors.New("page property index definition hash collision")
		}
	}
	if err := graph.Tx.Delete(definitionBucket, definitionKey); err != nil {
		return err
	}
	prefix := pagePropertyDefinitionHash(definition)
	return graph.Tx.Scan(ctx, postingBucket, prefix, pagePrefixEnd(prefix), func(key, value []byte) error {
		storedDefinition, _, err := decodePagePropertyPosting(value)
		if err != nil {
			return err
		}
		if storedDefinition != definition {
			return nil
		}
		return graph.Tx.Delete(postingBucket, key)
	})
}

func (graph *PageGraph) UpdateNodePropertyIndexes(old, next *NodeRecord) error {
	return graph.updatePagePropertyIndexes(true, old, next)
}

func (graph *PageGraph) UpdateEdgePropertyIndexes(old, next *EdgeRecord) error {
	return graph.updatePagePropertyIndexes(false, old, next)
}

func (graph *PageGraph) updatePagePropertyIndexes(node bool, old, next any) error {
	ctx := context.Background()
	indexes, err := graph.LoadPropertyIndexes(ctx, node)
	if err != nil {
		return err
	}
	backend := pagePropertyBackend{graph: graph, node: node}
	for definition := range indexes.Definitions() {
		var oldProperties, newProperties Properties
		var oldScopes, newScopes []string
		var oldID, nextID uint64
		var hasOld, hasNext bool
		if node {
			oldRecord, _ := old.(*NodeRecord)
			nextRecord, _ := next.(*NodeRecord)
			if oldRecord != nil {
				record := oldRecord
				oldProperties, oldScopes, oldID = record.Properties, record.Labels, record.ID
				hasOld = true
			}
			if nextRecord != nil {
				record := nextRecord
				newProperties, newScopes, nextID = record.Properties, record.Labels, record.ID
				hasNext = true
			}
		} else {
			oldRecord, _ := old.(*EdgeRecord)
			nextRecord, _ := next.(*EdgeRecord)
			if oldRecord != nil {
				record := oldRecord
				oldProperties, oldScopes, oldID = record.Properties, []string{record.Type}, record.ID
				hasOld = true
			}
			if nextRecord != nil {
				record := nextRecord
				newProperties, newScopes, nextID = record.Properties, []string{record.Type}, record.ID
				hasNext = true
			}
		}
		if hasOld && propertyIndexMatches(definition, oldScopes, oldProperties) {
			value, _ := oldProperties.Lookup(definition.Property)
			if err := backend.remove(definition, value, oldID); err != nil {
				return err
			}
		}
		if hasNext && propertyIndexMatches(definition, newScopes, newProperties) {
			value, _ := newProperties.Lookup(definition.Property)
			if err := backend.put(definition, value, nextID); err != nil {
				return err
			}
		}
	}
	return nil
}

func (backend pagePropertyBackend) visit(definition PropertyIndexDefinition, value any, key propertyValueKey, added, removed propertyPosting, visit func(uint64) bool) (bool, error) {
	_, bucket := pagePropertyBuckets(backend.node)
	prefix := pagePropertyPostingPrefix(definition, key)
	nextAdded, stopAdded := iter.Pull(added.all())
	defer stopAdded()
	addID, hasAdded := nextAdded()
	stopped := false
	emit := func(id uint64) (bool, error) {
		if removed.has(id) {
			return true, nil
		}
		valid, err := backend.matches(id, definition, value, key)
		if err != nil {
			return false, err
		}
		if !valid {
			return true, nil
		}
		if !visit(id) {
			stopped = true
			return false, nil
		}
		return true, nil
	}
	err := backend.graph.Tx.Scan(context.Background(), bucket, prefix, pagePrefixEnd(prefix), func(rowKey, rowValue []byte) error {
		rowDefinition, rowProperty, err := decodePagePropertyPosting(rowValue)
		if err != nil {
			return err
		}
		if rowDefinition != definition || rowProperty != key {
			return nil
		}
		if len(rowKey) != len(prefix)+8 {
			return errors.New("invalid page property posting key")
		}
		id := binary.BigEndian.Uint64(rowKey[len(prefix):])
		if err := ValidateEntityID(id); err != nil {
			return err
		}
		for hasAdded && addID < id {
			continued, err := emit(addID)
			if err != nil {
				return err
			}
			if !continued {
				return io.EOF
			}
			addID, hasAdded = nextAdded()
		}
		if hasAdded && addID == id {
			addID, hasAdded = nextAdded()
		}
		continued, err := emit(id)
		if err != nil {
			return err
		}
		if !continued {
			return io.EOF
		}
		return nil
	})
	if err != nil {
		return true, err
	}
	if stopped {
		return true, nil
	}
	for hasAdded {
		continued, err := emit(addID)
		if err != nil {
			return true, err
		}
		if !continued {
			break
		}
		addID, hasAdded = nextAdded()
	}
	return true, nil
}

func (backend pagePropertyBackend) matches(id uint64, definition PropertyIndexDefinition, value any, key propertyValueKey) (bool, error) {
	var properties Properties
	var scopes []string
	if backend.node {
		record, err := backend.graph.GetNode(id)
		if err != nil {
			return false, err
		}
		if record == nil {
			return false, nil
		}
		properties, scopes = record.Properties, record.Labels
	} else {
		record, err := backend.graph.GetEdge(id)
		if err != nil {
			return false, err
		}
		if record == nil {
			return false, nil
		}
		properties, scopes = record.Properties, []string{record.Type}
	}
	if !propertyIndexMatches(definition, scopes, properties) {
		return false, nil
	}
	actual, _ := properties.Lookup(definition.Property)
	actualKey, err := makePropertyValueKey(actual)
	if err != nil {
		return false, err
	}
	return actualKey == key && propertyIndexValuesEqual(actual, value), nil
}

func (backend pagePropertyBackend) lookup(definition PropertyIndexDefinition, value any, key propertyValueKey, added, removed propertyPosting) ([]uint64, bool, error) {
	ids := make([]uint64, 0)
	_, err := backend.visit(definition, value, key, added, removed, func(id uint64) bool { ids = append(ids, id); return true })
	return ids, true, err
}

func (backend pagePropertyBackend) put(definition PropertyIndexDefinition, value any, id uint64) error {
	key, err := makePropertyValueKey(value)
	if err != nil {
		return err
	}
	_, bucket := pagePropertyBuckets(backend.node)
	prefix := pagePropertyPostingPrefix(definition, key)
	postingKey := append(bytes.Clone(prefix), pageID(id)...)
	stored, err := backend.graph.Tx.Get(bucket, postingKey)
	if err != nil {
		return err
	}
	if stored != nil {
		storedDefinition, storedKey, err := decodePagePropertyPosting(stored)
		if err != nil {
			return err
		}
		if storedDefinition != definition || storedKey != key {
			return errors.New("page property posting hash collision")
		}
	}
	return backend.graph.Tx.Put(bucket, postingKey, encodePagePropertyPosting(definition, key))
}

func (backend pagePropertyBackend) remove(definition PropertyIndexDefinition, value any, id uint64) error {
	key, err := makePropertyValueKey(value)
	if err != nil {
		return err
	}
	_, bucket := pagePropertyBuckets(backend.node)
	prefix := pagePropertyPostingPrefix(definition, key)
	postingKey := append(bytes.Clone(prefix), pageID(id)...)
	stored, err := backend.graph.Tx.Get(bucket, postingKey)
	if err != nil || stored == nil {
		return err
	}
	storedDefinition, storedKey, err := decodePagePropertyPosting(stored)
	if err != nil {
		return err
	}
	if storedDefinition != definition || storedKey != key {
		return nil
	}
	return backend.graph.Tx.Delete(bucket, postingKey)
}

func propertyIndexMatches(definition PropertyIndexDefinition, scopes []string, properties Properties) bool {
	if !slices.Contains(scopes, definition.Scope) {
		return false
	}
	_, found := properties.Lookup(definition.Property)
	return found
}

func pagePropertyBuckets(node bool) (string, string) {
	if node {
		return pageNodePropertyIndexes, pageNodePropertyPostings
	}
	return pageEdgePropertyIndexes, pageEdgePropertyPostings
}

func pagePropertyDefinitionHash(definition PropertyIndexDefinition) []byte {
	return pagePropertyDigest([]byte("definition"), []byte(definition.Scope), []byte(definition.Property))
}

func pagePropertyPostingPrefix(definition PropertyIndexDefinition, key propertyValueKey) []byte {
	result := make([]byte, 0, sha256.Size*2)
	result = append(result, pagePropertyDefinitionHash(definition)...)
	result = append(result, pagePropertyDigest([]byte("value"), encodePagePropertyKey(key))...)
	return result
}

func pagePropertyDigest(parts ...[]byte) []byte {
	h := sha256.New()
	for _, part := range parts {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(part)))
		_, _ = h.Write(length[:])
		_, _ = h.Write(part)
	}
	return h.Sum(nil)
}

func encodePagePropertyDefinition(definition PropertyIndexDefinition) []byte {
	var output bytes.Buffer
	writePageString(&output, definition.Scope)
	writePageString(&output, definition.Property)
	return output.Bytes()
}

func decodePagePropertyDefinition(input []byte) (PropertyIndexDefinition, error) {
	reader := bytes.NewReader(input)
	scope, err := readPageString(reader)
	if err != nil {
		return PropertyIndexDefinition{}, err
	}
	property, err := readPageString(reader)
	if err != nil || reader.Len() != 0 {
		return PropertyIndexDefinition{}, errors.New("invalid page property index definition")
	}
	return PropertyIndexDefinition{Scope: scope, Property: property}, nil
}

func encodePagePropertyKey(key propertyValueKey) []byte {
	var output bytes.Buffer
	output.WriteByte(key.kind)
	var number [8]byte
	binary.BigEndian.PutUint64(number[:], key.number)
	output.Write(number[:])
	output.Write(key.digest[:])
	writePageString(&output, key.text)
	return output.Bytes()
}

func decodePagePropertyKey(input []byte) (propertyValueKey, error) {
	if len(input) < 1+8+sha256.Size {
		return propertyValueKey{}, errors.New("invalid page property value key")
	}
	key := propertyValueKey{kind: input[0], number: binary.BigEndian.Uint64(input[1:9])}
	copy(key.digest[:], input[9:9+sha256.Size])
	reader := bytes.NewReader(input[9+sha256.Size:])
	text, err := readPageString(reader)
	if err != nil || reader.Len() != 0 {
		return propertyValueKey{}, errors.New("invalid page property value key")
	}
	key.text = text
	return key, nil
}

func encodePagePropertyPosting(definition PropertyIndexDefinition, key propertyValueKey) []byte {
	var output bytes.Buffer
	writePageString(&output, definition.Scope)
	writePageString(&output, definition.Property)
	encodedKey := encodePagePropertyKey(key)
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(encodedKey)))
	output.Write(length[:])
	output.Write(encodedKey)
	return output.Bytes()
}

func decodePagePropertyPosting(input []byte) (PropertyIndexDefinition, propertyValueKey, error) {
	reader := bytes.NewReader(input)
	scope, err := readPageString(reader)
	if err != nil {
		return PropertyIndexDefinition{}, propertyValueKey{}, err
	}
	property, err := readPageString(reader)
	if err != nil {
		return PropertyIndexDefinition{}, propertyValueKey{}, err
	}
	var length [4]byte
	if _, err := io.ReadFull(reader, length[:]); err != nil {
		return PropertyIndexDefinition{}, propertyValueKey{}, err
	}
	size := binary.BigEndian.Uint32(length[:])
	if uint64(size) > uint64(reader.Len()) {
		return PropertyIndexDefinition{}, propertyValueKey{}, errors.New("invalid page property posting")
	}
	encodedKey := make([]byte, size)
	if _, err := io.ReadFull(reader, encodedKey); err != nil || reader.Len() != 0 {
		return PropertyIndexDefinition{}, propertyValueKey{}, errors.New("invalid page property posting")
	}
	key, err := decodePagePropertyKey(encodedKey)
	return PropertyIndexDefinition{Scope: scope, Property: property}, key, err
}

func writePageString(output *bytes.Buffer, value string) {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(value)))
	output.Write(length[:])
	output.WriteString(value)
}

func readPageString(input *bytes.Reader) (string, error) {
	var length [4]byte
	if _, err := io.ReadFull(input, length[:]); err != nil {
		return "", err
	}
	size := binary.BigEndian.Uint32(length[:])
	if uint64(size) > uint64(input.Len()) {
		return "", errors.New("invalid page property string")
	}
	value := make([]byte, size)
	if _, err := io.ReadFull(input, value); err != nil {
		return "", err
	}
	return string(value), nil
}
