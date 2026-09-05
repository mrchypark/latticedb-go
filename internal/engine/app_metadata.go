package engine

import (
	"fmt"
	"math"
	"slices"

	"github.com/mrchypark/latticedb-go/internal/store"
)

type appMetadataChange struct {
	value  []byte
	delete bool
}

func (tx *Tx) GetAppMetadata(key []byte) ([]byte, bool, error) {
	if tx == nil || tx.closed {
		return nil, false, ErrInactiveTx
	}
	if err := validateAppMetadataKey(key); err != nil {
		return nil, false, err
	}
	value, ok := tx.graph.AppMetadata.Get(string(key))
	return slices.Clone(value), ok, nil
}

func (tx *Tx) PutAppMetadata(key, value []byte) error {
	if err := tx.ensureWritable(); err != nil {
		return err
	}
	if err := validateAppMetadataKey(key); err != nil {
		return err
	}
	tx.ensureAppMetadataWritable()
	cloned := slices.Clone(value)
	textKey := string(key)
	tx.graph.AppMetadata.Set(textKey, cloned)
	tx.changes.appMetadata[textKey] = appMetadataChange{value: cloned}
	return nil
}

func (tx *Tx) DeleteAppMetadata(key []byte) error {
	if err := tx.ensureWritable(); err != nil {
		return err
	}
	if err := validateAppMetadataKey(key); err != nil {
		return err
	}
	tx.ensureAppMetadataWritable()
	textKey := string(key)
	tx.graph.AppMetadata.Delete(textKey)
	tx.changes.appMetadata[textKey] = appMetadataChange{delete: true}
	return nil
}

func (tx *Tx) ensureAppMetadataWritable() {
	if tx.changes.appMetadata != nil {
		return
	}
	tx.changes.appMetadata = make(map[string]appMetadataChange)
}

func validateAppMetadataKey(key []byte) error {
	if len(key) == 0 || len(key) > math.MaxUint16 {
		return fmt.Errorf("%w: application metadata key length must be 1..%d", ErrInvalidArgument, math.MaxUint16)
	}
	return nil
}

func persistedAppMetadataChanges(changes map[string]appMetadataChange) []store.AppMetadataChange {
	keys := make([]string, 0, len(changes))
	for key := range changes {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	result := make([]store.AppMetadataChange, 0, len(keys))
	for _, key := range keys {
		change := changes[key]
		result = append(result, store.AppMetadataChange{Key: []byte(key), Value: slices.Clone(change.value), Delete: change.delete})
	}
	return result
}
