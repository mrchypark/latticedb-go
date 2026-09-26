package store

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
)

func (page *PageGraph) PutMetadata(key, value []byte, remove bool) error {
	if len(key) == 0 || len(key) > maxAppMetadataKeyBytes {
		return errors.New("invalid application metadata key length")
	}
	digest := sha256.Sum256(key)
	old, err := page.Tx.Get(pageMetadataBucket, digest[:])
	if err != nil {
		return err
	}
	if old != nil {
		storedKey, _, err := page.decodeMetadata(digest[:], old)
		if err != nil {
			return err
		}
		if !bytes.Equal(storedKey, key) {
			return errors.New("metadata key hash collision")
		}
	}
	if remove {
		return page.Tx.Delete(pageMetadataBucket, digest[:])
	}
	data, err := encodePageRecord(6, func(e *binaryEncoder) { e.bytes(key); e.bytes(value) })
	if err != nil {
		return err
	}
	if uint64(len(data)) > page.recordLimit() {
		return fmt.Errorf("%w: metadata record too large", ErrLoadResourceLimit)
	}
	return page.Tx.Put(pageMetadataBucket, digest[:], data)
}
func (page *PageGraph) decodeMetadata(hash, data []byte) ([]byte, []byte, error) {
	d, err := decodePageRecord(data, 6, page.recordLimit())
	if err != nil {
		return nil, nil, err
	}
	key, value := d.bytes(), d.bytes()
	if err := d.finish(); err != nil {
		return nil, nil, fmt.Errorf("decode metadata page: %w", err)
	}
	digest := sha256.Sum256(key)
	if !bytes.Equal(digest[:], hash) {
		return nil, nil, errors.New("metadata page key mismatch")
	}
	return key, value, nil
}
