package store

import (
	"iter"
	"maps"
)

// AppMetadata shares immutable values between generations. Callers must clone
// byte slices before storing or exposing them. Forked writers copy only the
// touched hash bucket. Exact string keys distinguish all hash collisions.
// ponytail: 256 buckets avoid a map per key; repartition if individual bucket
// copies become expensive for much larger or skewed metadata sets.
type AppMetadata struct {
	buckets ShardMap[map[string][]byte]
	length  int
	cloned  [4]uint64
}

func (metadata *AppMetadata) Fork() *AppMetadata {
	if metadata == nil {
		return new(AppMetadata)
	}
	return &AppMetadata{buckets: metadata.buckets.Fork(), length: metadata.length}
}

func (metadata *AppMetadata) Len() int {
	if metadata == nil {
		return 0
	}
	return metadata.length
}

func (metadata *AppMetadata) Get(key string) ([]byte, bool) {
	if metadata == nil {
		return nil, false
	}
	value, ok := metadata.buckets.Get(hashString(key) & 255)[key]
	return value, ok
}

func (metadata *AppMetadata) Set(key string, value []byte) {
	bucket := metadata.writableBucket(hashString(key) & 255)
	if _, exists := bucket[key]; !exists {
		metadata.length++
	}
	bucket[key] = value
}

func (metadata *AppMetadata) Delete(key string) {
	if _, exists := metadata.Get(key); !exists {
		return
	}
	hash := hashString(key) & 255
	bucket := metadata.writableBucket(hash)
	delete(bucket, key)
	metadata.length--
	if len(bucket) == 0 {
		metadata.buckets.Delete(hash)
		metadata.cloned[hash/64] &^= uint64(1) << (hash % 64)
	}
}

func (metadata *AppMetadata) writableBucket(hash uint64) map[string][]byte {
	word, bit := hash/64, uint64(1)<<(hash%64)
	if metadata.cloned[word]&bit == 0 {
		metadata.buckets.CloneShardOnce(hash)
		bucket := maps.Clone(metadata.buckets.Get(hash))
		if bucket == nil {
			bucket = make(map[string][]byte)
		}
		metadata.buckets.Set(hash, bucket)
		metadata.cloned[word] |= bit
	}
	return metadata.buckets.Get(hash)
}

func (metadata *AppMetadata) All() iter.Seq2[string, []byte] {
	return func(yield func(string, []byte) bool) {
		if metadata == nil {
			return
		}
		for _, bucket := range metadata.buckets.All() {
			for key, value := range bucket {
				if !yield(key, value) {
					return
				}
			}
		}
	}
}
