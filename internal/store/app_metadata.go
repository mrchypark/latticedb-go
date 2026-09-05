package store

import (
	"iter"
	"maps"
)

// AppMetadata shares immutable values between generations. Callers must clone
// byte slices before storing or exposing them. Forked writers copy only the
// touched ShardMap shard and full-hash collision bucket.
// ponytail: fixed 65,536 shards bound fanout, not occupancy; repartition if
// metadata grows enough that individual shard copies become expensive.
type AppMetadata struct {
	buckets ShardMap[map[string][]byte]
	length  int
}

func (metadata AppMetadata) Fork() AppMetadata {
	return AppMetadata{buckets: metadata.buckets.Fork(), length: metadata.length}
}

func (metadata AppMetadata) Len() int { return metadata.length }

func (metadata AppMetadata) Get(key string) ([]byte, bool) {
	value, ok := metadata.buckets.Get(hashString(key))[key]
	return value, ok
}

func (metadata *AppMetadata) Set(key string, value []byte) {
	hash := hashString(key)
	metadata.buckets.CloneShardOnce(hash)
	bucket := maps.Clone(metadata.buckets.Get(hash))
	if bucket == nil {
		bucket = make(map[string][]byte)
	}
	if _, exists := bucket[key]; !exists {
		metadata.length++
	}
	bucket[key] = value
	metadata.buckets.Set(hash, bucket)
}

func (metadata *AppMetadata) Delete(key string) {
	if _, exists := metadata.Get(key); !exists {
		return
	}
	hash := hashString(key)
	metadata.buckets.CloneShardOnce(hash)
	bucket := maps.Clone(metadata.buckets.Get(hash))
	delete(bucket, key)
	metadata.length--
	if len(bucket) == 0 {
		metadata.buckets.Delete(hash)
	} else {
		metadata.buckets.Set(hash, bucket)
	}
}

func (metadata AppMetadata) All() iter.Seq2[string, []byte] {
	return func(yield func(string, []byte) bool) {
		for _, bucket := range metadata.buckets.All() {
			for key, value := range bucket {
				if !yield(key, value) {
					return
				}
			}
		}
	}
}
