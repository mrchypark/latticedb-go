package pagestore

import (
	"bytes"
	"container/list"
	"strings"
	"sync"
)

type recordCacheKey struct {
	generation  int
	bucket, key string
}

type cachedRecord struct {
	key   recordCacheKey
	value []byte
	size  uint64
}

// The key includes the immutable read generation. Writers never use the cache.
// limit bounds retained payloads plus a conservative logical entry allowance,
// not allocator overhead or the operating system page cache.
type recordCache struct {
	mu          sync.Mutex
	limit, used uint64
	entries     map[recordCacheKey]*list.Element
	lru         list.List
}

func (c *recordCache) get(key recordCacheKey) ([]byte, bool) {
	if c.limit == 0 {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.entries[key]
	if e == nil {
		return nil, false
	}
	c.lru.MoveToFront(e)
	return bytes.Clone(e.Value.(cachedRecord).value), true
}

func (c *recordCache) put(key recordCacheKey, value []byte) {
	if c.limit == 0 || value == nil {
		return
	}
	size := uint64(len(key.bucket)) + uint64(len(key.key)) + uint64(len(value)) + 192
	if size > c.limit {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries[key] != nil {
		return
	}
	for c.used > c.limit-size {
		e := c.lru.Back()
		r := e.Value.(cachedRecord)
		delete(c.entries, r.key)
		c.used -= r.size
		c.lru.Remove(e)
	}
	if c.entries == nil {
		c.entries = make(map[recordCacheKey]*list.Element)
	}
	key.bucket = strings.Clone(key.bucket)
	key.key = strings.Clone(key.key)
	c.entries[key] = c.lru.PushFront(cachedRecord{key, bytes.Clone(value), size})
	c.used += size
}

func (c *recordCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = nil
	c.lru.Init()
	c.used = 0
}
