package schema

import (
	"sync"
	"time"
)

// SchemaCache caches compiled schemas by schemaID and content hash.
type SchemaCache interface {
	Get(schemaID, contentHash string) (CompiledSchema, bool)
	Set(schemaID, contentHash string, schema CompiledSchema, ttl time.Duration)
	Invalidate(schemaID string)
}

type cacheEntry struct {
	schema   CompiledSchema
	expiresAt time.Time
}

// InMemoryCache is a TTL-based in-memory implementation of SchemaCache.
// When the number of entries exceeds maxSize, expired entries are removed on the next Set.
type InMemoryCache struct {
	mu       sync.RWMutex
	entries  map[string]cacheEntry
	maxSize  int
	defaultTTL time.Duration
}

// NewInMemoryCache creates a cache with the given max size and default TTL.
// defaultTTL is used when Set is called with ttl <= 0.
func NewInMemoryCache(maxSize int, defaultTTL time.Duration) *InMemoryCache {
	if maxSize <= 0 {
		maxSize = 1000
	}
	if defaultTTL <= 0 {
		defaultTTL = 5 * time.Minute
	}
	return &InMemoryCache{
		entries:    make(map[string]cacheEntry),
		maxSize:    maxSize,
		defaultTTL: defaultTTL,
	}
}

func cacheKey(schemaID, contentHash string) string {
	return schemaID + ":" + contentHash
}

// Get returns the cached schema if present and not expired.
func (c *InMemoryCache) Get(schemaID, contentHash string) (CompiledSchema, bool) {
	c.mu.RLock()
	ent, ok := c.entries[cacheKey(schemaID, contentHash)]
	c.mu.RUnlock()
	if !ok || time.Now().After(ent.expiresAt) {
		return nil, false
	}
	return ent.schema, true
}

// Set stores a compiled schema with the given TTL.
func (c *InMemoryCache) Set(schemaID, contentHash string, schema CompiledSchema, ttl time.Duration) {
	if ttl <= 0 {
		ttl = c.defaultTTL
	}
	expiresAt := time.Now().Add(ttl)
	key := cacheKey(schemaID, contentHash)

	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.entries) >= c.maxSize {
		c.evictExpiredLocked()
	}
	c.entries[key] = cacheEntry{schema: schema, expiresAt: expiresAt}
}

// Invalidate removes all cached entries for the given schemaID.
func (c *InMemoryCache) Invalidate(schemaID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	prefix := schemaID + ":"
	for key := range c.entries {
		if len(key) > len(prefix) && key[:len(prefix)] == prefix {
			delete(c.entries, key)
		}
	}
}

func (c *InMemoryCache) evictExpiredLocked() {
	now := time.Now()
	for key, ent := range c.entries {
		if now.After(ent.expiresAt) {
			delete(c.entries, key)
		}
	}
}
