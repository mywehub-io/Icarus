package storage

import (
	"context"
	"sync"
)

// MaxUnitCachedBlobBytes is the largest blob a UnitCache keeps. It matches the read-ahead window:
// a blob this size is fetched whole in one GET anyway, so keeping it costs no extra transfer, and
// the bound keeps what one unit holds to a few small archives.
const MaxUnitCachedBlobBytes = DefaultReadAheadBytes

// UnitCache holds the bytes of small blobs one unit has already read, keyed by URL, so a second
// read of the same blob in that unit costs no GET. The resolver reads a unit's upstream archives
// once for its input and again for its embedded nodes (prior unit outputs); with the cache the
// second read is served from memory.
//
// A cache lives exactly as long as the unit's context. A result archive is written once by the
// unit that produced it, before any downstream unit is dispatched, so a copy cannot go stale
// while one unit runs.
type UnitCache struct {
	mu    sync.Mutex
	blobs map[string][]byte
}

type unitCacheKey struct{}

// WithUnitCache returns ctx carrying a new, empty UnitCache, or ctx unchanged when it already
// carries one.
func WithUnitCache(ctx context.Context) context.Context {
	if UnitCacheFrom(ctx) != nil {
		return ctx
	}
	return context.WithValue(ctx, unitCacheKey{}, &UnitCache{blobs: map[string][]byte{}})
}

// UnitCacheFrom returns the context's UnitCache, or nil when there is none. A nil UnitCache is
// valid: Get misses and Put does nothing.
func UnitCacheFrom(ctx context.Context) *UnitCache {
	if ctx == nil {
		return nil
	}
	c, _ := ctx.Value(unitCacheKey{}).(*UnitCache)
	return c
}

// Get returns the cached bytes for url. The caller must not modify them.
func (c *UnitCache) Get(url string) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	b, ok := c.blobs[url]
	return b, ok
}

// Put keeps data for url when it is no larger than MaxUnitCachedBlobBytes. The cache takes
// ownership of data; the caller must not modify it afterwards.
func (c *UnitCache) Put(url string, data []byte) {
	if c == nil || int64(len(data)) > MaxUnitCachedBlobBytes {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.blobs[url] = data
}
