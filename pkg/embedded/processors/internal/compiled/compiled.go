// Package compiled caches values processors used to rebuild for every item (raw payloads phase 16):
// compiled regular expressions and loaded time zones. Both are safe to share between goroutines.
// Each cache is bounded; when full it starts again empty, so a stream of distinct user patterns
// cannot grow it without limit. Failures are not cached.
package compiled

import (
	"regexp"
	"sync"
	"time"
)

// maxEntries bounds each cache.
const maxEntries = 1024

type cache[V any] struct {
	mu sync.RWMutex
	m  map[string]V
}

func (c *cache[V]) get(key string, build func(string) (V, error)) (V, error) {
	c.mu.RLock()
	v, ok := c.m[key]
	c.mu.RUnlock()
	if ok {
		return v, nil
	}
	v, err := build(key)
	if err != nil {
		return v, err
	}
	c.mu.Lock()
	if c.m == nil || len(c.m) >= maxEntries {
		c.m = make(map[string]V)
	}
	c.m[key] = v
	c.mu.Unlock()
	return v, nil
}

var (
	regexps   cache[*regexp.Regexp]
	locations cache[*time.Location]
)

// Regexp returns pattern compiled, from the cache when it has been compiled before.
func Regexp(pattern string) (*regexp.Regexp, error) {
	return regexps.get(pattern, regexp.Compile)
}

// Location returns the time zone name loaded, from the cache when it has been loaded before.
// time.LoadLocation reads the zone database on every call.
func Location(name string) (*time.Location, error) {
	return locations.get(name, time.LoadLocation)
}
