package runtime

import (
	"bytes"
	"encoding/json"
	"sync"
)

// ConfigPreparer is implemented by an EmbeddedNode that parses its configuration once. The
// SubflowProcessor calls Prepare with the node's normalised configuration when it is built, and
// every item of the unit then reuses the parsed value: workers share the node, so the parsed value
// is read-only. A configuration that does not parse is not an error of Prepare: the node keeps
// the error and reports it from Process, as it did when it parsed on every item.
type ConfigPreparer interface {
	Prepare(rawConfig json.RawMessage)
}

// ConfigCache keeps one parsed configuration for a node. The zero value is ready. It is safe for
// concurrent use; the cached value must not be modified by its users.
type ConfigCache[T any] struct {
	mu  sync.RWMutex
	raw []byte
	cfg *T
	err error
	set bool
}

// Get returns the configuration parsed from raw, parsing it the first time and whenever raw differs
// from what was parsed, so a node reused with another configuration (in tests) is still right.
func (c *ConfigCache[T]) Get(raw json.RawMessage, parse func(json.RawMessage) (*T, error)) (*T, error) {
	c.mu.RLock()
	if c.set && bytes.Equal(c.raw, raw) {
		cfg, err := c.cfg, c.err
		c.mu.RUnlock()
		return cfg, err
	}
	c.mu.RUnlock()

	cfg, err := parse(raw)
	c.mu.Lock()
	c.raw, c.cfg, c.err, c.set = append([]byte(nil), raw...), cfg, err, true
	c.mu.Unlock()
	return cfg, err
}

// ParseJSON is the common parse function: unmarshal raw into a new T.
func ParseJSON[T any](raw json.RawMessage) (*T, error) {
	var cfg T
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
