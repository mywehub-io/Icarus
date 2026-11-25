package audit

import (
	"context"
	"sync"

	"github.com/wehubfusion/Icarus/platform/audit/auditpb"
)

type collectorKey struct{}

// Collector gathers what a handler learns about one request: the authorisation decision that
// decided it, the job it started, the resource it acted on, and a few typed attrs. The grpcaudit
// and httpaudit hooks put one in the request's context and read it after the handler returns.
// It is safe for concurrent use.
type Collector struct {
	mu        sync.Mutex
	decision  *auditpb.Decision
	denied    bool
	granted   bool
	jobID     string
	resType   string
	resID     string
	resSet    bool
	attrs     map[string]string
	reason    string
	truncated bool
	identity  *Identity
}

// Identity is the caller of one request, as the service's authentication worked it out.
type Identity struct {
	ActorID   string
	ActorType auditpb.ActorType
	// ClientID is the organisation UUID the caller named, as a string. It may be empty.
	ClientID string
}

// Snapshot is a copy of what a Collector holds.
type Snapshot struct {
	// Decision is the deciding authorisation check: the first denial if there was one, else the
	// last grant. Nil when none was recorded.
	Decision *auditpb.Decision
	// Granted reports whether any recorded decision was a grant.
	Granted bool
	// JobID is the Iris job the request started or concerns.
	JobID string
	// ResourceSet reports whether SetResource was called; ResourceType and ResourceID are its
	// values.
	ResourceSet  bool
	ResourceType string
	ResourceID   string
	// Attrs holds at most MaxAttrs keys, the first ones set.
	Attrs map[string]string
	// Reason is a machine-readable reason code.
	Reason string
	// Truncated reports that SetAttr calls were ignored because MaxAttrs keys were already set.
	Truncated bool
	// Identity is the caller recorded with SetIdentity, or nil when none was.
	Identity *Identity
}

// WithCollector returns a context carrying a new Collector, and the Collector.
func WithCollector(ctx context.Context) (context.Context, *Collector) {
	c := &Collector{}
	return context.WithValue(ctx, collectorKey{}, c), c
}

// CollectorFrom returns the Collector in ctx, or nil when there is none.
func CollectorFrom(ctx context.Context) *Collector {
	if ctx == nil {
		return nil
	}
	c, _ := ctx.Value(collectorKey{}).(*Collector)
	return c
}

// RecordDecision records an authorisation check. The collector keeps the deciding one: the first
// denial if any, else the last grant. Call it from a decorator on the service's authorisation
// client. It does nothing when ctx has no collector.
func RecordDecision(ctx context.Context, resourceType, action, resourceID string, granted bool) {
	c := CollectorFrom(ctx)
	if c == nil {
		return
	}
	d := &auditpb.Decision{ResourceType: resourceType, Action: action, ResourceId: resourceID, Granted: granted}
	c.mu.Lock()
	defer c.mu.Unlock()
	if granted {
		c.granted = true
		if !c.denied {
			c.decision = d
		}
		return
	}
	if !c.denied {
		c.denied = true
		c.decision = d
	}
}

// SetJobID records the Iris job the request started or concerns. jobs.Coordinator.Begin calls
// it. It does nothing when ctx has no collector.
func SetJobID(ctx context.Context, jobID string) {
	if c := CollectorFrom(ctx); c != nil {
		c.mu.Lock()
		c.jobID = jobID
		c.mu.Unlock()
	}
}

// SetResource records the resource the request acted on. It takes precedence over the resource
// of the deciding decision. It does nothing when ctx has no collector.
func SetResource(ctx context.Context, typ, id string) {
	if c := CollectorFrom(ctx); c != nil {
		c.mu.Lock()
		c.resType, c.resID, c.resSet = typ, id, true
		c.mu.Unlock()
	}
}

// SetAttr records one typed attr: an id or an enum value, never free text, a name or a payload.
// At most MaxAttrs distinct keys are kept; later new keys are ignored. It does nothing when ctx
// has no collector.
func SetAttr(ctx context.Context, key, value string) {
	c := CollectorFrom(ctx)
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.attrs == nil {
		c.attrs = make(map[string]string, 2)
	}
	if _, ok := c.attrs[key]; !ok && len(c.attrs) >= MaxAttrs {
		c.truncated = true
		return
	}
	c.attrs[key] = value
}

// SetReason records a machine-readable reason code, such as LEASE_EXPIRED. It does nothing when
// ctx has no collector.
func SetReason(ctx context.Context, reason string) {
	if c := CollectorFrom(ctx); c != nil {
		c.mu.Lock()
		c.reason = reason
		c.mu.Unlock()
	}
}

// SetIdentity records the caller of the request. Call it after the service's own authentication
// has run, typically from grpcaudit.IdentityRecorder placed after the auth interceptor, so the
// audit hook, which wraps every other interceptor, still learns who called. It does nothing when
// ctx has no collector.
func SetIdentity(ctx context.Context, id Identity) {
	if c := CollectorFrom(ctx); c != nil {
		c.mu.Lock()
		cp := id
		c.identity = &cp
		c.mu.Unlock()
	}
}

// Snapshot returns a copy of what c holds. A nil Collector returns the zero Snapshot.
func (c *Collector) Snapshot() Snapshot {
	if c == nil {
		return Snapshot{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	s := Snapshot{
		Granted:      c.granted,
		JobID:        c.jobID,
		ResourceSet:  c.resSet,
		ResourceType: c.resType,
		ResourceID:   c.resID,
		Reason:       c.reason,
		Truncated:    c.truncated,
	}
	if c.identity != nil {
		id := *c.identity
		s.Identity = &id
	}
	if d := c.decision; d != nil {
		s.Decision = &auditpb.Decision{
			ResourceType: d.GetResourceType(),
			Action:       d.GetAction(),
			ResourceId:   d.GetResourceId(),
			Granted:      d.GetGranted(),
		}
	}
	if len(c.attrs) > 0 {
		s.Attrs = make(map[string]string, len(c.attrs))
		for k, v := range c.attrs {
			s.Attrs[k] = v
		}
	}
	return s
}
