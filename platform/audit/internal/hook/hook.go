// Package hook holds the rules the grpcaudit and httpaudit hooks share: when a call is
// recorded, which chain it goes to, the denial rate limiter, and how caller details are read.
package hook

import (
	"context"
	"net"
	"net/netip"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/wehubfusion/Icarus/platform/audit"
	"github.com/wehubfusion/Icarus/platform/audit/auditpb"
)

// Kind is what a call does, as far as recording is concerned.
type Kind int

const (
	KindWrite Kind = iota
	KindRead
	KindSensitiveRead
)

// Attr keys the hooks add.
const (
	AttrClaimedClientID = "claimed_client_id"
	AttrSuppressed      = "suppressed"
)

// Call is everything a hook learned about one finished call.
type Call struct {
	Kind  Kind
	Entry bool
	// Denied is true for Unauthenticated or PermissionDenied (HTTP 401 or 403).
	Denied  bool
	Outcome auditpb.Outcome
	Code    uint32
	Action  string
	// LimitKey names the method for the denial rate limiter.
	LimitKey  string
	ActorID   string
	ActorType auditpb.ActorType
	ClientID  string
	RequestID string
	UserAgent string
	SourceIP  []byte
	Snapshot  audit.Snapshot
}

// Recorder turns a finished call into at most one event.
type Recorder struct {
	emitter *audit.Emitter
	limiter *Limiter
	logger  *zap.Logger
}

// NewRecorder returns a Recorder that emits on em. The denial limiter follows em's clock.
func NewRecorder(em *audit.Emitter, logger *zap.Logger) *Recorder {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Recorder{
		emitter: em,
		limiter: NewLimiter(time.Minute, em.Now),
		logger:  logger,
	}
}

// Recorded reports whether a call is recorded at all: a denial, or a write or sensitive read at
// an entry. A hook checks it before doing the work of building a Call.
func Recorded(kind Kind, entry, denied bool) bool {
	return denied || (entry && (kind == KindWrite || kind == KindSensitiveRead))
}

// Record applies the recording rules to c and emits the event, if any:
//   - a denial is always recorded, at any hop, ModeSync;
//   - a write at an entry is recorded, ModeSync;
//   - a sensitive read at an entry is recorded, ModeAsync;
//   - anything else is not.
//
// A denial or any other failed call goes to the organisation's chain only when the caller named a
// valid organisation id and at least one authorisation check granted something; otherwise it goes
// to the platform chain with the claimed id in attrs, at most once a minute per claimed
// organisation, source address and method. So a forged X-Client-Id on a call that fails before
// any check can neither land in, nor flood, that organisation's chain. A call that succeeded is
// recorded against the organisation it named. Any panic is logged and swallowed: audit never
// fails the call.
func (r *Recorder) Record(ctx context.Context, c Call) {
	defer func() {
		if p := recover(); p != nil {
			r.logger.Error("audit hook panicked", zap.Any("panic", p))
		}
	}()

	var category auditpb.Category
	mode := audit.ModeSync
	switch {
	case c.Denied:
		category = auditpb.Category_CATEGORY_DENIAL
	case c.Entry && c.Kind == KindWrite:
		category = auditpb.Category_CATEGORY_WRITE
	case c.Entry && c.Kind == KindSensitiveRead:
		category = auditpb.Category_CATEGORY_SENSITIVE_READ
		mode = audit.ModeAsync
	default:
		return
	}

	s := c.Snapshot
	ev := &auditpb.Event{
		Action:    c.Action,
		Category:  category,
		Outcome:   c.Outcome,
		Code:      c.Code,
		Reason:    s.Reason,
		JobId:     s.JobID,
		RequestId: c.RequestID,
		UserAgent: c.UserAgent,
		SourceIp:  c.SourceIP,
		Decision:  s.Decision,
		Truncated: s.Truncated,
		Actor:     &auditpb.Actor{Id: c.ActorID, Type: c.ActorType},
	}
	if c.ActorID == "" {
		ev.Actor.Type = auditpb.ActorType_ACTOR_TYPE_UNSPECIFIED
	}
	switch {
	case s.ResourceSet:
		ev.Resource = &auditpb.Resource{Type: s.ResourceType, Id: s.ResourceID}
	case s.Decision != nil:
		ev.Resource = &auditpb.Resource{Type: s.Decision.GetResourceType(), Id: s.Decision.GetResourceId()}
	}

	extra := map[string]string{}
	org, valid := ClientIDBytes(c.ClientID)
	failed := c.Denied || c.Outcome != auditpb.Outcome_OUTCOME_OK
	trusted := valid && (!failed || s.Granted)
	if trusted {
		ev.ClientId = org
	} else if c.ClientID != "" {
		extra[AttrClaimedClientID] = c.ClientID
	}
	if failed && !trusted {
		key := c.ClientID + "\x00" + IPString(c.SourceIP) + "\x00" + c.LimitKey
		ok, suppressed := r.limiter.Allow(key)
		if !ok {
			return
		}
		if suppressed > 0 {
			extra[AttrSuppressed] = strconv.FormatUint(suppressed, 10)
		}
	}
	var cutAttrs bool
	ev.Attrs, cutAttrs = MergeAttrs(s.Attrs, extra)
	ev.Truncated = ev.Truncated || cutAttrs

	r.emitter.Emit(ctx, ev, mode)
}

// MergeAttrs returns base plus extra in at most audit.MaxAttrs keys. Extra keys always survive;
// base keys are removed, last in sorted order first, to make room. cut reports a removal.
func MergeAttrs(base, extra map[string]string) (out map[string]string, cut bool) {
	if len(base) == 0 && len(extra) == 0 {
		return nil, false
	}
	out = make(map[string]string, len(base)+len(extra))
	keys := make([]string, 0, len(base))
	for k := range base {
		if _, ok := extra[k]; !ok {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	room := audit.MaxAttrs - len(extra)
	for i, k := range keys {
		if i >= room {
			cut = true
			break
		}
		out[k] = base[k]
	}
	for k, v := range extra {
		out[k] = v
	}
	return out, cut
}

// ClientIDBytes parses an organisation id. ok is false for an empty or invalid UUID.
func ClientIDBytes(s string) ([]byte, bool) {
	if s == "" {
		return nil, false
	}
	id, err := uuid.Parse(s)
	if err != nil {
		return nil, false
	}
	return id[:], true
}

// SourceIP picks the caller's address: x-envoy-external-address, else the last element of
// x-forwarded-for, else the peer address. It returns 4 bytes for IPv4, 16 for IPv6, or nil.
func SourceIP(envoyExternal string, forwardedFor []string, peerAddr string) []byte {
	if ip := ParseIP(envoyExternal); ip != nil {
		return ip
	}
	if n := len(forwardedFor); n > 0 {
		parts := strings.Split(forwardedFor[n-1], ",")
		if ip := ParseIP(parts[len(parts)-1]); ip != nil {
			return ip
		}
	}
	return ParseIP(peerAddr)
}

// ParseIP parses an address with or without a port. It returns 4 bytes for IPv4 (including an
// IPv4-mapped IPv6 address), 16 for IPv6, or nil.
func ParseIP(s string) []byte {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	addr, err := netip.ParseAddr(s)
	if err != nil {
		host, _, splitErr := net.SplitHostPort(s)
		if splitErr != nil {
			return nil
		}
		if addr, err = netip.ParseAddr(host); err != nil {
			return nil
		}
	}
	addr = addr.Unmap().WithZone("")
	if addr.Is4() {
		b := addr.As4()
		return b[:]
	}
	b := addr.As16()
	return b[:]
}

// IPString formats a 4 or 16 byte address, or returns "" for anything else.
func IPString(b []byte) string {
	if addr, ok := netip.AddrFromSlice(b); ok {
		return addr.String()
	}
	return ""
}

// LastXFCCURI returns the URI field of the last element of the last x-forwarded-client-cert
// value: the hop closest to this service. It returns "" when there is none.
func LastXFCCURI(values []string) string {
	if len(values) == 0 {
		return ""
	}
	elems := strings.Split(values[len(values)-1], ",")
	for _, field := range strings.Split(elems[len(elems)-1], ";") {
		k, v, ok := strings.Cut(strings.TrimSpace(field), "=")
		if ok && strings.EqualFold(k, "URI") {
			return strings.Trim(v, `"`)
		}
	}
	return ""
}

// PrincipalSet is a set of mesh principals. Each may be given as a SPIFFE id or without the
// spiffe:// prefix.
type PrincipalSet map[string]struct{}

// NewPrincipalSet builds a PrincipalSet from a list, ignoring blanks.
func NewPrincipalSet(principals []string) PrincipalSet {
	set := make(PrincipalSet, len(principals))
	for _, p := range principals {
		if p = normalisePrincipal(p); p != "" {
			set[p] = struct{}{}
		}
	}
	return set
}

// Contains reports whether uri is in the set.
func (s PrincipalSet) Contains(uri string) bool {
	if len(s) == 0 {
		return false
	}
	uri = normalisePrincipal(uri)
	if uri == "" {
		return false
	}
	_, ok := s[uri]
	return ok
}

func normalisePrincipal(p string) string {
	return strings.TrimPrefix(strings.TrimSpace(p), "spiffe://")
}

// maxLimiterEntries bounds the limiter's memory. When full, a key it is not already tracking is
// suppressed without being counted.
const maxLimiterEntries = 65536

// Limiter allows one event per key per window and counts what it suppresses, so the next allowed
// event for that key can carry the count.
type Limiter struct {
	mu        sync.Mutex
	window    time.Duration
	now       func() time.Time
	entries   map[string]*limitEntry
	lastSweep time.Time
}

type limitEntry struct {
	start      time.Time
	suppressed uint64
}

// NewLimiter returns a Limiter with the given window and clock.
func NewLimiter(window time.Duration, now func() time.Time) *Limiter {
	if now == nil {
		now = time.Now
	}
	return &Limiter{window: window, now: now, entries: make(map[string]*limitEntry)}
}

// Allow reports whether an event for key may be emitted now and, when it may, how many events
// for key were suppressed since the last one allowed.
func (l *Limiter) Allow(key string) (ok bool, suppressed uint64) {
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sweep(now)
	e := l.entries[key]
	if e != nil && now.Sub(e.start) < l.window {
		e.suppressed++
		return false, 0
	}
	if e == nil && len(l.entries) >= maxLimiterEntries {
		return false, 0
	}
	if e != nil {
		suppressed = e.suppressed
	}
	l.entries[key] = &limitEntry{start: now}
	return true, suppressed
}

// sweep forgets keys whose window has passed with nothing suppressed, and keys with a
// suppressed count that has waited ten windows for another event.
func (l *Limiter) sweep(now time.Time) {
	if now.Sub(l.lastSweep) < l.window {
		return
	}
	l.lastSweep = now
	for k, e := range l.entries {
		age := now.Sub(e.start)
		if (age >= l.window && e.suppressed == 0) || age >= 10*l.window {
			delete(l.entries, k)
		}
	}
}
