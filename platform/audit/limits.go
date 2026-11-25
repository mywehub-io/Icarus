package audit

import (
	"sort"
	"strings"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"

	"github.com/wehubfusion/Icarus/platform/audit/auditpb"
)

// MaxEventBytes caps an encoded event. It is part of the event contract, so it is the same in
// every environment. An event over it loses attrs (largest first), then user_agent, and is marked
// truncated; one still over it is dropped and counted.
const MaxEventBytes = 1024

// Field limits from the event contract, in bytes. A value is cut on a UTF-8 boundary, so it is
// also within the limit in characters.
const (
	MaxActionLen       = 64
	MaxReasonLen       = 32
	MaxResourceTypeLen = 32
	MaxResourceIDLen   = 64
	MaxActorIDLen      = 64
	MaxRequestIDLen    = 64
	MaxUserAgentLen    = 64
	MaxAttrs           = 6
	MaxAttrKeyLen      = 32
	MaxAttrValueLen    = 64
)

// cut returns s as valid UTF-8 of at most max bytes, cut on a rune boundary, and whether it had
// to shorten it. Invalid bytes become U+FFFD, since protobuf refuses to encode a string that is
// not valid UTF-8.
func cut(s string, max int) (string, bool) {
	if !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, "�")
	}
	if len(s) <= max {
		return s, false
	}
	if max <= 0 {
		return "", true
	}
	i := max
	for i > 0 && !utf8.RuneStart(s[i]) {
		i--
	}
	return s[:i], true
}

// enforceLimits cuts every limited field and the attrs map, and sets Truncated when anything was
// shortened or removed.
func enforceLimits(ev *auditpb.Event) {
	t := false
	apply := func(p *string, max int) {
		var c bool
		*p, c = cut(*p, max)
		t = t || c
	}
	apply(&ev.Action, MaxActionLen)
	apply(&ev.Reason, MaxReasonLen)
	apply(&ev.RequestId, MaxRequestIDLen)
	apply(&ev.UserAgent, MaxUserAgentLen)
	if r := ev.GetResource(); r != nil {
		apply(&r.Type, MaxResourceTypeLen)
		apply(&r.Id, MaxResourceIDLen)
	}
	if a := ev.GetActor(); a != nil {
		apply(&a.Id, MaxActorIDLen)
	}
	// Strings without a contract limit still have to be valid UTF-8 to encode.
	ev.JobId, _ = cut(ev.JobId, len(ev.JobId))
	if d := ev.GetDecision(); d != nil {
		d.ResourceType, _ = cut(d.ResourceType, len(d.ResourceType))
		d.Action, _ = cut(d.Action, len(d.Action))
		d.ResourceId, _ = cut(d.ResourceId, len(d.ResourceId))
	}

	if len(ev.Attrs) > 0 {
		keys := make([]string, 0, len(ev.Attrs))
		for k := range ev.Attrs {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make(map[string]string, min(len(keys), MaxAttrs))
		for _, k := range keys {
			v := ev.Attrs[k]
			ck, kc := cut(k, MaxAttrKeyLen)
			cv, vc := cut(v, MaxAttrValueLen)
			if kc || vc {
				t = true
			}
			if _, dup := out[ck]; dup || len(out) == MaxAttrs {
				t = true
				continue
			}
			out[ck] = cv
		}
		ev.Attrs = out
	}
	if t {
		ev.Truncated = true
	}
}

// fitSize trims ev to MaxEventBytes: attrs first, largest entry first, then user_agent. It
// reports whether ev now fits.
func fitSize(ev *auditpb.Event) bool {
	if proto.Size(ev) <= MaxEventBytes {
		return true
	}
	ev.Truncated = true
	for proto.Size(ev) > MaxEventBytes && len(ev.Attrs) > 0 {
		var largest string
		size := -1
		for k, v := range ev.Attrs {
			s := len(k) + len(v)
			if s > size || (s == size && k < largest) {
				largest, size = k, s
			}
		}
		delete(ev.Attrs, largest)
	}
	if len(ev.Attrs) == 0 {
		ev.Attrs = nil
	}
	if over := proto.Size(ev) - MaxEventBytes; over > 0 && ev.UserAgent != "" {
		ev.UserAgent, _ = cut(ev.UserAgent, len(ev.UserAgent)-over)
		if proto.Size(ev) > MaxEventBytes {
			ev.UserAgent = ""
		}
	}
	return proto.Size(ev) <= MaxEventBytes
}
