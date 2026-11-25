package audit

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/wehubfusion/Icarus/platform/audit/auditpb"
)

func TestCutKeepsUTF8Boundaries(t *testing.T) {
	s, c := cut("abc", 5)
	assert.Equal(t, "abc", s)
	assert.False(t, c)

	// "é" is two bytes: a cut through it backs off to the rune start.
	s, c = cut("aé", 2)
	assert.Equal(t, "a", s)
	assert.True(t, c)

	s, c = cut("a\xffb", 10)
	assert.True(t, utf8.ValidString(s))
	assert.False(t, c)
}

func TestEnforceLimitsCutsFieldsAndAttrs(t *testing.T) {
	long := strings.Repeat("x", 200)
	ev := &auditpb.Event{
		Action:    long,
		Reason:    long,
		RequestId: long,
		UserAgent: long,
		Resource:  &auditpb.Resource{Type: long, Id: long},
		Actor:     &auditpb.Actor{Id: long},
		Attrs: map[string]string{
			"a": "1", "b": "2", "c": "3", "d": "4", "e": "5", "f": "6", "g": "7",
		},
	}
	enforceLimits(ev)

	assert.Len(t, ev.Action, MaxActionLen)
	assert.Len(t, ev.Reason, MaxReasonLen)
	assert.Len(t, ev.RequestId, MaxRequestIDLen)
	assert.Len(t, ev.UserAgent, MaxUserAgentLen)
	assert.Len(t, ev.Resource.Type, MaxResourceTypeLen)
	assert.Len(t, ev.Resource.Id, MaxResourceIDLen)
	assert.Len(t, ev.Actor.Id, MaxActorIDLen)
	assert.Len(t, ev.Attrs, MaxAttrs)
	assert.NotContains(t, ev.Attrs, "g", "keys beyond the sixth, in sorted order, are dropped")
	assert.True(t, ev.Truncated)

	ev = &auditpb.Event{Attrs: map[string]string{long: long}}
	enforceLimits(ev)
	for k, v := range ev.Attrs {
		assert.Len(t, k, MaxAttrKeyLen)
		assert.Len(t, v, MaxAttrValueLen)
	}
	assert.True(t, ev.Truncated)

	ev = &auditpb.Event{Action: "hermes.PublishWorkflow"}
	enforceLimits(ev)
	assert.False(t, ev.Truncated, "an event within its limits is not marked")
}

func TestFitSizeDropsLargestAttrsThenUserAgent(t *testing.T) {
	max := func(n int) string { return strings.Repeat("v", n) }
	ev := &auditpb.Event{
		EventId:   make([]byte, 16),
		ClientId:  make([]byte, 16),
		TraceId:   make([]byte, 16),
		SourceIp:  make([]byte, 16),
		Action:    max(MaxActionLen),
		Reason:    max(MaxReasonLen),
		RequestId: max(MaxRequestIDLen),
		UserAgent: max(MaxUserAgentLen),
		JobId:     max(64),
		Resource:  &auditpb.Resource{Type: max(MaxResourceTypeLen), Id: max(MaxResourceIDLen)},
		Actor:     &auditpb.Actor{Id: max(MaxActorIDLen)},
		Decision:  &auditpb.Decision{ResourceType: max(32), Action: max(64), ResourceId: max(64)},
		Producer:  &auditpb.Producer{Service: "hermes", InstanceId: make([]byte, 16), Seq: 1 << 40},
		Attrs: map[string]string{
			"small":  "1",
			"medium": max(30),
			"big1":   max(MaxAttrValueLen),
			"big2":   max(MaxAttrValueLen),
			"big3":   max(MaxAttrValueLen),
			"big4":   max(MaxAttrValueLen),
		},
	}
	enforceLimits(ev)
	require.Greater(t, proto.Size(ev), MaxEventBytes, "precondition: the event starts over the cap")

	require.True(t, fitSize(ev))
	assert.LessOrEqual(t, proto.Size(ev), MaxEventBytes)
	assert.True(t, ev.Truncated)
	assert.Contains(t, ev.Attrs, "small", "the smallest attr survives")
	assert.NotContains(t, ev.Attrs, "big1", "the largest attrs go first")
	assert.Equal(t, max(MaxUserAgentLen), ev.UserAgent, "user_agent is kept while dropping attrs is enough")

	// With no attrs left to drop, user_agent is cut next; past that the event cannot fit.
	ev = &auditpb.Event{UserAgent: max(MaxUserAgentLen), JobId: max(MaxEventBytes - 40)}
	require.True(t, fitSize(ev))
	assert.Less(t, len(ev.UserAgent), MaxUserAgentLen)
	assert.LessOrEqual(t, proto.Size(ev), MaxEventBytes)

	ev = &auditpb.Event{JobId: max(2 * MaxEventBytes)}
	assert.False(t, fitSize(ev))
}
