package audit_test

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/wehubfusion/Icarus/platform/audit"
	"github.com/wehubfusion/Icarus/platform/audit/auditpb"
	"github.com/wehubfusion/Icarus/platform/internal/testnats"
)

var fixedNow = time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)

func newEmitter(t *testing.T, nc *nats.Conn, mutate func(*audit.Options)) *audit.Emitter {
	t.Helper()
	opts := audit.Options{
		Service:        "hermes",
		PublishTimeout: 300 * time.Millisecond,
		Now:            func() time.Time { return fixedNow },
	}
	if mutate != nil {
		mutate(&opts)
	}
	em, err := audit.NewEmitter(nc, opts)
	require.NoError(t, err)
	return em
}

func decode(t *testing.T, msgs []jetstream.RawStreamMsg) []*auditpb.Event {
	t.Helper()
	out := make([]*auditpb.Event, 0, len(msgs))
	for _, m := range msgs {
		ev := &auditpb.Event{}
		require.NoError(t, proto.Unmarshal(m.Data, ev))
		out = append(out, ev)
	}
	return out
}

func writeEvent() *auditpb.Event {
	return &auditpb.Event{
		Action:   "hermes.PublishWorkflow",
		Category: auditpb.Category_CATEGORY_WRITE,
		Outcome:  auditpb.Outcome_OUTCOME_OK,
		Actor:    &auditpb.Actor{Id: "user-1", Type: auditpb.ActorType_ACTOR_TYPE_USER},
	}
}

func TestNewEmitterValidates(t *testing.T) {
	srv := testnats.Start(t)
	_, err := audit.NewEmitter(nil, audit.Options{Service: "hermes"})
	assert.Error(t, err)
	_, err = audit.NewEmitter(srv.Conn, audit.Options{})
	assert.Error(t, err)
	_, err = audit.NewEmitter(srv.Conn, audit.Options{Service: "a.b"})
	assert.Error(t, err)
}

func TestEmitFillsAndPublishes(t *testing.T) {
	srv := testnats.Start(t)
	srv.CreateStream(t, "AUDIT", "audit.v1.>")
	em := newEmitter(t, srv.Conn, nil)

	traceID := trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: traceID, SpanID: trace.SpanID{1}, TraceFlags: trace.FlagsSampled,
	}))
	in := writeEvent()
	em.Emit(ctx, in, audit.ModeSync)

	assert.Empty(t, in.GetEventId(), "the caller's event is not modified")

	msgs := srv.Messages(t, "AUDIT")
	require.Len(t, msgs, 1)
	assert.Equal(t, "audit.v1.hermes", msgs[0].Subject)
	ev := decode(t, msgs)[0]

	id, err := uuid.FromBytes(ev.GetEventId())
	require.NoError(t, err)
	assert.Equal(t, uuid.Version(7), id.Version())
	assert.Equal(t, id.String(), msgs[0].Header.Get(jetstream.MsgIDHeader))
	assert.Equal(t, fixedNow.UnixMilli(), ev.GetOccurredAtMs())
	assert.Equal(t, traceID[:], ev.GetTraceId())
	assert.Equal(t, "hermes", ev.GetProducer().GetService())
	assert.Len(t, ev.GetProducer().GetInstanceId(), 16)
	assert.Equal(t, uint64(1), ev.GetProducer().GetSeq())
	assert.Equal(t, "hermes.PublishWorkflow", ev.GetAction())

	st := em.Stats()
	assert.Equal(t, uint64(1), st.Published)
	assert.Equal(t, uint64(1), st.Seq)
	assert.Zero(t, st.Dropped)
}

func TestEmitDeduplicatesByEventID(t *testing.T) {
	srv := testnats.Start(t)
	srv.CreateStream(t, "AUDIT", "audit.v1.>")
	em := newEmitter(t, srv.Conn, nil)

	id := uuid.Must(uuid.NewV7())
	ev := writeEvent()
	ev.EventId = id[:]
	em.Emit(context.Background(), ev, audit.ModeSync)
	em.Emit(context.Background(), ev, audit.ModeSync)

	msgs := srv.Messages(t, "AUDIT")
	require.Len(t, msgs, 1, "Nats-Msg-Id is the event id, so a redelivery is dropped by the stream")
	assert.Equal(t, id.String(), msgs[0].Header.Get(jetstream.MsgIDHeader))
	assert.Equal(t, uint64(2), em.Stats().Seq)
}

func TestEmitDropsEventOverSizeCap(t *testing.T) {
	srv := testnats.Start(t)
	srv.CreateStream(t, "AUDIT", "audit.v1.>")
	em := newEmitter(t, srv.Conn, nil)

	ev := writeEvent()
	ev.JobId = strings.Repeat("j", 2*audit.MaxEventBytes)
	em.Emit(context.Background(), ev, audit.ModeSync)
	em.Emit(context.Background(), writeEvent(), audit.ModeSync)

	st := em.Stats()
	assert.Equal(t, uint64(1), st.Dropped)
	assert.Equal(t, uint64(2), st.Seq, "a dropped event still takes a sequence number")
	events := decode(t, srv.Messages(t, "AUDIT"))
	require.Len(t, events, 1)
	assert.Equal(t, uint64(2), events[0].GetProducer().GetSeq())
}

func TestEmitTrimsToSizeCap(t *testing.T) {
	srv := testnats.Start(t)
	srv.CreateStream(t, "AUDIT", "audit.v1.>")
	em := newEmitter(t, srv.Conn, nil)

	ev := writeEvent()
	ev.UserAgent = strings.Repeat("u", 500)
	ev.JobId = strings.Repeat("j", 500)
	ev.Attrs = map[string]string{}
	for i := 0; i < 6; i++ {
		ev.Attrs["key"+strconv.Itoa(i)] = strings.Repeat("v", 64)
	}
	em.Emit(context.Background(), ev, audit.ModeSync)

	msgs := srv.Messages(t, "AUDIT")
	require.Len(t, msgs, 1)
	assert.LessOrEqual(t, len(msgs[0].Data), audit.MaxEventBytes)
	got := decode(t, msgs)[0]
	assert.True(t, got.GetTruncated())
	assert.Less(t, len(got.GetAttrs()), 6)
}

// A PubAck that never comes fails the publish at PublishTimeout. The event goes to the queue;
// with the queue full the next one is dropped, and its sequence number is a gap in the stream.
func TestPubAckTimeoutQueuesThenDropsLeavingSeqGap(t *testing.T) {
	srv := testnats.Start(t)
	other := srv.Connect(t)
	// A plain subscriber that never answers makes every publish wait for a PubAck.
	sub, err := other.Subscribe("audit.v1.hermes", func(*nats.Msg) {})
	require.NoError(t, err)
	require.NoError(t, other.Flush())

	em := newEmitter(t, srv.Conn, func(o *audit.Options) {
		o.PublishTimeout = 200 * time.Millisecond
		o.RetryQueueSize = 1
	})

	start := time.Now()
	em.Emit(context.Background(), writeEvent(), audit.ModeSync) // seq 1: queued
	assert.Less(t, time.Since(start), time.Second, "Emit returns soon after PublishTimeout")
	em.Emit(context.Background(), writeEvent(), audit.ModeSync) // seq 2: queue full, dropped
	st := em.Stats()
	assert.Equal(t, uint64(1), st.Dropped)
	assert.Equal(t, 1, st.QueueDepth)

	require.NoError(t, sub.Unsubscribe())
	require.NoError(t, other.Flush())
	srv.CreateStream(t, "AUDIT", "audit.v1.>")

	em.Emit(context.Background(), writeEvent(), audit.ModeSync) // seq 3: published
	em.Start(context.Background())                              // seq 4: producer.started; worker sends seq 1
	t.Cleanup(func() { _ = em.Close(context.Background()) })

	testnats.WaitFor(t, 5*time.Second, "three events in the stream", func() bool {
		return len(srv.Messages(t, "AUDIT")) == 3
	})
	var seqs []uint64
	for _, ev := range decode(t, srv.Messages(t, "AUDIT")) {
		seqs = append(seqs, ev.GetProducer().GetSeq())
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	assert.Equal(t, []uint64{1, 3, 4}, seqs, "seq 2 was dropped and shows as a gap")
	assert.Equal(t, uint64(1), em.Stats().Dropped)
	assert.GreaterOrEqual(t, em.Stats().Retried, uint64(1))
}

func TestMissingStreamIsCountedAndNeverCreated(t *testing.T) {
	srv := testnats.Start(t)
	em := newEmitter(t, srv.Conn, func(o *audit.Options) { o.RetryQueueSize = 1 })

	em.Emit(context.Background(), writeEvent(), audit.ModeSync)
	em.Emit(context.Background(), writeEvent(), audit.ModeSync)
	st := em.Stats()
	assert.Equal(t, uint64(1), st.Dropped)
	assert.Equal(t, 1, st.QueueDepth)
	assert.Zero(t, st.Published)

	em.Start(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	assert.Error(t, em.Close(ctx), "close reports what it could not flush")
	assert.GreaterOrEqual(t, em.Stats().Dropped, uint64(2))
	assert.Zero(t, em.Stats().Published)

	names := srv.JS.StreamNames(context.Background())
	var got []string
	for n := range names.Name() {
		got = append(got, n)
	}
	assert.Empty(t, got, "the emitter never creates a stream")
}

func TestLifecycleEvents(t *testing.T) {
	srv := testnats.Start(t)
	srv.CreateStream(t, "AUDIT", "audit.v1.>")
	em := newEmitter(t, srv.Conn, func(o *audit.Options) { o.HeartbeatEvery = 50 * time.Millisecond })

	em.Start(context.Background())
	testnats.WaitFor(t, 5*time.Second, "a heartbeat", func() bool {
		return len(srv.Messages(t, "AUDIT")) >= 2
	})
	require.NoError(t, em.Close(context.Background()))
	require.NoError(t, em.Close(context.Background()), "a second close does nothing")

	events := decode(t, srv.Messages(t, "AUDIT"))
	require.GreaterOrEqual(t, len(events), 3)
	first, last := events[0], events[len(events)-1]
	assert.Equal(t, audit.ActionProducerStarted, first.GetAction())
	assert.Equal(t, uint64(1), first.GetProducer().GetSeq())
	assert.Equal(t, audit.ActionProducerHeartbeat, events[1].GetAction())
	assert.Equal(t, "0", events[1].GetAttrs()["dropped"])
	assert.Equal(t, audit.ActionProducerStopped, last.GetAction())
	assert.Equal(t, strconv.FormatUint(last.GetProducer().GetSeq(), 10), last.GetAttrs()["final_seq"])
	assert.Equal(t, "0", last.GetAttrs()["dropped"])
	for _, ev := range events {
		assert.Equal(t, auditpb.Category_CATEGORY_SYSTEM, ev.GetCategory())
		assert.Equal(t, auditpb.ActorType_ACTOR_TYPE_SYSTEM, ev.GetActor().GetType())
		assert.Equal(t, "hermes", ev.GetActor().GetId())
		assert.Empty(t, ev.GetClientId(), "producer events go to the platform chain")
	}
}

func TestAsyncPublishesThroughTheQueue(t *testing.T) {
	srv := testnats.Start(t)
	srv.CreateStream(t, "AUDIT", "audit.v1.>")
	em := newEmitter(t, srv.Conn, nil)
	em.Start(context.Background())

	ev := writeEvent()
	ev.Category = auditpb.Category_CATEGORY_SENSITIVE_READ
	em.Emit(context.Background(), ev, audit.ModeAsync)
	require.NoError(t, em.Close(context.Background()))

	var actions []string
	for _, e := range decode(t, srv.Messages(t, "AUDIT")) {
		actions = append(actions, e.GetAction())
	}
	assert.Equal(t, []string{audit.ActionProducerStarted, "hermes.PublishWorkflow", audit.ActionProducerStopped}, actions)
	assert.Equal(t, uint64(3), em.Stats().Published)
	assert.Zero(t, em.Stats().Retried, "an async event's first attempt is not a retry")
}

func TestNoopAndNilEmitters(t *testing.T) {
	for _, em := range []*audit.Emitter{audit.NewNoopEmitter(), nil} {
		em.Start(context.Background())
		em.Emit(context.Background(), writeEvent(), audit.ModeSync)
		em.Emit(context.Background(), writeEvent(), audit.ModeAsync)
		assert.NoError(t, em.Close(context.Background()))
		assert.Equal(t, audit.Stats{}, em.Stats())
	}
}
