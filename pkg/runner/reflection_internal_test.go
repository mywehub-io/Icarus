package runner

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"

	"github.com/wehubfusion/Icarus/pkg/message"
)

// deliveryMsg is a fetched message with a chosen subject, body, headers and delivering consumer.
type deliveryMsg struct {
	*ackMsg
	subject  string
	stream   string
	consumer string
}

func (m *deliveryMsg) Subject() string { return m.subject }

func (m *deliveryMsg) Metadata() (*jetstream.MsgMetadata, error) {
	md, err := m.ackMsg.Metadata()
	if err != nil {
		return md, err
	}
	cp := *md
	cp.Stream, cp.Consumer = m.stream, m.consumer
	return &cp, nil
}

func newDelivery(t *testing.T, subject string, h nats.Header, data []byte) *deliveryMsg {
	t.Helper()
	m := newAckMsg(t)
	m.headers = h
	if data != nil {
		m.data = data
	}
	return &deliveryMsg{ackMsg: m, subject: subject, stream: "TEST", consumer: "test-consumer"}
}

// filteredConsumer is a fakeStallConsumer whose info names a filter subject.
type filteredConsumer struct {
	*fakeStallConsumer
	filter string
}

func (c *filteredConsumer) CachedInfo() *jetstream.ConsumerInfo {
	return &jetstream.ConsumerInfo{Config: jetstream.ConsumerConfig{FilterSubject: c.filter}}
}

// runOne runs a runner whose consumer delivers msg once, and reports how often Process ran.
func runOne(t *testing.T, cons func(jetstream.Msg) jetstream.Consumer, msg jetstream.Msg) *atomic.Int64 {
	t.Helper()
	var calls atomic.Int64
	proc := funcProcessor(func(context.Context, *message.Message) (message.Message, error) {
		calls.Add(1)
		return message.Message{}, nil
	})
	js := &fakeStallJS{consumerFn: func(int) (jetstream.Consumer, error) { return cons(msg), nil }}
	r := newFakeRunner(js, proc, 1, -1, time.Minute)
	stop := startRun(t, r)
	time.Sleep(100 * time.Millisecond)
	stop()
	return &calls
}

func plainConsumer(m jetstream.Msg) jetstream.Consumer { return consumerDelivering(m) }

// Each kind of reflected message in the work stream is terminated: never processed, acked or
// redelivered.
func TestRun_TerminatesReflectedDeliveries(t *testing.T) {
	cases := map[string]*deliveryMsg{
		"direct get answer": newDelivery(t, "TEST.subject", nats.Header{
			"Nats-Stream":     {"KV_RUNNER_A"},
			"Nats-Subject":    {"$KV.RUNNER_A.mine"},
			"Nats-Sequence":   {"3"},
			"Nats-Time-Stamp": {"2026-11-13T10:00:00Z"},
		}, nil),
		"refused direct get": newDelivery(t, "TEST.subject", nats.Header{"Status": {"408"}}, []byte{}),
		"publish ack":        newDelivery(t, "TEST.subject", nil, []byte(`{"stream":"KV_RUNNER_A","seq":9}`)),
		"API response":       newDelivery(t, "TEST.subject", nil, []byte(`{"type":"io.nats.jetstream.api.v1.stream_info_response"}`)),
	}
	for name, msg := range cases {
		t.Run(name, func(t *testing.T) {
			calls := runOne(t, plainConsumer, msg)
			if calls.Load() != 0 || msg.acks.Load() != 0 || msg.naks.Load() != 0 {
				t.Fatalf("processed %d, acked %d, nak'd %d; want none", calls.Load(), msg.acks.Load(), msg.naks.Load())
			}
			if msg.terms.Load() != 1 {
				t.Fatalf("terminated %d times, want 1", msg.terms.Load())
			}
		})
	}
}

// A delivery outside the consumer's filter is terminated; one inside it runs.
func TestRun_RefusesSubjectOutsideFilter(t *testing.T) {
	filtered := func(m jetstream.Msg) jetstream.Consumer {
		return &filteredConsumer{fakeStallConsumer: consumerDelivering(m), filter: "HTTP_REQUESTS.runnerB.>"}
	}
	outside := newDelivery(t, "HTTP_REQUESTS.runnerA.x", nil, nil)
	if calls := runOne(t, filtered, outside); calls.Load() != 0 || outside.terms.Load() != 1 {
		t.Fatalf("outside the filter: processed %d, terminated %d; want 0 and 1", calls.Load(), outside.terms.Load())
	}
	inside := newDelivery(t, "HTTP_REQUESTS.runnerB.x", nil, nil)
	if calls := runOne(t, filtered, inside); calls.Load() != 1 || inside.terms.Load() != 0 {
		t.Fatalf("inside the filter: processed %d, terminated %d; want 1 and 0", calls.Load(), inside.terms.Load())
	}
}

// A delivery from another consumer, redirected to this runner's fetch inbox, is neither processed
// nor acknowledged in any way: its acknowledgement belongs to that consumer.
func TestRun_IgnoresDeliveryFromAnotherConsumer(t *testing.T) {
	msg := newDelivery(t, "TEST.subject", nil, nil)
	msg.consumer = "runner-a"
	calls := runOne(t, plainConsumer, msg)
	if calls.Load() != 0 || msg.acks.Load() != 0 || msg.naks.Load() != 0 || msg.terms.Load() != 0 {
		t.Fatalf("processed %d, acked %d, nak'd %d, terminated %d; want none",
			calls.Load(), msg.acks.Load(), msg.naks.Load(), msg.terms.Load())
	}
}

// A genuine unit, with a message id, still runs.
func TestRun_GenuineDeliveryRuns(t *testing.T) {
	msg := newDelivery(t, "TEST.subject", nats.Header{jetstream.MsgIDHeader: {"wf:run:node"}}, nil)
	if calls := runOne(t, plainConsumer, msg); calls.Load() != 1 || msg.terms.Load() != 0 {
		t.Fatalf("processed %d, terminated %d; want 1 and 0", calls.Load(), msg.terms.Load())
	}
}

func TestSubjectMatches(t *testing.T) {
	cases := []struct {
		filter, subject string
		want            bool
	}{
		{"a.b.c", "a.b.c", true},
		{"a.b.c", "a.b", false},
		{"a.*.c", "a.x.c", true},
		{"a.*", "a.x.c", false},
		{"a.>", "a.x.c", true},
		{"a.>", "a", false},
		{"a.b.>", "a.c.d", false},
		{">", "anything.at.all", true},
	}
	for _, tc := range cases {
		if got := subjectMatches(tc.filter, tc.subject); got != tc.want {
			t.Errorf("subjectMatches(%q, %q) = %v, want %v", tc.filter, tc.subject, got, tc.want)
		}
	}
}

const testHeartbeatKey = "runnerB.wf1.run1.node1"

// directGetAnswer is the server's answer to a direct get of key: the stored headers, then its own.
func directGetAnswer(stored nats.Header, data []byte, seq string) *nats.Msg {
	h := nats.Header{}
	for k, v := range stored {
		h[k] = append([]string(nil), v...)
	}
	h.Add("Nats-Stream", "KV_"+executionHeartbeatBucket)
	h.Add("Nats-Subject", "$KV."+executionHeartbeatBucket+"."+testHeartbeatKey)
	h.Add("Nats-Sequence", seq)
	h.Add("Nats-Time-Stamp", "2026-11-13T10:00:00.5Z")
	return &nats.Msg{Header: h, Data: data}
}

func guardedOver(fallback jetstream.KeyValue, answer func(subject string) (*nats.Msg, error)) (*guardedHeartbeats, *[]string) {
	var asked []string
	return &guardedHeartbeats{
		KeyValue: fallback,
		bucket:   executionHeartbeatBucket,
		request: func(_ context.Context, subject string) (*nats.Msg, error) {
			asked = append(asked, subject)
			return answer(subject)
		},
		logger: zap.NewNop(),
	}, &asked
}

func TestGuardedHeartbeats_Get(t *testing.T) {
	body := []byte(`{"state":"running","owner":"someone"}`)

	t.Run("genuine entry", func(t *testing.T) {
		g, asked := guardedOver(nil, func(string) (*nats.Msg, error) {
			return directGetAnswer(nats.Header{"Nats-Expected-Last-Subject-Sequence": {"4"}}, body, "7"), nil
		})
		e, err := g.Get(context.Background(), testHeartbeatKey)
		if err != nil {
			t.Fatal(err)
		}
		want := "$JS.API.DIRECT.GET.KV_EXECUTION_HEARTBEATS.$KV.EXECUTION_HEARTBEATS." + testHeartbeatKey
		if len(*asked) != 1 || (*asked)[0] != want {
			t.Fatalf("asked %v, want [%s]", *asked, want)
		}
		if reflectedEntry(e) || string(e.Value()) != string(body) || e.Revision() != 7 || e.Key() != testHeartbeatKey {
			t.Fatalf("entry = reflected %v, value %q, revision %d", reflectedEntry(e), e.Value(), e.Revision())
		}
		if !e.Created().Equal(time.Date(2026, 11, 13, 10, 0, 0, 5e8, time.UTC)) {
			t.Fatalf("created = %v", e.Created())
		}
	})

	t.Run("reflected direct get answer stored over the key", func(t *testing.T) {
		// What runner A stored there: a direct get answer of its own key.
		stored := nats.Header{
			"Nats-Stream":     {"KV_EXECUTION_HEARTBEATS"},
			"Nats-Subject":    {"$KV.EXECUTION_HEARTBEATS.runnerA.mine"},
			"Nats-Sequence":   {"99"},
			"Nats-Time-Stamp": {"2026-11-13T09:00:00Z"},
		}
		g, _ := guardedOver(nil, func(string) (*nats.Msg, error) { return directGetAnswer(stored, body, "12"), nil })
		e, err := g.Get(context.Background(), testHeartbeatKey)
		if err != nil {
			t.Fatal(err)
		}
		if !reflectedEntry(e) || e.Value() != nil || e.Revision() != 12 {
			t.Fatalf("entry = reflected %v, value %q, revision %d; want reflected, no value, revision 12 (the server's)",
				reflectedEntry(e), e.Value(), e.Revision())
		}
	})

	t.Run("reflected status answer stored over the key", func(t *testing.T) {
		g, _ := guardedOver(nil, func(string) (*nats.Msg, error) {
			return directGetAnswer(nats.Header{"Status": {"408"}, "Description": {"Empty Request"}}, nil, "5"), nil
		})
		e, err := g.Get(context.Background(), testHeartbeatKey)
		if err != nil || !reflectedEntry(e) || e.Revision() != 5 {
			t.Fatalf("entry %v, err %v; want a reflected entry at revision 5", e, err)
		}
	})

	t.Run("reflected publish ack stored over the key", func(t *testing.T) {
		g, _ := guardedOver(nil, func(string) (*nats.Msg, error) {
			return directGetAnswer(nil, []byte(`{"stream":"KV_EXECUTION_HEARTBEATS","seq":3}`), "6"), nil
		})
		e, err := g.Get(context.Background(), testHeartbeatKey)
		if err != nil || !reflectedEntry(e) {
			t.Fatalf("entry %v, err %v; want a reflected entry", e, err)
		}
	})

	t.Run("missing key", func(t *testing.T) {
		g, _ := guardedOver(nil, func(string) (*nats.Msg, error) {
			return &nats.Msg{Header: nats.Header{"Status": {"404"}, "Description": {"Message Not Found"}}}, nil
		})
		if _, err := g.Get(context.Background(), testHeartbeatKey); !errors.Is(err, jetstream.ErrKeyNotFound) {
			t.Fatalf("err = %v, want ErrKeyNotFound", err)
		}
	})

	t.Run("delete marker", func(t *testing.T) {
		g, _ := guardedOver(nil, func(string) (*nats.Msg, error) {
			return directGetAnswer(nats.Header{"KV-Operation": {"DEL"}}, nil, "8"), nil
		})
		if _, err := g.Get(context.Background(), testHeartbeatKey); !errors.Is(err, jetstream.ErrKeyNotFound) {
			t.Fatalf("err = %v, want ErrKeyNotFound", err)
		}
	})

	t.Run("no direct gets falls back to the KV", func(t *testing.T) {
		kv := &fakeHeartbeatKV{}
		kv.seed(t, testHeartbeatKey, executionHeartbeat{State: heartbeatRunning}, time.Now())
		g, _ := guardedOver(kv, func(string) (*nats.Msg, error) { return nil, nats.ErrNoResponders })
		e, err := g.Get(context.Background(), testHeartbeatKey)
		if err != nil || reflectedEntry(e) || e.Revision() != 1 {
			t.Fatalf("entry %v, err %v; want the KV's own entry", e, err)
		}
	})

	t.Run("request error fails", func(t *testing.T) {
		g, _ := guardedOver(nil, func(string) (*nats.Msg, error) { return nil, context.DeadlineExceeded })
		if _, err := g.Get(context.Background(), testHeartbeatKey); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v, want the request's error", err)
		}
	})
}

// reflectingKV is a fakeHeartbeatKV whose Get reports the entries in forged as reflections, as
// guardedHeartbeats does.
type reflectingKV struct {
	*fakeHeartbeatKV
	forged map[string]bool
}

func (kv *reflectingKV) Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
	e, err := kv.fakeHeartbeatKV.Get(ctx, key)
	if err != nil || !kv.forged[key] {
		return e, err
	}
	return &heartbeatEntry{key: key, revision: e.Revision(), created: e.Created(), reflected: "forged"}, nil
}

// A reflected entry claiming the unit is fresh and running, or done, is no claim: it is taken over
// on its revision, which replaces it.
func TestClaim_ReflectedEntryIsTakenOver(t *testing.T) {
	for _, state := range []string{heartbeatRunning, heartbeatDone} {
		t.Run(state, func(t *testing.T) {
			base := &fakeHeartbeatKV{}
			base.seed(t, testHeartbeatKey, executionHeartbeat{ExecutionID: "exec1", State: state, Owner: "attacker"}, time.Now())
			kv := &reflectingKV{fakeHeartbeatKV: base, forged: map[string]bool{testHeartbeatKey: true}}
			hb := executionHeartbeat{ExecutionID: "exec1", State: heartbeatRunning, Owner: processToken}
			outcome, err := claimExecutionUnit(context.Background(), kv, testHeartbeatKey, hb, time.Now())
			if err != nil || outcome != claimWon {
				t.Fatalf("outcome %v, err %v; want claimWon", outcome, err)
			}
			got, _ := base.entry(t, testHeartbeatKey)
			if got.Owner != processToken || got.State != heartbeatRunning {
				t.Fatalf("stored entry %+v; want this process's running claim", got)
			}
		})
	}
}

// A hand-off over a reflected entry does not read its owner: it replaces it with the queued entry.
func TestMarkQueued_ReplacesReflectedEntry(t *testing.T) {
	base := &fakeHeartbeatKV{}
	key := executionHeartbeatKey("wf1", "run1", "node1")
	base.seed(t, key, executionHeartbeat{ExecutionID: "exec1", State: heartbeatRunning, Owner: processToken}, time.Now())
	kv := &reflectingKV{fakeHeartbeatKV: base, forged: map[string]bool{key: true}}
	r := newHeartbeatRunnerWithKV(kv)
	_, msg := unitMsg(t, 1, nil)
	if rev := r.markQueued(msg, "wf1", "run1", "node1", "exec1"); rev != 2 {
		t.Fatalf("markQueued revision = %d, want 2 (written over the forgery)", rev)
	}
	got, _ := base.entry(t, key)
	if got.State != heartbeatQueued || got.Owner != processToken {
		t.Fatalf("stored entry %+v; want this process's queued entry", got)
	}
}

// A hand-off copy carries no header that would make its own delivery look like a server answer.
func TestHandOff_DropsServerAnswerHeaders(t *testing.T) {
	kv := &fakeHeartbeatKV{}
	r, copies := handOffRunner(kv, nil)
	r.clearLocalRunning("exec1")
	_, msg := wrapForHandOff(t, 1, nats.Header{
		"Status":       {"100"},
		"Nats-Stream":  {"TEST"},
		"nats-subject": {"TEST.other"},
		"X-Custom":     {"kept"},
	})
	if err := r.handOff(msg, "wf1", "run1", "node1", "exec1"); err != nil {
		t.Fatal(err)
	}
	if len(*copies) != 1 {
		t.Fatalf("%d copies, want 1", len(*copies))
	}
	c := (*copies)[0]
	for _, k := range []string{"Status", "Nats-Stream", "nats-subject"} {
		if _, ok := c.header[k]; ok {
			t.Fatalf("copy kept header %s", k)
		}
	}
	if c.header.Get("X-Custom") != "kept" {
		t.Fatal("copy lost X-Custom")
	}
	if bad, reason := message.IsReflection(c.subject, c.header, c.data); bad {
		t.Fatalf("the copy would be refused on delivery: %s", reason)
	}
}
