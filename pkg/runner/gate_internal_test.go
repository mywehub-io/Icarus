package runner

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/wehubfusion/Icarus/pkg/message"
)

// fakeGate admits up to capacity units at once and records every grant.
type fakeGate struct {
	mu       sync.Mutex
	capacity int
	used     int
	maxUsed  int
	grants   []int
	closed   bool // when true, Acquire waits until ctx ends
	acquires atomic.Int64
}

func (g *fakeGate) Acquire(ctx context.Context, max int) (int, error) {
	g.acquires.Add(1)
	for {
		g.mu.Lock()
		if !g.closed && g.used < g.capacity {
			n := min(max, g.capacity-g.used)
			g.used += n
			g.maxUsed = maxInt(g.maxUsed, g.used)
			g.grants = append(g.grants, n)
			g.mu.Unlock()
			return n, nil
		}
		g.mu.Unlock()
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

func (g *fakeGate) Release(n int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.used -= n
	if g.used < 0 {
		panic("gate released more than it granted")
	}
}

func (g *fakeGate) inUse() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.used
}

func (g *fakeGate) peak() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.maxUsed
}

func (g *fakeGate) setClosed(closed bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closed = closed
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// queueConsumer hands out queued messages, at most as many as each fetch asks for, and records the
// size of every fetch.
type queueConsumer struct {
	jetstream.Consumer
	mu      sync.Mutex
	queue   []jetstream.Msg
	fetches []int
}

func (c *queueConsumer) Fetch(batch int, _ ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	c.mu.Lock()
	c.fetches = append(c.fetches, batch)
	n := min(batch, len(c.queue))
	out := make([]jetstream.Msg, n)
	for i, m := range c.queue[:n] {
		// As JetStream does: each delivery carries how many the consumer still has undelivered.
		out[i] = &pendingMsg{Msg: m, pending: uint64(len(c.queue) - i - 1)}
	}
	c.queue = c.queue[n:]
	c.mu.Unlock()
	if n == 0 {
		return emptyFetch(batch)
	}
	return batchOf(out...), nil
}

// pendingMsg adds NumPending to a fake delivery's metadata.
type pendingMsg struct {
	jetstream.Msg
	pending uint64
}

func (m *pendingMsg) Metadata() (*jetstream.MsgMetadata, error) {
	md, err := m.Msg.Metadata()
	if err != nil || md == nil {
		return md, err
	}
	cp := *md
	cp.NumPending = m.pending
	return &cp, nil
}

func (c *queueConsumer) CachedInfo() *jetstream.ConsumerInfo { return &jetstream.ConsumerInfo{} }

func (c *queueConsumer) fetchSizes() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]int(nil), c.fetches...)
}

func (c *queueConsumer) remaining() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.queue)
}

func newGatedRunner(t *testing.T, cons jetstream.Consumer, proc Processor, gate Gate, batch int) *Runner {
	t.Helper()
	js := &fakeStallJS{consumerFn: func(int) (jetstream.Consumer, error) { return cons, nil }}
	r := newFakeRunner(js, proc, 1, -1, time.Minute)
	r.gate = gate
	r.batchSize = batch
	return r
}

// A gated runner never holds more units than its gate admits, and every slot comes back.
func TestGated_TakesOnlyWhatTheGateAdmits(t *testing.T) {
	const capacity, units = 4, 12
	msgs := make([]*ackMsg, units)
	cons := &queueConsumer{}
	for i := range msgs {
		msgs[i] = newAckMsg(t)
		cons.queue = append(cons.queue, msgs[i])
	}
	release := make(chan struct{})
	var running, peak atomic.Int64
	proc := funcProcessor(func(ctx context.Context, _ *message.Message) (message.Message, error) {
		n := running.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		<-release
		running.Add(-1)
		return message.Message{}, nil
	})
	gate := &fakeGate{capacity: capacity}
	r := newGatedRunner(t, cons, proc, gate, 10)
	startRun(t, r)

	eventually(t, 2*time.Second, "the gate to fill", func() bool { return running.Load() == capacity })
	time.Sleep(50 * time.Millisecond) // a runner that over-takes would start a fifth unit here
	if got := running.Load(); got != capacity {
		t.Fatalf("running = %d, want %d: the runner took more than the gate admitted", got, capacity)
	}
	if got := cons.remaining(); got != units-capacity {
		t.Fatalf("%d messages left in the stream, want %d: a gated runner must leave unadmitted work undelivered", got, units-capacity)
	}
	close(release)
	eventually(t, 3*time.Second, "every unit to be acked", func() bool {
		for _, m := range msgs {
			if m.acks.Load() != 1 {
				return false
			}
		}
		return true
	})
	if p := peak.Load(); p > capacity {
		t.Fatalf("peak concurrency %d exceeds gate capacity %d", p, capacity)
	}
	eventually(t, time.Second, "every slot to be released", func() bool { return gate.inUse() <= 1 })
	for _, size := range cons.fetchSizes() {
		if size > 10 {
			t.Fatalf("a fetch asked for %d, above batchSize 10", size)
		}
	}
}

// The gate, not a worker count, bounds concurrency: a runner configured with one worker runs as
// many units at once as the gate admits.
func TestGated_WorkerCountIsNotACap(t *testing.T) {
	const capacity = 8
	cons := &queueConsumer{}
	for i := 0; i < capacity; i++ {
		cons.queue = append(cons.queue, newAckMsg(t))
	}
	release := make(chan struct{})
	var running atomic.Int64
	proc := funcProcessor(func(context.Context, *message.Message) (message.Message, error) {
		running.Add(1)
		<-release
		return message.Message{}, nil
	})
	r := newGatedRunner(t, cons, proc, &fakeGate{capacity: capacity}, capacity)
	if r.config.WorkerCount != 1 {
		t.Fatalf("test setup: WorkerCount = %d, want 1", r.config.WorkerCount)
	}
	startRun(t, r)
	defer close(release)
	eventually(t, 2*time.Second, "all units to run at once", func() bool { return running.Load() == capacity })
}

// An idle runner's open pull holds one slot, not a batch; with a backlog it asks for what the
// consumer reports waiting, up to batchSize, and never for more than is there, so no fetch sits
// open holding slots for work that does not exist.
func TestGated_AdaptivePrefetch(t *testing.T) {
	cons := &queueConsumer{}
	gate := &fakeGate{capacity: 100}
	r := newGatedRunner(t, cons, okProcessor(), gate, 8)
	startRun(t, r)

	eventually(t, time.Second, "a few idle fetches", func() bool { return len(cons.fetchSizes()) >= 3 })
	for _, size := range cons.fetchSizes() {
		if size != 1 {
			t.Fatalf("an idle runner fetched %d; it must ask for one unit while the stream is empty", size)
		}
	}

	cons.mu.Lock()
	for i := 0; i < 40; i++ {
		cons.queue = append(cons.queue, newAckMsg(t))
	}
	idle := len(cons.fetches)
	cons.mu.Unlock()
	eventually(t, 2*time.Second, "the backlog to drain", func() bool { return cons.remaining() == 0 })
	sizes := cons.fetchSizes()[idle:]
	// 40 waiting: one probe finds the backlog, then batches of 8 while more than 8 are left, then
	// exactly what is left.
	if len(sizes) < 2 || sizes[1] != 8 {
		t.Fatalf("fetch sizes after the backlog arrived: %v; want a full batch of 8 once the backlog is known", sizes)
	}
	remaining := 40
	for i, size := range sizes {
		if size > 8 {
			t.Fatalf("a fetch asked for %d, above batchSize 8", size)
		}
		if remaining > 0 && size > remaining {
			t.Fatalf("fetch %d asked for %d with only %d waiting: %v", i, size, remaining, sizes)
		}
		remaining -= min(size, remaining)
	}
	eventually(t, time.Second, "an idle pull to hold one slot", func() bool { return gate.inUse() <= 1 })
}

// A runner waiting on a closed gate is at capacity, not stalled: the watchdog must not fail
// liveness for it however long the wait lasts.
func TestGated_WaitingOnTheGateIsNotAStall(t *testing.T) {
	gate := &fakeGate{capacity: 1, closed: true}
	cons := &queueConsumer{}
	js := &fakeStallJS{consumerFn: func(int) (jetstream.Consumer, error) { return cons, nil }}
	r := newFakeRunner(js, okProcessor(), 1, 50*time.Millisecond, time.Minute)
	r.gate = gate
	startRun(t, r)

	eventually(t, time.Second, "the fetch loop to wait on the gate", func() bool { return r.gateWaiting.Load() })
	time.Sleep(200 * time.Millisecond) // four stall timeouts
	if err := r.Health(); err != nil {
		t.Fatalf("Health() = %v while the runner waits on its gate; want nil", err)
	}
	if got := js.resolves.Load(); got != 1 {
		t.Fatalf("consumer resolved %d times; the watchdog must not act while the gate is closed", got)
	}

	// Once the gate opens, the time spent waiting on it does not count as time without a fetch.
	gate.setClosed(false)
	time.Sleep(20 * time.Millisecond)
	if err := r.Health(); err != nil {
		t.Fatalf("Health() = %v right after the gate opened; want nil", err)
	}
}

// With the gate open and fetches failing, the watchdog still reports the stall.
func TestGated_OpenGateWithoutFetches_IsAStall(t *testing.T) {
	failing := &fakeStallConsumer{fetch: func(int) (jetstream.MessageBatch, error) {
		time.Sleep(2 * time.Millisecond)
		return nil, errors.New("fetch keeps failing")
	}}
	js := &fakeStallJS{consumerFn: func(int) (jetstream.Consumer, error) { return failing, nil }}
	r := newFakeRunner(js, okProcessor(), 1, 50*time.Millisecond, time.Minute)
	r.gate = &fakeGate{capacity: 4}
	startRun(t, r)
	eventually(t, 2*time.Second, "the stall to be reported", func() bool { return r.Health() != nil })
}

// Shutdown waits for every gated unit and gives every slot back.
func TestGated_ShutdownDrainsAndReleases(t *testing.T) {
	cons := &queueConsumer{}
	for i := 0; i < 3; i++ {
		cons.queue = append(cons.queue, newAckMsg(t))
	}
	var finished atomic.Int64
	proc := funcProcessor(func(ctx context.Context, _ *message.Message) (message.Message, error) {
		time.Sleep(100 * time.Millisecond)
		finished.Add(1)
		return message.Message{}, nil
	})
	gate := &fakeGate{capacity: 3}
	r := newGatedRunner(t, cons, proc, gate, 3)
	stop := startRun(t, r)
	eventually(t, time.Second, "units to start", func() bool { return gate.inUse() == 3 })
	stop()
	if got := finished.Load(); got != 3 {
		t.Fatalf("Run returned with %d of 3 units finished; it must wait for in-flight units", got)
	}
	if got := gate.inUse(); got != 0 {
		t.Fatalf("%d slots still held after Run returned", got)
	}
}

// --- hand-off ---

// unitMsg is a JetStream delivery of an execution unit of a run.
func unitMsg(t *testing.T, delivered uint64, headers nats.Header) (*heartbeatMsg, *message.Message) {
	t.Helper()
	m := message.NewMessage()
	m.Workflow = &message.Workflow{WorkflowID: "wf1", RunID: "run1"}
	m.Payload = &message.Payload{ExecutionID: "exec1", NodeID: "node1", WorkflowID: "wf1", RunID: "run1"}
	data, err := m.ToBytes()
	if err != nil {
		t.Fatal(err)
	}
	jsMsg := &heartbeatMsg{data: data, numDelivered: delivered, headers: headers}
	msg, err := message.FromJetStreamMsg(jsMsg)
	if err != nil {
		t.Fatal(err)
	}
	return jsMsg, msg
}

// doubleAckMsg records DoubleAck, which a hand-off uses to ack the original.
type doubleAckMsg struct {
	*heartbeatMsg
	doubleAcks atomic.Int64
}

func (m *doubleAckMsg) DoubleAck(context.Context) error { m.doubleAcks.Add(1); return nil }

type publishedCopy struct {
	subject string
	data    []byte
	header  nats.Header
}

// handOffRunner returns a runner over kv whose re-publishes are captured, failing with pubErr.
func handOffRunner(kv *fakeHeartbeatKV, pubErr error) (*Runner, *[]publishedCopy) {
	r := newHeartbeatRunnerWithKV(kv)
	r.processTimeout = time.Minute
	r.tracer = newFakeRunner(&fakeStallJS{}, okProcessor(), 1, -1, time.Minute).tracer
	var mu sync.Mutex
	copies := []publishedCopy{}
	r.republish = func(_ context.Context, m *nats.Msg) (*jetstream.PubAck, error) {
		if pubErr != nil {
			return nil, pubErr
		}
		mu.Lock()
		defer mu.Unlock()
		copies = append(copies, publishedCopy{subject: m.Subject, data: m.Data, header: m.Header})
		return &jetstream.PubAck{Stream: "TEST", Sequence: 42}, nil
	}
	return r, &copies
}

func wrapForHandOff(t *testing.T, delivered uint64, headers nats.Header) (*doubleAckMsg, *message.Message) {
	t.Helper()
	hb, _ := unitMsg(t, delivered, headers)
	jsMsg := &doubleAckMsg{heartbeatMsg: hb}
	msg, err := message.FromJetStreamMsg(jsMsg)
	if err != nil {
		t.Fatal(err)
	}
	return jsMsg, msg
}

func handOffProcessor(calls *atomic.Int64, allowedSeen *[]bool, mu *sync.Mutex) Processor {
	return funcProcessor(func(ctx context.Context, _ *message.Message) (message.Message, error) {
		calls.Add(1)
		mu.Lock()
		*allowedSeen = append(*allowedSeen, HandOffAllowed(ctx))
		mu.Unlock()
		if HandOffAllowed(ctx) {
			return message.Message{}, ErrHandOff
		}
		<-ctx.Done() // the fallback run waits until the test shuts the runner down
		return message.Message{}, ctx.Err()
	})
}

// A hand-off re-publishes the unit with its attempts carried over, records it as queued at the
// copy's sequence, and acks the original: no delivery attempt is used and nothing is reported.
func TestHandOff_RepublishesQueuesAndAcks(t *testing.T) {
	kv := &fakeHeartbeatKV{}
	r, copies := handOffRunner(kv, nil)
	var calls atomic.Int64
	var mu sync.Mutex
	var allowed []bool
	r.processor = handOffProcessor(&calls, &allowed, &mu)

	jsMsg, msg := wrapForHandOff(t, 2, nats.Header{
		jetstream.MsgIDHeader: []string{"wf1:run1:node1"},
		"X-Custom":            []string{"kept"},
	})
	if err := r.processMessage(context.Background(), msg); err != nil {
		t.Fatalf("processMessage: %v", err)
	}

	if calls.Load() != 1 || !allowed[0] {
		t.Fatalf("Process called %d times with allowed=%v; want once, with hand-off allowed", calls.Load(), allowed)
	}
	if len(*copies) != 1 {
		t.Fatalf("%d copies published, want 1", len(*copies))
	}
	c := (*copies)[0]
	if c.subject != jsMsg.Subject() || string(c.data) != string(jsMsg.Data()) {
		t.Fatalf("copy went to %q with different data; want the original subject and body", c.subject)
	}
	if got := c.header.Get(HeaderHandOffs); got != "1" {
		t.Fatalf("%s = %q, want 1", HeaderHandOffs, got)
	}
	if got := c.header.Get(HeaderAttemptOffset); got != "2" {
		t.Fatalf("%s = %q, want 2 (the attempts the original used)", HeaderAttemptOffset, got)
	}
	if got := c.header.Get(jetstream.MsgIDHeader); got != "wf1:run1:node1:handoff:1" {
		t.Fatalf("Nats-Msg-Id = %q, want wf1:run1:node1:handoff:1", got)
	}
	if got := c.header.Get("X-Custom"); got != "kept" {
		t.Fatalf("custom header = %q, want it carried over", got)
	}
	if jsMsg.doubleAcks.Load() != 1 {
		t.Fatalf("original acked %d times, want once", jsMsg.doubleAcks.Load())
	}
	if n := jsMsg.naks.Load() + int64(len(jsMsg.delayedNaks())) + jsMsg.terms.Load(); n != 0 {
		t.Fatalf("original nak'd or terminated %d times; a hand-off must not use a delivery attempt", n)
	}
	hb, ok := kv.entry(t, executionHeartbeatKey("wf1", "run1", "node1"))
	if !ok || hb.State != heartbeatQueued || hb.QueuedStream != "TEST" || hb.QueuedSeq != 42 || hb.QueuedSubject != jsMsg.Subject() {
		t.Fatalf("heartbeat entry = %+v (found %v); want queued at TEST seq 42", hb, ok)
	}
	if r.localExecution("exec1") != 0 {
		t.Fatal("a handed-off unit must not stay recorded as running here, or its copy is refused if it comes back")
	}
}

// A second hand-off counts up, keeps the base id, and carries the total attempts used.
func TestHandOff_SecondHandOffCountsUp(t *testing.T) {
	kv := &fakeHeartbeatKV{}
	r, copies := handOffRunner(kv, nil)
	var calls atomic.Int64
	var mu sync.Mutex
	var allowed []bool
	r.processor = handOffProcessor(&calls, &allowed, &mu)

	_, msg := wrapForHandOff(t, 1, nats.Header{
		jetstream.MsgIDHeader: []string{"wf1:run1:node1:handoff:1"},
		HeaderHandOffs:        []string{"1"},
		HeaderAttemptOffset:   []string{"2"},
	})
	if got := deliverAttempt(msg); got != 3 {
		t.Fatalf("deliverAttempt = %d, want 3 (one delivery of the copy plus two before)", got)
	}
	if err := r.processMessage(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	c := (*copies)[0]
	if c.header.Get(HeaderHandOffs) != "2" || c.header.Get(HeaderAttemptOffset) != "3" ||
		c.header.Get(jetstream.MsgIDHeader) != "wf1:run1:node1:handoff:2" {
		t.Fatalf("second copy headers = %v", c.header)
	}
}

// When the re-publish fails, the unit runs here: Process is called again with hand-off not
// allowed, the original is not acked, and the claim stays this process's running entry.
func TestHandOff_RepublishFailure_RunsHere(t *testing.T) {
	kv := &fakeHeartbeatKV{}
	r, _ := handOffRunner(kv, errors.New("stream full"))
	var calls atomic.Int64
	var mu sync.Mutex
	var allowed []bool
	r.processor = handOffProcessor(&calls, &allowed, &mu)

	jsMsg, msg := wrapForHandOff(t, 1, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = r.processMessage(ctx, msg)
	}()
	eventually(t, time.Second, "the fallback run", func() bool { return calls.Load() == 2 })
	mu.Lock()
	if !allowed[0] || allowed[1] {
		t.Fatalf("hand-off allowed on calls = %v, want [true false]", allowed)
	}
	mu.Unlock()
	hb, _ := kv.entry(t, executionHeartbeatKey("wf1", "run1", "node1"))
	if hb.State != heartbeatRunning {
		t.Fatalf("entry state = %q during the fallback run, want running", hb.State)
	}
	cancel()
	<-done
	if jsMsg.doubleAcks.Load() != 0 {
		t.Fatal("the original was acked although the hand-off failed")
	}
}

// ErrHandOff from a processor that was not allowed to hand off is an ordinary failure.
func TestHandOff_NotAllowedWithoutRunIDs(t *testing.T) {
	r, copies := handOffRunner(&fakeHeartbeatKV{}, nil)
	msg := buildMessageFor(t, &heartbeatMsg{})
	if r.canHandOff(msg, "", "", "") {
		t.Fatal("a message that is not an execution unit cannot be handed off")
	}
	if len(*copies) != 0 {
		t.Fatal("unexpected publish")
	}
}

// The queued claim is made only over this process's own running claim, and the copy's sequence is
// added only while the claim is still that queued entry: neither overwrites a claim another delivery
// has made.
func TestHandOff_QueuedWritesKeepANewerClaim(t *testing.T) {
	kv := &fakeHeartbeatKV{}
	r := newHeartbeatRunnerWithKV(kv)
	key := executionHeartbeatKey("wf1", "run1", "node1")
	_, msg := unitMsg(t, 1, nil)

	kv.seed(t, key, executionHeartbeat{ExecutionID: "exec1", State: heartbeatRunning, Owner: "another-process"}, time.Now())
	if rev := r.markQueued(msg, "wf1", "run1", "node1", "exec1"); rev != 0 {
		t.Fatal("marked another process's claim queued")
	}

	kv.seed(t, key, executionHeartbeat{ExecutionID: "exec1", State: heartbeatRunning, Owner: processToken}, time.Now())
	rev := r.markQueued(msg, "wf1", "run1", "node1", "exec1")
	if hb, _ := kv.entry(t, key); rev == 0 || hb.State != heartbeatQueued || hb.QueuedSeq != 0 {
		t.Fatalf("rev %d, entry %+v; want this process's claim queued, no sequence yet", rev, hb)
	}
	// The copy is claimed (here by another process) before the sequence is written.
	kv.seed(t, key, executionHeartbeat{ExecutionID: "exec1", State: heartbeatRunning, Owner: "another-process"}, time.Now())
	r.setQueuedSeq(msg, "wf1", "run1", "node1", "exec1", rev, "TEST", "TEST.subject", 42)
	if hb, _ := kv.entry(t, key); hb.State != heartbeatRunning || hb.Owner != "another-process" {
		t.Fatalf("entry = %+v; the copy's claim must stand", hb)
	}
}

// The copy can be delivered to this same process while the hand-off is still finishing. It must
// find the unit forgotten here and its claim queued, so it runs at once: it used to be refused as
// "still being handled by this process" and nak'd for 30 s, using an attempt (found by the e2e
// hand-off test).
func TestHandOff_CopyDeliveredHereDuringTheHandOffRuns(t *testing.T) {
	kv := &fakeHeartbeatKV{}
	r, _ := handOffRunner(kv, nil)
	var calls atomic.Int64
	var mu sync.Mutex
	var allowed []bool
	r.processor = handOffProcessor(&calls, &allowed, &mu)
	key := executionHeartbeatKey("wf1", "run1", "node1")

	var localAtPublish int
	var claimAtPublish claimOutcome = -1
	r.republish = func(_ context.Context, m *nats.Msg) (*jetstream.PubAck, error) {
		// The copy arrives here immediately: what would its processMessage see?
		localAtPublish = r.localExecution("exec1")
		claimAtPublish, _ = claimExecutionUnit(context.Background(), kv, key,
			executionHeartbeat{ExecutionID: "exec1", State: heartbeatRunning, Owner: processToken}, time.Now())
		return &jetstream.PubAck{Stream: "TEST", Sequence: 42}, nil
	}
	_, msg := wrapForHandOff(t, 1, nil)
	if err := r.processMessage(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if localAtPublish != 0 {
		t.Fatal("the copy would be refused: this process still recorded the unit as running")
	}
	if claimAtPublish != claimWon {
		t.Fatalf("the copy's claim = %v, want claimWon", claimAtPublish)
	}
	if hb, _ := kv.entry(t, key); hb.State != heartbeatRunning {
		t.Fatalf("entry = %+v after the hand-off; the copy's running claim must stand", hb)
	}
}

// A delivery of a handed-off copy takes the queued claim over.
func TestClaim_QueuedIsClaimable(t *testing.T) {
	kv := &fakeHeartbeatKV{}
	key := executionHeartbeatKey("wf1", "run1", "node1")
	kv.seed(t, key, executionHeartbeat{ExecutionID: "exec1", State: heartbeatQueued, Owner: "another-process", QueuedSeq: 42}, time.Now())
	outcome, err := claimExecutionUnit(context.Background(), kv, key,
		executionHeartbeat{ExecutionID: "exec1", State: heartbeatRunning, Owner: processToken}, time.Now())
	if err != nil || outcome != claimWon {
		t.Fatalf("claim = %v, %v; want claimWon", outcome, err)
	}
}

// The copy's claim races the hand-off adding the copy's sequence to the queued entry. Losing that
// race is not "busy": the claim reads the entry again, finds it still queued, and takes it.
func TestClaim_QueuedEntryRewrittenUnderTheClaimIsStillTaken(t *testing.T) {
	kv := &fakeHeartbeatKV{}
	key := executionHeartbeatKey("wf1", "run1", "node1")
	kv.seed(t, key, executionHeartbeat{ExecutionID: "exec1", State: heartbeatQueued, Owner: processToken}, time.Now())
	kv.afterGet = func() {
		kv.seed(t, key, executionHeartbeat{ExecutionID: "exec1", State: heartbeatQueued, Owner: processToken, QueuedSeq: 42}, time.Now())
	}
	outcome, err := claimExecutionUnit(context.Background(), kv, key,
		executionHeartbeat{ExecutionID: "exec1", State: heartbeatRunning, Owner: "copy-process"}, time.Now())
	if err != nil || outcome != claimWon {
		t.Fatalf("claim = %v, %v; want claimWon after reading the entry again", outcome, err)
	}
	// A rival claim that wins the race is still busy.
	kv.seed(t, key, executionHeartbeat{ExecutionID: "exec1", State: heartbeatQueued, Owner: processToken}, time.Now())
	kv.afterGet = func() {
		kv.seed(t, key, executionHeartbeat{ExecutionID: "exec1", State: heartbeatRunning, Owner: "rival"}, time.Now())
	}
	outcome, _ = claimExecutionUnit(context.Background(), kv, key,
		executionHeartbeat{ExecutionID: "exec1", State: heartbeatRunning, Owner: "copy-process"}, time.Now())
	if outcome != claimBusy {
		t.Fatalf("claim = %v against a rival that claimed first, want claimBusy", outcome)
	}
}

// Attempts carried over by a hand-off count towards the limit: a copy on its first delivery after
// four earlier attempts is the last attempt, so a transient failure is reported, not retried.
func TestWillRetry_CountsCarriedAttempts(t *testing.T) {
	r := newHeartbeatRunner()
	r.maxDeliver.Store(5)
	_, msg := unitMsg(t, 1, nats.Header{HeaderAttemptOffset: []string{"4"}})
	attempt := deliverAttempt(msg)
	if attempt != 5 {
		t.Fatalf("deliverAttempt = %d, want 5", attempt)
	}
	transient := fmt.Errorf("call dependency: %w", syscall.ECONNRESET)
	if r.willRetry(msg, transient, attempt) {
		t.Fatal("a copy on the unit's fifth attempt must not be retried")
	}
	_, fresh := unitMsg(t, 1, nil)
	if !r.willRetry(fresh, transient, deliverAttempt(fresh)) {
		t.Fatal("a first attempt with no carried attempts must be retried")
	}
}

func TestHandOffMsgID(t *testing.T) {
	cases := []struct {
		header nats.Header
		n      int
		want   string
	}{
		{nats.Header{jetstream.MsgIDHeader: []string{"a:b:c"}}, 1, "a:b:c:handoff:1"},
		{nats.Header{jetstream.MsgIDHeader: []string{"a:b:c:handoff:3"}}, 4, "a:b:c:handoff:4"},
		{nil, 1, "exec1:handoff:1"},
	}
	for _, tc := range cases {
		jsMsg := &heartbeatMsg{headers: tc.header}
		if got := handOffMsgID(jsMsg, "exec1", tc.n); got != tc.want {
			t.Errorf("handOffMsgID(%v, %d) = %q, want %q", tc.header, tc.n, got, tc.want)
		}
	}
}

// --- consumer observation ---

type consumerObs struct {
	UnitObserver
	mu      sync.Mutex
	pending []uint64
}

func (o *consumerObs) ConsumerObserved(_ string, numPending uint64, _ int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.pending = append(o.pending, numPending)
}

type infoConsumer struct {
	queueConsumer
	pending uint64
}

func (c *infoConsumer) CachedInfo() *jetstream.ConsumerInfo {
	return &jetstream.ConsumerInfo{NumPending: c.pending}
}

func TestObserveConsumer_ReportsBacklog(t *testing.T) {
	original := consumerObserveInterval
	consumerObserveInterval = 10 * time.Millisecond
	t.Cleanup(func() { consumerObserveInterval = original })

	cons := &infoConsumer{pending: 17}
	obs := &consumerObs{}
	js := &fakeStallJS{consumerFn: func(int) (jetstream.Consumer, error) { return cons, nil }}
	r := newFakeRunner(js, okProcessor(), 1, -1, time.Minute)
	r.unitObserver = obs
	startRun(t, r)
	eventually(t, time.Second, "the backlog to be observed", func() bool {
		obs.mu.Lock()
		defer obs.mu.Unlock()
		return len(obs.pending) >= 2 && obs.pending[0] == 17
	})
}

// slowPutKV completes each Put after a delay, recording when it landed.
type slowPutKV struct {
	*fakeHeartbeatKV
	delay  time.Duration
	mu     sync.Mutex
	landed []time.Time
}

func (kv *slowPutKV) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	time.Sleep(kv.delay) // the write is on the wire; cancelling ctx cannot recall it
	rev, err := kv.fakeHeartbeatKV.Put(ctx, key, value)
	kv.mu.Lock()
	kv.landed = append(kv.landed, time.Now())
	kv.mu.Unlock()
	return rev, err
}

// Once stopKV returns, no running refresh can land: a hand-off writes queued right after, and a
// refresh landing later turned it back into running.
func TestStartAckHeartbeat_NoRefreshLandsAfterStopKV(t *testing.T) {
	withShortHeartbeat(t, time.Millisecond)
	kv := &slowPutKV{fakeHeartbeatKV: &fakeHeartbeatKV{}, delay: 30 * time.Millisecond}
	msg := buildMessageFor(t, &heartbeatMsg{})
	r := newHeartbeatRunnerWithKV(kv)
	for i := 0; i < 20; i++ {
		stopKV, stop := r.startAckHeartbeat(context.Background(), msg, "wf1", "run1", "node1")
		time.Sleep(time.Duration(5+i) * time.Millisecond) // stop at varying points, often mid-write
		stopKV()
		stoppedAt := time.Now()
		time.Sleep(50 * time.Millisecond)
		kv.mu.Lock()
		for _, at := range kv.landed {
			if at.After(stoppedAt) {
				kv.mu.Unlock()
				t.Fatalf("iteration %d: a refresh landed %s after stopKV returned", i, at.Sub(stoppedAt))
			}
		}
		kv.landed = nil
		kv.mu.Unlock()
		stop()
	}
}
