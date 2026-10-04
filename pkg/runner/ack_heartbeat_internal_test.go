package runner

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"

	sdkerrors "github.com/wehubfusion/Icarus/pkg/errors"
	"github.com/wehubfusion/Icarus/pkg/message"
)

// heartbeatMsg is a jetstream.Msg that only implements what startAckHeartbeat touches.
// Unimplemented methods panic via the embedded nil interface, which is the point: if the
// heartbeat ever calls something else, the test fails loudly rather than silently passing.
type heartbeatMsg struct {
	jetstream.Msg

	data         []byte
	inProgress   atomic.Int64
	naks         atomic.Int64
	terms        atomic.Int64
	numDelivered uint64 // 0 means 1

	mu        sync.Mutex
	nakDelays []time.Duration
	err       error
}

func (m *heartbeatMsg) Data() []byte    { return m.data }
func (m *heartbeatMsg) Subject() string { return "TEST.subject" }

func (m *heartbeatMsg) Metadata() (*jetstream.MsgMetadata, error) {
	delivered := m.numDelivered
	if delivered == 0 {
		delivered = 1
	}
	return &jetstream.MsgMetadata{
		NumDelivered: delivered,
		Sequence:     jetstream.SequencePair{Stream: 1, Consumer: 1},
	}, nil
}

func (m *heartbeatMsg) NakWithDelay(d time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nakDelays = append(m.nakDelays, d)
	return nil
}

func (m *heartbeatMsg) Term() error {
	m.terms.Add(1)
	return nil
}

func (m *heartbeatMsg) delayedNaks() []time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]time.Duration(nil), m.nakDelays...)
}

func (m *heartbeatMsg) InProgress() error {
	m.inProgress.Add(1)
	return m.err
}

func (m *heartbeatMsg) Nak() error {
	m.naks.Add(1)
	return nil
}

// buildMessageFor wraps a jetstream.Msg in an SDK Message so the ack handle is attached,
// which is what startAckHeartbeat keys off.
func buildMessageFor(t *testing.T, jsMsg *heartbeatMsg) *message.Message {
	t.Helper()
	data, err := message.NewMessage().ToBytes()
	if err != nil {
		t.Fatalf("serialising message: %v", err)
	}
	jsMsg.data = data
	msg, err := message.FromJetStreamMsg(jsMsg)
	if err != nil {
		t.Fatalf("wrapping jetstream message: %v", err)
	}
	return msg
}

func withShortHeartbeat(t *testing.T, d time.Duration) {
	t.Helper()
	original := ackHeartbeatInterval
	ackHeartbeatInterval = d
	t.Cleanup(func() { ackHeartbeatInterval = original })
}

func newHeartbeatRunner() *Runner {
	return &Runner{logger: zap.NewNop(), stream: "TEST", consumer: "test-consumer"}
}

// fakeHeartbeatKV is an in-memory jetstream.KeyValue implementing Get, Create, Update and Put
// with revisions and entry times — everything this package's heartbeat/claim code calls.
// Unimplemented methods panic via the embedded nil interface, same rationale as heartbeatMsg.
type fakeHeartbeatKV struct {
	jetstream.KeyValue

	mu        sync.Mutex
	entries   map[string]*fakeKVEntry
	rev       uint64
	puts      int
	getErr    error // returned by every Get call when set
	createErr error // returned by every Create call when set
	updateErr error // returned by every Update call when set
	putErr    error // returned by every Put call when set
}

type fakeKVEntry struct {
	jetstream.KeyValueEntry
	key     string
	value   []byte
	rev     uint64
	created time.Time
}

func (e *fakeKVEntry) Key() string        { return e.key }
func (e *fakeKVEntry) Value() []byte      { return e.value }
func (e *fakeKVEntry) Revision() uint64   { return e.rev }
func (e *fakeKVEntry) Created() time.Time { return e.created }

func (kv *fakeHeartbeatKV) store(key string, value []byte, created time.Time) uint64 {
	if kv.entries == nil {
		kv.entries = map[string]*fakeKVEntry{}
	}
	kv.rev++
	kv.entries[key] = &fakeKVEntry{key: key, value: value, rev: kv.rev, created: created}
	return kv.rev
}

// seed stores hb under key as if written at created.
func (kv *fakeHeartbeatKV) seed(t *testing.T, key string, hb executionHeartbeat, created time.Time) {
	t.Helper()
	b, err := json.Marshal(hb)
	if err != nil {
		t.Fatal(err)
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.store(key, b, created)
}

// entry returns the heartbeat stored under key, or false.
func (kv *fakeHeartbeatKV) entry(t *testing.T, key string) (executionHeartbeat, bool) {
	t.Helper()
	kv.mu.Lock()
	defer kv.mu.Unlock()
	e, ok := kv.entries[key]
	if !ok {
		return executionHeartbeat{}, false
	}
	var hb executionHeartbeat
	if err := json.Unmarshal(e.value, &hb); err != nil {
		t.Fatal(err)
	}
	return hb, true
}

func (kv *fakeHeartbeatKV) Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.getErr != nil {
		return nil, kv.getErr
	}
	e, ok := kv.entries[key]
	if !ok {
		return nil, jetstream.ErrKeyNotFound
	}
	return e, nil
}

func (kv *fakeHeartbeatKV) Create(ctx context.Context, key string, value []byte, opts ...jetstream.KVCreateOpt) (uint64, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.createErr != nil {
		return 0, kv.createErr
	}
	if _, ok := kv.entries[key]; ok {
		return 0, jetstream.ErrKeyExists
	}
	return kv.store(key, value, time.Now()), nil
}

func (kv *fakeHeartbeatKV) Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.updateErr != nil {
		return 0, kv.updateErr
	}
	if e, ok := kv.entries[key]; !ok || e.rev != revision {
		return 0, jetstream.ErrKeyExists
	}
	return kv.store(key, value, time.Now()), nil
}

func (kv *fakeHeartbeatKV) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.puts++
	if kv.putErr != nil {
		return 0, kv.putErr
	}
	return kv.store(key, value, time.Now()), nil
}

func (kv *fakeHeartbeatKV) putCount() int {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.puts
}

func newHeartbeatRunnerWithKV(kv jetstream.KeyValue) *Runner {
	r := newHeartbeatRunner()
	r.heartbeatKV = kv
	return r
}

// TestStartAckHeartbeat_ExtendsUntilStopped is the guard for the redelivery-while-running bug.
// Durables are created without an explicit AckWait, so the 30s server default applies while
// handlers run for minutes. The heartbeat must keep firing for as long as Process does.
func TestStartAckHeartbeat_ExtendsUntilStopped(t *testing.T) {
	withShortHeartbeat(t, 5*time.Millisecond)

	jsMsg := &heartbeatMsg{}
	msg := buildMessageFor(t, jsMsg)

	stop := newHeartbeatRunner().startAckHeartbeat(context.Background(), msg, "wf1", "run1", "node1")
	time.Sleep(60 * time.Millisecond)
	stop()

	got := jsMsg.inProgress.Load()
	if got < 2 {
		t.Fatalf("expected the ack deadline to be extended repeatedly while processing, got %d extensions", got)
	}

	// After stop() returns the goroutine has exited, so the count must not move again.
	time.Sleep(30 * time.Millisecond)
	if after := jsMsg.inProgress.Load(); after != got {
		t.Fatalf("heartbeat kept running after stop(): %d then %d", got, after)
	}
}

// TestStartAckHeartbeat_StopsOnContextCancellation covers the shutdown path: when the process
// context is cancelled the goroutine must exit rather than extend a deadline for work that is
// being abandoned.
func TestStartAckHeartbeat_StopsOnContextCancellation(t *testing.T) {
	withShortHeartbeat(t, 5*time.Millisecond)

	jsMsg := &heartbeatMsg{}
	msg := buildMessageFor(t, jsMsg)

	ctx, cancel := context.WithCancel(context.Background())
	stop := newHeartbeatRunner().startAckHeartbeat(ctx, msg, "wf1", "run1", "node1")
	cancel()

	// stop() blocks until the goroutine exits, so this returning at all proves cancellation
	// was observed.
	done := make(chan struct{})
	go func() { stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("heartbeat goroutine did not exit after context cancellation")
	}
}

// TestStartAckHeartbeat_SurvivesExtensionFailure asserts a failed extension is logged and
// retried rather than terminating the heartbeat. Giving up on the first error would silently
// reintroduce the redelivery bug for the rest of a long-running handler.
func TestStartAckHeartbeat_SurvivesExtensionFailure(t *testing.T) {
	withShortHeartbeat(t, 5*time.Millisecond)

	jsMsg := &heartbeatMsg{err: context.DeadlineExceeded}
	msg := buildMessageFor(t, jsMsg)

	stop := newHeartbeatRunner().startAckHeartbeat(context.Background(), msg, "wf1", "run1", "node1")
	time.Sleep(60 * time.Millisecond)
	stop()

	if got := jsMsg.inProgress.Load(); got < 2 {
		t.Fatalf("expected the heartbeat to keep retrying after a failed extension, got %d attempts", got)
	}
}

// TestStartAckHeartbeat_NoJetStreamMessageIsNoOp covers Core NATS and synthesised messages,
// which have no ack handle. stop() must still be safe to call.
func TestStartAckHeartbeat_NoJetStreamMessageIsNoOp(t *testing.T) {
	withShortHeartbeat(t, 5*time.Millisecond)

	stop := newHeartbeatRunner().startAckHeartbeat(context.Background(), message.NewMessage(), "wf1", "run1", "node1")
	time.Sleep(20 * time.Millisecond)
	stop()
	stop() // idempotent
}

// TestStartAckHeartbeat_StopIsIdempotent guards the double-close panic.
func TestStartAckHeartbeat_StopIsIdempotent(t *testing.T) {
	withShortHeartbeat(t, 5*time.Millisecond)

	jsMsg := &heartbeatMsg{}
	msg := buildMessageFor(t, jsMsg)

	stop := newHeartbeatRunner().startAckHeartbeat(context.Background(), msg, "wf1", "run1", "node1")

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); stop() }()
	}
	wg.Wait()
}

// TestStartAckHeartbeat_WritesExecutionHeartbeat asserts the KV write fires on the 3rd
// ack-extension tick, not every tick, per executionHeartbeatEveryNTicks.
func TestStartAckHeartbeat_WritesExecutionHeartbeat(t *testing.T) {
	withShortHeartbeat(t, 5*time.Millisecond)

	jsMsg := &heartbeatMsg{}
	msg := buildMessageFor(t, jsMsg)
	kv := &fakeHeartbeatKV{}

	stop := newHeartbeatRunnerWithKV(kv).startAckHeartbeat(context.Background(), msg, "wf1", "run1", "node1")
	time.Sleep(60 * time.Millisecond) // ~12 ticks at 5ms, so ~4 KV writes expected
	stop()

	extensions := jsMsg.inProgress.Load()
	puts := kv.putCount()
	if puts == 0 {
		t.Fatalf("expected at least one KV heartbeat write, got none (extensions=%d)", extensions)
	}
	// Every write is on a tick that also called msg.InProgress(); the ratio must be roughly
	// executionHeartbeatEveryNTicks (extensions per put), not 1 (every tick).
	if extensions > 0 && int(extensions)/puts < executionHeartbeatEveryNTicks-1 {
		t.Fatalf("KV write fired too often: %d extensions produced %d puts, expected roughly 1 per %d",
			extensions, puts, executionHeartbeatEveryNTicks)
	}
}

// TestStartAckHeartbeat_SurvivesKVWriteFailure mirrors
// TestStartAckHeartbeat_SurvivesExtensionFailure: a failing KV write must be logged and retried,
// not stop the heartbeat goroutine (which would also stop the ack-extension half of the tick).
func TestStartAckHeartbeat_SurvivesKVWriteFailure(t *testing.T) {
	withShortHeartbeat(t, 5*time.Millisecond)

	jsMsg := &heartbeatMsg{}
	msg := buildMessageFor(t, jsMsg)
	kv := &fakeHeartbeatKV{putErr: context.DeadlineExceeded}

	stop := newHeartbeatRunnerWithKV(kv).startAckHeartbeat(context.Background(), msg, "wf1", "run1", "node1")
	time.Sleep(60 * time.Millisecond)
	stop()

	if got := jsMsg.inProgress.Load(); got < 2 {
		t.Fatalf("expected ack-extension to keep running despite KV write failures, got %d extensions", got)
	}
	if kv.putCount() == 0 {
		t.Fatal("expected the heartbeat to keep attempting KV writes despite failures")
	}
}

const claimKey = "wf1.run1.node1"

func claimWith(t *testing.T, kv *fakeHeartbeatKV, jsMsg *heartbeatMsg, executionID string) bool {
	t.Helper()
	msg := buildMessageFor(t, jsMsg)
	return newHeartbeatRunnerWithKV(kv).claimOrNak(context.Background(), msg, "wf1", "run1", "node1", executionID)
}

// A unit another pod is running, and heartbeating, is not run twice: the delivery is nak'd with
// claimBusyNakDelay. A plain Nak would come straight back and use up every delivery attempt in
// milliseconds, which is what happened before v0.28.0.
func TestClaimOrNak_RunningElsewhere_NaksWithDelay(t *testing.T) {
	kv := &fakeHeartbeatKV{}
	kv.seed(t, claimKey, executionHeartbeat{ExecutionID: "exec1", State: heartbeatRunning}, time.Now())
	jsMsg := &heartbeatMsg{}

	if claimWith(t, kv, jsMsg, "exec1") {
		t.Fatal("a unit running elsewhere must not be processed again")
	}
	if got := jsMsg.naks.Load(); got != 0 {
		t.Fatalf("plain Nak() calls = %d, want 0", got)
	}
	if got := jsMsg.delayedNaks(); len(got) != 1 || got[0] != claimBusyNakDelay {
		t.Fatalf("NakWithDelay calls = %v, want [%v]", got, claimBusyNakDelay)
	}
}

// Entries written before v0.28.0 have no state; they are running.
func TestClaimOrNak_EntryWithoutState_CountsAsRunning(t *testing.T) {
	kv := &fakeHeartbeatKV{}
	kv.seed(t, claimKey, executionHeartbeat{ExecutionID: "exec1"}, time.Now())

	if claimWith(t, kv, &heartbeatMsg{}, "exec1") {
		t.Fatal("a fresh entry with no state must be treated as running")
	}
}

// The redelivery after a transient failure takes the claim over from the retrying entry.
func TestClaimOrNak_Retrying_TakesOver(t *testing.T) {
	kv := &fakeHeartbeatKV{}
	kv.seed(t, claimKey, executionHeartbeat{ExecutionID: "exec1", State: heartbeatRetrying}, time.Now())

	if !claimWith(t, kv, &heartbeatMsg{numDelivered: 2}, "exec1") {
		t.Fatal("the retry delivery must take the claim over")
	}
	hb, _ := kv.entry(t, claimKey)
	if hb.State != heartbeatRunning || hb.Attempt != 2 {
		t.Fatalf("entry after takeover = %+v, want running, attempt 2", hb)
	}
}

// A running entry with no write for claimStaleAfter belongs to a dead pod; its redelivery takes
// over instead of waiting out the TTL.
func TestClaimOrNak_StaleRunning_TakesOver(t *testing.T) {
	kv := &fakeHeartbeatKV{}
	kv.seed(t, claimKey, executionHeartbeat{ExecutionID: "exec1", State: heartbeatRunning}, time.Now().Add(-claimStaleAfter-time.Second))

	if !claimWith(t, kv, &heartbeatMsg{numDelivered: 2}, "exec1") {
		t.Fatal("a stale running entry must be taken over")
	}
}

// A delivery of an execution whose result is already published is a duplicate and is dropped.
func TestClaimOrNak_DoneSameExecution_Terminates(t *testing.T) {
	kv := &fakeHeartbeatKV{}
	kv.seed(t, claimKey, executionHeartbeat{ExecutionID: "exec1", State: heartbeatDone}, time.Now())
	jsMsg := &heartbeatMsg{}

	if claimWith(t, kv, jsMsg, "exec1") {
		t.Fatal("a duplicate of a completed execution must not run")
	}
	if got := jsMsg.terms.Load(); got != 1 {
		t.Fatalf("Term() calls = %d, want 1", got)
	}
}

// Zeus dispatching the node again (a new execution ID) after the last one finished runs it.
func TestClaimOrNak_DoneOtherExecution_TakesOver(t *testing.T) {
	kv := &fakeHeartbeatKV{}
	kv.seed(t, claimKey, executionHeartbeat{ExecutionID: "exec1", State: heartbeatDone}, time.Now())

	if !claimWith(t, kv, &heartbeatMsg{}, "exec2") {
		t.Fatal("a new execution of a completed node must run")
	}
}

// Two deliveries racing to take over the same entry: the one whose Update loses the revision
// check backs off.
func TestClaimOrNak_LosesTakeoverRace_NaksWithDelay(t *testing.T) {
	kv := &fakeHeartbeatKV{updateErr: jetstream.ErrKeyExists}
	kv.seed(t, claimKey, executionHeartbeat{ExecutionID: "exec1", State: heartbeatRetrying}, time.Now())
	jsMsg := &heartbeatMsg{}

	if claimWith(t, kv, jsMsg, "exec1") {
		t.Fatal("the delivery that lost the takeover race must not run")
	}
	if got := jsMsg.delayedNaks(); len(got) != 1 {
		t.Fatalf("NakWithDelay calls = %v, want one", got)
	}
}

// TestClaimOrNak_KVUnreachable_ProcessesAnyway asserts that a KV error other than a lost claim
// (bucket unreachable, etc.) fails OPEN: processing proceeds rather than being blocked by an
// unrelated KV outage.
func TestClaimOrNak_KVUnreachable_ProcessesAnyway(t *testing.T) {
	kv := &fakeHeartbeatKV{getErr: context.DeadlineExceeded}

	if !claimWith(t, kv, &heartbeatMsg{}, "exec1") {
		t.Fatal("expected claimOrNak to fail open (proceed=true) when the KV is unreachable")
	}
}

// TestClaimOrNak_WinsClaim_Processes is the baseline: a fresh key claims cleanly and processing
// proceeds.
func TestClaimOrNak_WinsClaim_Processes(t *testing.T) {
	kv := &fakeHeartbeatKV{}

	if !claimWith(t, kv, &heartbeatMsg{}, "exec1") {
		t.Fatal("expected claimOrNak to report true (proceed) when the claim is won")
	}
	if hb, ok := kv.entry(t, claimKey); !ok || hb.State != heartbeatRunning {
		t.Fatalf("entry after claim = %+v (present %v), want running", hb, ok)
	}
}

// A transient failure is retried while attempts remain and reported on the last one; a
// permanent failure, or one that cannot be redelivered, is reported at once.
func TestWillRetry(t *testing.T) {
	r := newHeartbeatRunner()
	r.maxDeliver.Store(5)
	transient := errors.New("connection reset")

	for attempt := 1; attempt <= 5; attempt++ {
		msg := buildMessageFor(t, &heartbeatMsg{numDelivered: uint64(attempt)})
		want := attempt < 5
		if got := r.willRetry(msg, transient, attempt); got != want {
			t.Errorf("attempt %d: willRetry = %v, want %v", attempt, got, want)
		}
	}

	msg := buildMessageFor(t, &heartbeatMsg{})
	if r.willRetry(msg, sdkerrors.NewBadRequestError("bad input", "BAD_INPUT", nil), 1) {
		t.Error("a permanent error must not be retried")
	}
	if r.willRetry(&message.Message{}, transient, 1) {
		t.Error("a message with no JetStream handle cannot be redelivered, so must not be retried")
	}

	unset := newHeartbeatRunner()
	if got := unset.attemptLimit(); got != defaultMaxDeliver {
		t.Errorf("attemptLimit with no MaxDeliver = %d, want %d", got, defaultMaxDeliver)
	}
}

func TestRetryDelay_StaysBelowHeartbeatTTL(t *testing.T) {
	want := []time.Duration{5 * time.Second, 15 * time.Second, 30 * time.Second, 60 * time.Second, 60 * time.Second}
	for i, w := range want {
		got := retryDelay(i + 1)
		if got != w {
			t.Errorf("retryDelay(%d) = %v, want %v", i+1, got, w)
		}
		if got >= executionHeartbeatTTL {
			t.Errorf("retryDelay(%d) = %v must stay below the heartbeat TTL %v, or the retrying entry expires first", i+1, got, executionHeartbeatTTL)
		}
	}
}

// TestClaimOrNak_NoHeartbeatKV_ProcessesAnyway guards the nil-bucket degradation: if the
// EXECUTION_HEARTBEATS bucket could not be reached at startup, claimOrNak must not block
// processing.
func TestClaimOrNak_NoHeartbeatKV_ProcessesAnyway(t *testing.T) {
	jsMsg := &heartbeatMsg{}
	msg := buildMessageFor(t, jsMsg)

	proceed := newHeartbeatRunner().claimOrNak(context.Background(), msg, "wf1", "run1", "node1", "exec1")

	if !proceed {
		t.Fatal("expected claimOrNak to proceed when heartbeatKV is nil")
	}
}
