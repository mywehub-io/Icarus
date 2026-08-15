package runner

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"

	"github.com/wehubfusion/Icarus/pkg/message"
)

// heartbeatMsg is a jetstream.Msg that only implements what startAckHeartbeat touches.
// Unimplemented methods panic via the embedded nil interface, which is the point: if the
// heartbeat ever calls something else, the test fails loudly rather than silently passing.
type heartbeatMsg struct {
	jetstream.Msg

	data       []byte
	inProgress atomic.Int64
	naks       atomic.Int64
	err        error
}

func (m *heartbeatMsg) Data() []byte    { return m.data }
func (m *heartbeatMsg) Subject() string { return "TEST.subject" }

func (m *heartbeatMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{
		NumDelivered: 1,
		Sequence:     jetstream.SequencePair{Stream: 1, Consumer: 1},
	}, nil
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

// fakeHeartbeatKV is a jetstream.KeyValue that only implements Create and Put — everything this
// package's heartbeat/claim code calls. Unimplemented methods panic via the embedded nil
// interface, same rationale as heartbeatMsg above.
type fakeHeartbeatKV struct {
	jetstream.KeyValue

	mu        sync.Mutex
	puts      int
	createErr error // returned by every Create call when set
	putErr    error // returned by every Put call when set
}

func (kv *fakeHeartbeatKV) Create(ctx context.Context, key string, value []byte, opts ...jetstream.KVCreateOpt) (uint64, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.createErr != nil {
		return 0, kv.createErr
	}
	return 1, nil
}

func (kv *fakeHeartbeatKV) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.puts++
	if kv.putErr != nil {
		return 0, kv.putErr
	}
	return uint64(kv.puts + 1), nil
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

// TestClaimOrNak_LosesClaimRace_NaksWithoutProcessing asserts that when another pod already
// holds the execution unit's heartbeat key (Create returns ErrKeyExists), claimOrNak naks the
// message and reports that processing must not proceed.
func TestClaimOrNak_LosesClaimRace_NaksWithoutProcessing(t *testing.T) {
	jsMsg := &heartbeatMsg{}
	msg := buildMessageFor(t, jsMsg)
	kv := &fakeHeartbeatKV{createErr: jetstream.ErrKeyExists}

	proceed := newHeartbeatRunnerWithKV(kv).claimOrNak(context.Background(), msg, "wf1", "run1", "node1", "exec1")

	if proceed {
		t.Fatal("expected claimOrNak to report false (do not process) when the claim is already held")
	}
	if got := jsMsg.naks.Load(); got != 1 {
		t.Fatalf("expected exactly one Nak() call, got %d", got)
	}
}

// TestClaimOrNak_KVUnreachable_ProcessesAnyway asserts that a KV error other than ErrKeyExists
// (bucket unreachable, etc.) fails OPEN: processing proceeds rather than being blocked by an
// unrelated KV outage.
func TestClaimOrNak_KVUnreachable_ProcessesAnyway(t *testing.T) {
	jsMsg := &heartbeatMsg{}
	msg := buildMessageFor(t, jsMsg)
	kv := &fakeHeartbeatKV{createErr: context.DeadlineExceeded}

	proceed := newHeartbeatRunnerWithKV(kv).claimOrNak(context.Background(), msg, "wf1", "run1", "node1", "exec1")

	if !proceed {
		t.Fatal("expected claimOrNak to fail open (proceed=true) when the KV is unreachable, not ErrKeyExists")
	}
}

// TestClaimOrNak_WinsClaim_Processes is the baseline: a fresh key claims cleanly and processing
// proceeds.
func TestClaimOrNak_WinsClaim_Processes(t *testing.T) {
	jsMsg := &heartbeatMsg{}
	msg := buildMessageFor(t, jsMsg)
	kv := &fakeHeartbeatKV{}

	proceed := newHeartbeatRunnerWithKV(kv).claimOrNak(context.Background(), msg, "wf1", "run1", "node1", "exec1")

	if !proceed {
		t.Fatal("expected claimOrNak to report true (proceed) when the claim is won")
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
