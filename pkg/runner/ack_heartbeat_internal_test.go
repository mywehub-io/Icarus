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
