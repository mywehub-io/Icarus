package runner

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/wehubfusion/Icarus/pkg/message"
)

// liveJetStream connects to the local estate's NATS (or NATS_URL), skipping when there is none.
func liveJetStream(t *testing.T) jetstream.JetStream {
	t.Helper()
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = "nats://localhost:14222"
	}
	nc, err := nats.Connect(url, nats.Timeout(time.Second))
	if err != nil {
		t.Skipf("no NATS at %s: %v", url, err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	return js
}

// liveUnitStream creates a throwaway stream and durable, publishes one execution unit with Zeus's
// kind of message id, fetches it, and returns the stream, the consumer and the delivery.
func liveUnitStream(t *testing.T, js jetstream.JetStream, cfg jetstream.StreamConfig) (jetstream.Stream, jetstream.Consumer, jetstream.Msg) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	name := fmt.Sprintf("HANDOFF_%d", time.Now().UnixNano())
	cfg.Name = name
	cfg.Subjects = []string{name + ".>"}
	stream, err := js.CreateStream(ctx, cfg)
	if err != nil {
		t.Skipf("JetStream is not available: %v", err)
	}
	t.Cleanup(func() { _ = js.DeleteStream(context.Background(), name) })
	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable: "c", AckPolicy: jetstream.AckExplicitPolicy, MaxDeliver: 2, AckWait: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	m := message.NewMessage()
	m.Workflow = &message.Workflow{WorkflowID: "wf1", RunID: "run1"}
	m.Payload = &message.Payload{ExecutionID: "exec1", NodeID: "node1", WorkflowID: "wf1", RunID: "run1"}
	data, err := m.ToBytes()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.Publish(ctx, name+".env.exec1", data, jetstream.WithMsgID("wf1:run1:node1")); err != nil {
		t.Fatal(err)
	}
	return stream, cons, fetchOne(t, cons)
}

func fetchOne(t *testing.T, cons jetstream.Consumer) jetstream.Msg {
	t.Helper()
	batch, err := cons.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var got jetstream.Msg
	for m := range batch.Messages() {
		got = m
	}
	return got
}

func liveHandOffRunner(t *testing.T, js jetstream.JetStream, kv *fakeHeartbeatKV, proc Processor) *Runner {
	t.Helper()
	r, _ := handOffRunner(kv, nil)
	r.republish = func(ctx context.Context, m *nats.Msg) (*jetstream.PubAck, error) { return js.PublishMsg(ctx, m) }
	r.processor = proc
	return r
}

// Against a real server: the hand-off stores one copy (a retried re-publish is deduplicated),
// acks the original without a redelivery, and the copy's first delivery carries the attempt
// the original used, so the unit's attempts continue rather than restart.
func TestHandOffLive_CopyCarriesAttemptsAndOriginalIsAcked(t *testing.T) {
	js := liveJetStream(t)
	stream, cons, original := liveUnitStream(t, js, jetstream.StreamConfig{Duplicates: time.Minute})
	if original == nil {
		t.Fatal("no message fetched")
	}
	var calls atomic.Int64
	var mu sync.Mutex
	var allowed []bool
	kv := &fakeHeartbeatKV{}
	r := liveHandOffRunner(t, js, kv, handOffProcessor(&calls, &allowed, &mu))

	msg, err := message.FromJetStreamMsg(original)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.processMessage(context.Background(), msg); err != nil {
		t.Fatalf("processMessage: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if info.NumAckPending != 0 || info.NumRedelivered != 0 {
		t.Fatalf("after the hand-off: ack pending %d, redelivered %d; want 0 and 0", info.NumAckPending, info.NumRedelivered)
	}
	hb, _ := kv.entry(t, executionHeartbeatKey("wf1", "run1", "node1"))
	if hb.State != heartbeatQueued || hb.QueuedSeq != 2 {
		t.Fatalf("heartbeat = %+v; want queued at the copy's sequence 2", hb)
	}

	// A re-publish of the same hand-off (a retry after a lost reply) is stored once.
	dup := nats.NewMsg(original.Subject())
	dup.Data = original.Data()
	dup.Header.Set(jetstream.MsgIDHeader, "wf1:run1:node1:handoff:1")
	ack, err := js.PublishMsg(ctx, dup)
	if err != nil {
		t.Fatal(err)
	}
	if !ack.Duplicate {
		t.Fatal("a repeated hand-off publish was stored twice")
	}

	copyMsg := fetchOne(t, cons)
	if copyMsg == nil {
		t.Fatal("the handed-off copy was not delivered")
	}
	copyUnit, err := message.FromJetStreamMsg(copyMsg)
	if err != nil {
		t.Fatal(err)
	}
	md, _ := copyMsg.Metadata()
	if md.NumDelivered != 1 || deliverAttempt(copyUnit) != 2 {
		t.Fatalf("copy: NumDelivered %d, attempt %d; want 1 and 2", md.NumDelivered, deliverAttempt(copyUnit))
	}
	if handOffCount(copyUnit) != 1 {
		t.Fatalf("copy hand-off count = %d, want 1", handOffCount(copyUnit))
	}
	_ = copyMsg.Ack()
	state, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state.State.Msgs != 2 {
		t.Fatalf("stream holds %d messages, want 2 (the original and one copy)", state.State.Msgs)
	}
}

// When the stream refuses the copy (full, DiscardNew), the unit runs where it is: the original is
// not acked and Process is called again with hand-off not allowed.
func TestHandOffLive_FullStreamRunsHere(t *testing.T) {
	js := liveJetStream(t)
	_, cons, original := liveUnitStream(t, js, jetstream.StreamConfig{MaxMsgs: 1, Discard: jetstream.DiscardNew})
	if original == nil {
		t.Fatal("no message fetched")
	}
	var calls atomic.Int64
	var mu sync.Mutex
	var allowed []bool
	r := liveHandOffRunner(t, js, &fakeHeartbeatKV{}, handOffProcessor(&calls, &allowed, &mu))
	msg, err := message.FromJetStreamMsg(original)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = r.processMessage(ctx, msg)
	}()
	eventually(t, 15*time.Second, "the fallback run", func() bool { return calls.Load() == 2 })
	infoCtx, infoCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer infoCancel()
	info, err := cons.Info(infoCtx)
	if err != nil {
		t.Fatal(err)
	}
	if info.NumAckPending != 1 {
		t.Fatalf("ack pending = %d during the fallback run, want 1 (the original, still held)", info.NumAckPending)
	}
	cancel()
	<-done
}
