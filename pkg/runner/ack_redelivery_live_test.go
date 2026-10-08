package runner

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/wehubfusion/Icarus/pkg/message"
)

// A unit that waits (for size-aware admission, raw payloads phase 7) while its message is in
// flight is not redelivered: the ack heartbeat keeps extending the deadline for as long as Process
// runs, so AckWait shorter than the wait does not hand the message to another consumer. Without the
// heartbeat the same wait is redelivered, which is what the control half shows.
//
// Needs a NATS server with JetStream: NATS_URL, else the local estate's nats://localhost:14222
// (scripts/local/dev.sh); skipped when there is none. It creates and deletes its own stream.
func TestWaitingUnitIsNotRedelivered(t *testing.T) {
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = "nats://localhost:14222"
	}
	nc, err := nats.Connect(url, nats.Timeout(time.Second))
	if err != nil {
		t.Skipf("no NATS at %s: %v", url, err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	withShortHeartbeat(t, 100*time.Millisecond)
	const ackWait = 600 * time.Millisecond
	const wait = 2 * time.Second // well over AckWait

	run := func(t *testing.T, heartbeat bool) (redelivered int) {
		name := fmt.Sprintf("REDELIVERY_%d", time.Now().UnixNano())
		stream, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: name, Subjects: []string{name + ".>"}})
		if err != nil {
			t.Skipf("JetStream is not available: %v", err)
		}
		defer js.DeleteStream(context.Background(), name)
		cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
			Durable: "c", AckPolicy: jetstream.AckExplicitPolicy, AckWait: ackWait,
		})
		if err != nil {
			t.Fatal(err)
		}
		data, _ := message.NewMessage().ToBytes()
		if _, err := js.Publish(ctx, name+".unit", data); err != nil {
			t.Fatal(err)
		}
		batch, err := cons.Fetch(1, jetstream.FetchMaxWait(2*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		var jsMsg jetstream.Msg
		for m := range batch.Messages() {
			jsMsg = m
		}
		if jsMsg == nil {
			t.Fatal("no message fetched")
		}
		msg, err := message.FromJetStreamMsg(jsMsg)
		if err != nil {
			t.Fatal(err)
		}

		stop := func() {}
		if heartbeat {
			stop = newHeartbeatRunner().startAckHeartbeat(ctx, msg, "wf", "run", "node")
		}
		time.Sleep(wait) // the unit waiting for the admission budget

		// Another consumer asking for work now is handed the message only if its ack deadline passed.
		again, err := cons.Fetch(1, jetstream.FetchMaxWait(500*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		for m := range again.Messages() {
			redelivered++
			_ = m.Nak()
		}
		stop()
		_ = jsMsg.Ack()
		return redelivered
	}

	t.Run("with the heartbeat the wait is not redelivered", func(t *testing.T) {
		if n := run(t, true); n != 0 {
			t.Fatalf("redelivered %d times while the unit waited", n)
		}
	})
	t.Run("without it the same wait is redelivered", func(t *testing.T) {
		if n := run(t, false); n == 0 {
			t.Fatal("expected a redelivery without the heartbeat; the test would prove nothing")
		}
	})
}
