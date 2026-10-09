// Not built under the race detector: reconnecting the client after its connection is closed
// races in pkg/client itself (Client.Connect writes config.Logger, client.go:162, while the closed
// connection's ClosedHandler reads it, internal/nats/connection.go:119), which this test cannot
// avoid since the reconnect is the path under test. Run it with `go test -run
// TestRunnerPullsAgainAfterItsConnectionIsClosed ./pkg/runner/`.

//go:build !race

package runner

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"

	"github.com/wehubfusion/Icarus/pkg/client"
	"github.com/wehubfusion/Icarus/pkg/message"
)

type countingProcessor chan struct{}

func (p countingProcessor) Process(context.Context, *message.Message) (message.Message, error) {
	p <- struct{}{}
	return message.Message{}, nil
}

// The UAT incident of 08/10/2026, against a real server: the connection the runner's consumer was
// resolved on is closed while the runner is up, as nats.go does when a restarting server rejects
// it, and the runner must go on pulling once the client has reconnected. Before the fix the fetch
// loop reconnected the client but kept the consumer handle bound to the closed connection, so
// every fetch failed with nats.ErrConnectionClosed before sending a pull, and the consumer showed
// no waiting pulls until the pod was restarted.
//
// Restarting the server itself would need an embedded nats-server, which Icarus does not depend
// on; closing the connection reaches the same closed state.
//
// Needs a NATS server with JetStream: NATS_URL, else the local estate's nats://localhost:14222
// (scripts/local/dev.sh); skipped when there is none. It creates and deletes its own stream.
func TestRunnerPullsAgainAfterItsConnectionIsClosed(t *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cl := client.NewClient(url, "", "")
	if err := cl.Connect(ctx); err != nil {
		t.Skipf("client cannot connect to %s: %v", url, err)
	}
	stream := fmt.Sprintf("STALL_%d", time.Now().UnixNano())
	processed := make(countingProcessor, 4)
	r, err := NewRunner(cl, processed, stream, "stall-consumer", 1, 10*time.Second, zap.NewNop(), nil,
		&Config{WorkerCount: 1, StallTimeout: -1})
	if err != nil {
		t.Skipf("JetStream is not available: %v", err)
	}
	defer js.DeleteStream(context.Background(), stream)

	runCtx, stopRun := context.WithCancel(ctx)
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		_ = r.Run(runCtx)
	}()
	defer func() {
		stopRun()
		<-runDone
		_ = cl.Close()
	}()

	publish := func() {
		t.Helper()
		data, err := message.NewMessage().ToBytes()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := js.Publish(ctx, stream+".unit", data); err != nil {
			t.Fatal(err)
		}
	}
	awaitProcessed := func(what string) {
		t.Helper()
		select {
		case <-processed:
		case <-time.After(15 * time.Second):
			t.Fatalf("%s was not processed", what)
		}
	}

	publish()
	awaitProcessed("the message before the connection closed")

	cl.Connection().Close()

	publish()
	awaitProcessed("the message after the connection closed")
}
