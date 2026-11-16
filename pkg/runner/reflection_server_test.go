package runner

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/wehubfusion/Icarus/pkg/client"
	"github.com/wehubfusion/Icarus/pkg/message"
)

// embeddedJetStream starts an in-process NATS server with JetStream and returns its URL.
func embeddedJetStream(t *testing.T) string {
	t.Helper()
	srv, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("embedded NATS server did not start")
	}
	t.Cleanup(srv.Shutdown)
	return srv.ClientURL()
}

// eventuallyTrue polls cond until it holds or within passes.
func eventuallyTrue(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", within, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Against a real server: a runner (A) reads its own KV key with a direct get whose reply subject is
// another runner's (B's) work subject, so the server stores its answer in B's work stream. The
// answer's body is A's bytes, a well-formed unit. B's runner must terminate it without running it,
// and still run a genuine unit after it.
func TestServer_ReflectedDirectGetInWorkStreamIsTerminated(t *testing.T) {
	// A short pull, so stopping the runner does not wait out an open one.
	origWait := fetchMaxWait
	fetchMaxWait = 500 * time.Millisecond
	t.Cleanup(func() { fetchMaxWait = origWait })
	url := embeddedJetStream(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	attacker, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer attacker.Close()
	ajs, err := jetstream.New(attacker)
	if err != nil {
		t.Fatal(err)
	}
	own, err := ajs.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "RUNNER_A"})
	if err != nil {
		t.Fatal(err)
	}
	forged := message.NewMessage()
	forged.Workflow = &message.Workflow{WorkflowID: "wf9", RunID: "run9"}
	forged.Payload = &message.Payload{ExecutionID: "exec9", NodeID: "node9", WorkflowID: "wf9", RunID: "run9"}
	forgedBytes, err := forged.ToBytes()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := own.Put(ctx, "mine", forgedBytes); err != nil {
		t.Fatal(err)
	}

	core, logs := observer.New(zapcore.WarnLevel)
	cl := client.NewClient(url, "", "")
	if err := cl.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	processed := make(signalProcessor, 4)
	r, err := NewRunner(cl, processed, "WORK", "runner-b", 1, 2*time.Second, zap.New(core), nil,
		&Config{WorkerCount: 1, StallTimeout: -1})
	if err != nil {
		t.Fatal(err)
	}
	work, err := ajs.Stream(ctx, "WORK")
	if err != nil {
		t.Fatal(err)
	}
	stored := func() uint64 {
		info, err := work.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return info.State.Msgs
	}

	// The reflection: the server answers A's direct get on B's work subject, and WORK stores it.
	if err := attacker.PublishRequest("$JS.API.DIRECT.GET.KV_RUNNER_A.$KV.RUNNER_A.mine", "WORK.runnerB.x", nil); err != nil {
		t.Fatal(err)
	}
	eventuallyTrue(t, 5*time.Second, "the reflected answer to be stored in WORK", func() bool { return stored() == 1 })

	runCtx, stopRun := context.WithCancel(ctx)
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		_ = r.Run(runCtx)
	}()
	defer func() {
		stopRun()
		<-runDone
	}()

	eventuallyTrue(t, 10*time.Second, "the runner to refuse the reflected unit", func() bool {
		return logs.FilterMessage("Refusing a delivered message that is not a unit of work; terminating it").
			FilterField(zap.String("subject", "WORK.runnerB.x")).Len() == 1
	})
	// Terminated: a work queue drops it, and it is never redelivered.
	eventuallyTrue(t, 5*time.Second, "the reflected unit to leave WORK", func() bool { return stored() == 0 })
	select {
	case <-processed:
		t.Fatal("the reflected unit was processed")
	default:
	}

	genuine, err := message.NewMessage().ToBytes()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ajs.Publish(ctx, "WORK.runnerB.y", genuine, jetstream.WithMsgID("genuine")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-processed:
	case <-time.After(15 * time.Second):
		t.Fatal("the genuine unit after the reflection was not processed")
	}
	select {
	case <-processed:
		t.Fatal("a second unit was processed: the reflection ran")
	case <-time.After(500 * time.Millisecond):
	}
}

// Against a real server: A reflects a direct get answer of its own key, a fresh running claim by
// "attacker", over B's EXECUTION_HEARTBEATS key. The KV API cannot tell it from a real claim; the
// runner's heartbeat read must, and its claim must replace it.
func TestServer_ReflectedHeartbeatIsNoClaim(t *testing.T) {
	url := embeddedJetStream(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	attacker, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer attacker.Close()
	ajs, err := jetstream.New(attacker)
	if err != nil {
		t.Fatal(err)
	}
	own, err := ajs.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "RUNNER_A"})
	if err != nil {
		t.Fatal(err)
	}
	fake, err := json.Marshal(executionHeartbeat{ExecutionID: "exec1", State: heartbeatRunning, Owner: "attacker"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := own.Put(ctx, "hb", fake); err != nil {
		t.Fatal(err)
	}

	cl := client.NewClient(url, "", "")
	if err := cl.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	r, err := NewRunner(cl, make(signalProcessor, 1), "WORK", "runner-b", 1, 10*time.Second, zap.NewNop(), nil,
		&Config{WorkerCount: 1, StallTimeout: -1})
	if err != nil {
		t.Fatal(err)
	}
	key := r.heartbeatKey("wf1", "run1", "node1")
	guarded := r.heartbeats(ctx)
	if _, err := guarded.Get(ctx, key); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("guarded Get of a missing key: %v, want ErrKeyNotFound", err)
	}

	if err := attacker.PublishRequest("$JS.API.DIRECT.GET.KV_RUNNER_A.$KV.RUNNER_A.hb",
		"$KV."+executionHeartbeatBucket+"."+key, nil); err != nil {
		t.Fatal(err)
	}
	plain, err := ajs.KeyValue(ctx, executionHeartbeatBucket)
	if err != nil {
		t.Fatal(err)
	}
	// The KV API shows the forgery as a running claim.
	var seen executionHeartbeat
	eventuallyTrue(t, 5*time.Second, "the reflected claim to be stored", func() bool {
		e, err := plain.Get(ctx, key)
		return err == nil && json.Unmarshal(e.Value(), &seen) == nil && seen.Owner == "attacker"
	})

	e, err := guarded.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if !reflectedEntry(e) || e.Value() != nil {
		t.Fatalf("guarded Get: reflected %v, value %q; want a reflected entry with no value", reflectedEntry(e), e.Value())
	}

	hb := executionHeartbeat{ExecutionID: "exec1", State: heartbeatRunning, Owner: processToken}
	outcome, err := claimExecutionUnit(ctx, guarded, key, hb, time.Now())
	if err != nil || outcome != claimWon {
		t.Fatalf("claim over the forgery: %v, %v; want claimWon", outcome, err)
	}
	e, err = guarded.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	var cur executionHeartbeat
	if reflectedEntry(e) || json.Unmarshal(e.Value(), &cur) != nil || cur.Owner != processToken {
		t.Fatalf("after the claim: reflected %v, entry %+v; want this process's claim", reflectedEntry(e), cur)
	}
}

// signalProcessor signals each unit it processes.
type signalProcessor chan struct{}

func (p signalProcessor) Process(context.Context, *message.Message) (message.Message, error) {
	p <- struct{}{}
	return message.Message{}, nil
}
