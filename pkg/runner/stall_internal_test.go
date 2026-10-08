package runner

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/wehubfusion/Icarus/pkg/client"
	"github.com/wehubfusion/Icarus/pkg/message"
)

// fakeStallJS is a message.JSContext whose Consumer returns whatever consumerFn does. The fetch
// loop resolves its consumer through it, so a test can see each resolve and hand out a different
// consumer each time.
type fakeStallJS struct {
	consumerFn func(call int) (jetstream.Consumer, error)
	resolves   atomic.Int64
	creates    atomic.Int64
}

func (js *fakeStallJS) Publish(context.Context, string, []byte, ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	return nil, errors.New("not used")
}
func (js *fakeStallJS) Stream(context.Context, string) (jetstream.Stream, error) {
	return nil, errors.New("not used")
}
func (js *fakeStallJS) CreateStream(context.Context, jetstream.StreamConfig) (jetstream.Stream, error) {
	return nil, errors.New("not used")
}
func (js *fakeStallJS) Consumer(_ context.Context, _, _ string) (jetstream.Consumer, error) {
	return js.consumerFn(int(js.resolves.Add(1)))
}
func (js *fakeStallJS) CreateConsumer(context.Context, string, jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	js.creates.Add(1)
	return nil, nil
}

// fakeStallConsumer is a jetstream.Consumer implementing only what the fetch loop calls.
type fakeStallConsumer struct {
	jetstream.Consumer
	fetch func(batch int) (jetstream.MessageBatch, error)
}

func (c *fakeStallConsumer) Fetch(batch int, _ ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	return c.fetch(batch)
}
func (c *fakeStallConsumer) CachedInfo() *jetstream.ConsumerInfo { return &jetstream.ConsumerInfo{} }

type fakeBatch struct {
	msgs chan jetstream.Msg
	err  error
}

func (b *fakeBatch) Messages() <-chan jetstream.Msg { return b.msgs }
func (b *fakeBatch) Error() error                   { return b.err }

func batchOf(msgs ...jetstream.Msg) *fakeBatch {
	ch := make(chan jetstream.Msg, len(msgs))
	for _, m := range msgs {
		ch <- m
	}
	close(ch)
	return &fakeBatch{msgs: ch}
}

// emptyFetch stands in for a fetch that waited out its max wait and found nothing.
func emptyFetch(int) (jetstream.MessageBatch, error) {
	time.Sleep(5 * time.Millisecond)
	return batchOf(), nil
}

// ackMsg is a heartbeatMsg that can also be acked, which a unit without workflow ids is.
type ackMsg struct {
	*heartbeatMsg
	acks atomic.Int64
}

func (m *ackMsg) Ack() error { m.acks.Add(1); return nil }

func newAckMsg(t *testing.T) *ackMsg {
	t.Helper()
	data, err := message.NewMessage().ToBytes()
	if err != nil {
		t.Fatal(err)
	}
	return &ackMsg{heartbeatMsg: &heartbeatMsg{data: data}}
}

// consumerDelivering returns a consumer that hands out msg on its first fetch and then nothing.
func consumerDelivering(msg jetstream.Msg) *fakeStallConsumer {
	var once sync.Once
	return &fakeStallConsumer{fetch: func(batch int) (jetstream.MessageBatch, error) {
		var out *fakeBatch
		once.Do(func() { out = batchOf(msg) })
		if out != nil {
			return out, nil
		}
		return emptyFetch(batch)
	}}
}

type funcProcessor func(ctx context.Context, msg *message.Message) (message.Message, error)

func (f funcProcessor) Process(ctx context.Context, msg *message.Message) (message.Message, error) {
	return f(ctx, msg)
}

func okProcessor() Processor {
	return funcProcessor(func(context.Context, *message.Message) (message.Message, error) {
		return message.Message{}, nil
	})
}

// newFakeRunner builds a Runner over js without NewRunner, which needs a server. It reports the
// connection as up, as it was in the incident: the client had reconnected.
func newFakeRunner(js message.JSContext, proc Processor, workers int, stallTimeout, processTimeout time.Duration) *Runner {
	r := &Runner{
		client:         client.NewClientWithJSContext(js),
		processor:      proc,
		stream:         "TEST",
		consumer:       "test-consumer",
		batchSize:      1,
		logger:         zap.NewNop(),
		processTimeout: processTimeout,
		tracer:         otel.Tracer("icarus/runner/test"),
		config:         Config{WorkerCount: workers, StallTimeout: stallTimeout},
		jobChan:        make(chan *message.Message, workers),
		idle:           make(chan struct{}, workers),
		busySince:      make([]atomic.Int64, workers),
		connected:      func() bool { return true },
	}
	for i := 0; i < workers; i++ {
		r.idle <- struct{}{}
	}
	return r
}

// startRun runs r until the test ends, and returns a cancel that waits for Run to return.
func startRun(t *testing.T, r *Runner) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = r.Run(ctx)
	}()
	stop := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("Run did not return after its context was cancelled")
		}
	}
	t.Cleanup(stop)
	return stop
}

func eventually(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", within, what)
}

// The incident: after a NATS restart the connection the consumer was resolved on is closed, and
// Fetch on it fails with nats.ErrConnectionClosed before any pull is sent. The client reconnects
// (tryReconnectNATS), but the loop used to keep the old consumer handle, because
// isFatalConsumeError only knew jetstream.ErrConnectionClosed, so every fetch failed the same way
// and no pull reached the server until the pod was restarted. The loop must resolve the consumer
// again and pull on the new connection.
func TestFetchLoop_ClosedConnection_ResolvesConsumerAgain(t *testing.T) {
	dead := &fakeStallConsumer{fetch: func(int) (jetstream.MessageBatch, error) {
		return nil, nats.ErrConnectionClosed
	}}
	msg := newAckMsg(t)
	js := &fakeStallJS{consumerFn: func(call int) (jetstream.Consumer, error) {
		if call == 1 {
			return dead, nil
		}
		return consumerDelivering(msg), nil
	}}
	r := newFakeRunner(js, okProcessor(), 1, -1, time.Minute)
	startRun(t, r)

	eventually(t, 3*time.Second, "the message fetched on the resolved-again consumer to be acked", func() bool {
		return msg.acks.Load() == 1
	})
	if got := js.resolves.Load(); got < 2 {
		t.Fatalf("consumer resolved %d times, want at least 2", got)
	}
}

func TestIsFatalConsumeError(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{nats.ErrConnectionClosed, true},
		{fmt.Errorf("fetch: %w", nats.ErrConnectionClosed), true},
		{nats.ErrConnectionDraining, true},
		{jetstream.ErrConnectionClosed, true},
		{jetstream.ErrConsumerDeleted, true},
		{jetstream.ErrConsumerNotFound, true},
		{errors.New("pull rejected"), false},
		{nil, false},
	} {
		if got := isFatalConsumeError(tc.err); got != tc.want {
			t.Errorf("isFatalConsumeError(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}

// A consumer deleted while the runner is up (lost in a server restart, or by InactiveThreshold)
// can never be resolved again; the loop creates it again instead of retrying the lookup forever.
func TestFetchLoop_MissingConsumer_IsCreatedAgain(t *testing.T) {
	msg := newAckMsg(t)
	js := &fakeStallJS{}
	js.consumerFn = func(int) (jetstream.Consumer, error) {
		if js.creates.Load() == 0 {
			return nil, jetstream.ErrConsumerNotFound
		}
		return consumerDelivering(msg), nil
	}
	r := newFakeRunner(js, okProcessor(), 1, -1, time.Minute)
	startRun(t, r)

	eventually(t, 3*time.Second, "the message to be acked after the consumer was created again", func() bool {
		return msg.acks.Load() == 1
	})
	if got := js.creates.Load(); got != 1 {
		t.Fatalf("CreateConsumer calls = %d, want 1", got)
	}
}

// A loop that keeps failing to fetch with an error it does not act on is a stall: the watchdog
// reports it through Health and makes the loop resolve the consumer again, and Health clears
// once fetching works.
func TestWatchdog_NoFetch_ReportsAndResolvesAgain(t *testing.T) {
	rejecting := &fakeStallConsumer{fetch: func(int) (jetstream.MessageBatch, error) {
		return nil, errors.New("pull rejected")
	}}
	js := &fakeStallJS{consumerFn: func(call int) (jetstream.Consumer, error) {
		if call == 1 {
			return rejecting, nil
		}
		return &fakeStallConsumer{fetch: emptyFetch}, nil
	}}
	r := newFakeRunner(js, okProcessor(), 2, 200*time.Millisecond, time.Minute)
	if err := r.Health(); err != nil {
		t.Fatalf("Health before Run = %v, want nil", err)
	}
	stop := startRun(t, r)

	eventually(t, 3*time.Second, "Health to report the stall", func() bool { return r.Health() != nil })
	eventually(t, 3*time.Second, "the watchdog to resolve the consumer again", func() bool { return js.resolves.Load() >= 2 })
	eventually(t, 3*time.Second, "Health to clear once fetching works", func() bool { return r.Health() == nil })

	// Stays clear while the loop keeps fetching.
	time.Sleep(400 * time.Millisecond)
	if err := r.Health(); err != nil {
		t.Fatalf("Health while fetching = %v, want nil", err)
	}
	stop()
	if err := r.Health(); err != nil {
		t.Fatalf("Health after Run = %v, want nil", err)
	}
}

// A stall while NATS is down is the connection's to report: Health stays nil so a liveness
// probe does not restart every replica for an outage.
func TestHealth_ConnectionDown_IsNotAStall(t *testing.T) {
	js := &fakeStallJS{consumerFn: func(int) (jetstream.Consumer, error) {
		return nil, errors.New("nats: timeout")
	}}
	r := newFakeRunner(js, okProcessor(), 1, 50*time.Millisecond, time.Minute)
	var up atomic.Bool
	r.connected = up.Load
	startRun(t, r)

	time.Sleep(300 * time.Millisecond)
	if err := r.Health(); err != nil {
		t.Fatalf("Health with NATS down = %v, want nil", err)
	}
	up.Store(true)
	eventually(t, 2*time.Second, "Health to report the stall once NATS is up", func() bool { return r.Health() != nil })
}

// Every worker busy within its deadline is not a stall, however long since the last fetch. A
// worker that never returns (a Process ignoring its context) is, once it passes processTimeout
// plus workerStuckGrace, since its idle slot never comes back.
func TestHealth_BusyWorkers_StuckWorker(t *testing.T) {
	original := workerStuckGrace
	workerStuckGrace = 400 * time.Millisecond
	t.Cleanup(func() { workerStuckGrace = original })

	release := make(chan struct{})
	started := make(chan struct{}, 1)
	blocking := funcProcessor(func(context.Context, *message.Message) (message.Message, error) {
		started <- struct{}{}
		<-release // ignores its context
		return message.Message{}, nil
	})
	msg := newAckMsg(t)
	js := &fakeStallJS{consumerFn: func(int) (jetstream.Consumer, error) { return consumerDelivering(msg), nil }}
	r := newFakeRunner(js, blocking, 1, 50*time.Millisecond, 100*time.Millisecond)
	startRun(t, r)
	t.Cleanup(func() { close(release) }) // runs before startRun's cleanup, so Run can drain

	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("the unit never started")
	}
	time.Sleep(200 * time.Millisecond) // past StallTimeout, inside processTimeout + grace
	if err := r.Health(); err != nil {
		t.Fatalf("Health with every worker busy within its deadline = %v, want nil", err)
	}
	eventually(t, 3*time.Second, "Health to report the stuck worker", func() bool { return r.Health() != nil })
}

func TestResolveStallTimeout(t *testing.T) {
	t.Setenv("ICARUS_RUNNER_STALL_TIMEOUT", "")
	if got := resolveStallTimeout(0); got != defaultStallTimeout {
		t.Errorf("default = %v, want %v", got, defaultStallTimeout)
	}
	t.Setenv("ICARUS_RUNNER_STALL_TIMEOUT", "45s")
	if got := resolveStallTimeout(0); got != 45*time.Second {
		t.Errorf("from env = %v, want 45s", got)
	}
	if got := resolveStallTimeout(time.Minute); got != time.Minute {
		t.Errorf("configured = %v, want 1m: Config wins over env", got)
	}
	if got := resolveStallTimeout(-1); got != -1 {
		t.Errorf("disabled = %v, want -1", got)
	}
	t.Setenv("ICARUS_RUNNER_STALL_TIMEOUT", "soon")
	if got := resolveStallTimeout(0); got != defaultStallTimeout {
		t.Errorf("unparseable env = %v, want the default", got)
	}
}

// hangingKV never answers Get or Put until its context ends, like a request whose reply was lost
// when NATS restarted.
type hangingKV struct {
	*fakeHeartbeatKV
	putStarted chan struct{}
	once       sync.Once
}

func (kv *hangingKV) Get(ctx context.Context, _ string) (jetstream.KeyValueEntry, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (kv *hangingKV) Put(ctx context.Context, _ string, _ []byte) (uint64, error) {
	kv.once.Do(func() { close(kv.putStarted) })
	<-ctx.Done()
	return 0, ctx.Err()
}

// stop must not wait on a heartbeat write in flight. The write used to run on the process
// context, so a unit whose Process had returned could hold its worker for the rest of
// processTimeout (30 minutes for esr-operation) waiting on a reply lost in a NATS restart.
func TestStartAckHeartbeat_StopDoesNotWaitOnHungKVWrite(t *testing.T) {
	withShortHeartbeat(t, 5*time.Millisecond)
	original := heartbeatKVTimeout
	heartbeatKVTimeout = time.Hour // only stop can end the write
	t.Cleanup(func() { heartbeatKVTimeout = original })

	kv := &hangingKV{fakeHeartbeatKV: &fakeHeartbeatKV{}, putStarted: make(chan struct{})}
	msg := buildMessageFor(t, &heartbeatMsg{})
	stop := newHeartbeatRunnerWithKV(kv).startAckHeartbeat(context.Background(), msg, "wf1", "run1", "node1")

	select {
	case <-kv.putStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("the heartbeat never wrote to the KV")
	}
	done := make(chan struct{})
	go func() { stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stop() waited on a hung KV write")
	}
}

// A claim whose KV reply never comes fails open after heartbeatKVTimeout instead of holding the
// worker for the whole process context.
func TestClaimOrNak_HungKV_FailsOpenAfterTimeout(t *testing.T) {
	original := heartbeatKVTimeout
	heartbeatKVTimeout = 50 * time.Millisecond
	t.Cleanup(func() { heartbeatKVTimeout = original })

	kv := &hangingKV{fakeHeartbeatKV: &fakeHeartbeatKV{}, putStarted: make(chan struct{})}
	msg := buildMessageFor(t, &heartbeatMsg{})
	done := make(chan bool, 1)
	go func() {
		done <- newHeartbeatRunnerWithKV(kv).claimOrNak(context.Background(), msg, "wf1", "run1", "node1", "exec1")
	}()
	select {
	case proceed := <-done:
		if !proceed {
			t.Fatal("a claim the KV never answered must fail open")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("claimOrNak waited on a hung KV call")
	}
}
