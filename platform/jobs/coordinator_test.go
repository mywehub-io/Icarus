package jobs_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/wehubfusion/Icarus/platform/audit"
	"github.com/wehubfusion/Icarus/platform/internal/testnats"
	"github.com/wehubfusion/Icarus/platform/jobs"
	"github.com/wehubfusion/Icarus/platform/jobs/iriswire"
)

const orgID = "0b5b1b7e-6a43-4c0e-9a51-2f4f6b8e0d11"

// fakeIris serves the two Iris RPCs the client calls, through hand-written service descriptors
// that decode the wire messages.
type fakeIris struct {
	mu        sync.Mutex
	creates   []*iriswire.CreateJobWithLeaseRequest
	createMD  []metadata.MD
	renews    []*iriswire.RenewResourceLockRequest
	onCreate  func(n int, req *iriswire.CreateJobWithLeaseRequest) (*iriswire.CreateJobWithLeaseResponse, error)
	onRenew   func(req *iriswire.RenewResourceLockRequest) (*iriswire.RenewResourceLockResponse, error)
	renewSeen chan struct{}
}

func (f *fakeIris) create(ctx context.Context, req *iriswire.CreateJobWithLeaseRequest) (*iriswire.CreateJobWithLeaseResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	f.mu.Lock()
	f.creates = append(f.creates, req)
	f.createMD = append(f.createMD, md)
	n := len(f.creates)
	f.mu.Unlock()
	if f.onCreate == nil {
		return acquired("JOB-1", false), nil
	}
	return f.onCreate(n, req)
}

func (f *fakeIris) renew(_ context.Context, req *iriswire.RenewResourceLockRequest) (*iriswire.RenewResourceLockResponse, error) {
	f.mu.Lock()
	f.renews = append(f.renews, req)
	f.mu.Unlock()
	if f.renewSeen != nil {
		select {
		case f.renewSeen <- struct{}{}:
		default:
		}
	}
	if f.onRenew == nil {
		return &iriswire.RenewResourceLockResponse{Renewed: true}, nil
	}
	return f.onRenew(req)
}

func (f *fakeIris) createCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.creates)
}

func (f *fakeIris) renewCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.renews)
}

var jobServiceDesc = grpc.ServiceDesc{
	ServiceName: "iris.JobService",
	HandlerType: (*any)(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "CreateJobWithLease",
		Handler: func(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
			in := &iriswire.CreateJobWithLeaseRequest{}
			if err := dec(in); err != nil {
				return nil, err
			}
			return srv.(*fakeIris).create(ctx, in)
		},
	}},
}

var lockServiceDesc = grpc.ServiceDesc{
	ServiceName: "iris.ResourceLockService",
	HandlerType: (*any)(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "RenewResourceLock",
		Handler: func(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
			in := &iriswire.RenewResourceLockRequest{}
			if err := dec(in); err != nil {
				return nil, err
			}
			return srv.(*fakeIris).renew(ctx, in)
		},
	}},
}

func acquired(jobID string, reentrant bool) *iriswire.CreateJobWithLeaseResponse {
	return &iriswire.CreateJobWithLeaseResponse{
		Acquired:  true,
		Reentrant: reentrant,
		Job:       &iriswire.Job{JobId: jobID, ClientId: orgID},
	}
}

type env struct {
	iris *fakeIris
	conn *grpc.ClientConn
	srv  *testnats.Server
	c    *jobs.Coordinator
}

func newEnv(t *testing.T, f *fakeIris, mutate func(*jobs.Options)) *env {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	gs.RegisterService(&jobServiceDesc, f)
	gs.RegisterService(&lockServiceDesc, f)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	srv := testnats.Start(t)
	srv.CreateStream(t, "JOBS", "jobs.update")

	opts := jobs.Options{
		Producer:      "hermes",
		CreateTimeout: time.Second,
		RenewTimeout:  time.Second,
	}
	if mutate != nil {
		mutate(&opts)
	}
	tokens := func(context.Context) (string, error) { return "service-token", nil }
	return &env{iris: f, conn: conn, srv: srv, c: jobs.NewCoordinator(conn, srv.Conn, tokens, opts)}
}

func spec() jobs.Spec {
	return jobs.Spec{
		Kind:         "hermes.workflow.publish",
		ResourceType: "workflow",
		ResourceID:   "wf-1",
		RequestKey:   "req-1",
		ClientID:     orgID,
		UserID:       "user-1",
		Data:         map[string]any{"version": "3"},
		TTL:          90 * time.Second,
		ParentJobID:  "JOB-PARENT",
	}
}

func TestBeginCreatesJob(t *testing.T) {
	e := newEnv(t, &fakeIris{}, nil)
	ctx, col := audit.WithCollector(context.Background())
	op, err := e.c.Begin(ctx, spec())
	require.NoError(t, err)
	assert.Equal(t, "JOB-1", op.JobID())
	assert.False(t, op.Reentrant())
	assert.Equal(t, jobs.JobRef{JobID: "JOB-1", ClientID: orgID, Kind: "hermes.workflow.publish", Producer: "hermes"}, op.Ref())
	assert.Equal(t, "JOB-1", col.Snapshot().JobID, "Begin records the job on the audit collector")

	require.Equal(t, 1, e.iris.createCount())
	req := e.iris.creates[0]
	assert.Equal(t, orgID, req.GetClientId())
	assert.Equal(t, "hermes.workflow.publish", req.GetKind())
	assert.Equal(t, "hermes", req.GetProducer())
	assert.Equal(t, "req-1", req.GetRequestId())
	assert.Equal(t, "workflow", req.GetResource().GetType())
	assert.Equal(t, "wf-1", req.GetResource().GetId())
	assert.Equal(t, "user-1", req.GetUserId())
	assert.Equal(t, int32(90), req.GetTtlSeconds())
	assert.Equal(t, "JOB-PARENT", req.GetParentJobId())
	assert.Equal(t, "3", req.GetData().GetFields()["version"].GetStringValue())
	assert.Equal(t, []string{"Bearer service-token"}, e.iris.createMD[0].Get("authorization"))
	assert.Equal(t, []string{orgID}, e.iris.createMD[0].Get("x-client-id"))
}

func TestBeginRetriesOnceWithSameRequestID(t *testing.T) {
	f := &fakeIris{onCreate: func(n int, _ *iriswire.CreateJobWithLeaseRequest) (*iriswire.CreateJobWithLeaseResponse, error) {
		if n == 1 {
			return nil, status.Error(codes.Unavailable, "iris restarting")
		}
		return acquired("JOB-2", false), nil
	}}
	e := newEnv(t, f, nil)
	op, err := e.c.Begin(context.Background(), spec())
	require.NoError(t, err)
	assert.Equal(t, "JOB-2", op.JobID())
	require.Equal(t, 2, f.createCount())
	assert.Equal(t, f.creates[0].GetRequestId(), f.creates[1].GetRequestId())
}

func unavailableTwice(int, *iriswire.CreateJobWithLeaseRequest) (*iriswire.CreateJobWithLeaseResponse, error) {
	return nil, status.Error(codes.Unavailable, "down")
}

func TestBeginFailsOpen(t *testing.T) {
	f := &fakeIris{onCreate: unavailableTwice}
	e := newEnv(t, f, nil)
	op, err := e.c.Begin(context.Background(), spec())
	require.NoError(t, err)
	assert.Equal(t, "", op.JobID())
	assert.Equal(t, 2, f.createCount(), "one retry before failing open")

	// Every method of a job-less Op does nothing.
	ctx := context.Background()
	assert.NoError(t, op.Running(ctx))
	assert.NoError(t, op.Progress(ctx, 50, "half"))
	assert.NoError(t, op.Succeed(ctx, nil))
	assert.NoError(t, op.Fail(ctx, "X", "y"))
	assert.NoError(t, op.Cancel(ctx))
	op.KeepAlive(ctx, time.Millisecond)()
	assert.Empty(t, e.srv.Messages(t, "JOBS"))
}

func TestBeginFailsClosed(t *testing.T) {
	f := &fakeIris{onCreate: unavailableTwice}
	e := newEnv(t, f, nil)
	s := spec()
	s.FailClosed = true
	op, err := e.c.Begin(context.Background(), s)
	assert.Nil(t, op)
	assert.ErrorIs(t, err, jobs.ErrUnavailable)
	assert.Equal(t, 2, f.createCount())
}

func TestDisabledCoordinator(t *testing.T) {
	srv := testnats.Start(t)
	for _, c := range []*jobs.Coordinator{
		jobs.NewCoordinator(nil, srv.Conn, nil, jobs.Options{Producer: "hermes"}),
		jobs.NewCoordinator(nil, nil, nil, jobs.Options{Producer: "hermes"}),
		nil,
	} {
		assert.False(t, c.Enabled())
		op, err := c.Begin(context.Background(), spec())
		require.NoError(t, err)
		assert.Equal(t, "", op.JobID())

		s := spec()
		s.FailClosed = true
		_, err = c.Begin(context.Background(), s)
		assert.ErrorIs(t, err, jobs.ErrUnavailable)

		ref := jobs.JobRef{JobID: "JOB-1", ClientID: orgID}
		_, err = c.Renew(context.Background(), ref, 0)
		assert.ErrorIs(t, err, jobs.ErrUnavailable)
		assert.ErrorIs(t, c.PublishStatus(context.Background(), ref, jobs.SeqRunning, jobs.StatusRunning, "", "", nil), jobs.ErrUnavailable)
	}
}

func TestBeginRequestKey(t *testing.T) {
	e := newEnv(t, &fakeIris{}, nil)
	s := spec()
	s.RequestKey = ""
	_, err := e.c.Begin(context.Background(), s)
	assert.ErrorIs(t, err, jobs.ErrMissingRequestKey)
	assert.Zero(t, e.iris.createCount())

	s.AllowRandomKey = true
	_, err = e.c.Begin(context.Background(), s)
	require.NoError(t, err)
	_, err = uuid.Parse(e.iris.creates[0].GetRequestId())
	assert.NoError(t, err, "an allowed random key is a UUID")

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-request-id", "rid", "idempotency-key", "ik"))
	assert.Equal(t, "ik", jobs.RequestKeyFromContext(ctx))
	ctx = metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-request-id", "rid"))
	assert.Equal(t, "rid", jobs.RequestKeyFromContext(ctx))
	assert.Equal(t, "", jobs.RequestKeyFromContext(context.Background()))
}

func TestBeginLockedReportsHolder(t *testing.T) {
	f := &fakeIris{onCreate: func(int, *iriswire.CreateJobWithLeaseRequest) (*iriswire.CreateJobWithLeaseResponse, error) {
		return &iriswire.CreateJobWithLeaseResponse{
			Acquired: false,
			Holder:   &iriswire.ResourceLock{JobId: "JOB-HOLD", Kind: "styx.import"},
		}, nil
	}}
	e := newEnv(t, f, nil)
	_, err := e.c.Begin(context.Background(), spec())
	var locked *jobs.ErrResourceLocked
	require.ErrorAs(t, err, &locked)
	assert.Equal(t, "JOB-HOLD", locked.HolderJobID)
	assert.Equal(t, "styx.import", locked.HolderKind)
	assert.Contains(t, err.Error(), "JOB-HOLD")
}

func TestBeginAlreadyCompleted(t *testing.T) {
	withInfo, err := status.New(codes.FailedPrecondition, "the operation for this request has already completed").
		WithDetails(&errdetails.ErrorInfo{Reason: "REQUEST_ALREADY_COMPLETED", Metadata: map[string]string{"job_id": "JOB-INFO"}})
	require.NoError(t, err)

	cases := map[string]error{
		"JOB-INFO": withInfo.Err(),
		"JOB-MSG":  status.Error(codes.FailedPrecondition, "the operation for this request has already completed (job_id=JOB-MSG)"),
	}
	for want, rpcErr := range cases {
		t.Run(want, func(t *testing.T) {
			f := &fakeIris{onCreate: func(int, *iriswire.CreateJobWithLeaseRequest) (*iriswire.CreateJobWithLeaseResponse, error) {
				return nil, rpcErr
			}}
			e := newEnv(t, f, nil)
			ctx, col := audit.WithCollector(context.Background())
			_, err := e.c.Begin(ctx, spec())
			var done *jobs.ErrAlreadyCompleted
			require.ErrorAs(t, err, &done)
			assert.Equal(t, want, done.JobID)
			assert.Equal(t, want, col.Snapshot().JobID)
			assert.Equal(t, 1, f.createCount(), "not retried")
		})
	}

	// Another FailedPrecondition is not mistaken for it: it fails open.
	f := &fakeIris{onCreate: func(int, *iriswire.CreateJobWithLeaseRequest) (*iriswire.CreateJobWithLeaseResponse, error) {
		return nil, status.Error(codes.FailedPrecondition, "something else")
	}}
	e := newEnv(t, f, nil)
	op, err := e.c.Begin(context.Background(), spec())
	require.NoError(t, err)
	assert.Equal(t, "", op.JobID())
}

func TestBeginReentrant(t *testing.T) {
	f := &fakeIris{onCreate: func(int, *iriswire.CreateJobWithLeaseRequest) (*iriswire.CreateJobWithLeaseResponse, error) {
		return acquired("JOB-EXISTING", true), nil
	}}
	e := newEnv(t, f, nil)
	op, err := e.c.Begin(context.Background(), spec())
	require.NoError(t, err)
	assert.True(t, op.Reentrant())
	assert.Equal(t, "JOB-EXISTING", op.JobID())
}

func TestBeginTokenFailureFailsOpen(t *testing.T) {
	e := newEnv(t, &fakeIris{}, nil)
	c := jobs.NewCoordinator(e.conn, e.srv.Conn, func(context.Context) (string, error) {
		return "", errors.New("no secret")
	}, jobs.Options{Producer: "hermes"})
	op, err := c.Begin(context.Background(), spec())
	require.NoError(t, err)
	assert.Equal(t, "", op.JobID())
	assert.Zero(t, e.iris.createCount())
}

type update struct {
	header map[string][]string
	fields map[string]any
	msg    *iriswire.JobUpdateMessage
}

func updates(t *testing.T, srv *testnats.Server) []update {
	t.Helper()
	var out []update
	for _, m := range srv.Messages(t, "JOBS") {
		u := update{header: m.Header, msg: &iriswire.JobUpdateMessage{}}
		require.NoError(t, json.Unmarshal(m.Data, &u.fields))
		require.NoError(t, protojson.Unmarshal(m.Data, u.msg))
		out = append(out, u)
	}
	return out
}

func TestOpUpdates(t *testing.T) {
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	e := newEnv(t, &fakeIris{}, nil)
	live := make(chan *nats.Msg, 8)
	sub, err := e.srv.Connect(t).ChanSubscribe("jobs.update", live)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	op, err := e.c.Begin(context.Background(), spec())
	require.NoError(t, err)

	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1, 2, 3}, SpanID: trace.SpanID{4, 5, 6}, TraceFlags: trace.FlagsSampled,
	}))
	require.NoError(t, op.Running(ctx))
	require.NoError(t, op.Progress(ctx, 150, "copying"))
	require.NoError(t, op.Progress(ctx, 60, "indexing"))
	require.NoError(t, op.Succeed(ctx, map[string]any{"version": 4}))

	// The NATS server rewrites the traceparent key to lower case in transit, so the delivered
	// headers are read back without regard to case.
	for i := 0; i < 4; i++ {
		select {
		case m := <-live:
			h := http.Header{}
			for k, vs := range m.Header {
				for _, v := range vs {
					h.Add(k, v)
				}
			}
			sc := trace.SpanContextFromContext(otel.GetTextMapPropagator().Extract(context.Background(),
				propagation.HeaderCarrier(h)))
			assert.Equal(t, trace.TraceID{1, 2, 3}, sc.TraceID(), "a traceparent travels with every update")
		case <-time.After(5 * time.Second):
			t.Fatal("update not delivered")
		}
	}

	got := updates(t, e.srv)
	require.Len(t, got, 4)
	wantSeq := []uint64{jobs.SeqRunning, jobs.SeqProgressStart, jobs.SeqProgressStart + 1, jobs.TerminalSeq}
	for i, u := range got {
		assert.Equal(t, wantSeq[i], u.msg.GetSeq())
		assert.Equal(t, jobs.MessageID("JOB-1", wantSeq[i]), u.msg.GetId())
		assert.Equal(t, []string{u.msg.GetId()}, u.header[jetstream.MsgIDHeader])
		assert.Equal(t, "v1", u.msg.GetV())
		assert.Equal(t, orgID, u.msg.GetClientId())
		assert.Equal(t, "hermes", u.msg.GetProducer())
		_, err := time.Parse(time.RFC3339, u.msg.GetTimestamp())
		assert.NoError(t, err)
	}
	assert.Equal(t, jobs.UpdateTypeStatusChanged, got[0].msg.GetType())
	assert.Equal(t, jobs.StatusRunning, got[0].msg.GetStatus())
	assert.Equal(t, jobs.UpdateTypeProgress, got[1].msg.GetType())
	assert.Equal(t, int32(100), got[1].msg.GetProgress().GetPercent(), "percent is clamped")
	assert.Equal(t, "indexing", got[2].msg.GetProgress().GetMessage())
	assert.Equal(t, jobs.StatusSucceeded, got[3].msg.GetStatus())
	assert.Equal(t, float64(4), got[3].msg.GetData().GetFields()["version"].GetNumberValue())

	// The JSON uses the proto field names, as Iris expects.
	for _, k := range []string{"id", "v", "job_id", "seq", "type", "timestamp", "client_id", "producer", "status"} {
		assert.Contains(t, got[0].fields, k)
	}
	assert.Contains(t, got[1].fields, "progress")
	assert.Contains(t, got[3].fields, "data")
	assert.NotContains(t, got[0].fields, "jobId")
}

func TestOpFailAndCancel(t *testing.T) {
	e := newEnv(t, &fakeIris{}, nil)
	op, err := e.c.Begin(context.Background(), spec())
	require.NoError(t, err)
	require.NoError(t, op.Fail(context.Background(), "LEASE_EXPIRED", "lease lost"))

	got := updates(t, e.srv)
	require.Len(t, got, 1)
	assert.Equal(t, jobs.StatusFailed, got[0].msg.GetStatus())
	assert.Equal(t, jobs.TerminalSeq, got[0].msg.GetSeq())
	assert.Equal(t, "LEASE_EXPIRED", got[0].msg.GetError().GetCode())
	assert.Equal(t, "lease lost", got[0].msg.GetError().GetMessage())
	assert.Contains(t, got[0].fields, "error")

	// A second terminal update has the same message id, so the stream drops it.
	require.NoError(t, op.Cancel(context.Background()))
	assert.Len(t, updates(t, e.srv), 1)

	ref := jobs.JobRef{JobID: "JOB-OTHER", ClientID: orgID, Kind: "k", Producer: "temporal-worker"}
	require.NoError(t, e.c.PublishStatus(context.Background(), ref, jobs.TerminalSeq, jobs.StatusCanceled, "", "", nil))
	got = updates(t, e.srv)
	require.Len(t, got, 2)
	assert.Equal(t, jobs.StatusCanceled, got[1].msg.GetStatus())
	assert.Equal(t, "temporal-worker", got[1].msg.GetProducer(), "the ref's producer is kept")
	assert.Nil(t, got[1].msg.GetError())
}

func TestPublishNeverCreatesStream(t *testing.T) {
	e := newEnv(t, &fakeIris{}, func(o *jobs.Options) {
		o.JobsStream = "MISSING"
		o.JobsSubject = "jobs.nowhere"
		o.PublishTimeout = 300 * time.Millisecond
	})
	op, err := e.c.Begin(context.Background(), spec())
	require.NoError(t, err)
	assert.Error(t, op.Running(context.Background()))

	_, err = e.srv.JS.Stream(context.Background(), "MISSING")
	assert.ErrorIs(t, err, jetstream.ErrStreamNotFound)
}

func TestRenewAndKeepAlive(t *testing.T) {
	f := &fakeIris{renewSeen: make(chan struct{}, 1)}
	e := newEnv(t, f, nil)
	ref := jobs.JobRef{JobID: "JOB-1", ClientID: orgID}

	ok, err := e.c.Renew(context.Background(), ref, 45*time.Second)
	require.NoError(t, err)
	assert.True(t, ok)
	req := f.renews[0]
	assert.Equal(t, "JOB-1", req.GetJobId())
	assert.Equal(t, orgID, req.GetClientId())
	assert.Equal(t, int32(45), req.GetTtlSeconds())

	f.onRenew = func(*iriswire.RenewResourceLockRequest) (*iriswire.RenewResourceLockResponse, error) {
		return &iriswire.RenewResourceLockResponse{Renewed: false}, nil
	}
	ok, err = e.c.Renew(context.Background(), ref, 0)
	require.NoError(t, err, "a lost lease is not an error")
	assert.False(t, ok)
	assert.Equal(t, int32(120), f.renews[1].GetTtlSeconds(), "zero means the default ttl")

	ok, err = e.c.Renew(context.Background(), jobs.JobRef{}, 0)
	assert.NoError(t, err)
	assert.False(t, ok)

	f.onRenew = nil
	op, err := e.c.Begin(context.Background(), spec())
	require.NoError(t, err)
	before := f.renewCount()
	stop := op.KeepAlive(context.Background(), 20*time.Millisecond)
	testnats.WaitFor(t, 5*time.Second, "two renewals", func() bool { return f.renewCount() >= before+2 })
	stop()
	stop()
	after := f.renewCount()
	time.Sleep(60 * time.Millisecond)
	assert.Equal(t, after, f.renewCount(), "no renewals after stop")
	assert.Equal(t, int32(90), f.renews[len(f.renews)-1].GetTtlSeconds(), "keep alive renews for the Spec's ttl")
}
