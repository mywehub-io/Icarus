package jobs

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/wehubfusion/Icarus/platform/audit"
	"github.com/wehubfusion/Icarus/platform/jobs/iriswire"
)

// Full gRPC method names of the Iris RPCs the client calls, as declared in the Iris proto.
const (
	MethodCreateJobWithLease = "/iris.JobService/CreateJobWithLease"
	MethodRenewResourceLock  = "/iris.ResourceLockService/RenewResourceLock"
)

// Defaults for the Options fields left zero.
const (
	DefaultJobsSubject    = "jobs.update"
	DefaultJobsStream     = "JOBS"
	DefaultCreateTimeout  = 5 * time.Second
	DefaultRenewTimeout   = 3 * time.Second
	DefaultPublishTimeout = 3 * time.Second
	DefaultTTL            = 120 * time.Second
)

// reasonAlreadyCompleted is Iris's reason for a request id whose job has already finished.
const reasonAlreadyCompleted = "REQUEST_ALREADY_COMPLETED"

var (
	// ErrUnavailable means Iris or NATS could not be used and the caller asked to fail closed,
	// or the coordinator is disabled.
	ErrUnavailable = errors.New("jobs: iris unavailable")
	// ErrMissingRequestKey means Spec.RequestKey was empty and AllowRandomKey was not set.
	ErrMissingRequestKey = errors.New("jobs: request key is required")
)

// ErrResourceLocked is returned by Begin when another job holds the resource's lease.
type ErrResourceLocked struct {
	HolderJobID string
	HolderKind  string
}

func (e *ErrResourceLocked) Error() string {
	return fmt.Sprintf("jobs: resource is locked by job %s (%s)", e.HolderJobID, e.HolderKind)
}

// ErrAlreadyCompleted is returned by Begin when the request key belongs to a job that has
// already finished.
type ErrAlreadyCompleted struct {
	JobID string
}

func (e *ErrAlreadyCompleted) Error() string {
	return fmt.Sprintf("jobs: request already completed (job_id=%s)", e.JobID)
}

// TokenSource returns the bearer token the coordinator sends to Iris: the service's own
// internal service token. The library holds no secrets.
type TokenSource func(ctx context.Context) (string, error)

// Options configures a Coordinator. Every value comes from the service's configuration.
type Options struct {
	// Producer names the service in every job and update, such as hermes. Required.
	Producer string
	// JobsSubject is the subject updates are published on. Default "jobs.update".
	JobsSubject string
	// JobsStream is the stream every publish expects to land on. Default "JOBS".
	JobsStream string
	// CreateTimeout bounds one CreateJobWithLease attempt. Default 5s.
	CreateTimeout time.Duration
	// RenewTimeout bounds one RenewResourceLock call. Default 3s.
	RenewTimeout time.Duration
	// PublishTimeout bounds one update publish. Default 3s.
	PublishTimeout time.Duration
	// DefaultTTL is the lease time when Spec.TTL or a Renew ttl is zero. Default 120s.
	DefaultTTL time.Duration
	// Logger receives fail-open warnings and renewal failures. Default: no logging.
	Logger *zap.Logger
}

// Coordinator creates Iris jobs and publishes their updates. It is safe for concurrent use.
type Coordinator struct {
	iris   grpc.ClientConnInterface
	nc     *nats.Conn
	js     jetstream.JetStream
	tokens TokenSource
	opts   Options
	logger *zap.Logger
	now    func() time.Time
}

// NewCoordinator returns a Coordinator that calls Iris on iris and publishes on nc. With iris or
// nc nil, or an empty Options.Producer, it is disabled: Begin fails open (or returns
// ErrUnavailable when the Spec fails closed). A nil tokens sends no authorization metadata.
func NewCoordinator(iris grpc.ClientConnInterface, nc *nats.Conn, tokens TokenSource, opts Options) *Coordinator {
	if opts.JobsSubject == "" {
		opts.JobsSubject = DefaultJobsSubject
	}
	if opts.JobsStream == "" {
		opts.JobsStream = DefaultJobsStream
	}
	if opts.CreateTimeout <= 0 {
		opts.CreateTimeout = DefaultCreateTimeout
	}
	if opts.RenewTimeout <= 0 {
		opts.RenewTimeout = DefaultRenewTimeout
	}
	if opts.PublishTimeout <= 0 {
		opts.PublishTimeout = DefaultPublishTimeout
	}
	if opts.DefaultTTL <= 0 {
		opts.DefaultTTL = DefaultTTL
	}
	logger := opts.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	c := &Coordinator{
		tokens: tokens,
		opts:   opts,
		logger: logger.With(zap.String("component", "jobs"), zap.String("producer", opts.Producer)),
		now:    time.Now,
	}
	if opts.Producer == "" {
		c.logger.Error("jobs coordinator disabled: Options.Producer is empty")
		return c
	}
	if iris == nil || nc == nil {
		return c
	}
	js, err := jetstream.New(nc)
	if err != nil {
		c.logger.Error("jobs coordinator disabled: jetstream", zap.Error(err))
		return c
	}
	c.iris, c.nc, c.js = iris, nc, js
	return c
}

// Enabled reports whether the coordinator can create jobs.
func (c *Coordinator) Enabled() bool {
	return c != nil && c.iris != nil && c.js != nil
}

// Spec describes the job Begin creates.
type Spec struct {
	// Kind is the job kind, such as hermes.workflow.publish. Required.
	Kind string
	// ResourceType and ResourceID name the resource to lease. Required.
	ResourceType string
	ResourceID   string
	// RequestKey identifies the logical request across retries: RequestKeyFromContext, or a
	// Temporal workflow id. Required unless AllowRandomKey is set, in which case an empty key
	// becomes a fresh UUID and a retry is a new job.
	RequestKey     string
	AllowRandomKey bool
	// ClientID is the organisation, UserID the Entra object id of the person behind the work.
	ClientID string
	UserID   string
	// Data is the kind-specific payload shown with the job.
	Data map[string]any
	// TTL is the lease time; zero means Options.DefaultTTL.
	TTL time.Duration
	// ParentJobID makes this job a child of another.
	ParentJobID string
	// FailClosed makes Begin return ErrUnavailable instead of going ahead without a job when
	// Iris cannot be used.
	FailClosed bool
}

// JobRef identifies a job for the stateless helpers. It is JSON-encodable, so it can travel in a
// Temporal workflow's input.
type JobRef struct {
	JobID    string `json:"job_id"`
	ClientID string `json:"client_id"`
	Kind     string `json:"kind"`
	Producer string `json:"producer"`
}

// Begin creates the job and takes its lease. It returns:
//   - an Op for the new job, or for the live job an earlier delivery of the same request key
//     created (Op.Reentrant reports which);
//   - *ErrResourceLocked when another job holds the lease;
//   - *ErrAlreadyCompleted when the request key's job has already finished;
//   - ErrMissingRequestKey when no request key was given;
//   - when Iris cannot be used: a no-op Op (JobID "") and nil, or ErrUnavailable when
//     s.FailClosed is set.
//
// It retries once, with the same request key, on Unavailable or DeadlineExceeded, and records
// the job id on the request's audit collector.
func (c *Coordinator) Begin(ctx context.Context, s Spec) (*Op, error) {
	if s.RequestKey == "" {
		if !s.AllowRandomKey {
			return nil, ErrMissingRequestKey
		}
		s.RequestKey = uuid.NewString()
	}
	if s.Kind == "" || s.ResourceType == "" || s.ResourceID == "" {
		return nil, errors.New("jobs: Spec.Kind, ResourceType and ResourceID are required")
	}
	ttl := s.TTL
	if ttl <= 0 && c != nil {
		ttl = c.opts.DefaultTTL
	}
	ref := JobRef{ClientID: s.ClientID, Kind: s.Kind}
	if c != nil {
		ref.Producer = c.opts.Producer
	}
	noop := &Op{ref: ref, ttl: ttl}

	if !c.Enabled() {
		if s.FailClosed {
			return nil, ErrUnavailable
		}
		return noop, nil
	}
	if !c.nc.IsConnected() {
		// Only a terminal update releases a lease, so a lease is never taken without a
		// working publisher.
		return c.unavailable(s, noop, errors.New("nats is not connected"))
	}

	req := &iriswire.CreateJobWithLeaseRequest{
		ClientId:    s.ClientID,
		Kind:        s.Kind,
		Producer:    c.opts.Producer,
		RequestId:   s.RequestKey,
		Resource:    &iriswire.JobResource{Type: s.ResourceType, Id: s.ResourceID},
		UserId:      s.UserID,
		TtlSeconds:  int32(ttl / time.Second),
		ParentJobId: s.ParentJobID,
	}
	if len(s.Data) > 0 {
		data, err := structpb.NewStruct(s.Data)
		if err != nil {
			return nil, fmt.Errorf("jobs: encode Spec.Data: %w", err)
		}
		req.Data = data
	}

	resp := &iriswire.CreateJobWithLeaseResponse{}
	err := c.invoke(ctx, MethodCreateJobWithLease, c.opts.CreateTimeout, s.ClientID, req, resp)
	if retryable(err) {
		resp = &iriswire.CreateJobWithLeaseResponse{}
		err = c.invoke(ctx, MethodCreateJobWithLease, c.opts.CreateTimeout, s.ClientID, req, resp)
	}
	if err != nil {
		if jobID, ok := alreadyCompletedJobID(err); ok {
			audit.SetJobID(ctx, jobID)
			return nil, &ErrAlreadyCompleted{JobID: jobID}
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return c.unavailable(s, noop, err)
	}

	if !resp.GetAcquired() {
		h := resp.GetHolder()
		return nil, &ErrResourceLocked{HolderJobID: h.GetJobId(), HolderKind: h.GetKind()}
	}
	jobID := resp.GetJob().GetJobId()
	if jobID == "" {
		return c.unavailable(s, noop, errors.New("iris acquired the lease but returned no job id"))
	}
	ref.JobID = jobID
	audit.SetJobID(ctx, jobID)
	return &Op{c: c, ref: ref, ttl: ttl, reentrant: resp.GetReentrant()}, nil
}

// unavailable fails closed or open, as the Spec asks.
func (c *Coordinator) unavailable(s Spec, noop *Op, cause error) (*Op, error) {
	if s.FailClosed {
		return nil, fmt.Errorf("%w: %w", ErrUnavailable, cause)
	}
	c.logger.Warn("iris unavailable: continuing without a job",
		zap.String("kind", s.Kind), zap.String("resource_type", s.ResourceType), zap.Error(cause))
	return noop, nil
}

// Renew extends every lease the job holds, for ttl (zero means Options.DefaultTTL). renewed is
// false when the lease was already lost. A failure or a lost lease is logged; callers treat both
// as best effort. A ref with no job id does nothing.
func (c *Coordinator) Renew(ctx context.Context, ref JobRef, ttl time.Duration) (renewed bool, err error) {
	if ref.JobID == "" {
		return false, nil
	}
	if !c.Enabled() {
		return false, ErrUnavailable
	}
	if ttl <= 0 {
		ttl = c.opts.DefaultTTL
	}
	req := &iriswire.RenewResourceLockRequest{
		ClientId:   ref.ClientID,
		JobId:      ref.JobID,
		TtlSeconds: int32(ttl / time.Second),
	}
	resp := &iriswire.RenewResourceLockResponse{}
	if err := c.invoke(ctx, MethodRenewResourceLock, c.opts.RenewTimeout, ref.ClientID, req, resp); err != nil {
		c.logger.Warn("lease renewal failed", zap.String("job_id", ref.JobID), zap.Error(err))
		return false, err
	}
	if !resp.GetRenewed() {
		c.logger.Warn("lease lost", zap.String("job_id", ref.JobID))
	}
	return resp.GetRenewed(), nil
}

// PublishStatus publishes a job.status_changed update for ref at seq. errCode and errMessage set
// the job error when either is non-empty; data, when not nil, is sent whole. It is stateless, for
// Temporal activities: pass SeqRunning for running and TerminalSeq for a terminal status. A ref
// with no job id does nothing.
func (c *Coordinator) PublishStatus(ctx context.Context, ref JobRef, seq uint64, status, errCode, errMessage string, data map[string]any) error {
	if ref.JobID == "" {
		return nil
	}
	if !c.Enabled() {
		return ErrUnavailable
	}
	msg := &iriswire.JobUpdateMessage{
		JobId:    ref.JobID,
		Seq:      seq,
		Type:     UpdateTypeStatusChanged,
		ClientId: ref.ClientID,
		Producer: ref.Producer,
		Status:   status,
	}
	if errCode != "" || errMessage != "" {
		msg.Error = &iriswire.JobError{Code: errCode, Message: errMessage}
	}
	var encErr error
	if data != nil {
		msg.Data, encErr = structpb.NewStruct(data)
		if encErr != nil {
			// The update still goes out without its data: a terminal update is what releases
			// the lease.
			msg.Data = nil
			encErr = fmt.Errorf("jobs: encode update data: %w", encErr)
		}
	}
	return errors.Join(encErr, c.publish(ctx, msg))
}

var updateJSON = protojson.MarshalOptions{UseProtoNames: true}

// publish stamps and publishes one update. It is bounded by PublishTimeout and not by ctx's
// cancellation, so a terminal update still goes out after the request that asked for it ends.
func (c *Coordinator) publish(ctx context.Context, msg *iriswire.JobUpdateMessage) error {
	if msg.Producer == "" {
		msg.Producer = c.opts.Producer
	}
	msg.V = ContractVersion
	msg.Id = MessageID(msg.GetJobId(), msg.GetSeq())
	msg.Timestamp = c.now().UTC().Format(time.RFC3339)

	raw, err := updateJSON.Marshal(msg)
	if err != nil {
		return fmt.Errorf("jobs: encode update: %w", err)
	}
	m := nats.NewMsg(c.opts.JobsSubject)
	m.Data = raw
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(http.Header(m.Header)))

	pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.opts.PublishTimeout)
	defer cancel()
	if _, err := c.js.PublishMsg(pctx, m,
		jetstream.WithMsgID(msg.Id),
		jetstream.WithExpectStream(c.opts.JobsStream)); err != nil {
		c.logger.Warn("job update not published",
			zap.String("job_id", msg.GetJobId()), zap.Uint64("seq", msg.GetSeq()), zap.Error(err))
		return fmt.Errorf("jobs: publish update %s: %w", msg.Id, err)
	}
	return nil
}

// invoke makes one Iris call with its own timeout and the service token.
func (c *Coordinator) invoke(ctx context.Context, method string, timeout time.Duration, clientID string, req, resp any) error {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if c.tokens != nil {
		tok, err := c.tokens(cctx)
		if err != nil {
			return fmt.Errorf("jobs: service token: %w", err)
		}
		cctx = metadata.AppendToOutgoingContext(cctx, "authorization", "Bearer "+tok)
	}
	if clientID != "" {
		cctx = metadata.AppendToOutgoingContext(cctx, "x-client-id", clientID)
	}
	return c.iris.Invoke(cctx, method, req, resp)
}

func retryable(err error) bool {
	if err == nil {
		return false
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded:
		return true
	}
	return false
}

// alreadyCompletedJobID reports whether err is Iris's REQUEST_ALREADY_COMPLETED refusal and
// returns the finished job's id: from the google.rpc.ErrorInfo metadata "job_id" when present,
// else from the "(job_id=...)" suffix of the message, which is all an older Iris sends.
func alreadyCompletedJobID(err error) (string, bool) {
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		return "", false
	}
	matched := false
	jobID := ""
	for _, d := range st.Details() {
		info, ok := d.(*errdetails.ErrorInfo)
		if !ok {
			continue
		}
		if info.GetReason() == reasonAlreadyCompleted {
			matched = true
			if id := info.GetMetadata()["job_id"]; id != "" {
				jobID = id
			}
		}
	}
	msg := st.Message()
	const marker = "(job_id="
	if strings.Contains(msg, reasonAlreadyCompleted) || strings.Contains(msg, marker) {
		matched = true
	}
	if !matched {
		return "", false
	}
	if jobID == "" {
		if i := strings.Index(msg, marker); i >= 0 {
			rest := msg[i+len(marker):]
			if j := strings.IndexByte(rest, ')'); j >= 0 {
				jobID = rest[:j]
			}
		}
	}
	return jobID, true
}

// RequestKeyFromContext returns the incoming Idempotency-Key metadata, else X-Request-Id, or "".
func RequestKeyFromContext(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	for _, k := range []string{"idempotency-key", "x-request-id"} {
		for _, v := range md.Get(k) {
			if v != "" {
				return v
			}
		}
	}
	return ""
}
