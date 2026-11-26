// Package audit publishes audit events to a NATS JetStream stream.
//
// An Emitter builds each event (id, time, producer and sequence, trace id), enforces the size
// and field limits of the event contract, and publishes it binary encoded on
// <SubjectPrefix>.<Service>, with the event id as Nats-Msg-Id. It fails open: Emit never returns
// an error and never blocks for longer than Options.PublishTimeout. An event that cannot be
// published goes to a bounded queue served by one worker; when the queue is full the event is
// dropped and counted. The producer sequence increases for every event built, dropped or not, so
// a lost event shows as a gap where the stream is consumed.
//
// Publish is the one exception to failing open, for a caller that keeps its own durable copy of
// each event until it is acknowledged, such as a transactional outbox. It returns the result,
// queues and retries nothing, and sends the event without a producer sequence, so a retry after a
// failure never shows as a gap.
//
// The emitter never creates or updates the stream. The service that consumes the stream owns it,
// and a missing stream is a publish failure like any other, visible in the metrics.
//
// The package also holds the request-scoped Collector that the grpcaudit and httpaudit hooks put
// in a request's context, and that handlers fill with RecordDecision, SetJobID, SetResource,
// SetAttr and SetReason.
package audit

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/wehubfusion/Icarus/platform/audit/auditpb"
)

// Defaults for the Options fields left zero.
const (
	DefaultStream         = "AUDIT"
	DefaultSubjectPrefix  = "audit.v1"
	DefaultPublishTimeout = 2 * time.Second
	DefaultRetryQueueSize = 1024
	DefaultHeartbeatEvery = 60 * time.Second
)

// Producer lifecycle actions.
const (
	ActionProducerStarted   = "producer.started"
	ActionProducerHeartbeat = "producer.heartbeat"
	ActionProducerStopped   = "producer.stopped"
)

const (
	retryBackoffStart = time.Second
	retryBackoffMax   = 30 * time.Second
	maxServiceLen     = 32
)

// Mode says whether Emit waits for the stream to acknowledge an event.
type Mode int

const (
	// ModeSync publishes at once and waits for the PubAck, within Options.PublishTimeout. Use it
	// for writes, denials and system events. A failed publish goes to the retry queue.
	ModeSync Mode = iota
	// ModeAsync puts the event on the queue and returns at once. Use it for sensitive reads.
	ModeAsync
)

// Options configures an Emitter. Every value comes from the calling service's configuration.
type Options struct {
	// Service names the producer, such as hermes. Required. It is the last subject token and
	// the producer service in every event.
	Service string
	// Stream is the stream every publish expects to land on. Default "AUDIT".
	Stream string
	// SubjectPrefix is the subject before the service name. Default "audit.v1".
	SubjectPrefix string
	// PublishTimeout bounds one publish, and so how long Emit can block. Default 2s.
	PublishTimeout time.Duration
	// RetryQueueSize bounds the queue of events waiting to be published or retried. Default 1024.
	RetryQueueSize int
	// HeartbeatEvery is the interval between producer.heartbeat events. Default 60s.
	HeartbeatEvery time.Duration
	// Logger receives publish failures. Default: no logging.
	Logger *zap.Logger
	// MeterProvider supplies the meter for the emitter's metrics. Default: the OTel global.
	MeterProvider metric.MeterProvider
	// Now is the clock, for tests. Default time.Now.
	Now func() time.Time
}

// Stats is a snapshot of an emitter's counters.
type Stats struct {
	// Published counts events the stream acknowledged.
	Published uint64
	// Dropped counts events given up on: over the size cap, queue full, or still queued when
	// Close ran out of time.
	Dropped uint64
	// Retried counts publish attempts for events whose earlier attempt failed.
	Retried uint64
	// QueueDepth is the number of events waiting in the queue.
	QueueDepth int
	// Seq is the last producer sequence number handed out.
	Seq uint64
}

// queued is an encoded event waiting for the worker.
type queued struct {
	data   []byte
	msgID  string
	failed bool
}

// Emitter publishes audit events. It is safe for concurrent use. A nil Emitter, like one from
// NewNoopEmitter, does nothing.
type Emitter struct {
	noop bool

	js         jetstream.JetStream
	opts       Options
	subject    string
	instanceID []byte
	logger     *zap.Logger
	metrics    *instruments

	seq       atomic.Uint64
	published atomic.Uint64
	dropped   atomic.Uint64
	retried   atomic.Uint64

	queue chan *queued

	// qmu orders enqueues against Close: once closed is set, nothing more is queued.
	qmu    sync.RWMutex
	closed bool

	lifecycle    sync.Mutex
	started      bool
	stopped      bool
	hbStop       chan struct{}
	hbDone       chan struct{}
	draining     chan struct{}
	workerCtx    context.Context
	workerCancel context.CancelFunc
	workerDone   chan struct{}
}

// NewEmitter returns an emitter that publishes on nc. Call Start before use and Close at
// shutdown. It returns an error only for a nil connection or an invalid Options.Service.
func NewEmitter(nc *nats.Conn, opts Options) (*Emitter, error) {
	if nc == nil {
		return nil, errors.New("audit: nats connection is required")
	}
	if err := validateService(opts.Service); err != nil {
		return nil, err
	}
	if opts.Stream == "" {
		opts.Stream = DefaultStream
	}
	if opts.SubjectPrefix == "" {
		opts.SubjectPrefix = DefaultSubjectPrefix
	}
	opts.SubjectPrefix = strings.TrimSuffix(opts.SubjectPrefix, ".")
	if opts.PublishTimeout <= 0 {
		opts.PublishTimeout = DefaultPublishTimeout
	}
	if opts.RetryQueueSize <= 0 {
		opts.RetryQueueSize = DefaultRetryQueueSize
	}
	if opts.HeartbeatEvery <= 0 {
		opts.HeartbeatEvery = DefaultHeartbeatEvery
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	logger := opts.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("audit: jetstream: %w", err)
	}

	instance := uuid.New()
	e := &Emitter{
		js:         js,
		opts:       opts,
		subject:    opts.SubjectPrefix + "." + opts.Service,
		instanceID: instance[:],
		logger:     logger.With(zap.String("component", "audit"), zap.String("audit_instance", instance.String())),
		queue:      make(chan *queued, opts.RetryQueueSize),
		hbStop:     make(chan struct{}),
		hbDone:     make(chan struct{}),
		draining:   make(chan struct{}),
		workerDone: make(chan struct{}),
	}
	e.workerCtx, e.workerCancel = context.WithCancel(context.Background())
	e.metrics = newInstruments(opts.MeterProvider, opts.Service, func() int64 { return int64(len(e.queue)) }, e.logger)
	return e, nil
}

// NewNoopEmitter returns an emitter whose methods do nothing, for a service with audit turned
// off.
func NewNoopEmitter() *Emitter {
	return &Emitter{noop: true}
}

func validateService(s string) error {
	if s == "" {
		return errors.New("audit: Options.Service is required")
	}
	if len(s) > maxServiceLen {
		return fmt.Errorf("audit: Options.Service is longer than %d bytes", maxServiceLen)
	}
	if strings.ContainsAny(s, ".*> \t\r\n") {
		return errors.New("audit: Options.Service must be a single subject token")
	}
	return nil
}

func (e *Emitter) inactive() bool {
	return e == nil || e.noop
}

// Now returns the emitter's clock: Options.Now, else time.Now. The hooks use it for their rate
// limiter, so a test controls both with one clock.
func (e *Emitter) Now() time.Time {
	if e.inactive() || e.opts.Now == nil {
		return time.Now()
	}
	return e.opts.Now()
}

// Service returns Options.Service, or "" for a no-op emitter.
func (e *Emitter) Service() string {
	if e.inactive() {
		return ""
	}
	return e.opts.Service
}

// Start emits producer.started, then starts the heartbeat and the queue worker. The heartbeat
// stops when ctx is done or Close is called; the worker runs until Close. Calls after the first,
// or after Close, do nothing.
func (e *Emitter) Start(ctx context.Context) {
	if e.inactive() {
		return
	}
	e.lifecycle.Lock()
	if e.started || e.stopped {
		e.lifecycle.Unlock()
		return
	}
	e.started = true
	e.lifecycle.Unlock()

	go e.runWorker()
	e.emitSystem(ctx, ActionProducerStarted, nil)
	go e.runHeartbeat(ctx)
}

// Close stops the heartbeat, gives the worker until ctx is done to publish what is queued, drops
// and counts whatever is left, then emits producer.stopped with attrs final_seq (the stopped
// event's own sequence) and dropped. It returns an error if events were left in the queue or the
// stopped event could not be published. Calls after the first return nil.
func (e *Emitter) Close(ctx context.Context) error {
	if e.inactive() {
		return nil
	}
	e.lifecycle.Lock()
	if e.stopped {
		e.lifecycle.Unlock()
		return nil
	}
	e.stopped = true
	started := e.started
	e.lifecycle.Unlock()

	e.qmu.Lock()
	e.closed = true
	e.qmu.Unlock()

	var errs []error
	if started {
		close(e.hbStop)
		<-e.hbDone

		close(e.draining)
		select {
		case <-e.workerDone:
		case <-ctx.Done():
			e.workerCancel()
			<-e.workerDone
		}
	}
	e.workerCancel()

	left := 0
drain:
	for {
		select {
		case <-e.queue:
			left++
		default:
			break drain
		}
	}
	if left > 0 {
		e.drop(uint64(left), "close: queue not flushed in time")
		errs = append(errs, fmt.Errorf("audit: %d queued events dropped at close", left))
	}

	if started {
		seq := e.seq.Add(1)
		ev := systemEvent(e.opts.Service, ActionProducerStopped, map[string]string{
			"final_seq": strconv.FormatUint(seq, 10),
			"dropped":   strconv.FormatUint(e.dropped.Load(), 10),
		})
		data, msgID, ok := e.build(ctx, ev, seq)
		if ok {
			pctx := context.WithoutCancel(ctx)
			if err := e.publish(pctx, data, msgID); err != nil {
				e.drop(1, "close: producer.stopped not published")
				errs = append(errs, fmt.Errorf("audit: publish producer.stopped: %w", err))
			} else {
				e.countPublished()
			}
		}
	}
	return errors.Join(errs...)
}

// Emit builds ev and publishes it in the given mode. It fills event_id (a UUIDv7) when empty,
// occurred_at_ms when zero, producer (always), and trace_id from ctx's span when empty, then
// enforces the field limits and MaxEventBytes. It never returns an error and never blocks for
// longer than Options.PublishTimeout. ev itself is not modified.
func (e *Emitter) Emit(ctx context.Context, ev *auditpb.Event, mode Mode) {
	if e.inactive() {
		return
	}
	seq := e.seq.Add(1)
	data, msgID, ok := e.build(ctx, ev, seq)
	if !ok {
		return
	}
	if mode == ModeAsync {
		e.enqueue(&queued{data: data, msgID: msgID})
		return
	}
	if err := e.publish(context.WithoutCancel(ctx), data, msgID); err != nil {
		e.logger.Debug("audit publish failed, queued for retry", zap.Error(err), zap.Uint64("seq", seq))
		e.enqueue(&queued{data: data, msgID: msgID, failed: true})
		return
	}
	e.countPublished()
}

// Errors Publish returns besides the stream's own.
var (
	// ErrEventTooLarge is an event that cannot be trimmed to MaxEventBytes.
	ErrEventTooLarge = errors.New("audit: event over the size cap")
	// ErrEmitterInactive is a Publish on a no-op, nil or closed emitter: nothing was sent.
	ErrEmitterInactive = errors.New("audit: emitter is not publishing")
)

// Publish builds ev like Emit and publishes it, waiting for the stream's acknowledgement within
// Options.PublishTimeout, and returns the result. It is for a caller that keeps its own durable
// copy of the event until Publish succeeds, such as a transactional outbox, so it differs from
// Emit:
//   - nothing is queued or retried, and a failure is returned, never counted as a drop;
//   - the event carries no producer sequence (seq 0), so retrying it after a failure never shows
//     as a gap where the stream is consumed;
//   - a no-op, nil or closed emitter returns ErrEmitterInactive, so the caller keeps its copy.
//
// Give the event a stable event_id: it is the Nats-Msg-Id, so a retry after a lost
// acknowledgement is deduplicated by the stream within its duplicate window, and an id derived
// from what the event records (a UUIDv5) is deduplicated by the consumer after that.
func (e *Emitter) Publish(ctx context.Context, ev *auditpb.Event) error {
	if e.inactive() {
		return ErrEmitterInactive
	}
	e.qmu.RLock()
	closed := e.closed
	e.qmu.RUnlock()
	if closed {
		return ErrEmitterInactive
	}
	data, msgID, err := e.encode(ctx, ev, 0)
	if err != nil {
		return err
	}
	if err := e.publish(ctx, data, msgID); err != nil {
		return fmt.Errorf("audit: publish %s: %w", ev.GetAction(), err)
	}
	e.countPublished()
	return nil
}

// Stats returns the emitter's counters. A no-op emitter returns zeros.
func (e *Emitter) Stats() Stats {
	if e.inactive() {
		return Stats{}
	}
	return Stats{
		Published:  e.published.Load(),
		Dropped:    e.dropped.Load(),
		Retried:    e.retried.Load(),
		QueueDepth: len(e.queue),
		Seq:        e.seq.Load(),
	}
}

// emitSystem emits a platform-chain SYSTEM event about the producer itself.
func (e *Emitter) emitSystem(ctx context.Context, action string, attrs map[string]string) {
	e.Emit(ctx, systemEvent(e.opts.Service, action, attrs), ModeSync)
}

func systemEvent(service, action string, attrs map[string]string) *auditpb.Event {
	return &auditpb.Event{
		Action:   action,
		Category: auditpb.Category_CATEGORY_SYSTEM,
		Outcome:  auditpb.Outcome_OUTCOME_OK,
		Actor:    &auditpb.Actor{Type: auditpb.ActorType_ACTOR_TYPE_SYSTEM, Id: service},
		Attrs:    attrs,
	}
}

// build fills, limits and encodes a copy of in for Emit. ok is false when the event was dropped,
// which is counted.
func (e *Emitter) build(ctx context.Context, in *auditpb.Event, seq uint64) (data []byte, msgID string, ok bool) {
	data, msgID, err := e.encode(ctx, in, seq)
	switch {
	case errors.Is(err, ErrEventTooLarge):
		e.drop(1, "event over size cap")
		e.logger.Warn("audit event dropped: over size cap", zap.String("action", in.GetAction()),
			zap.Uint64("seq", seq), zap.Error(err))
		return nil, "", false
	case err != nil:
		e.drop(1, "encode failed")
		e.logger.Warn("audit event dropped: encode failed", zap.String("action", in.GetAction()), zap.Error(err))
		return nil, "", false
	}
	return data, msgID, true
}

// encode fills, limits and encodes a copy of in with the given producer sequence (0 for none).
// It returns ErrEventTooLarge when the event cannot be trimmed to MaxEventBytes.
func (e *Emitter) encode(ctx context.Context, in *auditpb.Event, seq uint64) (data []byte, msgID string, err error) {
	var ev *auditpb.Event
	if in == nil {
		ev = &auditpb.Event{}
	} else {
		ev = proto.Clone(in).(*auditpb.Event)
	}
	ev.ProtoReflect().SetUnknown(nil)

	id, err := uuid.FromBytes(ev.GetEventId())
	if err != nil {
		id, err = uuid.NewV7()
		if err != nil {
			id = uuid.New()
		}
		ev.EventId = id[:]
	}
	if ev.GetOccurredAtMs() == 0 {
		ev.OccurredAtMs = e.Now().UnixMilli()
	}
	ev.Producer = &auditpb.Producer{
		Service:    e.opts.Service,
		InstanceId: e.instanceID,
		Seq:        seq,
	}
	if len(ev.GetTraceId()) == 0 {
		if sc := trace.SpanContextFromContext(ctx); sc.HasTraceID() {
			tid := sc.TraceID()
			ev.TraceId = tid[:]
		}
	}

	enforceLimits(ev)
	if !fitSize(ev) {
		return nil, "", fmt.Errorf("%w: %s is %d bytes", ErrEventTooLarge, ev.GetAction(), proto.Size(ev))
	}

	data, err = proto.MarshalOptions{Deterministic: true}.Marshal(ev)
	if err != nil {
		return nil, "", fmt.Errorf("audit: encode %s: %w", ev.GetAction(), err)
	}
	return data, id.String(), nil
}

// publish sends one encoded event and waits for its PubAck, within PublishTimeout.
func (e *Emitter) publish(ctx context.Context, data []byte, msgID string) error {
	pctx, cancel := context.WithTimeout(ctx, e.opts.PublishTimeout)
	defer cancel()
	_, err := e.js.Publish(pctx, e.subject, data,
		jetstream.WithMsgID(msgID),
		jetstream.WithExpectStream(e.opts.Stream))
	return err
}

// enqueue hands an event to the worker without blocking, dropping it when the queue is full or
// the emitter is closed.
func (e *Emitter) enqueue(q *queued) {
	e.qmu.RLock()
	defer e.qmu.RUnlock()
	if e.closed {
		e.drop(1, "emitter closed")
		return
	}
	select {
	case e.queue <- q:
	default:
		e.drop(1, "queue full")
	}
}

func (e *Emitter) runWorker() {
	defer close(e.workerDone)
	for {
		var q *queued
		select {
		case q = <-e.queue:
		case <-e.draining:
			select {
			case q = <-e.queue:
			default:
				return
			}
		case <-e.workerCtx.Done():
			return
		}
		e.deliver(q)
	}
}

// deliver publishes one queued event, retrying with backoff (1s doubling to 30s) until it is
// acknowledged or the worker is cancelled. A message the server can never take is dropped.
func (e *Emitter) deliver(q *queued) {
	wait := retryBackoffStart
	for {
		if q.failed {
			e.retried.Add(1)
			e.metrics.retried(e.workerCtx)
		}
		err := e.publish(e.workerCtx, q.data, q.msgID)
		if err == nil {
			e.countPublished()
			return
		}
		if errors.Is(err, nats.ErrMaxPayload) {
			e.drop(1, "message larger than the server allows")
			return
		}
		if e.workerCtx.Err() != nil {
			e.drop(1, "close: retry abandoned")
			return
		}
		e.logger.Debug("audit retry failed", zap.Error(err), zap.Duration("next_wait", wait))
		q.failed = true
		t := time.NewTimer(wait)
		select {
		case <-t.C:
		case <-e.workerCtx.Done():
			t.Stop()
			e.drop(1, "close: retry abandoned")
			return
		}
		wait *= 2
		if wait > retryBackoffMax {
			wait = retryBackoffMax
		}
	}
}

func (e *Emitter) runHeartbeat(ctx context.Context) {
	defer close(e.hbDone)
	t := time.NewTicker(e.opts.HeartbeatEvery)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			e.emitSystem(ctx, ActionProducerHeartbeat, map[string]string{
				"dropped": strconv.FormatUint(e.dropped.Load(), 10),
			})
		case <-ctx.Done():
			return
		case <-e.hbStop:
			return
		}
	}
}

func (e *Emitter) countPublished() {
	e.published.Add(1)
	e.metrics.published(context.Background())
}

func (e *Emitter) drop(n uint64, why string) {
	e.dropped.Add(n)
	e.metrics.dropped(context.Background(), int64(n))
	e.logger.Debug("audit events dropped", zap.Uint64("count", n), zap.String("reason", why))
}
