package jobs

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/wehubfusion/Icarus/platform/jobs/iriswire"
)

// Op is the handle on one job created by Begin. Its update methods publish on NATS and return
// an error on failure, which callers treat as best effort. When Begin went ahead without a job
// (Iris unavailable, fail open), JobID is "" and every method does nothing.
type Op struct {
	c         *Coordinator
	ref       JobRef
	ttl       time.Duration
	reentrant bool
	progress  atomic.Uint64
}

func (o *Op) inactive() bool {
	return o == nil || o.c == nil || o.ref.JobID == ""
}

// JobID returns the job's id, or "" when no job was created.
func (o *Op) JobID() string {
	if o == nil {
		return ""
	}
	return o.ref.JobID
}

// Ref returns the job's reference, for a Temporal workflow's input or a later PublishStatus.
func (o *Op) Ref() JobRef {
	if o == nil {
		return JobRef{}
	}
	return o.ref
}

// Reentrant reports whether Begin adopted a live job that an earlier delivery of the same
// request key created, rather than creating one.
func (o *Op) Reentrant() bool {
	return o != nil && o.reentrant
}

// Running publishes status running at SeqRunning.
func (o *Op) Running(ctx context.Context) error {
	if o.inactive() {
		return nil
	}
	return o.c.PublishStatus(ctx, o.ref, SeqRunning, StatusRunning, "", "", nil)
}

// Progress publishes a job.progress update, at SeqProgressStart for the first call and one
// higher for each call after. percent is clamped to 0..100.
func (o *Op) Progress(ctx context.Context, percent int, message string) error {
	if o.inactive() {
		return nil
	}
	seq := SeqProgressStart + o.progress.Add(1) - 1
	if seq >= TerminalSeq {
		return errors.New("jobs: progress sequence exhausted")
	}
	percent = max(0, min(100, percent))
	return o.c.publish(ctx, &iriswire.JobUpdateMessage{
		JobId:    o.ref.JobID,
		Seq:      seq,
		Type:     UpdateTypeProgress,
		ClientId: o.ref.ClientID,
		Producer: o.ref.Producer,
		Progress: &iriswire.JobProgress{Percent: int32(percent), Message: message},
	})
}

// Succeed publishes status succeeded at TerminalSeq, with data sent whole when not nil.
func (o *Op) Succeed(ctx context.Context, data map[string]any) error {
	if o.inactive() {
		return nil
	}
	return o.c.PublishStatus(ctx, o.ref, TerminalSeq, StatusSucceeded, "", "", data)
}

// Fail publishes status failed at TerminalSeq with the given error code and message. The message
// is shown to the job's viewers: no payloads, secrets or personal data.
func (o *Op) Fail(ctx context.Context, code, message string) error {
	if o.inactive() {
		return nil
	}
	return o.c.PublishStatus(ctx, o.ref, TerminalSeq, StatusFailed, code, message, nil)
}

// Cancel publishes status canceled at TerminalSeq.
func (o *Op) Cancel(ctx context.Context) error {
	if o.inactive() {
		return nil
	}
	return o.c.PublishStatus(ctx, o.ref, TerminalSeq, StatusCanceled, "", "", nil)
}

// KeepAlive renews the job's lease every interval until stop is called or ctx is done. An
// interval of zero or less means a third of the lease time. A failed or lost renewal is logged
// and the loop carries on. stop waits for the loop to end and is safe to call more than once.
func (o *Op) KeepAlive(ctx context.Context, every time.Duration) (stop func()) {
	if o.inactive() {
		return func() {}
	}
	if every <= 0 {
		every = o.ttl / 3
	}
	if every <= 0 {
		every = o.c.opts.DefaultTTL / 3
	}
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if _, err := o.c.Renew(ctx, o.ref, o.ttl); err != nil && ctx.Err() == nil {
					o.c.logger.Debug("keep alive renewal failed", zap.String("job_id", o.ref.JobID), zap.Error(err))
				}
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			<-done
		})
	}
}
