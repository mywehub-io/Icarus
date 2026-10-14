package runner

import (
	"context"
	"errors"
)

// Gate decides when a runner may take more work. A host that runs several runners in one process
// gives each a Gate backed by one shared view of the process's capacity (memory, disk, CPU), so a
// busy runner can use capacity an idle one is not using, instead of each runner owning a fixed
// pool of workers.
//
// With a Gate the runner has no worker pool: it asks the Gate how many units it may take, fetches
// at most that many, and runs each in its own goroutine. Work the Gate does not admit stays
// undelivered in the stream, where no ack deadline runs, no delivery attempt is used, and another
// replica with room can take it.
//
// Implementations must be safe for concurrent use.
type Gate interface {
	// Acquire blocks until the runner may take at least one more unit, and returns how many it
	// may take, from 1 to max. It returns an error only when ctx ends. Every unit taken, and every
	// slot not used, is given back with Release.
	Acquire(ctx context.Context, max int) (int, error)
	// Release gives back n slots taken with Acquire.
	Release(n int)
}

// WithGate makes the runner take work only as the Gate admits it, in place of a fixed worker
// pool. Config.WorkerCount is ignored.
func WithGate(g Gate) RunnerOption {
	return func(r *Runner) {
		r.gate = g
	}
}

// ConsumerObserver is an optional extension of UnitObserver. When the registered UnitObserver
// implements it, the runner reads its consumer's state every consumerObserveInterval and reports
// it, so a host can export its backlog without a separate NATS exporter. numPending is the number
// of messages the consumer has not delivered yet.
type ConsumerObserver interface {
	ConsumerObserved(runner string, numPending uint64, numAckPending int)
}

// ErrHandOff is returned by a Processor (wrapped or not) to hand the unit to another replica
// instead of running it here, because this process cannot start it soon enough. It is honoured
// only when HandOffAllowed(ctx) is true; otherwise the runner treats it as any other error.
//
// The runner re-publishes the unit as a new message with its delivery attempts carried over
// (HeaderAttemptOffset), records it in EXECUTION_HEARTBEATS as queued with the new message's
// stream sequence (so Zeus's liveness sweep can still find it), and acks the original. A hand-off
// therefore never uses up a delivery attempt. If the re-publish fails, Process is called again
// with hand-off not allowed, and the unit runs here.
var ErrHandOff = errors.New("runner: hand the unit off to another replica")

// Headers the runner sets on a handed-off unit.
const (
	// HeaderHandOffs counts how many times the unit has been handed off.
	HeaderHandOffs = "Icarus-Handoffs"
	// HeaderAttemptOffset is the number of delivery attempts the unit used before its last
	// hand-off. The unit's attempt is its JetStream NumDelivered plus this.
	HeaderAttemptOffset = "Icarus-Attempt-Offset"
)

type handOffCtxKey struct{}

type handOffState struct {
	allowed bool
	count   int
}

// HandOffAllowed reports whether a Processor called with ctx may return ErrHandOff.
func HandOffAllowed(ctx context.Context) bool {
	s, _ := ctx.Value(handOffCtxKey{}).(handOffState)
	return s.allowed
}

// HandOffCount returns how many times the unit being processed with ctx has already been handed
// off, so a Processor can stop handing off a unit that keeps coming back.
func HandOffCount(ctx context.Context) int {
	s, _ := ctx.Value(handOffCtxKey{}).(handOffState)
	return s.count
}

func withHandOff(ctx context.Context, allowed bool, count int) context.Context {
	return context.WithValue(ctx, handOffCtxKey{}, handOffState{allowed: allowed, count: count})
}

// ContextWithHandOff returns ctx as the runner passes it to Process: allowed says whether the
// processor may return ErrHandOff, and count how often the unit has been handed off already. For a
// processor's tests, and for a host that calls a Processor itself.
func ContextWithHandOff(ctx context.Context, allowed bool, count int) context.Context {
	return withHandOff(ctx, allowed, count)
}
