package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"

	"github.com/wehubfusion/Icarus/pkg/message"
)

// handOffTimeout bounds each NATS call of a hand-off: the re-publish, the queued write and the
// ack of the original. A var so tests can shorten it.
var handOffTimeout = 10 * time.Second

// publishMsgFunc publishes a message with headers to JetStream.
type publishMsgFunc func(ctx context.Context, m *nats.Msg) (*jetstream.PubAck, error)

// publisher returns how the runner re-publishes a handed-off unit, or nil when it cannot.
func (r *Runner) publisher() publishMsgFunc {
	if r.republish != nil {
		return r.republish
	}
	if r.client == nil {
		return nil
	}
	js := r.client.JetStream()
	if js == nil {
		return nil
	}
	return func(ctx context.Context, m *nats.Msg) (*jetstream.PubAck, error) {
		return js.PublishMsg(ctx, m)
	}
}

// canHandOff reports whether msg can be handed off: it is a JetStream delivery of an execution
// unit (the queued state needs the unit's key) and the runner can publish.
func (r *Runner) canHandOff(msg *message.Message, workflowID, runID, nodeID string) bool {
	return msg.GetJetStreamMsg() != nil && workflowID != "" && runID != "" && nodeID != "" && r.publisher() != nil
}

// headerInt reads a non-negative integer header, or 0.
func headerInt(jsMsg jetstream.Msg, key string) int {
	if jsMsg == nil {
		return 0
	}
	h := jsMsg.Headers()
	if h == nil {
		return 0
	}
	n, err := strconv.Atoi(h.Get(key))
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// handOffCount is how many times msg's unit has been handed off before this delivery.
func handOffCount(msg *message.Message) int {
	return headerInt(msg.GetJetStreamMsg(), HeaderHandOffs)
}

// handOffMsgID is the Nats-Msg-Id of the unit's n-th hand-off: the original id (Zeus's unit id)
// with the hand-off number, so a re-publish that is retried is stored once.
func handOffMsgID(jsMsg jetstream.Msg, executionID string, n int) string {
	base := ""
	if h := jsMsg.Headers(); h != nil {
		base = h.Get(jetstream.MsgIDHeader)
	}
	if i := strings.Index(base, ":handoff:"); i >= 0 {
		base = base[:i]
	}
	if base == "" {
		base = executionID
	}
	if base == "" {
		if md, err := jsMsg.Metadata(); err == nil && md != nil {
			base = fmt.Sprintf("%s:%d", md.Stream, md.Sequence.Stream)
		}
	}
	return fmt.Sprintf("%s:handoff:%d", base, n)
}

// handOff re-publishes msg's unit as a new message, records it as queued, and acks the original.
// It returns an error when the re-publish fails, having put back what it changed (the claim and
// this process's record of the unit), so the caller runs the unit here.
//
// The order matters because the copy can be delivered, to this process too, the moment it is
// published:
//  1. the claim becomes queued (no sequence yet), so the copy's delivery can take it over;
//  2. this process forgets it is running the unit, or the copy, delivered here, would be refused
//     as "still being handled" and nak'd with a delay, using an attempt;
//  3. the copy is published;
//  4. the copy's sequence is added to the queued claim, unless the copy has claimed it already;
//  5. the original is acked.
//
// Once the copy is published the unit is in the stream, so a failure in 4 or 5 is logged and not
// returned: a missing sequence only costs Zeus's view of the unit until it is claimed again, and an
// original that is redelivered meets the claim (queued once, then the copy's running or done).
func (r *Runner) handOff(msg *message.Message, workflowID, runID, nodeID, executionID string) error {
	jsMsg := msg.GetJetStreamMsg()
	publish := r.publisher()
	if jsMsg == nil || publish == nil {
		return fmt.Errorf("unit cannot be re-published")
	}
	count := handOffCount(msg) + 1
	attempts := deliverAttempt(msg)

	header := nats.Header{}
	for k, v := range jsMsg.Headers() {
		// Server-set and publish-control headers (Nats-Msg-Id, Nats-Expected-*) are not copied:
		// the copy is a new publish with its own id.
		if strings.HasPrefix(k, "Nats-") {
			continue
		}
		header[k] = append([]string(nil), v...)
	}
	header.Set(HeaderHandOffs, strconv.Itoa(count))
	header.Set(HeaderAttemptOffset, strconv.Itoa(attempts))
	header.Set(jetstream.MsgIDHeader, handOffMsgID(jsMsg, executionID, count))

	queuedRev := r.markQueued(msg, workflowID, runID, nodeID, executionID)
	r.clearLocalRunning(executionID)

	pubCtx, cancel := context.WithTimeout(context.Background(), handOffTimeout)
	ack, err := publish(pubCtx, &nats.Msg{Subject: jsMsg.Subject(), Data: jsMsg.Data(), Header: header})
	cancel()
	if err == nil && ack == nil {
		err = fmt.Errorf("no ack")
	}
	if err != nil {
		r.setLocalExecution(executionID, localRunning)
		r.restoreRunning(msg, workflowID, runID, nodeID, executionID, queuedRev)
		return fmt.Errorf("re-publish handed-off unit: %w", err)
	}

	r.setQueuedSeq(msg, workflowID, runID, nodeID, executionID, queuedRev, ack.Stream, jsMsg.Subject(), ack.Sequence)

	ackCtx, ackCancel := context.WithTimeout(context.Background(), handOffTimeout)
	defer ackCancel()
	if err := jsMsg.DoubleAck(ackCtx); err != nil {
		r.logger.Warn("Handed-off unit re-published but the original was not acked; its redelivery is dropped by the claim",
			zap.String("stream", r.stream),
			zap.String("execution_id", executionID),
			zap.Error(err))
	}
	r.logger.Info("Unit handed off to another replica",
		zap.String("stream", r.stream),
		zap.String("workflow_id", workflowID),
		zap.String("run_id", runID),
		zap.String("node_id", nodeID),
		zap.String("execution_id", executionID),
		zap.Int("handoffs", count),
		zap.Int("attempts_used", attempts),
		zap.Uint64("queued_seq", ack.Sequence),
		zap.Bool("duplicate", ack.Duplicate))
	return nil
}

// handOffEntry is the unit's EXECUTION_HEARTBEATS entry in state, written by this process.
func handOffEntry(msg *message.Message, workflowID, runID, nodeID, executionID, state string) executionHeartbeat {
	return executionHeartbeat{
		WorkflowID:  workflowID,
		RunID:       runID,
		NodeID:      nodeID,
		ExecutionID: executionID,
		Attempt:     deliverAttempt(msg),
		Pod:         os.Getenv("HOSTNAME"),
		StartedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		State:       state,
		Owner:       processToken,
	}
}

// markQueued turns this process's running claim into queued, with no sequence yet, and returns the
// entry's new revision, or 0 when it wrote nothing (no bucket, or the entry is not this process's
// running claim). With no entry at all it creates one.
func (r *Runner) markQueued(msg *message.Message, workflowID, runID, nodeID, executionID string) uint64 {
	kv := r.heartbeats(context.Background())
	if kv == nil {
		return 0
	}
	payload, err := json.Marshal(handOffEntry(msg, workflowID, runID, nodeID, executionID, heartbeatQueued))
	if err != nil {
		return 0
	}
	ctx, cancel := context.WithTimeout(context.Background(), heartbeatKVTimeout)
	defer cancel()
	key := executionHeartbeatKey(workflowID, runID, nodeID)
	var rev uint64
	entry, err := kv.Get(ctx, key)
	switch {
	case err == nil:
		var cur executionHeartbeat
		if json.Unmarshal(entry.Value(), &cur) != nil || cur.Owner != processToken ||
			cur.ExecutionID != executionID || (cur.State != heartbeatRunning && cur.State != "") {
			return 0
		}
		rev, err = kv.Update(ctx, key, payload, entry.Revision())
	case errors.Is(err, jetstream.ErrKeyNotFound):
		rev, err = kv.Create(ctx, key, payload)
	}
	if err != nil {
		r.logger.Warn("Failed to record handed-off unit as queued; Zeus may see it as silent until it is claimed",
			zap.String("workflow_id", workflowID), zap.String("run_id", runID), zap.String("node_id", nodeID), zap.Error(err))
		return 0
	}
	return rev
}

// setQueuedSeq adds the copy's stream sequence to the queued entry written at rev, so Zeus's sweep
// can tell the unit is still waiting in its stream. It writes nothing if the entry has moved on: the
// copy has already been claimed, and overwriting that claim would make it claimable again.
func (r *Runner) setQueuedSeq(msg *message.Message, workflowID, runID, nodeID, executionID string, rev uint64, stream, subject string, seq uint64) {
	kv := r.heartbeats(context.Background())
	if kv == nil || rev == 0 {
		return
	}
	hb := handOffEntry(msg, workflowID, runID, nodeID, executionID, heartbeatQueued)
	hb.QueuedStream, hb.QueuedSubject, hb.QueuedSeq = stream, subject, seq
	payload, err := json.Marshal(hb)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), heartbeatKVTimeout)
	defer cancel()
	if _, err := kv.Update(ctx, executionHeartbeatKey(workflowID, runID, nodeID), payload, rev); err != nil &&
		!errors.Is(err, jetstream.ErrKeyExists) {
		r.logger.Warn("Failed to record where the handed-off unit waits; Zeus may see it as silent until it is claimed",
			zap.String("workflow_id", workflowID), zap.String("run_id", runID), zap.String("node_id", nodeID), zap.Error(err))
	}
}

// restoreRunning puts back this process's running claim after a hand-off whose re-publish failed.
func (r *Runner) restoreRunning(msg *message.Message, workflowID, runID, nodeID, executionID string, rev uint64) {
	kv := r.heartbeats(context.Background())
	if kv == nil || rev == 0 {
		return
	}
	payload, err := json.Marshal(handOffEntry(msg, workflowID, runID, nodeID, executionID, heartbeatRunning))
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), heartbeatKVTimeout)
	defer cancel()
	if _, err := kv.Update(ctx, executionHeartbeatKey(workflowID, runID, nodeID), payload, rev); err != nil {
		r.logger.Warn("Failed to restore the claim after a failed hand-off; the heartbeat rewrites it",
			zap.String("workflow_id", workflowID), zap.String("run_id", runID), zap.String("node_id", nodeID), zap.Error(err))
	}
}
