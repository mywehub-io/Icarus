package runner

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"

	"github.com/wehubfusion/Icarus/pkg/message"
)

// NATS does not check the reply subject of a request, so a client allowed to send requests can
// make the JetStream server publish its answer to any subject, and a stream capturing that subject
// stores it. Another customer's runner can use that to put a message in this runner's work stream
// or over one of its EXECUTION_HEARTBEATS entries. message.IsReflection recognises such answers;
// this file applies it to deliveries and heartbeat reads.

// consumerFilters returns the subjects cons delivers: its configured filter subjects, else the
// filter the runner was built with, else nil (the whole stream, which the runner cannot check).
func consumerFilters(cons jetstream.Consumer, configured string) []string {
	if info := cons.CachedInfo(); info != nil {
		if len(info.Config.FilterSubjects) > 0 {
			return append([]string(nil), info.Config.FilterSubjects...)
		}
		if info.Config.FilterSubject != "" {
			return []string{info.Config.FilterSubject}
		}
	}
	if configured != "" {
		return []string{configured}
	}
	return nil
}

// subjectMatches reports whether subject matches filter, whose tokens may be * (one token) or a
// final > (one or more tokens).
func subjectMatches(filter, subject string) bool {
	ft := strings.Split(filter, ".")
	st := strings.Split(subject, ".")
	for i, f := range ft {
		if f == ">" && i == len(ft)-1 {
			return len(st) > i
		}
		if i >= len(st) {
			return false
		}
		if f != "*" && f != st[i] {
			return false
		}
	}
	return len(st) == len(ft)
}

// admitDelivery reports whether a fetched message may be processed as a unit. It refuses:
//   - a delivery from another stream or consumer (redirected to this runner's fetch inbox): it is
//     not acknowledged, since its acknowledgement belongs to that consumer;
//   - a reflection (message.IsReflection) or a subject outside the consumer's filters: it is
//     terminated, so it is never redelivered, executed or answered with a result.
func (r *Runner) admitDelivery(jsMsg jetstream.Msg, filters []string) bool {
	subject := jsMsg.Subject()
	if md, err := jsMsg.Metadata(); err == nil && md != nil &&
		((md.Stream != "" && md.Stream != r.stream) || (md.Consumer != "" && md.Consumer != r.consumer)) {
		r.logger.Warn("Refusing a delivery from another consumer; not acknowledged",
			zap.String("stream", r.stream),
			zap.String("consumer", r.consumer),
			zap.String("subject", subject),
			zap.String("delivered_by_stream", md.Stream),
			zap.String("delivered_by_consumer", md.Consumer))
		return false
	}
	reason := ""
	if bad, why := message.IsReflection(subject, jsMsg.Headers(), jsMsg.Data()); bad {
		reason = why
	} else if len(filters) > 0 {
		matched := false
		for _, f := range filters {
			if subjectMatches(f, subject) {
				matched = true
				break
			}
		}
		if !matched {
			reason = fmt.Sprintf("subject is outside the consumer's filter %q", strings.Join(filters, ","))
		}
	}
	if reason == "" {
		return true
	}
	r.logger.Warn("Refusing a delivered message that is not a unit of work; terminating it",
		zap.String("stream", r.stream),
		zap.String("consumer", r.consumer),
		zap.String("subject", subject),
		zap.String("reason", reason))
	if err := jsMsg.Term(); err != nil {
		r.logger.Warn("Failed to terminate the refused message", zap.String("stream", r.stream), zap.Error(err))
	}
	return false
}

// heartbeatKeyRe is the key syntax the nats.go KV accepts.
var heartbeatKeyRe = regexp.MustCompile(`^[-/_=\.a-zA-Z0-9]+$`)

// guardedHeartbeats is the EXECUTION_HEARTBEATS bucket with a Get that can see the entry's
// headers. The KV API hides them, so Get reads the entry with a direct get by subject
// ($JS.API.DIRECT.GET.KV_<bucket>.$KV.<bucket>.<key>, which a scoped runner may send for its own
// keys), and checks the answer with message.IsReflection. The server adds its own Nats-Subject
// (the key's subject) after the stored headers, so a stored reflection, which carries the
// Nats-Subject of whatever was read to make it, shows a second, different value.
//
// A reflected entry is returned as a reflectedEntry: it has the stored revision, so a write on
// that revision replaces it, but no value, so nothing in it (its owner least of all) is read.
// Every other method is the bucket's own.
type guardedHeartbeats struct {
	jetstream.KeyValue
	bucket  string
	request func(ctx context.Context, subject string) (*nats.Msg, error)
	logger  *zap.Logger
	// noDirect is logged once when the bucket answers no direct gets and reads fall back to the
	// KV's own Get, which cannot see headers.
	noDirect sync.Once
}

// guardHeartbeats wraps kv, opened on conn, in guardedHeartbeats. With no connection (tests) it
// returns kv itself.
func (r *Runner) guardHeartbeats(kv jetstream.KeyValue, conn *nats.Conn) jetstream.KeyValue {
	if kv == nil || conn == nil {
		return kv
	}
	return &guardedHeartbeats{
		KeyValue: kv,
		bucket:   executionHeartbeatBucket,
		request: func(ctx context.Context, subject string) (*nats.Msg, error) {
			return conn.RequestWithContext(ctx, subject, nil)
		},
		logger: r.logger,
	}
}

// heartbeatEntry is one EXECUTION_HEARTBEATS entry read by guardedHeartbeats.
type heartbeatEntry struct {
	bucket    string
	key       string
	value     []byte
	revision  uint64
	created   time.Time
	reflected string
}

func (e *heartbeatEntry) Bucket() string                  { return e.bucket }
func (e *heartbeatEntry) Key() string                     { return e.key }
func (e *heartbeatEntry) Value() []byte                   { return e.value }
func (e *heartbeatEntry) Revision() uint64                { return e.revision }
func (e *heartbeatEntry) Created() time.Time              { return e.created }
func (e *heartbeatEntry) Delta() uint64                   { return 0 }
func (e *heartbeatEntry) Operation() jetstream.KeyValueOp { return jetstream.KeyValuePut }
func (e *heartbeatEntry) reflection() string              { return e.reflected }

// reflectedEntry reports whether entry is a reflection a guardedHeartbeats read found. The claim
// and the hand-off treat such an entry as no entry, and write over it on its revision.
func reflectedEntry(entry jetstream.KeyValueEntry) bool {
	e, ok := entry.(interface{ reflection() string })
	return ok && e.reflection() != ""
}

// Get reads key's latest entry with a direct get. A key the KV would refuse, or a bucket that
// answers no direct gets (direct gets off, or the bucket gone), goes to the KV's own Get, which
// reports the error the callers already handle.
func (g *guardedHeartbeats) Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
	if key == "" || key[0] == '.' || key[len(key)-1] == '.' || !heartbeatKeyRe.MatchString(key) {
		return g.KeyValue.Get(ctx, key)
	}
	subject := "$KV." + g.bucket + "." + key
	resp, err := g.request(ctx, "$JS.API.DIRECT.GET.KV_"+g.bucket+"."+subject)
	if errors.Is(err, nats.ErrNoResponders) {
		g.noDirect.Do(func() {
			g.logger.Warn("EXECUTION_HEARTBEATS answers no direct gets; reading entries without the reflection check",
				zap.String("bucket", g.bucket))
		})
		return g.KeyValue.Get(ctx, key)
	}
	if err != nil {
		return nil, err
	}
	entry, err := heartbeatEntryFrom(g.bucket, key, subject, resp)
	if err == nil && entry.reflected != "" {
		g.logger.Warn("EXECUTION_HEARTBEATS entry was not written by a runner; treating it as absent",
			zap.String("bucket", g.bucket),
			zap.String("key", key),
			zap.Strings("nats_stream", resp.Header.Values("Nats-Stream")),
			zap.String("reason", entry.reflected))
	}
	return entry, err
}

// heartbeatEntryFrom turns a direct get answer for subject into an entry. Of repeated
// Nats-Sequence and Nats-Time-Stamp values the last is the server's: it appends its own after the
// stored headers. A missing key, or a delete or purge marker, is jetstream.ErrKeyNotFound, as from
// the KV's own Get.
func heartbeatEntryFrom(bucket, key, subject string, resp *nats.Msg) (*heartbeatEntry, error) {
	h := resp.Header
	seqs := h.Values("Nats-Sequence")
	if len(seqs) == 0 {
		// No stored message: the server's own answer, a status with no body.
		switch status := h.Get("Status"); status {
		case "404":
			return nil, jetstream.ErrKeyNotFound
		case "":
			return nil, errors.New("direct get of an EXECUTION_HEARTBEATS entry: answer has no Nats-Sequence")
		default:
			return nil, fmt.Errorf("direct get of an EXECUTION_HEARTBEATS entry: status %s %s", status, h.Get("Description"))
		}
	}
	rev, err := strconv.ParseUint(seqs[len(seqs)-1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("direct get of an EXECUTION_HEARTBEATS entry: bad Nats-Sequence: %w", err)
	}
	stamps := h.Values("Nats-Time-Stamp")
	if len(stamps) == 0 {
		return nil, errors.New("direct get of an EXECUTION_HEARTBEATS entry: answer has no Nats-Time-Stamp")
	}
	created, err := time.Parse(time.RFC3339Nano, stamps[len(stamps)-1])
	if err != nil {
		return nil, fmt.Errorf("direct get of an EXECUTION_HEARTBEATS entry: bad Nats-Time-Stamp: %w", err)
	}
	entry := &heartbeatEntry{bucket: bucket, key: key, revision: rev, created: created}
	if bad, reason := message.IsReflection(subject, h, resp.Data); bad {
		entry.reflected = reason
		return entry, nil
	}
	switch h.Get("KV-Operation") {
	case "DEL", "PURGE":
		return nil, jetstream.ErrKeyNotFound
	}
	if h.Get(jetstream.MarkerReasonHeader) != "" {
		return nil, jetstream.ErrKeyNotFound
	}
	entry.value = resp.Data
	return entry, nil
}
