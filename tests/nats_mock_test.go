package tests

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/wehubfusion/Icarus/pkg/message"
)

// mockMsg is an in-memory implementation of jetstream.Msg for unit tests.
// Unimplemented interface methods panic via the embedded nil interface.
type mockMsg struct {
	jetstream.Msg

	subject string
	data    []byte

	mu           sync.Mutex
	acked        bool
	nakked       bool
	termed       bool
	nakDelays    []time.Duration
	numDelivered uint64 // 0 means 1
}

func newMockMsg(subject string, data []byte) *mockMsg {
	return &mockMsg{subject: subject, data: data}
}

func (m *mockMsg) Data() []byte    { return m.data }
func (m *mockMsg) Subject() string { return m.subject }
func (m *mockMsg) Reply() string   { return "" }

func (m *mockMsg) Metadata() (*jetstream.MsgMetadata, error) {
	delivered := m.numDelivered
	if delivered == 0 {
		delivered = 1
	}
	return &jetstream.MsgMetadata{
		NumDelivered: delivered,
		NumPending:   0,
		Sequence:     jetstream.SequencePair{Stream: 1, Consumer: 1},
	}, nil
}

func (m *mockMsg) Ack() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acked = true
	return nil
}

func (m *mockMsg) Nak() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nakked = true
	return nil
}

func (m *mockMsg) Term() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.termed = true
	return nil
}

func (m *mockMsg) InProgress() error { return nil }

func (m *mockMsg) NakWithDelay(d time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nakDelays = append(m.nakDelays, d)
	return nil
}

func (m *mockMsg) wasTermed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.termed
}

func (m *mockMsg) delayedNaks() []time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]time.Duration(nil), m.nakDelays...)
}

func (m *mockMsg) wasAcked() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.acked
}

func (m *mockMsg) wasNakked() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.nakked
}

// MockJS is a lightweight in-memory implementation of message.JSContext
// (the new nats.go/jetstream API surface) suitable for unit tests without a
// running NATS server.
type MockJS struct {
	mu        sync.Mutex
	queue     []jetstream.Msg
	streams   map[string]jetstream.StreamConfig
	consumers map[string]map[string]jetstream.ConsumerConfig

	published []publishedRecord

	// reportError, when set, is returned by Publish for result subjects.
	reportError error

	// consumerError simulates failures resolving the consumer (GetConsumer).
	// consumerErrorBudget == 0 means fail forever; otherwise fail that many times.
	consumerError       error
	consumerErrorBudget int
	consumerAttempts    int

	// fetchBatches records the batch size of every Fetch, so tests can check the runner never
	// asks for more messages than it has idle workers.
	fetchBatches []int
	// fetchErrors are returned, one per Fetch call, before any message is delivered.
	fetchErrors []error
	// consumerLookups counts Consumer calls, failed or not.
	consumerLookups int
}

type publishedRecord struct {
	subject string
	data    []byte
}

func NewMockJS() *MockJS {
	return &MockJS{
		streams:   make(map[string]jetstream.StreamConfig),
		consumers: make(map[string]map[string]jetstream.ConsumerConfig),
	}
}

// addMessage queues a message for delivery to consumers created from this mock.
func (m *MockJS) addMessage(msg *message.Message) {
	data, _ := msg.ToBytes()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queue = append(m.queue, newMockMsg("test.subject", data))
}

// addMessageOnAttempt queues msg as JetStream delivery attempt n and returns its handle.
func (m *MockJS) addMessageOnAttempt(msg *message.Message, n uint64) *mockMsg {
	data, _ := msg.ToBytes()
	jsMsg := newMockMsg("test.subject", data)
	jsMsg.numDelivered = n
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queue = append(m.queue, jsMsg)
	return jsMsg
}

// publishedResults returns every result message published so far.
func (m *MockJS) publishedResults(t interface{ Fatalf(string, ...any) }) []message.ResultMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []message.ResultMessage
	for _, p := range m.published {
		if p.subject != "result" && !strings.HasPrefix(p.subject, "result") {
			continue
		}
		var rm message.ResultMessage
		if err := json.Unmarshal(p.data, &rm); err != nil {
			t.Fatalf("decoding published result: %v", err)
		}
		out = append(out, rm)
	}
	return out
}

// addRawMessage queues a raw payload (e.g. malformed JSON) for delivery.
func (m *MockJS) addRawMessage(subject string, data []byte) *mockMsg {
	msg := newMockMsg(subject, data)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queue = append(m.queue, msg)
	return msg
}

func (m *MockJS) setReportError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reportError = err
}

func (m *MockJS) setConsumerError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.consumerError = err
	m.consumerErrorBudget = 0
}

func (m *MockJS) setConsumerErrorBudget(err error, budget int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.consumerError = err
	m.consumerErrorBudget = budget
}

// failNextFetches makes the next Fetch calls return errs, one each.
func (m *MockJS) failNextFetches(errs ...error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fetchErrors = append(m.fetchErrors, errs...)
}

// fetchBatchSizes returns the batch size of every Fetch so far.
func (m *MockJS) fetchBatchSizes() []int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]int(nil), m.fetchBatches...)
}

// queued returns how many messages are still undelivered.
func (m *MockJS) queued() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.queue)
}

// consumerResolutions returns how many times the runner looked its consumer up.
func (m *MockJS) consumerResolutions() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.consumerLookups
}

func (m *MockJS) publishedSubjects() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	subjects := make([]string, 0, len(m.published))
	for _, p := range m.published {
		subjects = append(subjects, p.subject)
	}
	return subjects
}

// Publish implements message.JSContext.
func (m *MockJS) Publish(ctx context.Context, subject string, payload []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.reportError != nil && (subject == "result" || strings.HasPrefix(subject, "result")) {
		return nil, m.reportError
	}
	m.published = append(m.published, publishedRecord{subject: subject, data: payload})
	return &jetstream.PubAck{Stream: "MOCK", Sequence: uint64(len(m.published))}, nil
}

// Stream implements message.JSContext.
func (m *MockJS) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cfg, ok := m.streams[name]; ok {
		return &mockStream{name: name, cfg: cfg}, nil
	}
	return nil, jetstream.ErrStreamNotFound
}

// CreateStream implements message.JSContext.
func (m *MockJS) CreateStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.streams[cfg.Name] = cfg
	return &mockStream{name: cfg.Name, cfg: cfg}, nil
}

// Consumer implements message.JSContext.
func (m *MockJS) Consumer(ctx context.Context, stream, consumer string) (jetstream.Consumer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.consumerLookups++
	if m.consumerError != nil {
		m.consumerAttempts++
		if m.consumerErrorBudget == 0 || m.consumerAttempts <= m.consumerErrorBudget {
			return nil, m.consumerError
		}
	}
	if streamConsumers, ok := m.consumers[stream]; ok {
		if cfg, ok := streamConsumers[consumer]; ok {
			return &mockConsumer{owner: m, stream: stream, cfg: cfg}, nil
		}
	}
	return nil, jetstream.ErrConsumerNotFound
}

// CreateConsumer implements message.JSContext.
func (m *MockJS) CreateConsumer(ctx context.Context, stream string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.consumers[stream] == nil {
		m.consumers[stream] = make(map[string]jetstream.ConsumerConfig)
	}
	m.consumers[stream][cfg.Durable] = cfg
	return &mockConsumer{owner: m, stream: stream, cfg: cfg}, nil
}

// popMessages removes and returns up to max queued messages.
func (m *MockJS) popMessages(max int) []jetstream.Msg {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.queue) == 0 {
		return nil
	}
	n := max
	if n <= 0 || n > len(m.queue) {
		n = len(m.queue)
	}
	msgs := make([]jetstream.Msg, n)
	copy(msgs, m.queue[:n])
	m.queue = m.queue[n:]
	return msgs
}

// mockStream implements jetstream.Stream via embedding; only CachedInfo is used.
type mockStream struct {
	jetstream.Stream

	name string
	cfg  jetstream.StreamConfig
}

func (s *mockStream) CachedInfo() *jetstream.StreamInfo {
	return &jetstream.StreamInfo{Config: s.cfg}
}

// mockConsumer implements jetstream.Consumer via embedding; the runner uses
// Consume and CachedInfo.
type mockConsumer struct {
	jetstream.Consumer

	owner  *MockJS
	stream string
	cfg    jetstream.ConsumerConfig
}

func (c *mockConsumer) CachedInfo() *jetstream.ConsumerInfo {
	return &jetstream.ConsumerInfo{
		Stream: c.stream,
		Name:   c.cfg.Durable,
		Config: c.cfg,
	}
}

// Fetch delivers up to batch queued messages, waiting briefly when none are queued, as a pull
// request that expires empty would.
func (c *mockConsumer) Fetch(batch int, opts ...jetstream.FetchOpt) (jetstream.MessageBatch, error) {
	c.owner.mu.Lock()
	c.owner.fetchBatches = append(c.owner.fetchBatches, batch)
	if len(c.owner.fetchErrors) > 0 {
		err := c.owner.fetchErrors[0]
		c.owner.fetchErrors = c.owner.fetchErrors[1:]
		c.owner.mu.Unlock()
		return nil, err
	}
	c.owner.mu.Unlock()

	msgs := c.owner.popMessages(batch)
	if len(msgs) == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	ch := make(chan jetstream.Msg, len(msgs))
	for _, m := range msgs {
		ch <- m
	}
	close(ch)
	return &mockBatch{msgs: ch}, nil
}

// mockBatch implements jetstream.MessageBatch.
type mockBatch struct {
	msgs chan jetstream.Msg
}

func (b *mockBatch) Messages() <-chan jetstream.Msg { return b.msgs }
func (b *mockBatch) Error() error                   { return nil }
