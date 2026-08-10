package tests

import (
	"context"
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

	mu     sync.Mutex
	acked  bool
	nakked bool
	termed bool
}

func newMockMsg(subject string, data []byte) *mockMsg {
	return &mockMsg{subject: subject, data: data}
}

func (m *mockMsg) Data() []byte    { return m.data }
func (m *mockMsg) Subject() string { return m.subject }
func (m *mockMsg) Reply() string   { return "" }

func (m *mockMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{
		NumDelivered: 1,
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

	// activeConsumes tracks live Consume loops so tests can Stop them without
	// invoking ConsumeErrHandler (simulates nats.go stopping on consumer deleted).
	activeConsumes []*mockConsumeContext
	consumeStarts  int
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

// stopActiveConsumes stops all in-flight Consume loops without delivering an
// ErrHandler callback. Closed() fires so the runner supervision loop can restart.
func (m *MockJS) stopActiveConsumes() int {
	m.mu.Lock()
	cons := append([]*mockConsumeContext(nil), m.activeConsumes...)
	m.activeConsumes = nil
	m.mu.Unlock()
	for _, c := range cons {
		c.Stop()
	}
	return len(cons)
}

func (m *MockJS) consumeStartCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.consumeStarts
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

func (c *mockConsumer) Consume(handler jetstream.MessageHandler, opts ...jetstream.PullConsumeOpt) (jetstream.ConsumeContext, error) {
	cc := &mockConsumeContext{
		stopCh:   make(chan struct{}),
		closedCh: make(chan struct{}),
	}
	c.owner.mu.Lock()
	c.owner.consumeStarts++
	c.owner.activeConsumes = append(c.owner.activeConsumes, cc)
	c.owner.mu.Unlock()

	go func() {
		defer close(cc.closedCh)
		for {
			select {
			case <-cc.stopCh:
				return
			default:
			}
			msgs := c.owner.popMessages(10)
			if len(msgs) == 0 {
				select {
				case <-cc.stopCh:
					return
				case <-time.After(5 * time.Millisecond):
				}
				continue
			}
			for _, msg := range msgs {
				select {
				case <-cc.stopCh:
					return
				default:
					handler(msg)
				}
			}
		}
	}()
	return cc, nil
}

// mockConsumeContext implements jetstream.ConsumeContext.
type mockConsumeContext struct {
	stopCh   chan struct{}
	closedCh chan struct{}
	stopOnce sync.Once
}

func (c *mockConsumeContext) Stop() {
	c.stopOnce.Do(func() { close(c.stopCh) })
}

func (c *mockConsumeContext) Drain() { c.Stop() }

func (c *mockConsumeContext) Closed() <-chan struct{} { return c.closedCh }
