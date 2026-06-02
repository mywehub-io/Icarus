package message

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	sdkerrors "github.com/wehubfusion/Icarus/pkg/errors"
	"go.uber.org/zap"
)

// JSContext defines the minimal subset of JetStream operations the service depends on.
// This allows tests to provide a mock without requiring a running NATS server.
type JSContext interface {
	Publish(subj string, data []byte, opts ...nats.PubOpt) (*nats.PubAck, error)
	PullSubscribe(subj, durable string, opts ...nats.SubOpt) (JSSubscription, error)
	StreamInfo(stream string) (*nats.StreamInfo, error)
	AddStream(cfg *nats.StreamConfig) (*nats.StreamInfo, error)
	ConsumerInfo(stream, consumer string) (*nats.ConsumerInfo, error)
	AddConsumer(stream string, cfg *nats.ConsumerConfig) (*nats.ConsumerInfo, error)
}

// JSSubscription abstracts operations used by the SDK from a subscription.
// Implemented by the real nats.Subscription via adapter and by test doubles.
type JSSubscription interface {
	Unsubscribe() error
	Drain() error
	IsValid() bool
	Pending() (int, int, error)
	Fetch(batch int, opts ...nats.PullOpt) ([]*nats.Msg, error)
}

// WrapNATSJetStream adapts a nats.JetStreamContext to the JSContext interface.
func WrapNATSJetStream(js nats.JetStreamContext) JSContext {
	return &natsJSAdapter{js: js}
}

type natsJSAdapter struct {
	js nats.JetStreamContext
}

func (a *natsJSAdapter) Publish(subj string, data []byte, opts ...nats.PubOpt) (*nats.PubAck, error) {
	return a.js.Publish(subj, data, opts...)
}

func (a *natsJSAdapter) PullSubscribe(subj, durable string, opts ...nats.SubOpt) (JSSubscription, error) {
	sub, err := a.js.PullSubscribe(subj, durable, opts...)
	if err != nil {
		return nil, err
	}
	return &natsSubAdapter{sub: sub}, nil
}

func (a *natsJSAdapter) StreamInfo(stream string) (*nats.StreamInfo, error) {
	return a.js.StreamInfo(stream)
}

func (a *natsJSAdapter) AddStream(cfg *nats.StreamConfig) (*nats.StreamInfo, error) {
	return a.js.AddStream(cfg)
}

func (a *natsJSAdapter) ConsumerInfo(stream, consumer string) (*nats.ConsumerInfo, error) {
	return a.js.ConsumerInfo(stream, consumer)
}

func (a *natsJSAdapter) AddConsumer(stream string, cfg *nats.ConsumerConfig) (*nats.ConsumerInfo, error) {
	return a.js.AddConsumer(stream, cfg)
}

type natsSubAdapter struct {
	sub *nats.Subscription
}

func (s *natsSubAdapter) Unsubscribe() error         { return s.sub.Unsubscribe() }
func (s *natsSubAdapter) Drain() error               { return s.sub.Drain() }
func (s *natsSubAdapter) IsValid() bool              { return s.sub.IsValid() }
func (s *natsSubAdapter) Pending() (int, int, error) { return s.sub.Pending() }
func (s *natsSubAdapter) Fetch(batch int, opts ...nats.PullOpt) ([]*nats.Msg, error) {
	return s.sub.Fetch(batch, opts...)
}

// MessageService provides methods for publishing and managing messages over JetStream.
// All operations use JetStream exclusively with proper acknowledgment handling.
type MessageService struct {
	js                JSContext
	logger            *zap.Logger
	maxDeliver        int               // Maximum number of delivery attempts before giving up (default: 5)
	publishMaxRetries int               // Maximum number of retry attempts for publish operations (default: 3)
	resultStream          string            // JetStream stream name for publishing results (e.g., RESULTS_UAT)
	resultSubjectPrefix   string            // Result subject prefix (e.g., result_uat)
	tenantEnvironmentID   string            // Pod ENVIRONMENT_ID for tenant Elysium (empty = central)
	blobStorage           BlobStorageClient // Blob storage for large results
	// inactiveThreshold sets ConsumerConfig.InactiveThreshold on durables created via
	// EnsureConsumer. Zero (default) disables auto-GC: the durable persists until it is
	// explicitly deleted. Non-zero values let JetStream delete the durable after the
	// configured idle period (no pulls/acks). Intended for tenant pods whose lifecycle
	// is shorter than the platform's; central/shared consumers should leave this at 0.
	inactiveThreshold     time.Duration
}

// BlobStorageClient interface for storing large results
type BlobStorageClient interface {
	UploadResult(ctx context.Context, blobPath string, data []byte, metadata map[string]string) (string, error)
	DownloadResult(ctx context.Context, blobURL string) ([]byte, error)
}

// SetBlobStorage sets the blob storage client for large results
func (s *MessageService) SetBlobStorage(bs BlobStorageClient) {
	s.blobStorage = bs
}

// NewMessageService creates a new message service with the given JetStream context.
// resultSubject is the result subject prefix (e.g. result_uat); publish appends .<env>.<executionID>.
// tenantEnvironmentID is the pod-level ENVIRONMENT_ID for tenant runners (may be empty).
func NewMessageService(js JSContext, maxDeliver int, publishMaxRetries int, resultStream string, resultSubject string, tenantEnvironmentID string) (*MessageService, error) {
	if js == nil {
		return nil, fmt.Errorf("JetStream context cannot be nil")
	}

	// Use default if not set or invalid
	if maxDeliver == 0 {
		maxDeliver = 5 // Default: retry up to 5 times (2.5 minutes with 30s AckWait)
	}

	if publishMaxRetries == 0 {
		publishMaxRetries = 3 // Default: 3 retries for publish operations
	}

	if resultStream == "" {
		resultStream = "RESULTS" // Default result stream
	}

	if resultSubject == "" {
		resultSubject = "result" // Default result subject prefix
	}

	logger, _ := zap.NewProduction()
	return &MessageService{
		js:                  js,
		logger:              logger,
		maxDeliver:          maxDeliver,
		publishMaxRetries:   publishMaxRetries,
		resultStream:        resultStream,
		resultSubjectPrefix: resultSubject,
		tenantEnvironmentID: tenantEnvironmentID,
	}, nil
}

// SetLogger sets a custom zap logger for the message service
func (s *MessageService) SetLogger(logger *zap.Logger) {
	if logger != nil {
		s.logger = logger
	}
}

// SetInactiveThreshold configures ConsumerConfig.InactiveThreshold for durables
// created via EnsureConsumer. A positive duration enables JetStream auto-deletion
// of the durable after the configured idle period; zero (the default) disables
// auto-GC and the durable persists until explicitly removed.
//
// Recommended usage: tenant pods that may be decommissioned set a positive value
// (e.g. 72h) so orphan durables are reclaimed automatically; central/shared pods
// leave this at zero.
//
// Note: this only affects durables created after the call. Existing durables
// retain whatever threshold was set when they were first created.
func (s *MessageService) SetInactiveThreshold(d time.Duration) {
	if s == nil {
		return
	}
	if d < 0 {
		d = 0
	}
	s.inactiveThreshold = d
}

// EnsureStream creates the JetStream stream if it doesn't exist, or validates it exists.
// This is a public method that can be called by runners and other components.
func (s *MessageService) EnsureStream(streamName string) error {
	// Check if stream exists
	streamInfo, err := s.js.StreamInfo(streamName)
	if err != nil {
		// Stream doesn't exist, create it
		if err == nats.ErrStreamNotFound {
			s.logger.Info("Creating JetStream stream",
				zap.String("stream", streamName))

			streamConfig := &nats.StreamConfig{
				Name:     streamName,
				Subjects: []string{streamSubjectPattern(streamName)},
				Storage:  nats.FileStorage,
				MaxAge:   24 * time.Hour,
				MaxMsgs:  100000,
				Replicas: 1,
			}

			_, err = s.js.AddStream(streamConfig)
			if err != nil {
				return fmt.Errorf("failed to create stream '%s': %w", streamName, err)
			}

			s.logger.Info("Successfully created JetStream stream",
				zap.String("stream", streamName),
				zap.Strings("subjects", streamConfig.Subjects),
				zap.Duration("max_age", streamConfig.MaxAge),
				zap.Int64("max_msgs", streamConfig.MaxMsgs))
		} else {
			return fmt.Errorf("failed to get stream info for '%s': %w", streamName, err)
		}
	} else {
		// Stream exists, log its status
		s.logger.Info("JetStream stream already exists",
			zap.String("stream", streamName),
			zap.Uint64("messages", streamInfo.State.Msgs))
	}

	return nil
}

// EnsureConsumer creates the JetStream consumer if it doesn't exist, or validates it exists.
// filterSubject when non-empty sets FilterSubject on the durable consumer (tenant/default routing).
func (s *MessageService) EnsureConsumer(streamName, consumerName, filterSubject string) error {
	// Try to get consumer info first
	consumerInfo, err := s.js.ConsumerInfo(streamName, consumerName)
	if err != nil {
		// Consumer doesn't exist, create it
		if err == nats.ErrConsumerNotFound {
			s.logger.Info("Creating JetStream consumer",
				zap.String("stream", streamName),
				zap.String("consumer", consumerName))

			consumerConfig := &nats.ConsumerConfig{
				Durable:           consumerName,
				FilterSubject:     filterSubject,
				AckPolicy:         nats.AckExplicitPolicy,
				DeliverPolicy:     nats.DeliverAllPolicy,
				MaxAckPending:     1000,
				MaxDeliver:        s.maxDeliver,
				InactiveThreshold: s.inactiveThreshold,
			}

			_, err = s.js.AddConsumer(streamName, consumerConfig)
			if err != nil {
				return fmt.Errorf("failed to create consumer '%s' in stream '%s': %w", consumerName, streamName, err)
			}

			s.logger.Info("Successfully created JetStream consumer",
				zap.String("stream", streamName),
				zap.String("consumer", consumerName),
				zap.Int("max_deliver", s.maxDeliver),
				zap.Duration("inactive_threshold", s.inactiveThreshold))
		} else {
			return fmt.Errorf("failed to get consumer info for '%s' in stream '%s': %w", consumerName, streamName, err)
		}
	} else {
		// Consumer exists, log its status
		s.logger.Info("JetStream consumer already exists",
			zap.String("stream", streamName),
			zap.String("consumer", consumerName),
			zap.Uint64("pending", consumerInfo.NumPending))
	}

	return nil
}

// ensureStreamForSubject ensures a stream exists that can handle the given subject.
// For result subjects, it uses the configured resultStream.
// For other subjects, it extracts the stream name from the subject (first segment before dot)
// and creates the stream if it doesn't exist.
func (s *MessageService) ensureStreamForSubject(subject string) error {
	var streamName string
	var isResultSubject bool

	// Check if this is a result subject (prefix match)
	if s.resultSubjectPrefix != "" && (subject == s.resultSubjectPrefix || strings.HasPrefix(subject, s.resultSubjectPrefix+".")) {
		// Use the configured result stream
		streamName = s.resultStream
		isResultSubject = true
		s.logger.Debug("Using configured result stream",
			zap.String("stream", streamName),
			zap.String("subject", subject))
	} else {
		// Extract stream name from subject (first part before dot)
		streamName = subject
		if idx := len(subject); idx > 0 {
			// Find first dot to extract stream name
			for i, c := range subject {
				if c == '.' {
					streamName = subject[:i]
					break
				}
			}
		}
	}

	// Check if stream exists
	_, err := s.js.StreamInfo(streamName)
	if err != nil {
		// Stream doesn't exist
		if err == nats.ErrStreamNotFound {
			// Create stream (both result streams and regular streams)
			s.logger.Info("Creating JetStream stream for subject",
				zap.String("stream", streamName),
				zap.String("subject", subject),
				zap.Bool("is_result_stream", isResultSubject))

			// For result subjects, use the configured subject pattern
			// For other subjects, derive pattern from subject
			var subjectPattern string
			if isResultSubject {
				// Result subjects use the configured subject with > wildcard
				// e.g., "result.uat.>" matches "result.uat", "result.uat.X", etc.
				subjectPattern = fmt.Sprintf("%s.>", subject)
			} else {
				// Regular subjects use stream name derived pattern
				// e.g., "HTTP_REQUESTS_UAT.>" for stream "HTTP_REQUESTS_UAT"
				subjectPattern = fmt.Sprintf("%s.>", streamName)
			}

			streamConfig := &nats.StreamConfig{
				Name:     streamName,
				Subjects: []string{subjectPattern},
				Storage:  nats.FileStorage,
				MaxAge:   24 * time.Hour,
				MaxMsgs:  100000,
				Replicas: 1,
			}

			_, err = s.js.AddStream(streamConfig)
			if err != nil {
				return fmt.Errorf("failed to create stream '%s' for subject '%s': %w", streamName, subject, err)
			}

			s.logger.Info("Successfully created JetStream stream",
				zap.String("stream", streamName),
				zap.String("subject_pattern", subjectPattern),
				zap.Bool("is_result_stream", isResultSubject))
		} else {
			return fmt.Errorf("failed to get stream info for '%s': %w", streamName, err)
		}
	}

	return nil
}

// getMessageIdentifier creates a unique identifier for logging purposes
func (s *MessageService) getMessageIdentifier(msg *Message) string {
	// Prefer correlation ID if available
	if msg.CorrelationID != "" {
		return fmt.Sprintf("correlation:%s", msg.CorrelationID)
	}
	if msg.Workflow != nil {
		return fmt.Sprintf("workflow:%s/run:%s", msg.Workflow.WorkflowID, msg.Workflow.RunID)
	}
	if msg.Node != nil {
		return fmt.Sprintf("node:%s", msg.Node.NodeID)
	}
	// Check Payload for execution ID
	if msg.Payload != nil && msg.Payload.ExecutionID != "" {
		return fmt.Sprintf("execution:%s", msg.Payload.ExecutionID)
	}
	return fmt.Sprintf("timestamp:%s", msg.CreatedAt)
}

// Publish publishes a message to the specified subject using JetStream.
// The message is persisted according to the stream's configuration.
// If no stream exists for the subject, one will be created automatically.
// Returns an error if the publish fails.
func (s *MessageService) Publish(ctx context.Context, subject string, msg *Message) error {
	if subject == "" {
		s.logger.Error("Publish failed: subject cannot be empty")
		return sdkerrors.NewValidationError("subject cannot be empty", "INVALID_SUBJECT", nil)
	}

	if msg == nil {
		s.logger.Error("Publish failed: message cannot be nil")
		return sdkerrors.NewValidationError("message cannot be nil", "INVALID_MESSAGE", nil)
	}

	// Ensure a stream exists for this subject
	if err := s.ensureStreamForSubject(subject); err != nil {
		s.logger.Error("Failed to ensure stream exists",
			zap.String("subject", subject),
			zap.Error(err))
		return sdkerrors.NewInternalError("", "failed to ensure stream exists", "STREAM_ENSURE_FAILED", err)
	}

	s.logger.Debug("Publishing message",
		zap.String("subject", subject),
		zap.String("message_identifier", s.getMessageIdentifier(msg)))

	data, err := msg.ToBytes()
	if err != nil {
		s.logger.Error("Failed to marshal message",
			zap.String("subject", subject),
			zap.String("message_identifier", s.getMessageIdentifier(msg)),
			zap.Error(err))
		return sdkerrors.NewInternalError("", "failed to marshal message", "MARSHAL_FAILED", err)
	}

	// Create a channel to handle publish result
	resultCh := make(chan error, 1)

	go func() {
		_, err := s.js.Publish(subject, data)
		resultCh <- err
	}()

	select {
	case <-ctx.Done():
		s.logger.Warn("Publish cancelled",
			zap.String("subject", subject),
			zap.String("message_identifier", s.getMessageIdentifier(msg)),
			zap.Error(ctx.Err()))
		return fmt.Errorf("publish cancelled: %w", ctx.Err())
	case err := <-resultCh:
		if err != nil {
			s.logger.Error("Failed to publish message to JetStream",
				zap.String("subject", subject),
				zap.String("message_identifier", s.getMessageIdentifier(msg)),
				zap.Error(err))
			return sdkerrors.NewInternalError("", "failed to publish message to JetStream", "PUBLISH_FAILED", err)
		}
		s.logger.Info("Message published successfully",
			zap.String("subject", subject),
			zap.String("message_identifier", s.getMessageIdentifier(msg)))
		return nil
	}
}

// PullMessages pulls messages from a JetStream pull-based consumer.
// This method fetches messages in batches on demand, providing explicit flow control.
//
// Messages are NOT automatically acknowledged - the caller must handle acknowledgment
// by calling Ack(), Nak(), or Term() on the returned messages as appropriate.
// Use this method when you want explicit control over when messages are fetched and acknowledged.
//
// Parameters:
//   - stream: The name of the JetStream stream
//   - consumer: The name of the durable consumer
//   - batchSize: The maximum number of messages to fetch (defaults to 10 if <= 0)
//
// Returns the fetched messages or an error if the operation fails.
// Note: Returns empty slice (not error) when no messages are available within timeout.
func (s *MessageService) PullMessages(ctx context.Context, stream, consumer string, batchSize int) ([]*Message, error) {
	if stream == "" || consumer == "" {
		s.logger.Error("PullMessages failed: stream and consumer names are required")
		return nil, fmt.Errorf("stream and consumer names are required")
	}

	if batchSize <= 0 {
		batchSize = 10
	}

	s.logger.Debug("Pulling messages",
		zap.String("stream", stream),
		zap.String("consumer", consumer),
		zap.Int("batch_size", batchSize))

	// Create a channel to handle pull result
	type result struct {
		msgs []*Message
		err  error
	}
	resultCh := make(chan result, 1)

	go func() {
		// Bind to existing consumer
		sub, err := s.js.PullSubscribe("", consumer, nats.Bind(stream, consumer))
		if err != nil {
			resultCh <- result{err: err}
			return
		}
		defer sub.Unsubscribe()

		// Fetch messages with timeout - use context deadline if available, otherwise default to 3 seconds
		timeout := 3 * time.Second
		if deadline, ok := ctx.Deadline(); ok {
			if remaining := time.Until(deadline); remaining > 0 && remaining < timeout {
				timeout = remaining
			}
		}

		natsMessages, err := sub.Fetch(batchSize, nats.MaxWait(timeout))
		if err != nil {
			// Check if this is a timeout error - this is normal when no messages are available
			if err == nats.ErrTimeout {
				// Return empty slice for timeout, not an error
				resultCh <- result{msgs: []*Message{}}
				return
			}
			resultCh <- result{err: err}
			return
		}

		messages := make([]*Message, 0, len(natsMessages))
		for _, natsMsg := range natsMessages {
			msg, err := FromNATSMsg(natsMsg)
			if err != nil {
				deliverCount := ""
				if md, mdErr := natsMsg.Metadata(); mdErr == nil && md != nil {
					deliverCount = strconv.FormatUint(md.NumDelivered, 10)
				}
				s.logger.Warn("Dropping malformed pulled message; NAK for redelivery",
					zap.String("stream", stream),
					zap.String("consumer", consumer),
					zap.String("subject", natsMsg.Subject),
					zap.String("jetstream_deliver_count", deliverCount),
					zap.Error(err))
				_ = natsMsg.Nak()
				continue
			}
			// Do NOT acknowledge - let the application handle acknowledgment
			// Store the NATS message reference in the Message for later acknowledgment
			msg.natsMsg = natsMsg
			AttachJetStreamMetadata(msg, natsMsg)
			messages = append(messages, msg)
		}

		resultCh <- result{msgs: messages}
	}()

	select {
	case <-ctx.Done():
		// Use debug level for graceful shutdown, warn for unexpected cancellation
		if ctx.Err() == context.Canceled {
			s.logger.Debug("Pull messages cancelled during shutdown",
				zap.String("stream", stream),
				zap.String("consumer", consumer))
		} else {
			s.logger.Warn("Pull messages cancelled",
				zap.String("stream", stream),
				zap.String("consumer", consumer),
				zap.Error(ctx.Err()))
		}
		return nil, fmt.Errorf("pull cancelled: %w", ctx.Err())
	case res := <-resultCh:
		if res.err != nil {
			s.logger.Error("Failed to pull messages from JetStream",
				zap.String("stream", stream),
				zap.String("consumer", consumer),
				zap.Error(res.err))
			return nil, sdkerrors.NewInternalError("", "failed to pull messages from JetStream", "PULL_FAILED", res.err)
		}
		return res.msgs, nil
	}
}

// PublishResult publishes a ResultMessage to the result stream using JetStream.
// This is used for reporting unit execution results back to Zeus.
func (s *MessageService) PublishResult(ctx context.Context, resultMsg *ResultMessage) error {
	if s.resultSubjectPrefix == "" {
		s.logger.Error("PublishResult failed: result subject not configured")
		return sdkerrors.NewValidationError("result subject not configured", "INVALID_CONFIG", nil)
	}

	if resultMsg == nil {
		s.logger.Error("PublishResult failed: result message cannot be nil")
		return sdkerrors.NewValidationError("result message cannot be nil", "INVALID_MESSAGE", nil)
	}

	envID := s.tenantEnvironmentID
	if envID == "" && resultMsg.EnvironmentID != "" {
		envID = resultMsg.EnvironmentID
	}
	applyEnv := envID != ""
	publishSubject := composeSubject(s.resultSubjectPrefix, envID, resultMsg.ExecutionID, applyEnv)

	// Ensure result stream exists
	if err := s.ensureStreamForSubject(publishSubject); err != nil {
		s.logger.Error("Failed to ensure result stream exists",
			zap.String("stream", s.resultStream),
			zap.String("subject", publishSubject),
			zap.Error(err))
		return sdkerrors.NewInternalError("", "failed to ensure result stream exists", "STREAM_ENSURE_FAILED", err)
	}

	s.logger.Debug("Publishing result message",
		zap.String("execution_id", resultMsg.ExecutionID),
		zap.String("workflow_id", resultMsg.WorkflowID),
		zap.String("node_id", resultMsg.NodeID),
		zap.String("status", resultMsg.Status),
		zap.String("subject", publishSubject))

	data, err := resultMsg.ToBytes()
	if err != nil {
		s.logger.Error("Failed to marshal result message",
			zap.String("execution_id", resultMsg.ExecutionID),
			zap.Error(err))
		return sdkerrors.NewInternalError("", "failed to marshal result message", "MARSHAL_FAILED", err)
	}

	// Retry logic for critical result publishing
	var publishErr error
	var pubAck *nats.PubAck
	for attempt := 1; attempt <= s.publishMaxRetries; attempt++ {
		pubAck, publishErr = s.js.Publish(publishSubject, data)
		if publishErr == nil {
			break
		}

		if attempt < s.publishMaxRetries {
			s.logger.Warn("Failed to publish result, retrying",
				zap.String("execution_id", resultMsg.ExecutionID),
				zap.Int("attempt", attempt),
				zap.Int("max_retries", s.publishMaxRetries),
				zap.Error(publishErr))
			// Brief backoff before retry
			time.Sleep(time.Duration(attempt) * time.Second)
		}
	}

	if publishErr != nil {
		s.logger.Error("Failed to publish result after all retries",
			zap.String("execution_id", resultMsg.ExecutionID),
			zap.String("workflow_id", resultMsg.WorkflowID),
			zap.String("node_id", resultMsg.NodeID),
			zap.Int("attempts", s.publishMaxRetries),
			zap.Error(publishErr))
		return sdkerrors.NewInternalError("", "failed to publish result after retries", "PUBLISH_FAILED", publishErr)
	}

	seq := uint64(0)
	stream := ""
	if pubAck != nil {
		seq = pubAck.Sequence
		stream = pubAck.Stream
	}
	s.logger.Info("Successfully published result message",
		zap.String("execution_id", resultMsg.ExecutionID),
		zap.String("workflow_id", resultMsg.WorkflowID),
		zap.String("node_id", resultMsg.NodeID),
		zap.String("status", resultMsg.Status),
		zap.String("subject", publishSubject),
		zap.String("jetstream_stream", stream),
		zap.Uint64("jetstream_stream_sequence", seq))

	return nil
}

// ReportSuccess publishes unit execution result to JetStream result stream.
// For results <1.5MB, includes full payload inline. For larger results, stores in
// blob storage and includes blob reference.
func (s *MessageService) ReportSuccess(ctx context.Context, resultMessage Message, msg *nats.Msg) error {
	startTime := time.Now()

	if ctx.Err() != nil {
		return fmt.Errorf("report success cancelled: %w", ctx.Err())
	}

	// Extract execution metadata from Payload (single source of truth)
	if resultMessage.Payload == nil {
		s.logger.Error("Missing payload for success report")
		if msg != nil {
			_ = msg.Nak()
		}
		return fmt.Errorf("missing payload")
	}

	executionID := resultMessage.Payload.ExecutionID
	workflowID := resultMessage.Payload.WorkflowID
	runID := resultMessage.Payload.RunID
	nodeID := resultMessage.Payload.NodeID
	correlationID := resultMessage.CorrelationID

	if executionID == "" {
		s.logger.Error("Missing execution_id for success report")
		if msg != nil {
			_ = msg.Nak()
		}
		return fmt.Errorf("missing execution_id")
	}

	if workflowID == "" || runID == "" {
		s.logger.Error("Missing workflow metadata for success report",
			zap.String("workflow_id", workflowID),
			zap.String("run_id", runID),
			zap.String("execution_id", executionID),
			zap.Bool("has_workflow_struct", resultMessage.Workflow != nil),
			zap.Bool("has_node_struct", resultMessage.Node != nil))
		if msg != nil {
			_ = msg.Nak()
		}
		return fmt.Errorf("missing workflow metadata")
	}

	s.logger.Debug("Preparing to publish success result",
		zap.String("workflow_id", workflowID),
		zap.String("run_id", runID),
		zap.String("execution_id", executionID),
		zap.String("node_id", nodeID),
		zap.Bool("has_workflow_struct", resultMessage.Workflow != nil),
		zap.Bool("has_node_struct", resultMessage.Node != nil))

	// Extract plugin type and execution time from metadata
	pluginType := resultMessage.Metadata["plugin_type"]
	var executionTimeMs int64
	if execTimeStr := resultMessage.Metadata["execution_time_ms"]; execTimeStr != "" {
		if execTime, err := strconv.ParseInt(execTimeStr, 10, 64); err == nil {
			executionTimeMs = execTime
		}
	}

	// Create result message
	resultMsg := NewResultMessage(executionID, workflowID, runID, nodeID, "success")
	if resultMessage.Metadata != nil {
		resultMsg.EnvironmentID = resultMessage.Metadata["environment_id"]
	}
	if correlationID != "" {
		resultMsg.WithCorrelationID(correlationID)
	}
	if pluginType != "" {
		resultMsg.WithPluginType(pluginType)
	}
	if executionTimeMs > 0 {
		resultMsg.WithExecutionTime(executionTimeMs)
	}
	eventsJSON := resultMessage.Metadata["events"]
	if eventsJSON != "" {
		resultMsg.WithEvents(json.RawMessage(eventsJSON))
	}

	// Respect resolver's decision - use whatever it returned (blob or inline)
	if resultMessage.Payload.BlobReference != nil {
		// Resolver decided to use blob storage
		s.logger.Info("Publishing result with blob reference from resolver",
			zap.String("execution_id", executionID),
			zap.String("blob_url", resultMessage.Payload.BlobReference.URL),
			zap.Int("size_bytes", resultMessage.Payload.BlobReference.SizeBytes))

		resultMsg.WithBlobReference(resultMessage.Payload.BlobReference)
		resultMsg.ResultSize = resultMessage.Payload.BlobReference.SizeBytes
	} else if resultMessage.Payload.HasInlineData() {
		// Resolver decided to use inline data
		inlineData := resultMessage.Payload.GetInlineData()
		resultSize := len(inlineData)

		s.logger.Info("Publishing result with inline data from resolver",
			zap.String("execution_id", executionID),
			zap.Int("size_bytes", resultSize))

		resultMsg.WithInlineResult(json.RawMessage(inlineData))
		resultMsg.ResultSize = resultSize
	} else {
		s.logger.Error("Payload has neither blob reference nor inline data")
		if msg != nil {
			_ = msg.Nak()
		}
		return fmt.Errorf("invalid payload: no data or blob reference")
	}

	// Publish result to JetStream
	if err := s.PublishResult(ctx, resultMsg); err != nil {
		s.logger.Error("Failed to publish result to JetStream",
			zap.String("workflow_id", workflowID),
			zap.String("execution_id", executionID),
			zap.String("run_id", runID),
			zap.Error(err))

		// Report the publish failure as an error
		publishErr := fmt.Errorf("failed to publish result: %w", err)
		if reportErr := s.ReportError(ctx, executionID, workflowID, runID, correlationID, publishErr, msg); reportErr != nil {
			s.logger.Error("Failed to report publish error (cascading failure)",
				zap.String("execution_id", executionID),
				zap.String("workflow_id", workflowID),
				zap.String("run_id", runID),
				zap.Error(reportErr))
		}

		if msg != nil {
			_ = msg.Nak() // Retry on publish failure
		}
		return publishErr
	}

	publishDuration := time.Since(startTime)
	s.logger.Info("Successfully published result to JetStream",
		zap.String("workflow_id", workflowID),
		zap.String("run_id", runID),
		zap.String("execution_id", executionID),
		zap.Duration("publish_duration", publishDuration),
		zap.Int("payload_size", resultMsg.ResultSize),
		zap.Bool("used_blob_reference", resultMsg.HasBlobReference()))

	// Acknowledge the source message
	if msg != nil {
		reportSuccessTotalMs := time.Since(startTime).Milliseconds()
		s.logger.Info("JetStream source message ack after successful result publish",
			zap.String("workflow_id", workflowID),
			zap.String("run_id", runID),
			zap.String("execution_id", executionID),
			zap.String("node_id", nodeID),
			zap.String("jetstream_deliver_count", jetStreamDeliverCountStr(msg)),
			zap.Int64("report_success_total_ms", reportSuccessTotalMs),
			zap.Duration("publish_duration", publishDuration))
		if err := msg.Ack(); err != nil {
			s.logger.Error("Failed to acknowledge source message after successful result publish (may cause redelivery/duplicate results)",
				zap.String("workflow_id", workflowID),
				zap.String("run_id", runID),
				zap.String("execution_id", executionID),
				zap.String("node_id", nodeID),
				zap.String("jetstream_deliver_count", jetStreamDeliverCountStr(msg)),
				zap.String("jetstream_source_ack_action", "ack_after_success_result_publish_failed"),
				zap.Error(err))
			return fmt.Errorf("failed to acknowledge: %w", err)
		}
	}

	return nil
}

// ReportError publishes error result to JetStream result stream.
//
// Error Classification:
//   - Internal errors (transient failures): NAK the message for retry
//   - Other errors (permanent failures like BadRequest, NotFound, etc.): ACK the message to prevent redelivery
//
// Parameters:
//   - executionID: The unique identifier for this execution (required)
//   - workflowID: The unique identifier of the workflow that failed
//   - runID: The unique identifier of this specific workflow execution run
//   - correlationID: Optional correlation ID for tracking
//   - err: The error that occurred (can be *AppError or regular error)
//   - msg: NATS message to acknowledge/nak after error reporting (can be nil)
//
// Returns an error if the result cannot be published.
func (s *MessageService) ReportError(ctx context.Context, executionID, workflowID, runID, correlationID string, err error, msg *nats.Msg) error {
	startTime := time.Now()

	if ctx.Err() != nil {
		return fmt.Errorf("report error cancelled: %w", ctx.Err())
	}

	if executionID == "" {
		s.logger.Warn("Missing executionID for error report",
			zap.String("jetstream_deliver_count", jetStreamDeliverCountStr(msg)),
			zap.String("jetstream_source_ack_action", "nak_missing_execution_id"))
		if msg != nil {
			_ = msg.Nak()
		}
		return fmt.Errorf("missing executionID")
	}
	if workflowID == "" || runID == "" {
		s.logger.Warn("Missing workflow_id or run_id for error report",
			zap.String("workflow_id", workflowID),
			zap.String("run_id", runID),
			zap.String("jetstream_deliver_count", jetStreamDeliverCountStr(msg)),
			zap.String("jetstream_source_ack_action", "nak_missing_workflow_context"))
		if msg != nil {
			_ = msg.Nak()
		}
		return fmt.Errorf("workflow_id and run_id are required for error report")
	}

	// Determine if transient or permanent
	isTransient := true
	errorMsg := err.Error()
	errorCode := "INTERNAL_ERROR"
	errorType := "internal"

	if appErr, ok := err.(*sdkerrors.AppError); ok {
		isTransient = (appErr.Type == sdkerrors.Internal)
		errorCode = appErr.Code
		if errorCode == "" {
			errorCode = "APP_ERROR"
		}

		// Map error type
		switch appErr.Type {
		case sdkerrors.BadRequest:
			errorType = "bad_request"
		case sdkerrors.NotFound:
			errorType = "not_found"
		case sdkerrors.Unauthorized:
			errorType = "unauthorized"
		case sdkerrors.Conflict:
			errorType = "conflict"
		case sdkerrors.ValidationFailed:
			errorType = "validation_failed"
		case sdkerrors.PermissionDenied:
			errorType = "permission_denied"
		case sdkerrors.Internal:
			errorType = "internal"
		default:
			errorType = "internal"
		}
	}

	s.logger.Info("Publishing error result",
		zap.String("execution_id", executionID),
		zap.String("workflow_id", workflowID),
		zap.String("run_id", runID),
		zap.Bool("is_transient", isTransient),
		zap.String("error_code", errorCode))

	// Extract node ID from execution ID
	// Format: {workflowID}-{nodeID}-{timestamp}
	// We need to extract the nodeID part
	nodeID := ExtractNodeIDFromExecutionID(executionID, workflowID)

	s.logger.Debug("Extracted nodeID from executionID",
		zap.String("execution_id", executionID),
		zap.String("workflow_id", workflowID),
		zap.String("node_id", nodeID))

	// Build error result message
	resultMsg := NewResultMessage(executionID, workflowID, runID, nodeID, "failed")
	if correlationID != "" {
		resultMsg.WithCorrelationID(correlationID)
	}

	resultMsg.WithError(&ResultError{
		Code:      errorCode,
		Message:   errorMsg,
		Retryable: isTransient,
		Type:      errorType,
	})

	// Publish error result to JetStream
	if err := s.PublishResult(ctx, resultMsg); err != nil {
		s.logger.Error("Failed to publish error result after retries",
			zap.String("execution_id", executionID),
			zap.String("workflow_id", workflowID),
			zap.String("run_id", runID),
			zap.Error(err))
		if msg != nil {
			_ = msg.Nak()
			s.logger.Info("JetStream source message disposition after error result publish FAILED",
				zap.String("execution_id", executionID),
				zap.String("workflow_id", workflowID),
				zap.String("run_id", runID),
				zap.String("correlation_id", correlationID),
				zap.String("jetstream_deliver_count", jetStreamDeliverCountStr(msg)),
				zap.String("jetstream_source_ack_action", "nak_error_result_publish_failed"))
		}
		return fmt.Errorf("failed to publish error result: %w", err)
	}

	s.logger.Info("Successfully published error result to JetStream",
		zap.String("execution_id", executionID),
		zap.String("workflow_id", workflowID),
		zap.Duration("duration", time.Since(startTime)))

	// Ack/Nak based on error type
	if msg != nil {
		ackAction := "ack_permanent_suppress_redelivery"
		if isTransient {
			ackAction = "nak_transient_redelivery"
			if nakErr := msg.Nak(); nakErr != nil {
				s.logger.Warn("Failed to NAK source message after error result publish (redelivery semantics may be broken)",
					zap.String("execution_id", executionID),
					zap.String("workflow_id", workflowID),
					zap.String("run_id", runID),
					zap.String("correlation_id", correlationID),
					zap.String("jetstream_deliver_count", jetStreamDeliverCountStr(msg)),
					zap.String("jetstream_source_ack_action", "nak_transient_redelivery_failed"),
					zap.Error(nakErr))
			}
		} else {
			if ackErr := msg.Ack(); ackErr != nil {
				s.logger.Warn("Failed to ACK source message after permanent error result publish (may cause redelivery)",
					zap.String("execution_id", executionID),
					zap.String("workflow_id", workflowID),
					zap.String("run_id", runID),
					zap.String("correlation_id", correlationID),
					zap.String("jetstream_deliver_count", jetStreamDeliverCountStr(msg)),
					zap.String("jetstream_source_ack_action", "ack_permanent_suppress_redelivery_failed"),
					zap.Error(ackErr))
			}
		}
		s.logger.Info("JetStream source message disposition after error result publish",
			zap.String("execution_id", executionID),
			zap.String("workflow_id", workflowID),
			zap.String("run_id", runID),
			zap.String("correlation_id", correlationID),
			zap.String("node_id", nodeID),
			zap.String("jetstream_deliver_count", jetStreamDeliverCountStr(msg)),
			zap.String("jetstream_source_ack_action", ackAction),
			zap.Bool("is_transient", isTransient),
			zap.String("error_code", errorCode))
	}

	return nil
}

// jetStreamDeliverCountStr returns JetStream NumDelivered for grep-friendly diagnostics, or "".
func jetStreamDeliverCountStr(msg *nats.Msg) string {
	if msg == nil {
		return ""
	}
	if md, err := msg.Metadata(); err == nil && md != nil {
		return strconv.FormatUint(md.NumDelivered, 10)
	}
	return ""
}

// ExtractNodeIDFromExecutionID extracts the base nodeID from a compound executionID.
// ExecutionID format: {workflowID}-{nodeID}-{timestamp}
// Example: "4b45d6f2-ee47-490e-b5c4-86754d95aaa9-8ff422ee-5ed1-421c-8967-1dc1c996b895-1767712791219504964"
// Returns: "8ff422ee-5ed1-421c-8967-1dc1c996b895"
// Exported for testing purposes.
func ExtractNodeIDFromExecutionID(executionID, workflowID string) string {
	// Remove workflowID prefix (if present)
	if len(executionID) > len(workflowID)+1 && executionID[:len(workflowID)+1] == workflowID+"-" {
		remaining := executionID[len(workflowID)+1:]

		// Find the last dash followed by all digits (timestamp)
		// Work backwards to find where timestamp starts
		lastDash := -1
		for i := len(remaining) - 1; i >= 0; i-- {
			if remaining[i] == '-' {
				// Check if everything after this dash is digits
				isAllDigits := true
				for j := i + 1; j < len(remaining); j++ {
					if remaining[j] < '0' || remaining[j] > '9' {
						isAllDigits = false
						break
					}
				}
				if isAllDigits && i+1 < len(remaining) {
					lastDash = i
					break
				}
			}
		}

		if lastDash > 0 {
			return remaining[:lastDash]
		}

		// If no timestamp found, return remaining part
		return remaining
	}

	// If format doesn't match, return executionID as-is (fallback)
	return executionID
}

func composeSubject(prefix, environmentID, executionID string, applyEnvironment bool) string {
	if applyEnvironment && environmentID != "" {
		return prefix + "." + environmentID + "." + executionID
	}
	return prefix + "." + executionID
}

func streamSubjectPattern(streamName string) string {
	return streamName + ".>"
}
