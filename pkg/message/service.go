package message

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	sdkerrors "github.com/wehubfusion/Icarus/pkg/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"
)

// injectTraceParent extracts the W3C traceparent from ctx (typically carrying the
// Runner.processMessage span) and stamps it onto resultMsg.TraceParent, so a downstream
// result consumer (e.g. Zeus's result_consumer.go) can Extract it and continue the same trace
// instead of starting a disconnected root span.
func injectTraceParent(ctx context.Context, resultMsg *ResultMessage) {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if tp := carrier["traceparent"]; tp != "" {
		resultMsg.TraceParent = tp
	}
}

// JSContext defines the minimal subset of the new nats.go/jetstream API the service
// depends on. jetstream.JetStream satisfies this interface directly; tests can
// provide a mock without requiring a running NATS server.
type JSContext interface {
	Publish(ctx context.Context, subject string, payload []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)
	Stream(ctx context.Context, name string) (jetstream.Stream, error)
	CreateStream(ctx context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error)
	Consumer(ctx context.Context, stream, consumer string) (jetstream.Consumer, error)
	CreateConsumer(ctx context.Context, stream string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error)
}

// WrapJetStream adapts a jetstream.JetStream to the JSContext interface.
// jetstream.JetStream already implements JSContext, so this is a type-safe cast.
func WrapJetStream(js jetstream.JetStream) JSContext {
	return js
}

// MessageService provides methods for publishing and managing messages over JetStream.
// All operations use JetStream exclusively with proper acknowledgment handling.
type MessageService struct {
	js                JSContext
	logger            *zap.Logger
	maxDeliver        int               // Maximum number of delivery attempts before giving up (default: 5)
	publishMaxRetries int               // Maximum number of retry attempts for publish operations (default: 3)
	resultStream      string            // JetStream stream name for publishing results (e.g., RESULTS_UAT)
	resultSubject     string            // Subject for publishing results (e.g., result_uat)
	blobStorage       BlobStorageClient // Blob storage for large results
	// inactiveThreshold sets ConsumerConfig.InactiveThreshold on durables created via
	// EnsureConsumer. Zero (default) disables auto-GC: the durable persists until it is
	// explicitly deleted. Non-zero values let JetStream delete the durable after the
	// configured idle period (no pulls/acks). Intended for tenant pods whose lifecycle
	// is shorter than the platform's; central/shared consumers should leave this at 0.
	inactiveThreshold time.Duration
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
// The resultStream and resultSubject parameters configure where results are published
// (e.g., RESULTS_UAT, result_uat). Results are published flat to resultSubject.
func NewMessageService(js JSContext, maxDeliver int, publishMaxRetries int, resultStream string, resultSubject string) (*MessageService, error) {
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
		js:                js,
		logger:            logger,
		maxDeliver:        maxDeliver,
		publishMaxRetries: publishMaxRetries,
		resultStream:      resultStream,
		resultSubject:     resultSubject,
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
// Existing streams are never modified.
func (s *MessageService) EnsureStream(ctx context.Context, streamName string) error {
	// Check if stream exists
	stream, err := s.js.Stream(ctx, streamName)
	if err != nil {
		// Stream doesn't exist, create it
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			s.logger.Info("Creating JetStream stream",
				zap.String("stream", streamName))

			subjects := []string{fmt.Sprintf("%s.>", streamName)}
			if streamName == s.resultStream && s.resultSubject != "" {
				subjects = []string{s.resultSubject}
			}

			streamConfig := jetstream.StreamConfig{
				Name:     streamName,
				Subjects: subjects,
				Storage:  jetstream.FileStorage,
				MaxAge:   24 * time.Hour,
				MaxMsgs:  100000,
				Replicas: 1,
			}

			_, err = s.js.CreateStream(ctx, streamConfig)
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
		msgs := uint64(0)
		if info := stream.CachedInfo(); info != nil {
			msgs = info.State.Msgs
		}
		s.logger.Info("JetStream stream already exists",
			zap.String("stream", streamName),
			zap.Uint64("messages", msgs))
	}

	return nil
}

// EnsureConsumer creates the JetStream consumer if it doesn't exist, or validates it exists.
// filterSubject when non-empty sets FilterSubject on the durable consumer (tenant/default routing).
// Existing durables are never modified (their config is left exactly as deployed).
func (s *MessageService) EnsureConsumer(ctx context.Context, streamName, consumerName, filterSubject string) error {
	// Try to get consumer info first
	consumer, err := s.js.Consumer(ctx, streamName, consumerName)
	if err != nil {
		// Consumer doesn't exist, create it
		if errors.Is(err, jetstream.ErrConsumerNotFound) {
			s.logger.Info("Creating JetStream consumer",
				zap.String("stream", streamName),
				zap.String("consumer", consumerName))

			consumerConfig := jetstream.ConsumerConfig{
				Durable:           consumerName,
				FilterSubject:     filterSubject,
				AckPolicy:         jetstream.AckExplicitPolicy,
				DeliverPolicy:     jetstream.DeliverAllPolicy,
				MaxAckPending:     1000,
				MaxDeliver:        s.maxDeliver,
				InactiveThreshold: s.inactiveThreshold,
			}

			_, err = s.js.CreateConsumer(ctx, streamName, consumerConfig)
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
		pending := uint64(0)
		if info := consumer.CachedInfo(); info != nil {
			pending = info.NumPending
		}
		s.logger.Info("JetStream consumer already exists",
			zap.String("stream", streamName),
			zap.String("consumer", consumerName),
			zap.Uint64("pending", pending))
	}

	return nil
}

// GetConsumer returns a handle to an existing JetStream consumer, bound to the
// stream. The handle is used by the runner to start a Consume() message loop.
// The consumer must already exist (see EnsureConsumer); this method never
// creates or modifies consumers.
func (s *MessageService) GetConsumer(ctx context.Context, stream, consumer string) (jetstream.Consumer, error) {
	if stream == "" || consumer == "" {
		s.logger.Error("GetConsumer failed: stream and consumer names are required")
		return nil, fmt.Errorf("stream and consumer names are required")
	}

	cons, err := s.js.Consumer(ctx, stream, consumer)
	if err != nil {
		return nil, fmt.Errorf("consumer %q in %q: %w", consumer, stream, err)
	}
	return cons, nil
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

// ensureResultStream ensures the configured result stream exists so results can
// be published to resultSubject. The stream is created on first use and never
// modified afterwards.
func (s *MessageService) ensureResultStream(ctx context.Context) error {
	streamName := s.resultStream

	// Check if stream exists
	_, err := s.js.Stream(ctx, streamName)
	if err != nil {
		// Stream doesn't exist
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			s.logger.Info("Creating JetStream stream for subject",
				zap.String("stream", streamName),
				zap.String("subject", s.resultSubject),
				zap.Bool("is_result_stream", true))

			streamConfig := jetstream.StreamConfig{
				Name:     streamName,
				Subjects: []string{s.resultSubject},
				Storage:  jetstream.FileStorage,
				MaxAge:   24 * time.Hour,
				MaxMsgs:  100000,
				Replicas: 1,
			}

			_, err = s.js.CreateStream(ctx, streamConfig)
			if err != nil {
				return fmt.Errorf("failed to create stream '%s' for subject '%s': %w", streamName, s.resultSubject, err)
			}

			s.logger.Info("Successfully created JetStream stream",
				zap.String("stream", streamName),
				zap.Strings("subjects", streamConfig.Subjects),
				zap.Bool("is_result_stream", true))
		} else {
			return fmt.Errorf("failed to get stream info for '%s': %w", streamName, err)
		}
	}

	return nil
}

// PublishResult publishes a ResultMessage to the result stream using JetStream.
// This is used for reporting unit execution results back to Zeus.
func (s *MessageService) PublishResult(ctx context.Context, resultMsg *ResultMessage) error {
	if s.resultSubject == "" {
		s.logger.Error("PublishResult failed: result subject not configured")
		return sdkerrors.NewValidationError("result subject not configured", "INVALID_CONFIG", nil)
	}

	if resultMsg == nil {
		s.logger.Error("PublishResult failed: result message cannot be nil")
		return sdkerrors.NewValidationError("result message cannot be nil", "INVALID_MESSAGE", nil)
	}

	// Results publish flat to the configured result subject. The subject is
	// tenant-free and shared; Zeus correlates results by run/execution ID.
	publishSubject := s.resultSubject

	// Ensure result stream exists
	if err := s.ensureResultStream(ctx); err != nil {
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
	var pubAck *jetstream.PubAck
	for attempt := 1; attempt <= s.publishMaxRetries; attempt++ {
		pubAck, publishErr = s.js.Publish(ctx, publishSubject, data)
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
func (s *MessageService) ReportSuccess(ctx context.Context, resultMessage Message, msg jetstream.Msg) error {
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
	injectTraceParent(ctx, resultMsg)
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
//   - msg: JetStream message to acknowledge/nak after error reporting (can be nil)
//
// Returns an error if the result cannot be published.
func (s *MessageService) ReportError(ctx context.Context, executionID, workflowID, runID, correlationID string, err error, msg jetstream.Msg) error {
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
	injectTraceParent(ctx, resultMsg)
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
func jetStreamDeliverCountStr(msg jetstream.Msg) string {
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
