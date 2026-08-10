package client

import (
	"context"
	"sync"
	"time"

	natsclient "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/wehubfusion/Icarus/internal/nats"
	sdkerrors "github.com/wehubfusion/Icarus/pkg/errors"
	"github.com/wehubfusion/Icarus/pkg/message"
	"go.uber.org/zap"
)

// BlobStorageClient interface for storing large workflow results
type BlobStorageClient interface {
	UploadResult(ctx context.Context, blobPath string, data []byte, metadata map[string]string) (string, error)
	DownloadResult(ctx context.Context, blobURL string) ([]byte, error)
}

// Client is the central JetStream client that manages the connection and provides access to services.
// It serves as the entry point for all JetStream operations and automatically initializes
// the JetStream context and service interfaces.
//
// Note: This SDK uses JetStream exclusively for all messaging operations.
// Standard NATS publish/subscribe is not supported.
//
// Example usage:
//
//	client := client.NewClient("nats://localhost:4222", "RESULTS", "result")
//	if err := client.Connect(ctx); err != nil {
//	    logger.Fatal("Failed to connect", zap.Error(err))
//	}
//	defer client.Close()
//
//	// Prefer pkg/runner for consume + ReportSuccess/ReportError, or use
//	// client.JetStream().Publish / client.Messages.GetConsumer for lower-level access.
type Client struct {
	conn        *natsclient.Conn
	js          jetstream.JetStream
	config      *nats.ConnectionConfig
	logger      *zap.Logger
	reconnectMu sync.Mutex

	// Messages provides access to all JetStream messaging operations including
	// publish, subscribe, request-reply, and pull-based consumers
	Messages *message.MessageService

	// Processes will provide process management functionality (reserved for future use)
	// Processes *process.ProcessService

	// blobStorage is used to store large execution results
	blobStorage BlobStorageClient
}

// NewClient creates a new JetStream SDK client with default connection configuration.
// The URL parameter specifies the NATS server address (e.g., "nats://localhost:4222").
// resultStream and resultSubject configure where processed results are published
// (e.g., "RESULTS_UAT" / "result.uat" for UAT; "RESULTS" / "result" for production).
//
// NewClient never fails — it returns an inert, disconnected client. All startup
// failures surface in Connect(). Callers must call Connect() before using
// Messages or any other service field; using an unconnected client causes a nil
// pointer panic.
//
// Note: JetStream must be enabled on the NATS server for this SDK to function.
//
// Example:
//
//	client := client.NewClient("nats://localhost:4222", "RESULTS_UAT", "result.uat")
func NewClient(url string, resultStream string, resultSubject string) *Client {
	logger, _ := zap.NewProduction()
	config := nats.DefaultConnectionConfig(url)
	if resultStream != "" {
		config.ResultStream = resultStream
	}
	if resultSubject != "" {
		config.ResultSubject = resultSubject
	}
	return &Client{
		config: config,
		logger: logger,
	}
}

// SetConsumerInactiveThreshold configures the InactiveThreshold applied to durables
// created via Messages.EnsureConsumer. Zero disables auto-GC. Positive values let
// JetStream delete the durable after the configured idle period (no pulls/acks).
// Call before Connect; after Connect, the value is propagated to the MessageService.
//
// Use a positive value (e.g. 72h) for tenant pods whose lifecycle is shorter than
// the platform's so orphan durables are reclaimed automatically. Leave at zero for
// central/shared pods where the durable must persist regardless of activity.
func (c *Client) SetConsumerInactiveThreshold(d time.Duration) {
	if c == nil || c.config == nil {
		return
	}
	if d < 0 {
		d = 0
	}
	c.config.ConsumerInactiveThreshold = d
	if c.Messages != nil {
		c.Messages.SetInactiveThreshold(d)
	}
}

// NewClientWithConfig creates a new JetStream SDK client with custom configuration.
// This allows full control over connection parameters such as reconnection settings,
// timeouts, and authentication.
//
// Note: JetStream must be enabled on the NATS server for this SDK to function.
//
// Example:
//
//	config := &nats.ConnectionConfig{
//	    URL:           "nats://localhost:4222",
//	    Name:          "my-service",
//	    MaxReconnects: -1,
//	    ReconnectWait: 2 * time.Second,
//	}
//	client := client.NewClientWithConfig(config)
func NewClientWithConfig(config *nats.ConnectionConfig) *Client {
	logger, _ := zap.NewProduction()
	return &Client{
		config: config,
		logger: logger,
	}
}

// Connect establishes a connection to the NATS server and initializes JetStream context.
// This method must be called before using any service methods.
//
// The method performs the following operations:
//   - Establishes TCP connection to NATS server
//   - Initializes JetStream context (REQUIRED - fails if JetStream is not enabled)
//   - Creates and initializes the MessageService with JetStream
//
// Returns an error if connection fails or if JetStream is not enabled on the server.
//
// Example:
//
//	ctx := context.Background()
//	if err := client.Connect(ctx); err != nil {
//	    client.logger.Fatal("Failed to connect", zap.Error(err))
//	}
func (c *Client) Connect(ctx context.Context) error {
	if c.conn != nil && c.conn.IsConnected() {
		return nil // Already connected
	}

	// Drop the dead connection/JS handles, but keep c.Messages non-nil until the
	// new MessageService is ready. Concurrent runners may still call Report*/GetConsumer
	// during reconnect; a stale service returns transport errors (retried) instead of
	// panicking on a nil pointer. This is a pointer swap only — no hot-path locking.
	if c.conn != nil && !c.conn.IsConnected() {
		c.conn = nil
		c.js = nil
	}

	if c.config != nil && c.logger != nil {
		c.config.Logger = c.logger
	}

	// Establish NATS connection
	conn, err := nats.Connect(ctx, c.config)
	if err != nil {
		return sdkerrors.NewInternalError("", "failed to connect to NATS", "CONNECTION_FAILED", err)
	}

	c.conn = conn

	// Initialize JetStream context (new nats.go/jetstream API) - REQUIRED for this SDK
	js, err := jetstream.New(conn)
	if err != nil {
		_ = nats.Close(c.conn)
		c.conn = nil
		return sdkerrors.NewInternalError("", "JetStream is not enabled on the NATS server", "JETSTREAM_NOT_ENABLED", err)
	}
	c.js = js

	// Initialize message service with JetStream (wrapped to interface for testability)
	msgService, err := message.NewMessageService(
		message.WrapJetStream(c.js),
		c.config.MaxDeliver,
		c.config.PublishMaxRetries,
		c.config.ResultStream,
		c.config.ResultSubject,
	)
	if err != nil {
		// Clean up connection on service initialization failure
		_ = nats.Close(c.conn)
		c.conn = nil
		c.js = nil
		return sdkerrors.NewInternalError("", "failed to initialize message service", "SERVICE_INIT_FAILED", err)
	}
	if c.config.ConsumerInactiveThreshold > 0 {
		msgService.SetInactiveThreshold(c.config.ConsumerInactiveThreshold)
	}
	if c.blobStorage != nil {
		msgService.SetBlobStorage(c.blobStorage)
	}
	c.Messages = msgService

	return nil
}

// EnsureConnected reconnects when the NATS connection is missing or closed.
// Safe for concurrent use from multiple runners sharing one client.
func (c *Client) EnsureConnected(ctx context.Context) error {
	c.reconnectMu.Lock()
	defer c.reconnectMu.Unlock()

	if c.IsConnected() {
		return nil
	}
	return c.Connect(ctx)
}

// NewClientWithJSContext creates a client wired to a provided JSContext implementation.
// Useful for tests to avoid connecting to a real NATS server.
func NewClientWithJSContext(js message.JSContext) *Client {
	logger, _ := zap.NewProduction()
	// Use defaults: MaxDeliver=5, PublishMaxRetries=3, ResultStream=RESULTS, ResultSubject=result
	svc, _ := message.NewMessageService(js, 5, 3, "RESULTS", "result")
	return &Client{
		Messages: svc,
		logger:   logger,
	}
}

// SetLogger sets a custom zap logger for the client
func (c *Client) SetLogger(logger *zap.Logger) {
	if logger != nil {
		c.logger = logger
		if c.config != nil {
			c.config.Logger = logger
		}
	}
}

// Close gracefully drains in-flight messages and closes the NATS connection.
//
// Precedence / call order: always stop the Runner first (by cancelling its
// context and waiting for Run to return), then call Close. If Close is called
// while the runner's puller or worker goroutines are still active they may
// receive errors on the next publish/pull attempt; those errors are logged but
// do not cause data loss because NATS will redeliver unacked messages.
//
// Idempotency: calling Close on an already-closed or never-connected client
// is a no-op (returns nil).
//
// After Close, c.Messages is set to nil. Any subsequent call to c.Messages
// methods panics with a nil pointer dereference.
//
// Example:
//
//	if err := client.Connect(ctx); err != nil {
//	    logger.Fatal("Failed to connect", zap.Error(err))
//	}
//	defer client.Close()
func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}

	if err := nats.Close(c.conn); err != nil {
		return sdkerrors.NewInternalError("", "failed to close connection", "CLOSE_FAILED", err)
	}

	// Clean up resources
	c.conn = nil
	c.js = nil
	c.Messages = nil

	return nil
}

// IsConnected returns true if the client is currently connected to the NATS server.
// This can be used to check connection health before performing operations.
//
// Example:
//
//	if !client.IsConnected() {
//	    client.EnsureConnected(ctx)
//	}
func (c *Client) IsConnected() bool {
	return nats.IsConnected(c.conn)
}

// Connection returns the underlying NATS connection.
// This is exposed for advanced use cases where direct access to the connection is needed.
//
// Warning: Direct manipulation of the connection can interfere with the SDK's
// connection management. Use with caution.
func (c *Client) Connection() *natsclient.Conn {
	return c.conn
}

// JetStream returns the underlying JetStream context (new nats.go/jetstream API)
// for advanced operations such as creating streams, inspecting consumer state, or
// publishing with custom options.
//
// Returns nil if Connect() has not been called.
//
// Prefer the higher-level c.Messages methods for all routine publish/consume/report
// operations; JetStream() is intended for administrative tasks (stream/consumer
// management) and for test helpers that need direct JetStream access.
//
// Example:
//
//	js := client.JetStream()
//	if js != nil {
//	    js.CreateStream(ctx, jetstream.StreamConfig{
//	        Name:     "EVENTS",
//	        Subjects: []string{"events.>"},
//	    })
//	}
func (c *Client) JetStream() jetstream.JetStream {
	return c.js
}

// SetBlobStorage injects the blob storage client for large results
func (c *Client) SetBlobStorage(bs BlobStorageClient) {
	c.blobStorage = bs
	if c.Messages != nil {
		c.Messages.SetBlobStorage(bs)
	}
	c.logger.Info("Azure Blob storage enabled for Icarus client")
}
