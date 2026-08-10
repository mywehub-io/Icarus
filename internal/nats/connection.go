package nats

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

// ConnectionConfig holds configuration for NATS connection
type ConnectionConfig struct {
	// URL is the NATS server URL (e.g., "nats://localhost:4222")
	URL string

	// Name is the client name for identifying this connection
	Name string

	// MaxReconnects is the maximum number of reconnection attempts
	// Use -1 for unlimited reconnects
	MaxReconnects int

	// ReconnectWait is the time to wait between reconnection attempts
	ReconnectWait time.Duration

	// Timeout is the connection timeout
	Timeout time.Duration

	// Token is an optional authentication token
	Token string

	// Username is an optional username for authentication
	Username string

	// Password is an optional password for authentication
	Password string

	// MaxDeliver is the maximum number of delivery attempts for JetStream consumers.
	// This controls how many times a message will be redelivered before being considered failed.
	// Default is 5 retries. Set to -1 for unlimited retries (not recommended).
	//
	// Example with 30s AckWait:
	//   - MaxDeliver: 5 = 2.5 minutes total retry time
	//   - MaxDeliver: 20 = 10 minutes total retry time
	//   - MaxDeliver: 100 = 50 minutes total retry time
	MaxDeliver int

	// PublishMaxRetries is the maximum number of retry attempts when publishing messages fails.
	// This applies to callback publishing (ReportSuccess, ReportError) to ensure reliable delivery.
	// Default is 3 retries with 1 second delay between retries.
	PublishMaxRetries int

	// ResultStream is the name of the JetStream stream where results are published.
	// This should be environment-specific (e.g., RESULTS_UAT, RESULTS_PROD).
	// Default is "RESULTS".
	ResultStream string

	// ResultSubject is the subject prefix where results are published (e.g., result_uat).
	// Default is "result".
	ResultSubject string

	// ConsumerInactiveThreshold sets ConsumerConfig.InactiveThreshold on durables
	// created via MessageService.EnsureConsumer. Zero (default) disables auto-GC:
	// the durable persists until explicitly deleted. A positive value lets JetStream
	// delete the durable after the configured idle period (no pulls/acks). Intended
	// for tenant pods whose lifecycle is shorter than the platform's; central pods
	// should leave this at 0.
	ConsumerInactiveThreshold time.Duration

	// Logger receives disconnect, reconnect, and closed events. Optional; when nil
	// connection lifecycle events are not logged.
	Logger *zap.Logger
}

// DefaultConnectionConfig returns a configuration with sensible defaults
func DefaultConnectionConfig(url string) *ConnectionConfig {
	return &ConnectionConfig{
		URL:               url,
		Name:              "nats-sdk-client",
		MaxReconnects:     -1,
		ReconnectWait:     2 * time.Second,
		Timeout:           5 * time.Second,
		MaxDeliver:        5, // Default: retry up to 5 times (2.5 minutes with 30s AckWait)
		PublishMaxRetries: 3, // Default: 3 retries for publish operations
		ResultStream:      "RESULTS",
		ResultSubject:     "result",
	}
}

// Connect establishes a connection to NATS with the provided configuration
func Connect(ctx context.Context, config *ConnectionConfig) (*nats.Conn, error) {
	if config == nil {
		return nil, fmt.Errorf("connection config cannot be nil")
	}

	if config.URL == "" {
		return nil, fmt.Errorf("NATS URL cannot be empty")
	}

	opts := []nats.Option{
		nats.Name(config.Name),
		nats.MaxReconnects(config.MaxReconnects),
		nats.ReconnectWait(config.ReconnectWait),
		nats.Timeout(config.Timeout),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			if err == nil || config.Logger == nil {
				return
			}
			config.Logger.Warn("NATS disconnected", zap.Error(err))
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			if config.Logger == nil {
				return
			}
			config.Logger.Info("NATS reconnected", zap.String("connected_url", nc.ConnectedUrl()))
		}),
		nats.ClosedHandler(func(_ *nats.Conn) {
			if config.Logger == nil {
				return
			}
			config.Logger.Warn("NATS connection closed")
		}),
	}

	// Add authentication options if provided
	if config.Token != "" {
		opts = append(opts, nats.Token(config.Token))
	} else if config.Username != "" && config.Password != "" {
		opts = append(opts, nats.UserInfo(config.Username, config.Password))
	}

	// Create a channel to handle connection result
	type result struct {
		conn *nats.Conn
		err  error
	}
	resultCh := make(chan result, 1)

	go func() {
		conn, err := nats.Connect(config.URL, opts...)
		resultCh <- result{conn: conn, err: err}
	}()

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("connection cancelled: %w", ctx.Err())
	case res := <-resultCh:
		if res.err != nil {
			return nil, fmt.Errorf("failed to connect to NATS: %w", res.err)
		}
		return res.conn, nil
	}
}

// Close safely closes a NATS connection
func Close(conn *nats.Conn) error {
	if conn == nil {
		return nil
	}

	// Drain the connection to allow in-flight messages to complete
	if err := conn.Drain(); err != nil {
		// If drain fails, force close
		conn.Close()
		return fmt.Errorf("error draining connection: %w", err)
	}

	return nil
}

// IsConnected checks if the connection is active
func IsConnected(conn *nats.Conn) bool {
	return conn != nil && conn.IsConnected()
}
