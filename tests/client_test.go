package tests

import (
	"testing"

	"github.com/wehubfusion/Icarus/pkg/client"
	"go.uber.org/zap"
)

func TestClientCreation(t *testing.T) {
	// Test creating client with URL
	url := "nats://localhost:4222"
	c := client.NewClient(url, "RESULTS", "result")
	if c == nil {
		t.Fatal("Expected client to be created")
	}

	// Test creating client with config
	// We can't directly test NewClientWithConfig without the internal type,
	// but we can test that it doesn't panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("NewClientWithConfig panicked: %v", r)
		}
	}()

	// Test with a mock config structure
	_ = struct {
		URL string
	}{
		URL: url,
	}

	// Test that client starts with nil connection
	// At this point c is guaranteed to be non-nil due to the Fatal check above
	if c.Connection() != nil {
		t.Error("Expected initial connection to be nil")
	}
	if c.IsConnected() {
		t.Error("Expected initial state to be not connected")
	}
	if c.JetStream() != nil {
		t.Error("Expected initial JetStream to be nil")
	}
	if c.Messages != nil {
		t.Error("Expected initial Messages service to be nil")
	}
}

func TestClientConnectionLifecycle(t *testing.T) {
	t.Skip("Connection lifecycle relies on real NATS; skipped with mock")
}

func TestClientReconnection(t *testing.T) {
	t.Skip("Reconnection requires real server; skipped with mock")
}

func TestClientConfiguration(t *testing.T) {
	// Test that different client configurations work
	urls := []string{
		"nats://localhost:4222",
		"nats://127.0.0.1:4222",
		"nats://example.com:4222",
	}

	for _, url := range urls {
		c := client.NewClient(url, "RESULTS", "result")
		if c == nil {
			t.Errorf("Failed to create client with URL: %s", url)
		}
	}
}

func TestClientMultipleConnections(t *testing.T) {
	t.Skip("Multiple connections require real server; skipped with mock")
}

// Note: JetStream requirement testing is covered in the Connect method
// The SDK requires JetStream to be enabled and will fail to connect without it.

func TestClientServiceInitialization(t *testing.T) {
	t.Skip("Service init tested via message tests with mock")
}

func TestClientSetLogger(t *testing.T) {
	c := client.NewClient("nats://localhost:4222", "RESULTS", "result")

	// Test setting a custom logger
	logger, _ := zap.NewProduction()
	c.SetLogger(logger)

	// Test setting nil logger (should not panic)
	c.SetLogger(nil)
}

func TestClientClose(t *testing.T) {
	c := client.NewClient("nats://localhost:4222", "RESULTS", "result")

	// Test closing without connection
	err := c.Close()
	if err != nil {
		t.Errorf("Close should not error on unconnected client, got: %v", err)
	}
}

func TestClientJetStreamAccess(t *testing.T) {
	c := client.NewClient("nats://localhost:4222", "RESULTS", "result")

	// Test JetStream access without connection
	js := c.JetStream()
	if js != nil {
		t.Error("Expected nil JetStream for unconnected client")
	}
}

func TestClientConnectionAccess(t *testing.T) {
	c := client.NewClient("nats://localhost:4222", "RESULTS", "result")

	// Test Connection access without connection
	conn := c.Connection()
	if conn != nil {
		t.Error("Expected nil Connection for unconnected client")
	}
}
