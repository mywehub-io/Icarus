package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/wehubfusion/Icarus/pkg/client"
	"github.com/wehubfusion/Icarus/pkg/message"
)

// With the in-memory mock JetStream, no server or stream setup is required.

func TestMessageCreation(t *testing.T) {
	// Test basic message creation
	msg := message.NewMessage()
	if msg.CreatedAt == "" {
		t.Error("Expected CreatedAt to be set")
	}
	if msg.UpdatedAt == "" {
		t.Error("Expected UpdatedAt to be set")
	}

	// Test workflow message creation
	workflowMsg := message.NewWorkflowMessage("workflow-123", "run-456")
	if workflowMsg.Workflow.WorkflowID != "workflow-123" {
		t.Errorf("Expected workflow ID 'workflow-123', got %s", workflowMsg.Workflow.WorkflowID)
	}
	if workflowMsg.Workflow.RunID != "run-456" {
		t.Errorf("Expected run ID 'run-456', got %s", workflowMsg.Workflow.RunID)
	}
}

func TestMessageWithComponents(t *testing.T) {
	msg := message.NewMessage().
		WithNode("node-123", map[string]interface{}{"type": "processor"}).
		WithPayload( "test data").
		WithOutput("stream")

	if msg.Node.NodeID != "node-123" {
		t.Errorf("Expected node ID 'node-123', got %s", msg.Node.NodeID)
	}
	if msg.Node.Configuration.(map[string]interface{})["type"] != "processor" {
		t.Errorf("Expected node config type 'processor', got %v", msg.Node.Configuration)
	}
	// Source field removed - plugin type available in Metadata["plugin_type"]
	if msg.Payload.GetInlineData() != "test data" {
		t.Errorf("Expected payload data 'test data', got %s", msg.Payload.GetInlineData())
	}
	if msg.Output.DestinationType != "stream" {
		t.Errorf("Expected output destination 'stream', got %s", msg.Output.DestinationType)
	}
}

func TestMessageSerialization(t *testing.T) {
	original := message.NewWorkflowMessage("workflow-123", "run-456").
		WithNode("node-789", map[string]interface{}{"priority": "high"}).
		WithPayload( "test data content").
		WithOutput("stream")

	// Serialize to bytes
	data, err := original.ToBytes()
	if err != nil {
		t.Fatalf("Failed to serialize message: %v", err)
	}

	// Deserialize from bytes
	deserialized, err := message.FromBytes(data)
	if err != nil {
		t.Fatalf("Failed to deserialize message: %v", err)
	}

	// Compare fields
	if deserialized.Workflow.WorkflowID != original.Workflow.WorkflowID {
		t.Errorf("Workflow ID mismatch: expected %s, got %s", original.Workflow.WorkflowID, deserialized.Workflow.WorkflowID)
	}
	if deserialized.Workflow.RunID != original.Workflow.RunID {
		t.Errorf("Run ID mismatch: expected %s, got %s", original.Workflow.RunID, deserialized.Workflow.RunID)
	}
	if deserialized.Node.NodeID != original.Node.NodeID {
		t.Errorf("Node ID mismatch: expected %s, got %s", original.Node.NodeID, deserialized.Node.NodeID)
	}
	// Source field removed - plugin type available in Metadata["plugin_type"]
	if deserialized.Payload.GetInlineData() != original.Payload.GetInlineData() {
		t.Errorf("Payload data mismatch: expected %s, got %s", original.Payload.GetInlineData(), deserialized.Payload.GetInlineData())
	}
	if deserialized.Output.DestinationType != original.Output.DestinationType {
		t.Errorf("Output destination mismatch: expected %s, got %s", original.Output.DestinationType, deserialized.Output.DestinationType)
	}
}

func TestClientConnection(t *testing.T) {
	// Using mock JS: no real connection; ensure service initializes via helper
	c := client.NewClientWithJSContext(NewMockJS())
	if c.Messages == nil {
		t.Fatal("Messages service should be initialized with mock JS")
	}
}

func TestMessagePublishing(t *testing.T) {
	c := client.NewClientWithJSContext(NewMockJS())
	ctx := context.Background()

	// Test publishing a message
	msg := message.NewWorkflowMessage("workflow-test", uuid.New().String()).
		WithPayload( "test message")

	err := c.Messages.Publish(ctx, "test.events.user.created", msg)
	if err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}
}

func TestPullMessages(t *testing.T) {
	c := client.NewClientWithJSContext(NewMockJS())
	ctx := context.Background()

	// Publish a message first
	msg := message.NewWorkflowMessage("workflow-pull", uuid.New().String()).
		WithPayload( "pull test message")

	err := c.Messages.Publish(ctx, "test.events.user.created", msg)
	if err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}

	// Wait for message to be stored
	time.Sleep(10 * time.Millisecond)

	if err := c.Messages.EnsureStream("TEST_EVENTS"); err != nil {
		t.Fatalf("Failed to ensure stream: %v", err)
	}
	if err := c.Messages.EnsureConsumer("TEST_EVENTS", "test_pull_consumer", ""); err != nil {
		t.Fatalf("Failed to ensure consumer: %v", err)
	}

	// Pull messages
	messages, err := c.Messages.PullMessages(ctx, "TEST_EVENTS", "test_pull_consumer", 10)
	if err != nil {
		t.Fatalf("Failed to pull messages: %v", err)
	}

	if len(messages) != 1 {
		t.Errorf("Expected 1 message, got %d", len(messages))
	}

	if len(messages) > 0 {
		if messages[0].Workflow.WorkflowID != msg.Workflow.WorkflowID {
			t.Errorf("Workflow ID mismatch: expected %s, got %s", msg.Workflow.WorkflowID, messages[0].Workflow.WorkflowID)
		}
		if messages[0].Payload.GetInlineData() != msg.Payload.GetInlineData() {
			t.Errorf("Payload data mismatch: expected %s, got %s", msg.Payload.GetInlineData(), messages[0].Payload.GetInlineData())
		}
	}
}

// Note: JetStream requirement testing is handled in client_test.go
// The SDK requires JetStream to be enabled on the NATS server.
// All tests in this file assume JetStream is available.

func TestMessageUpdateTimestamp(t *testing.T) {
	msg := message.NewMessage()
	originalUpdatedAt := msg.UpdatedAt

	// Wait to ensure timestamp changes (RFC3339 has second precision)
	time.Sleep(1001 * time.Millisecond)

	msg.UpdateTimestamp()
	if msg.UpdatedAt == originalUpdatedAt {
		t.Error("UpdateTimestamp should change the UpdatedAt field")
	}
}

func TestMessageWithMetadata(t *testing.T) {
	msg := message.NewMessage()

	// Test adding metadata
	msg.WithMetadata("key1", "value1")
	if msg.Metadata["key1"] != "value1" {
		t.Errorf("Expected metadata key1=value1, got %s", msg.Metadata["key1"])
	}

	// Test adding multiple metadata entries
	msg.WithMetadata("key2", "value2")
	if len(msg.Metadata) != 2 {
		t.Errorf("Expected 2 metadata entries, got %d", len(msg.Metadata))
	}

	// Test metadata on message without existing metadata map
	msg2 := &message.Message{}
	msg2.WithMetadata("test", "value")
	if msg2.Metadata == nil {
		t.Error("WithMetadata should initialize metadata map")
	}
	if msg2.Metadata["test"] != "value" {
		t.Error("WithMetadata should set the value correctly")
	}
}

func TestMessageAckNakTerm(t *testing.T) {
	msg := message.NewMessage()

	// Test Ack/Nak/Term without NATS message (should not error)
	err := msg.Ack()
	if err != nil {
		t.Errorf("Ack should not error without NATS message, got: %v", err)
	}

	err = msg.Nak()
	if err != nil {
		t.Errorf("Nak should not error without NATS message, got: %v", err)
	}

	err = msg.Term()
	if err != nil {
		t.Errorf("Term should not error without NATS message, got: %v", err)
	}

	// Test GetNATSMsg
	natsMsg := msg.GetNATSMsg()
	if natsMsg != nil {
		t.Error("GetNATSMsg should return nil for message without NATS message")
	}
}

func TestFromNATSMsg(t *testing.T) {
	// Create a test message
	originalMsg := message.NewWorkflowMessage("workflow-123", "run-456").
		WithPayload( "test data")

	data, err := originalMsg.ToBytes()
	if err != nil {
		t.Fatalf("Failed to serialize message: %v", err)
	}

	// Create NATS message
	natsMsg := &nats.Msg{
		Subject: "test.subject",
		Data:    data,
		Reply:   "test.reply",
	}

	// Test FromNATSMsg
	convertedMsg, err := message.FromNATSMsg(natsMsg)
	if err != nil {
		t.Fatalf("FromNATSMsg failed: %v", err)
	}

	// Verify the converted message
	if convertedMsg.Workflow.WorkflowID != originalMsg.Workflow.WorkflowID {
		t.Errorf("WorkflowID mismatch: expected %s, got %s",
			originalMsg.Workflow.WorkflowID, convertedMsg.Workflow.WorkflowID)
	}

	if convertedMsg.Payload.GetInlineData() != originalMsg.Payload.GetInlineData() {
		t.Errorf("Payload data mismatch: expected %s, got %s",
			originalMsg.Payload.GetInlineData(), convertedMsg.Payload.GetInlineData())
	}
}

func TestNATSMsgWrapper(t *testing.T) {
	// Create a test message
	msg := message.NewWorkflowMessage("workflow-123", "run-456").
		WithPayload( "test data")

	// Note: We don't need to create a real NATS message for this test
	// The NATSMsg wrapper is what we're testing

	// Create NATSMsg wrapper
	wrappedMsg := &message.NATSMsg{
		Message: msg,
		Subject: "test.subject",
		Reply:   "test.reply",
	}

	// Test that wrapper preserves message data
	if wrappedMsg.Workflow.WorkflowID != msg.Workflow.WorkflowID {
		t.Error("NATSMsg wrapper should preserve workflow ID")
	}

	if wrappedMsg.Subject != "test.subject" {
		t.Error("NATSMsg wrapper should preserve subject")
	}

	if wrappedMsg.Reply != "test.reply" {
		t.Error("NATSMsg wrapper should preserve reply")
	}

	// Test acknowledgment methods (should not error without real NATS message)
	err := wrappedMsg.Ack()
	if err != nil {
		t.Errorf("Ack should not error without real NATS message, got: %v", err)
	}

	err = wrappedMsg.Nak()
	if err != nil {
		t.Errorf("Nak should not error without real NATS message, got: %v", err)
	}

	err = wrappedMsg.InProgress()
	if err != nil {
		t.Errorf("InProgress should not error without real NATS message, got: %v", err)
	}

	err = wrappedMsg.Term()
	if err != nil {
		t.Errorf("Term should not error without real NATS message, got: %v", err)
	}

	// Test Respond method
	response := message.NewMessage().WithPayload( "test response")
	err = wrappedMsg.Respond(response)
	if err != nil {
		t.Errorf("Respond should not error without real NATS message, got: %v", err)
	}
}

// Test HasEmbeddedNodes method
func TestMessage_HasEmbeddedNodes(t *testing.T) {
	// Test message without embedded nodes
	msg := message.NewMessage()
	if msg.HasEmbeddedNodes() {
		t.Error("Message without embedded nodes should not be a unit")
	}

	// Test message with embedded nodes
	embeddedNodes := []message.EmbeddedNode{
		{NodeID: "node1", PluginType: "test", ExecutionOrder: 0},
	}
	msg.WithEmbeddedNodes(embeddedNodes)
	if !msg.HasEmbeddedNodes() {
		t.Error("Message with embedded nodes should be a unit")
	}
}

// Test ValidateEmbeddedNodes success
func TestMessage_ValidateEmbeddedNodes_Success(t *testing.T) {
	msg := message.NewMessage().WithNode("parent", nil)
	embeddedNodes := []message.EmbeddedNode{
		{NodeID: "node1", PluginType: "test", ExecutionOrder: 0},
		{NodeID: "node2", PluginType: "test", ExecutionOrder: 1},
	}
	msg.WithEmbeddedNodes(embeddedNodes)

	err := msg.ValidateEmbeddedNodes()
	if err != nil {
		t.Errorf("ValidateEmbeddedNodes should succeed, got: %v", err)
	}
}

// Test ValidateEmbeddedNodes with duplicate IDs
func TestMessage_ValidateEmbeddedNodes_DuplicateIDs(t *testing.T) {
	msg := message.NewMessage()
	embeddedNodes := []message.EmbeddedNode{
		{NodeID: "node1", PluginType: "test", ExecutionOrder: 0},
		{NodeID: "node1", PluginType: "test", ExecutionOrder: 1},
	}
	msg.WithEmbeddedNodes(embeddedNodes)

	err := msg.ValidateEmbeddedNodes()
	if err == nil {
		t.Error("ValidateEmbeddedNodes should fail with duplicate IDs")
	}
}

// Test ValidateEmbeddedNodes with missing NodeID
func TestMessage_ValidateEmbeddedNodes_MissingNodeID(t *testing.T) {
	msg := message.NewMessage()
	embeddedNodes := []message.EmbeddedNode{
		{NodeID: "", PluginType: "test", ExecutionOrder: 0},
	}
	msg.WithEmbeddedNodes(embeddedNodes)

	err := msg.ValidateEmbeddedNodes()
	if err == nil {
		t.Error("ValidateEmbeddedNodes should fail with missing NodeID")
	}
}

// Test ValidateEmbeddedNodes with missing PluginType
func TestMessage_ValidateEmbeddedNodes_MissingPluginType(t *testing.T) {
	msg := message.NewMessage()
	embeddedNodes := []message.EmbeddedNode{
		{NodeID: "node1", PluginType: "", ExecutionOrder: 0},
	}
	msg.WithEmbeddedNodes(embeddedNodes)

	err := msg.ValidateEmbeddedNodes()
	if err == nil {
		t.Error("ValidateEmbeddedNodes should fail with missing PluginType")
	}
}

// Test ValidateEmbeddedNodes with negative execution order
func TestMessage_ValidateEmbeddedNodes_NegativeOrder(t *testing.T) {
	msg := message.NewMessage()
	embeddedNodes := []message.EmbeddedNode{
		{NodeID: "node1", PluginType: "test", ExecutionOrder: -1},
	}
	msg.WithEmbeddedNodes(embeddedNodes)

	err := msg.ValidateEmbeddedNodes()
	if err == nil {
		t.Error("ValidateEmbeddedNodes should fail with negative execution order")
	}
}

// Test ValidateEmbeddedNodes with invalid field mapping references
func TestMessage_ValidateEmbeddedNodes_InvalidReferences(t *testing.T) {
	msg := message.NewMessage().WithNode("parent", nil)
	embeddedNodes := []message.EmbeddedNode{
		{
			NodeID:         "node1",
			PluginType:     "test",
			ExecutionOrder: 0,
			FieldMappings: []message.FieldMapping{
				{SourceNodeID: "unknown_node", SourceEndpoint: "output"},
			},
		},
	}
	msg.WithEmbeddedNodes(embeddedNodes)

	err := msg.ValidateEmbeddedNodes()
	if err == nil {
		t.Error("ValidateEmbeddedNodes should fail with invalid field mapping reference")
	}
}

// Test GetEmbeddedNodeByID
func TestMessage_GetEmbeddedNodeByID(t *testing.T) {
	msg := message.NewMessage()
	embeddedNodes := []message.EmbeddedNode{
		{NodeID: "node1", PluginType: "test1", ExecutionOrder: 0},
		{NodeID: "node2", PluginType: "test2", ExecutionOrder: 1},
	}
	msg.WithEmbeddedNodes(embeddedNodes)

	// Test finding existing node
	node := msg.GetEmbeddedNodeByID("node2")
	if node == nil {
		t.Fatal("GetEmbeddedNodeByID should find existing node")
	}
	if node.NodeID != "node2" {
		t.Errorf("Expected node2, got %s", node.NodeID)
	}
	if node.PluginType != "test2" {
		t.Errorf("Expected test2, got %s", node.PluginType)
	}

	// Test finding non-existent node
	node = msg.GetEmbeddedNodeByID("nonexistent")
	if node != nil {
		t.Error("GetEmbeddedNodeByID should return nil for non-existent node")
	}
}

// Test GetEmbeddedNodesByOrder
func TestMessage_GetEmbeddedNodesByOrder(t *testing.T) {
	msg := message.NewMessage()
	embeddedNodes := []message.EmbeddedNode{
		{NodeID: "node3", PluginType: "test3", ExecutionOrder: 2},
		{NodeID: "node1", PluginType: "test1", ExecutionOrder: 0},
		{NodeID: "node2", PluginType: "test2", ExecutionOrder: 1},
	}
	msg.WithEmbeddedNodes(embeddedNodes)

	// Get sorted nodes
	sorted := msg.GetEmbeddedNodesByOrder()
	if len(sorted) != 3 {
		t.Fatalf("Expected 3 nodes, got %d", len(sorted))
	}

	// Verify order
	if sorted[0].NodeID != "node1" || sorted[0].ExecutionOrder != 0 {
		t.Errorf("Expected node1 at index 0, got %s with order %d", sorted[0].NodeID, sorted[0].ExecutionOrder)
	}
	if sorted[1].NodeID != "node2" || sorted[1].ExecutionOrder != 1 {
		t.Errorf("Expected node2 at index 1, got %s with order %d", sorted[1].NodeID, sorted[1].ExecutionOrder)
	}
	if sorted[2].NodeID != "node3" || sorted[2].ExecutionOrder != 2 {
		t.Errorf("Expected node3 at index 2, got %s with order %d", sorted[2].NodeID, sorted[2].ExecutionOrder)
	}

	// Test with empty embedded nodes
	emptyMsg := message.NewMessage()
	emptySorted := emptyMsg.GetEmbeddedNodesByOrder()
	if emptySorted != nil {
		t.Error("GetEmbeddedNodesByOrder should return nil for message without embedded nodes")
	}
}

// Test HasConnection
func TestMessage_HasConnection(t *testing.T) {
	msg := message.NewMessage()

	// Test without connection
	if msg.HasConnection() {
		t.Error("Message without connection should return false")
	}

	// Test with connection but no ID
	msg.WithConnection(&message.ConnectionDetails{Type: "postgres"})
	if msg.HasConnection() {
		t.Error("Message with connection but no ID should return false")
	}

	// Test with valid connection
	msg.WithConnection(&message.ConnectionDetails{ConnectionID: "conn123", Type: "postgres"})
	if !msg.HasConnection() {
		t.Error("Message with valid connection should return true")
	}
}

// Test HasSchema
func TestMessage_HasSchema(t *testing.T) {
	msg := message.NewMessage()

	// Test without schema
	if msg.HasSchema() {
		t.Error("Message without schema should return false")
	}

	// Test with schema but no ID
	msg.WithSchema(&message.SchemaDetails{Name: "users"})
	if msg.HasSchema() {
		t.Error("Message with schema but no ID should return false")
	}

	// Test with valid schema
	msg.WithSchema(&message.SchemaDetails{SchemaID: "schema123", Name: "users"})
	if !msg.HasSchema() {
		t.Error("Message with valid schema should return true")
	}
}

// Test message serialization with embedded nodes
func TestMessage_SerializationWithEmbeddedNodes(t *testing.T) {
	original := message.NewWorkflowMessage("workflow-123", "run-456").
		WithNode("parent-node", nil).
		WithPayload( "test data")

	embeddedNodes := []message.EmbeddedNode{
		{
			NodeID:         "node1",
			PluginType:     "postgres",
			ExecutionOrder: 0,
			FieldMappings: []message.FieldMapping{
				{SourceNodeID: "parent-node", SourceEndpoint: "output", DataType: "string"},
			},
		},
		{NodeID: "node2", PluginType: "transform", ExecutionOrder: 1},
	}
	original.WithEmbeddedNodes(embeddedNodes)

	conn := &message.ConnectionDetails{ConnectionID: "conn123", Type: "postgres"}
	original.WithConnection(conn)

	schema := &message.SchemaDetails{SchemaID: "schema123", Name: "users"}
	original.WithSchema(schema)

	// Serialize
	data, err := original.ToBytes()
	if err != nil {
		t.Fatalf("Failed to serialize message with embedded nodes: %v", err)
	}

	// Deserialize
	deserialized, err := message.FromBytes(data)
	if err != nil {
		t.Fatalf("Failed to deserialize message with embedded nodes: %v", err)
	}

	// Verify embedded nodes
	if len(deserialized.EmbeddedNodes) != 2 {
		t.Fatalf("Expected 2 embedded nodes, got %d", len(deserialized.EmbeddedNodes))
	}

	if deserialized.EmbeddedNodes[0].NodeID != "node1" {
		t.Errorf("Expected node1, got %s", deserialized.EmbeddedNodes[0].NodeID)
	}
	if deserialized.EmbeddedNodes[0].PluginType != "postgres" {
		t.Errorf("Expected postgres, got %s", deserialized.EmbeddedNodes[0].PluginType)
	}
	if len(deserialized.EmbeddedNodes[0].FieldMappings) != 1 {
		t.Errorf("Expected 1 field mapping, got %d", len(deserialized.EmbeddedNodes[0].FieldMappings))
	}

	// Verify connection
	if !deserialized.HasConnection() {
		t.Error("Deserialized message should have connection")
	}
	if deserialized.Connection.ConnectionID != "conn123" {
		t.Errorf("Expected conn123, got %s", deserialized.Connection.ConnectionID)
	}

	// Verify schema
	if !deserialized.HasSchema() {
		t.Error("Deserialized message should have schema")
	}
	if deserialized.Schema.SchemaID != "schema123" {
		t.Errorf("Expected schema123, got %s", deserialized.Schema.SchemaID)
	}
}

// Test WithEmbeddedNodes builder
func TestMessage_WithEmbeddedNodes_Fluent(t *testing.T) {
	embeddedNodes := []message.EmbeddedNode{
		{NodeID: "node1", PluginType: "test", ExecutionOrder: 0},
	}

	msg := message.NewMessage().
		WithNode("parent", nil).
		WithEmbeddedNodes(embeddedNodes).
		WithPayload( "data")

	if !msg.HasEmbeddedNodes() {
		t.Error("Message should be a unit after WithEmbeddedNodes")
	}
	if len(msg.EmbeddedNodes) != 1 {
		t.Errorf("Expected 1 embedded node, got %d", len(msg.EmbeddedNodes))
	}
	if msg.Payload == nil {
		t.Error("Fluent builder should preserve all fields")
	}
}
