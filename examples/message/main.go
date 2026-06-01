package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/wehubfusion/Icarus/pkg/client"
	"github.com/wehubfusion/Icarus/pkg/message"
)

func main() {
	fmt.Println("=== Icarus Messaging System Demo ===")
	fmt.Println("Demonstrating JetStream-based messaging capabilities")
	fmt.Println()

	// Create a new client instance
	// In production, you would configure with your NATS server URL
	natsURL := "nats://localhost:4222"
	if url := os.Getenv("NATS_URL"); url != "" {
		natsURL = url
	}

	client := client.NewClient(natsURL, "RESULTS", "result")
	ctx := context.Background()

	// Connect to NATS JetStream
	fmt.Printf("Connecting to NATS at %s...\n", natsURL)
	if err := client.Connect(ctx); err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}
	defer client.Close()
	fmt.Println("✓ Connected to NATS JetStream")
	fmt.Println()

	// Set up graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Run different messaging examples
	runExamples(ctx, client, sigChan)
}

func runExamples(ctx context.Context, client *client.Client, sigChan chan os.Signal) {
	// Example 1: Basic Message Publishing
	fmt.Println("1. Basic Message Publishing")
	fmt.Println("===========================")

	// Create a basic message
	basicMsg := message.NewMessage()
	basicMsg.WithPayload("Hello from Icarus!")
	basicMsg.WithMetadata("example", "basic-publish")
	basicMsg.WithMetadata("timestamp", time.Now().Format(time.RFC3339))

	// Publish the message
	subject := "events.example.basic"
	fmt.Printf("Publishing basic message to subject: %s\n", subject)
	if err := client.Messages.Publish(ctx, subject, basicMsg); err != nil {
		log.Printf("Failed to publish basic message: %v", err)
	} else {
		fmt.Println("✓ Basic message published successfully")
	}
	fmt.Println()

	// Example 2: Workflow Message
	fmt.Println("2. Workflow Message")
	fmt.Println("===================")

	workflowID := uuid.New().String()
	runID := uuid.New().String()

	workflowMsg := message.NewWorkflowMessage(workflowID, runID)
	workflowMsg.WithNode("data-processor", map[string]interface{}{
		"processor_type": "json",
		"timeout":        30,
		"retry_count":    3,
	})
	workflowMsg.WithPayload(`{"user_id": 12345, "action": "purchase", "amount": 99.99}`)
	workflowMsg.WithOutput("database")
	workflowMsg.WithMetadata("priority", "high")
	workflowMsg.WithMetadata("environment", "production")

	subject = "workflows.orders.processing"
	fmt.Printf("Publishing workflow message to subject: %s\n", subject)
	fmt.Printf("  - Workflow ID: %s\n", workflowID)
	fmt.Printf("  - Run ID: %s\n", runID)
	if err := client.Messages.Publish(ctx, subject, workflowMsg); err != nil {
		log.Printf("Failed to publish workflow message: %v", err)
	} else {
		fmt.Println("✓ Workflow message published successfully")
	}
	fmt.Println()

	// Example 3: Pull-based Message Processing
	fmt.Println("3. Pull-based Message Processing")
	fmt.Println("=================================")

	// Note: In a real application, you would need to create streams and consumers first
	// This example demonstrates the pull-based approach using the available API
	subjectFilter := "workflows.*"
	streamName := "EVENTS"
	consumerName := "example-consumer"

	fmt.Printf("Using stream: %s\n", streamName)
	fmt.Printf("Consumer name: %s\n", consumerName)

	// Function to process messages using pull-based approach
	pullAndProcessMessages := func() {
		fmt.Println("Attempting to pull messages...")

		// Pull messages from the consumer
		messages, err := client.Messages.PullMessages(ctx, subjectFilter, streamName, consumerName, 5)
		if err != nil {
			log.Printf("Failed to pull messages: %v", err)
			return
		}

		if len(messages) == 0 {
			fmt.Println("No messages available")
			return
		}

		fmt.Printf("📨 Pulled %d messages\n", len(messages))

		// Process each message
		for i, msg := range messages {
			fmt.Printf("Processing message %d:\n", i+1)

			// Display message details
			if msg.Workflow != nil {
				fmt.Printf("  - Workflow: %s (Run: %s)\n", msg.Workflow.WorkflowID, msg.Workflow.RunID)
			}
			if msg.Node != nil {
				fmt.Printf("  - Node: %s\n", msg.Node.NodeID)
			}
			if msg.Payload != nil {
				fmt.Printf("  - Payload: %s\n", msg.Payload.GetInlineData())
			}
			if len(msg.Metadata) > 0 {
				fmt.Printf("  - Metadata: %v\n", msg.Metadata)
			}
			fmt.Printf("  - Created: %s\n", msg.CreatedAt)

			// Acknowledge the message (important for JetStream)
			if err := msg.Ack(); err != nil {
				log.Printf("Failed to acknowledge message: %v", err)
			} else {
				fmt.Println("  ✓ Message acknowledged")
			}
			fmt.Println()
		}
	}

	// For this demo, let's call the pull function once
	// In a real application, you would run this in a loop or on a schedule
	pullAndProcessMessages()

	fmt.Println("Pull-based message processing ready")
	fmt.Println("Note: Stream and consumer must be created externally")
	fmt.Println()

	// Example 4: Publish some test messages for the subscriber
	fmt.Println("4. Publishing Test Messages")
	fmt.Println("===========================")

	// Publish a few test messages with different types
	testMessages := []struct {
		subject string
		msgType string
		data    string
	}{
		{"events.example.user", "user-event", `{"user_id": 123, "event": "login"}`},
		{"events.example.order", "order-event", `{"order_id": 456, "status": "completed"}`},
		{"events.example.notification", "notification", `{"recipient": "user@example.com", "message": "Order shipped"}`},
	}

	for i, test := range testMessages {
		testMsg := message.NewMessage()
		testMsg.WithPayload(test.data)
		testMsg.WithMetadata("message_type", test.msgType)
		testMsg.WithMetadata("test_sequence", fmt.Sprintf("%d", i+1))

		fmt.Printf("Publishing test message %d to %s\n", i+1, test.subject)
		if err := client.Messages.Publish(ctx, test.subject, testMsg); err != nil {
			log.Printf("Failed to publish test message: %v", err)
		}

		// Small delay between messages
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("✓ Test messages published")
	fmt.Println()

	// Example 5: Callback Reporting
	fmt.Println("5. Callback Reporting")
	fmt.Println("=====================")

	// Demonstrate success callback reporting
	callbackWorkflowID := uuid.New().String()
	callbackRunID := uuid.New().String()

	fmt.Printf("Reporting success for workflow: %s, run: %s\n", callbackWorkflowID, callbackRunID)

	// Report a successful workflow execution
	successData := `{"status": "completed", "processed_items": 150, "duration": "45.2s"}`
	resultMessage := message.NewWorkflowMessage(callbackWorkflowID, callbackRunID).
		WithPayload(successData)

	if err := client.Messages.ReportSuccess(ctx, *resultMessage, nil); err != nil {
		log.Printf("Failed to report success: %v", err)
	} else {
		fmt.Println("✓ Success callback reported")
	}

	// Demonstrate error callback reporting
	errorExecutionID := uuid.New().String()
	errorWorkflowID := uuid.New().String()
	errorRunID := uuid.New().String()

	fmt.Printf("Reporting error for execution: %s, workflow: %s, run: %s\n", errorExecutionID, errorWorkflowID, errorRunID)

	// Report a failed workflow execution
	errorData := fmt.Errorf("Database connection timeout after 3 retries at 2024-10-07T10:30:00Z")
	if err := client.Messages.ReportError(ctx, errorExecutionID, errorWorkflowID, errorRunID, "", errorData, nil); err != nil {
		log.Printf("Failed to report error: %v", err)
	} else {
		fmt.Println("✓ Error callback reported")
	}
	fmt.Println()

	// Example 6: Message Serialization
	fmt.Println("6. Message Serialization")
	fmt.Println("========================")

	// Create a complex message
	complexMsg := message.NewWorkflowMessage("wf-serialization-test", "run-001")
	complexMsg.WithNode("serializer", map[string]interface{}{
		"format":  "json",
		"version": "v1.0",
		"options": map[string]interface{}{
			"pretty":   true,
			"validate": true,
		},
	})
	complexMsg.WithPayload(`{"complex": true, "nested": {"data": [1,2,3]}}`)
	complexMsg.WithOutput("file-system")
	complexMsg.WithMetadata("encoding", "utf-8")
	complexMsg.WithMetadata("compression", "gzip")

	// Serialize to bytes
	msgBytes, err := complexMsg.ToBytes()
	if err != nil {
		log.Printf("Failed to serialize message: %v", err)
	} else {
		fmt.Printf("✓ Message serialized to %d bytes\n", len(msgBytes))
	}

	// Deserialize from bytes
	deserializedMsg, err := message.FromBytes(msgBytes)
	if err != nil {
		log.Printf("Failed to deserialize message: %v", err)
	} else {
		fmt.Println("✓ Message deserialized successfully")
		fmt.Printf("  - Workflow ID: %s\n", deserializedMsg.Workflow.WorkflowID)
		fmt.Printf("  - Node ID: %s\n", deserializedMsg.Node.NodeID)
		fmt.Printf("  - Payload: %s\n", deserializedMsg.Payload.GetInlineData())
		fmt.Printf("  - Metadata count: %d\n", len(deserializedMsg.Metadata))
	}
	fmt.Println()

	// Wait for shutdown signal or let it run for a bit
	fmt.Println("Demo running... Press Ctrl+C to stop")

	select {
	case <-sigChan:
		fmt.Println("\n📟 Shutdown signal received, cleaning up...")
	case <-time.After(30 * time.Second):
		fmt.Println("\n⏰ Demo timeout reached, shutting down...")
	}

	fmt.Println("✓ Demo completed successfully")
}
