package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/wehubfusion/Icarus/pkg/client"
	"github.com/wehubfusion/Icarus/pkg/message"
	"github.com/wehubfusion/Icarus/pkg/runner"
	"go.uber.org/zap"
)

// SimpleProcessor implements the runner.Processor interface
// This is a simple processor that simulates processing work
type SimpleProcessor struct {
	name   string
	logger *zap.Logger
}

// NewSimpleProcessor creates a new processor
func NewSimpleProcessor(name string, logger *zap.Logger) *SimpleProcessor {
	if logger == nil {
		logger, _ = zap.NewDevelopment()
	}
	return &SimpleProcessor{
		name:   name,
		logger: logger,
	}
}

// Process implements the Processor interface
// This is where your business logic would go
func (p *SimpleProcessor) Process(ctx context.Context, msg *message.Message) (message.Message, error) {
	// Simulate some processing time (1-5 seconds instead of 50+ seconds)
	processingTime := time.Duration(rand.Intn(4000)+1000) * time.Millisecond

	// Log the message we received using structured logging
	fields := []zap.Field{
		zap.String("processor", p.name),
		zap.Duration("processing_time", processingTime),
	}

	if msg.Workflow != nil {
		fields = append(fields,
			zap.String("workflow_id", msg.Workflow.WorkflowID),
			zap.String("run_id", msg.Workflow.RunID))
	}
	if msg.Node != nil {
		fields = append(fields, zap.String("node_id", msg.Node.NodeID))
	}
	if msg.Payload != nil {
		fields = append(fields, zap.String("payload_data", msg.Payload.GetInlineData()))
	}

	p.logger.Info("Processor received message", fields...)
	p.logger.Info("Processor starting work", zap.String("processor", p.name), zap.Duration("processing_time", processingTime))

	select {
	case <-time.After(processingTime):
		// Processing completed successfully
		p.logger.Info("Processor completed processing", zap.String("processor", p.name))

		// Create a result message with the processing results
		resultMessage := message.NewMessage().
			WithPayload( fmt.Sprintf("Processed by %s in %v", p.name, processingTime))

		// Copy workflow information if it exists
		if msg.Workflow != nil {
			resultMessage.Workflow = msg.Workflow
		}

		return *resultMessage, nil
	case <-ctx.Done():
		// Processing was cancelled
		p.logger.Warn("Processor cancelled", zap.String("processor", p.name), zap.Error(ctx.Err()))
		return message.Message{}, ctx.Err()
	}
}

func main() {
	fmt.Println("=== Icarus Runner Example ===")
	fmt.Println("This example demonstrates how to:")
	fmt.Println("1. Set up NATS JetStream streams and consumers")
	fmt.Println("2. Publish some test messages")
	fmt.Println("3. Use the Runner to process messages concurrently")
	fmt.Println()

	// Create and connect to NATS with JetStream
	c := client.NewClient("nats://localhost:4222", "RESULTS", "result")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a logger for the runner
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("Failed to create logger: %v", err)
	}
	defer logger.Sync()

	if err := c.Connect(ctx); err != nil {
		logger.Fatal("Failed to connect to NATS", zap.Error(err))
	}
	defer c.Close()

	// Set the same logger for the client and message service
	c.SetLogger(logger)
	c.Messages.SetLogger(logger)

	// Note: MaxDeliver is configured in ConnectionConfig before Connect()
	// Default is 5 retries. With 30s AckWait, this gives 2.5 minutes total retry time.
	// To customize, use NewClientWithConfig with a custom config:
	// config := nats.DefaultConnectionConfig(natsURL)
	// config.MaxDeliver = 20  // 20 retries = 10 minutes total with 30s AckWait
	// c := client.NewClientWithConfig(config)

	// Get JetStream context for stream/consumer setup
	js := c.JetStream()
	if js == nil {
		logger.Fatal("JetStream not available")
	}

	// Setup streams and consumers
	if err := setupStreams(ctx, js, logger); err != nil {
		logger.Fatal("Failed to setup streams", zap.Error(err))
	}

	// Publish some test messages
	logger.Info("Publishing test messages...")
	if err := publishTestMessages(ctx, js); err != nil {
		logger.Fatal("Failed to publish test messages", zap.Error(err))
	}

	// Create a simple processor
	processor := NewSimpleProcessor("StringProcessor", logger)

	// Create the runner
	// - Pull from the "TASKS" stream using "task-processor" consumer
	// - Process 5 messages at a time (batch size)
	// - Use worker pool for concurrent processing
	// - Set 5 minute timeout for message processing
	cfg := runner.DefaultConfig()
	taskRunner, err := runner.NewRunner(
		c,                // client
		processor,        // processor implementation
		"TASKS",          // stream name
		"task-processor", // consumer name
		5,                // batch size
		5*time.Minute,    // process timeout (5 minutes)
		logger,           // zap logger
		nil,              // no tracing configuration
		&cfg,             // worker config (nil uses defaults/env)
	)
	if err != nil {
		log.Fatalf("Failed to create runner: %v", err)
	}
	defer taskRunner.Close() // Clean up resources

	// Setup graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start the runner in a goroutine
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Println("🚀 Starting message processor...")
		if err := taskRunner.Run(ctx); err != nil {
			if err == context.Canceled {
				fmt.Println("📋 Message processor stopped due to shutdown signal")
			} else {
				log.Printf("Runner error: %v", err)
			}
		}
	}()

	// Wait for shutdown signal
	fmt.Println("Press Ctrl+C to stop the processor...")
	<-sigChan
	fmt.Println("\n🛑 Shutdown signal received, stopping processor...")

	// Cancel context to stop the runner
	cancel()

	// Wait for runner to stop
	wg.Wait()
	fmt.Println("✅ Processor stopped gracefully")
}

// setupStreams creates the necessary streams and consumers for the example
func setupStreams(ctx context.Context, js jetstream.JetStream, logger *zap.Logger) error {
	logger.Info("Setting up JetStream streams and consumers...")

	// Create TASKS stream for incoming messages to process
	logger.Info("Creating TASKS stream...")
	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:        "TASKS",
		Description: "Stream for task messages to be processed",
		Subjects:    []string{"tasks.>"},
		Storage:     jetstream.FileStorage,
		Replicas:    1,
		MaxAge:      24 * time.Hour, // Keep messages for 24 hours
	})
	if err != nil {
		logger.Warn("Could not create TASKS stream (might already exist)", zap.Error(err))
	} else {
		logger.Info("✓ Created TASKS stream")
	}

	// Create RESULTS stream for callback reporting
	logger.Info("Creating RESULTS stream...")
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:        "RESULTS",
		Description: "Results stream for success/error callback reporting",
		Subjects:    []string{"result"},
		Storage:     jetstream.FileStorage,
		Replicas:    1,
		MaxAge:      24 * time.Hour, // Keep results for 24 hours
	})
	if err != nil {
		logger.Warn("Could not create RESULTS stream (might already exist)", zap.Error(err))
	} else {
		logger.Info("✓ Created RESULTS stream")
	}

	// Create consumer for the TASKS stream
	logger.Info("Creating task processor consumer...")
	_, err = js.CreateConsumer(ctx, "TASKS", jetstream.ConsumerConfig{
		Durable:       "task-processor",
		Description:   "Consumer for processing task messages",
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxDeliver:    3,               // Retry failed messages up to 3 times
		AckWait:       5 * time.Minute, // Wait 5 minutes for acknowledgment (increased from 30s)
		MaxAckPending: 100,             // Allow up to 100 unacknowledged messages
	})
	if err != nil {
		logger.Warn("Could not create task processor consumer (might already exist)", zap.Error(err))
	} else {
		logger.Info("✓ Created task processor consumer")
	}

	logger.Info("✅ Stream and consumer setup complete")
	return nil
}

// publishTestMessages publishes some sample messages to the TASKS stream
func publishTestMessages(ctx context.Context, js jetstream.JetStream) error {
	workflowID := uuid.New().String()

	// Create different types of test messages
	messages := []struct {
		subject string
		data    string
		runID   string
	}{
		{"tasks.process.string", "Hello, World!", uuid.New().String()},
		// {"tasks.process.string", "Process this text", uuid.New().String()},
		// {"tasks.process.string", "Another message to process", uuid.New().String()},
		// {"tasks.process.data", "Some data processing task", uuid.New().String()},
		// {"tasks.process.data", "More data to handle", uuid.New().String()},
		// {"tasks.process.string", "Final test message", uuid.New().String()},
		// {"tasks.process.string", "Hello, World!", uuid.New().String()},
		// {"tasks.process.string", "Process this text", uuid.New().String()},
		// {"tasks.process.string", "Another message to process", uuid.New().String()},
		// {"tasks.process.data", "Some data processing task", uuid.New().String()},
		// {"tasks.process.data", "More data to handle", uuid.New().String()},
		// {"tasks.process.string", "Final test message", uuid.New().String()},
		// {"tasks.process.string", "Hello, World!", uuid.New().String()},
		// {"tasks.process.string", "Process this text", uuid.New().String()},
		// {"tasks.process.string", "Another message to process", uuid.New().String()},
		// {"tasks.process.data", "Some data processing task", uuid.New().String()},
		// {"tasks.process.data", "More data to handle", uuid.New().String()},
		// {"tasks.process.string", "Final test message", uuid.New().String()},
		// {"tasks.process.string", "Hello, World!", uuid.New().String()},
		// {"tasks.process.string", "Process this text", uuid.New().String()},
		// {"tasks.process.string", "Another message to process", uuid.New().String()},
		// {"tasks.process.data", "Some data processing task", uuid.New().String()},
		// {"tasks.process.data", "More data to handle", uuid.New().String()},
		// {"tasks.process.string", "Final test message", uuid.New().String()},
		// {"tasks.process.string", "Hello, World!", uuid.New().String()},
		// {"tasks.process.string", "Process this text", uuid.New().String()},
		// {"tasks.process.string", "Another message to process", uuid.New().String()},
		// {"tasks.process.data", "Some data processing task", uuid.New().String()},
		// {"tasks.process.data", "More data to handle", uuid.New().String()},
		// {"tasks.process.string", "Final test message", uuid.New().String()},
		// {"tasks.process.string", "Hello, World!", uuid.New().String()},
		// {"tasks.process.string", "Process this text", uuid.New().String()},
		// {"tasks.process.string", "Another message to process", uuid.New().String()},
		// {"tasks.process.data", "Some data processing task", uuid.New().String()},
		// {"tasks.process.data", "More data to handle", uuid.New().String()},
		// {"tasks.process.string", "Final test message", uuid.New().String()},
		// {"tasks.process.string", "Hello, World!", uuid.New().String()},
		// {"tasks.process.string", "Process this text", uuid.New().String()},
		// {"tasks.process.string", "Another message to process", uuid.New().String()},
		// {"tasks.process.data", "Some data processing task", uuid.New().String()},
		// {"tasks.process.data", "More data to handle", uuid.New().String()},
		// {"tasks.process.string", "Final test message", uuid.New().String()},
		// {"tasks.process.string", "Hello, World!", uuid.New().String()},
		// {"tasks.process.string", "Process this text", uuid.New().String()},
		// {"tasks.process.string", "Another message to process", uuid.New().String()},
		// {"tasks.process.data", "Some data processing task", uuid.New().String()},
		// {"tasks.process.data", "More data to handle", uuid.New().String()},
		// {"tasks.process.string", "Final test message", uuid.New().String()},
		// {"tasks.process.string", "Hello, World!", uuid.New().String()},
		// {"tasks.process.string", "Process this text", uuid.New().String()},
		// {"tasks.process.string", "Another message to process", uuid.New().String()},
		// {"tasks.process.data", "Some data processing task", uuid.New().String()},
		// {"tasks.process.data", "More data to handle", uuid.New().String()},
		// {"tasks.process.string", "Final test message", uuid.New().String()},
		// {"tasks.process.string", "Hello, World!", uuid.New().String()},
		// {"tasks.process.string", "Process this text", uuid.New().String()},
		// {"tasks.process.string", "Another message to process", uuid.New().String()},
		// {"tasks.process.data", "Some data processing task", uuid.New().String()},
		// {"tasks.process.data", "More data to handle", uuid.New().String()},
		// {"tasks.process.string", "Final test message", uuid.New().String()},
		// {"tasks.process.string", "Hello, World!", uuid.New().String()},
		// {"tasks.process.string", "Process this text", uuid.New().String()},
		// {"tasks.process.string", "Another message to process", uuid.New().String()},
		// {"tasks.process.data", "Some data processing task", uuid.New().String()},
		// {"tasks.process.data", "More data to handle", uuid.New().String()},
		// {"tasks.process.string", "Final test message", uuid.New().String()},
		// {"tasks.process.string", "Hello, World!", uuid.New().String()},
		// {"tasks.process.string", "Process this text", uuid.New().String()},
		// {"tasks.process.string", "Another message to process", uuid.New().String()},
		// {"tasks.process.data", "Some data processing task", uuid.New().String()},
		// {"tasks.process.data", "More data to handle", uuid.New().String()},
		// {"tasks.process.string", "Final test message", uuid.New().String()},
		// {"tasks.process.string", "Hello, World!", uuid.New().String()},
		// {"tasks.process.string", "Process this text", uuid.New().String()},
		// {"tasks.process.string", "Another message to process", uuid.New().String()},
		// {"tasks.process.data", "Some data processing task", uuid.New().String()},
		// {"tasks.process.data", "More data to handle", uuid.New().String()},
		// {"tasks.process.string", "Final test message", uuid.New().String()},
		// {"tasks.process.string", "Hello, World!", uuid.New().String()},
		// {"tasks.process.string", "Process this text", uuid.New().String()},
		// {"tasks.process.string", "Another message to process", uuid.New().String()},
		// {"tasks.process.data", "Some data processing task", uuid.New().String()},
		// {"tasks.process.data", "More data to handle", uuid.New().String()},
		// {"tasks.process.string", "Final test message", uuid.New().String()},
		// {"tasks.process.string", "Hello, World!", uuid.New().String()},
		// {"tasks.process.string", "Process this text", uuid.New().String()},
		// {"tasks.process.string", "Another message to process", uuid.New().String()},
		// {"tasks.process.data", "Some data processing task", uuid.New().String()},
		// {"tasks.process.data", "More data to handle", uuid.New().String()},
		// {"tasks.process.string", "Final test message", uuid.New().String()},
		// {"tasks.process.string", "Hello, World!", uuid.New().String()},
		// {"tasks.process.string", "Process this text", uuid.New().String()},
		// {"tasks.process.string", "Another message to process", uuid.New().String()},
		// {"tasks.process.data", "Some data processing task", uuid.New().String()},
		// {"tasks.process.data", "More data to handle", uuid.New().String()},
		// {"tasks.process.string", "Final test message", uuid.New().String()},
		// {"tasks.process.string", "Hello, World!", uuid.New().String()},
		// {"tasks.process.string", "Process this text", uuid.New().String()},
		// {"tasks.process.string", "Another message to process", uuid.New().String()},
		// {"tasks.process.data", "Some data processing task", uuid.New().String()},
		// {"tasks.process.data", "More data to handle", uuid.New().String()},
		// {"tasks.process.string", "Final test message", uuid.New().String()},
	}

	for i, msgData := range messages {
		// Create a structured message
		msg := message.NewWorkflowMessage(workflowID, msgData.runID)

		// Add node information
		msg.WithNode(fmt.Sprintf("node-%d", i+1), map[string]interface{}{
			"type":       "processor",
			"priority":   rand.Intn(10) + 1,
			"retryCount": 0,
		})

		// Add payload
		data := msgData.data
		msg.Payload = &message.Payload{
			InlineData: &data,
		}

		// Add some metadata
		msg.WithMetadata("messageType", "task")
		msg.WithMetadata("batchId", fmt.Sprintf("batch-%d", (i/2)+1))

		// Publish the message
		payload, err := msg.ToBytes()
		if err != nil {
			return fmt.Errorf("failed to marshal message %d: %w", i+1, err)
		}
		if _, err := js.Publish(ctx, msgData.subject, payload); err != nil {
			return fmt.Errorf("failed to publish message %d: %w", i+1, err)
		}

		fmt.Printf("✓ Published message %d to %s: %s\n", i+1, msgData.subject, msgData.data)

		// Small delay between messages to make the output clearer
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Printf("✅ Published %d test messages\n\n", len(messages))
	return nil
}
