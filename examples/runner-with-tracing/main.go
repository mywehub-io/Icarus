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
func (p *SimpleProcessor) Process(ctx context.Context, msg *message.Message) (message.Message, error) {
	// Simulate some processing time (1-5 seconds)
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
	case <-ctx.Done():
		// Context cancelled
		p.logger.Warn("Processor cancelled", zap.String("processor", p.name), zap.Error(ctx.Err()))
		return message.Message{}, ctx.Err()
	}

	// Simulate occasional failures (10% chance)
	if rand.Float64() < 0.1 {
		err := fmt.Errorf("simulated processing error in processor '%s'", p.name)
		return message.Message{}, err
	}

	// Create a response message
	resultMsg := message.NewMessage()
	resultMsg.WithPayload( fmt.Sprintf("Processed by %s at %s", p.name, time.Now().Format(time.RFC3339)))
	resultMsg.WithNode("result-node", map[string]interface{}{
		"processor":      p.name,
		"processingTime": processingTime.String(),
		"timestamp":      time.Now().Unix(),
	})

	// If this was a workflow message, preserve the workflow information
	if msg.Workflow != nil {
		resultMsg.Workflow = msg.Workflow
	}

	p.logger.Info("Processor generated result", zap.String("processor", p.name))
	return *resultMsg, nil
}

// setupStreams creates the necessary streams and consumers for the example
func setupStreams(ctx context.Context, js jetstream.JetStream, logger *zap.Logger) error {
	logger.Info("Setting up JetStream streams and consumers...")

	// Create MESSAGES stream for incoming messages to process
	logger.Info("Creating MESSAGES stream...")
	_, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:        "MESSAGES",
		Description: "Stream for messages to be processed with tracing",
		Subjects:    []string{"messages.>"},
		Storage:     jetstream.FileStorage,
		Replicas:    1,
		MaxAge:      24 * time.Hour, // Keep messages for 24 hours
	})
	if err != nil {
		logger.Warn("Could not create MESSAGES stream (might already exist)", zap.Error(err))
	} else {
		logger.Info("✓ Created MESSAGES stream")
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

	// Create consumer for the MESSAGES stream
	logger.Info("Creating processor consumer...")
	_, err = js.CreateConsumer(ctx, "MESSAGES", jetstream.ConsumerConfig{
		Durable:       "processor-consumer",
		Description:   "Consumer for processing messages with tracing",
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxDeliver:    3,                // Retry failed messages up to 3 times
		AckWait:       30 * time.Second, // Wait 30 seconds for acknowledgment
		MaxAckPending: 100,              // Allow up to 100 unacknowledged messages
	})
	if err != nil {
		logger.Warn("Could not create processor consumer (might already exist)", zap.Error(err))
	} else {
		logger.Info("✓ Created processor consumer")
	}

	logger.Info("✅ Stream and consumer setup complete")
	return nil
}

func main() {
	// Create a logger (use Production for JSON format, Development for human-readable)
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("Failed to create logger: %v", err)
	}
	defer logger.Sync()

	// NATS server URL
	natsURL := "nats://localhost:4222"
	if url := os.Getenv("NATS_URL"); url != "" {
		natsURL = url
	}

	// Stream and consumer configuration
	streamName := "MESSAGES"
	consumerName := "processor-consumer"

	// Create Icarus client
	ctx := context.Background()
	icarusClient := client.NewClient(natsURL, "RESULTS", "result")
	icarusClient.SetLogger(logger) // Use the same logger for consistency
	err = icarusClient.Connect(ctx)
	if err != nil {
		logger.Fatal("Failed to connect Icarus client", zap.Error(err))
	}
	defer icarusClient.Close()

	// Set the same logger for the message service
	icarusClient.Messages.SetLogger(logger)

	// Get JetStream context for stream/consumer setup
	js := icarusClient.JetStream()
	if js == nil {
		logger.Fatal("JetStream not available")
	}

	// Setup streams and consumers
	if err := setupStreams(ctx, js, logger); err != nil {
		logger.Fatal("Failed to setup streams", zap.Error(err))
	}

	// Create processor
	processor := NewSimpleProcessor("example-processor", logger)

	// Setup tracing configuration
	tracingConfig := runner.JaegerTracingConfig("testSDK2")

	cfg := runner.DefaultConfig()

	// Create runner with built-in tracing
	runnerInstance, err := runner.NewRunner(
		icarusClient,
		processor,
		streamName,
		consumerName,
		10,             // batch size
		30*time.Second, // process timeout
		logger,
		&tracingConfig, // tracing configuration
		&cfg,           // worker config (nil uses defaults/env)
	)
	if err != nil {
		logger.Fatal("Failed to create runner", zap.Error(err))
	}
	defer runnerInstance.Close() // This will clean up tracing

	// Start message producer in a separate goroutine
	go func() {
		produceMessages(ctx, js, logger)
	}()

	// Create a context that can be cancelled
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start the runner in a goroutine
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Info("Starting runner...")
		if err := runnerInstance.Run(ctx); err != nil && err != context.Canceled {
			logger.Error("Runner error", zap.Error(err))
		}
		logger.Info("Runner stopped")
	}()

	// Wait for shutdown signal
	<-sigChan
	logger.Info("Shutdown signal received, stopping...")

	// Cancel context to stop runner
	cancel()

	// Wait for runner to stop with timeout
	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
	}()

	// Wait for graceful shutdown with timeout
	shutdownTimeout := 30 * time.Second
	select {
	case <-done:
		logger.Info("Application stopped gracefully")
	case <-time.After(shutdownTimeout):
		logger.Warn("Shutdown timeout reached, forcing exit", zap.Duration("timeout", shutdownTimeout))
	}
}

// produceMessages generates test messages for the runner to process
func produceMessages(ctx context.Context, js jetstream.JetStream, logger *zap.Logger) {
	for i := 0; i < 10; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Create a test message
		msg := message.NewWorkflowMessage(
			fmt.Sprintf("workflow-%d", i%3),
			uuid.New().String(),
		)

		msg.WithPayload(
			fmt.Sprintf("Test data %d - %s", i, time.Now().Format(time.RFC3339)),
		)

		msg.WithNode(
			fmt.Sprintf("node-%d", i%5),
			map[string]interface{}{
				"type":     "test",
				"sequence": i,
			},
		)

		// Publish to JetStream
		subject := fmt.Sprintf("messages.test.%d", i)
		data, err := msg.ToBytes()
		if err != nil {
			logger.Error("Failed to marshal message", zap.Error(err))
			continue
		}
		if _, err := js.Publish(ctx, subject, data); err != nil {
			logger.Error("Failed to publish message", zap.Error(err))
		} else {
			logger.Info("Published message", zap.String("subject", subject), zap.Int("sequence", i))
		}

		// Wait between messages
		time.Sleep(2 * time.Second)
	}

	logger.Info("Finished producing messages")
}
