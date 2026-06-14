package client

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestEnsureConnected_reconnectsAfterStaleConn(t *testing.T) {
	live, err := nats.Connect("nats://127.0.0.1:4222", nats.Timeout(2*time.Second))
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}
	live.Close()

	c := NewClient("nats://127.0.0.1:4222", "RESULTS", "result")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	stale := c.Connection()
	stale.Close()

	if c.IsConnected() {
		t.Fatal("expected stale connection to be closed")
	}
	if c.Messages == nil {
		t.Fatal("expected Messages before EnsureConnected")
	}

	if err := c.EnsureConnected(ctx); err != nil {
		t.Fatalf("EnsureConnected failed: %v", err)
	}
	if !c.IsConnected() {
		t.Fatal("expected connected after EnsureConnected")
	}
	if c.Messages == nil {
		t.Fatal("expected Messages restored after EnsureConnected")
	}
}

func TestEnsureConnected_idempotent(t *testing.T) {
	live, err := nats.Connect("nats://127.0.0.1:4222", nats.Timeout(2*time.Second))
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}
	live.Close()

	c := NewClient("nats://127.0.0.1:4222", "RESULTS", "result")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Connect(ctx); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- c.EnsureConnected(ctx)
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("EnsureConnected returned error: %v", err)
		}
	}
}
