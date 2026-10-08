package message

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

// An existing stream without a size cap gets one that discards its oldest messages, and nothing
// else about it changes (UAT incident 08/10/2026, C2). Runs against a real JetStream
// (ICARUS_TEST_NATS_URL, default the local estate's nats://127.0.0.1:14222) on a throwaway
// stream; skips when none is reachable.
func TestEnsureSizeCapCapsAnExistingStream(t *testing.T) {
	url := os.Getenv("ICARUS_TEST_NATS_URL")
	if url == "" {
		url = "nats://127.0.0.1:14222"
	}
	nc, err := nats.Connect(url, nats.Timeout(2*time.Second))
	if err != nil {
		t.Skipf("no NATS at %s: %v", url, err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	name := fmt.Sprintf("ICARUS_CAP_TEST_%d", time.Now().UnixNano())
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: name, Subjects: []string{name + ".>"}, Storage: jetstream.FileStorage, MaxAge: time.Hour, MaxMsgs: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = js.DeleteStream(ctx, name) }()

	if err := EnsureSizeCap(ctx, js, stream, 8<<20, zap.NewNop()); err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c := info.Config
	if c.MaxBytes != 8<<20 || c.Discard != jetstream.DiscardOld || c.MaxAge != time.Hour || c.MaxMsgs != 100 {
		t.Fatalf("max_bytes %d discard %v max_age %v max_msgs %d", c.MaxBytes, c.Discard, c.MaxAge, c.MaxMsgs)
	}
	// Already capped: no update needed, and calling again is harmless.
	if err := EnsureSizeCap(ctx, js, stream, 8<<20, zap.NewNop()); err != nil {
		t.Fatal(err)
	}
}

func TestWithSizeCapDiscardsTheOldest(t *testing.T) {
	cfg := WithSizeCap(jetstream.StreamConfig{Name: "X", MaxAge: time.Hour}, DefaultWorkStreamMaxBytes)
	if cfg.MaxBytes != DefaultWorkStreamMaxBytes || cfg.Discard != jetstream.DiscardOld || cfg.MaxAge != time.Hour {
		t.Fatalf("%+v", cfg)
	}
}
