package nats

import "testing"

func TestDefaultConnectionConfig_MaxReconnectsUnlimited(t *testing.T) {
	cfg := DefaultConnectionConfig("nats://localhost:4222")
	if cfg.MaxReconnects != -1 {
		t.Fatalf("MaxReconnects = %d, want -1", cfg.MaxReconnects)
	}
}
