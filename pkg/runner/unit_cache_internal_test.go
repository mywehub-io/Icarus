package runner

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/wehubfusion/Icarus/pkg/message"
	"github.com/wehubfusion/Icarus/pkg/storage"
)

// Each unit's context carries its own unit cache: the resolver reads a small upstream archive
// once per unit, and no unit sees another unit's cached bytes.
func TestProcessMessageGivesEachUnitItsOwnCache(t *testing.T) {
	r, _ := handOffRunner(&fakeHeartbeatKV{}, nil)
	var seen []*storage.UnitCache
	r.processor = funcProcessor(func(ctx context.Context, _ *message.Message) (message.Message, error) {
		seen = append(seen, storage.UnitCacheFrom(ctx))
		// Hand off so processMessage returns at once, with nothing to publish.
		return message.Message{}, ErrHandOff
	})

	for i, id := range []string{"wf1:run1:node1", "wf1:run1:node2"} {
		_, msg := wrapForHandOff(t, 1, nats.Header{jetstream.MsgIDHeader: []string{id}})
		if err := r.processMessage(context.Background(), msg); err != nil {
			t.Fatalf("unit %d: processMessage: %v", i, err)
		}
	}

	if len(seen) != 2 {
		t.Fatalf("Process called %d times, want 2", len(seen))
	}
	if seen[0] == nil || seen[1] == nil {
		t.Fatal("a unit's context carried no unit cache")
	}
	if seen[0] == seen[1] {
		t.Fatal("two units shared one unit cache")
	}
}
