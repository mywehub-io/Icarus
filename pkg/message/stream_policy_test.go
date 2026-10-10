package message

import (
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

func TestWorkAndDLQStreamConfigs(t *testing.T) {
	w := WorkStreamConfig(jetstream.StreamConfig{Name: "S", Subjects: []string{"S.>"}, MaxAge: 1})
	if w.Retention != jetstream.WorkQueuePolicy || w.Discard != jetstream.DiscardNew || w.MaxBytes != DefaultWorkStreamMaxBytes {
		t.Fatalf("work stream config = %+v", w)
	}
	if w.Name != "S" || w.MaxAge != 1 {
		t.Fatal("work stream config must keep the caller's other fields")
	}
	d := DLQStreamConfig(jetstream.StreamConfig{Name: "S_DLQ"})
	if d.Retention != jetstream.LimitsPolicy || d.Discard != jetstream.DiscardNew || d.MaxBytes != DefaultDLQStreamMaxBytes {
		t.Fatalf("DLQ stream config = %+v", d)
	}
}

// A work stream still on limits retention must keep dropping its oldest messages: refusing new
// ones when full would stop all work, because its acknowledged messages count towards the cap.
func TestPolicyForAnExistingStream(t *testing.T) {
	cases := []struct {
		kind      StreamKind
		retention jetstream.RetentionPolicy
		maxBytes  int64
		discard   jetstream.DiscardPolicy
	}{
		{WorkStream, jetstream.WorkQueuePolicy, DefaultWorkStreamMaxBytes, jetstream.DiscardNew},
		{WorkStream, jetstream.LimitsPolicy, DefaultWorkStreamMaxBytes, jetstream.DiscardOld},
		{DLQStream, jetstream.LimitsPolicy, DefaultDLQStreamMaxBytes, jetstream.DiscardNew},
	}
	for _, c := range cases {
		maxBytes, discard := policyFor(c.kind, c.retention)
		if maxBytes != c.maxBytes || discard != c.discard {
			t.Errorf("policyFor(%v, %v) = %d, %v; want %d, %v", c.kind, c.retention, maxBytes, discard, c.maxBytes, c.discard)
		}
	}
}
