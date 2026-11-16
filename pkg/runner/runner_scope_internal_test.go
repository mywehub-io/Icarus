package runner

import (
	"testing"

	"github.com/wehubfusion/Icarus/pkg/client"
)

func TestScopedExecutionHeartbeatKey(t *testing.T) {
	if got := scopedExecutionHeartbeatKey("", "wf1", "run1", "node1"); got != "wf1.run1.node1" {
		t.Fatalf("unscoped key = %q", got)
	}
	if got := scopedExecutionHeartbeatKey("r1", "wf1", "run1", "node1"); got != "r1.wf1.run1.node1" {
		t.Fatalf("scoped key = %q", got)
	}
}

// A runner whose client has a runner scope claims and heartbeats under <runner id>.<wf>.<run>.<node>,
// the form Zeus's liveness sweeper reads first.
func TestClaimOrNak_RunnerScope_UsesScopedKey(t *testing.T) {
	kv := &fakeHeartbeatKV{}
	r := newHeartbeatRunnerWithKV(kv)
	c := client.NewClient("nats://127.0.0.1:4222", "RESULTS", "result")
	c.SetRunnerScope("4f6c2a1e-9b7d-4c3a-8e21-5d0f7a9b3c11")
	r.client = c

	msg := buildMessageFor(t, &heartbeatMsg{})
	if !r.claimOrNak(t.Context(), msg, "wf1", "run1", "node1", "exec1") {
		t.Fatal("expected to win a fresh claim")
	}
	if _, ok := kv.entry(t, "4f6c2a1e-9b7d-4c3a-8e21-5d0f7a9b3c11."+claimKey); !ok {
		t.Fatal("claim was not written under the runner-scoped key")
	}
	if _, ok := kv.entry(t, claimKey); ok {
		t.Fatal("claim was also written under the unscoped key")
	}
}
