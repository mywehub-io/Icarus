package jobs

import (
	"strings"
	"testing"
)

func TestNewJobID(t *testing.T) {
	id := NewJobID()
	if !strings.HasPrefix(id, "JOB-") {
		t.Fatalf("expected JOB- prefix, got %q", id)
	}
	if len(id) <= len("JOB-") {
		t.Fatalf("expected uuid suffix, got %q", id)
	}
}

func TestMessageID(t *testing.T) {
	if got := MessageID("JOB-abc", 1); got != "JOB-abc:1" {
		t.Fatalf("MessageID = %q", got)
	}
	if got := MessageID("JOB-abc", 42); got != "JOB-abc:42" {
		t.Fatalf("MessageID = %q", got)
	}
}
