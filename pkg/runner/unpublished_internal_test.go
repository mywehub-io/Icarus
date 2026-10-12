package runner

import (
	"fmt"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/message"
)

// A unit whose result could not be published keeps that result, and its redelivery takes it
// exactly once: the unit is not run again for its side effects.
func TestUnpublishedResultIsKeptAndTakenOnce(t *testing.T) {
	r := &Runner{}
	want := message.Message{}
	want.Workflow = &message.Workflow{WorkflowID: "wf1", RunID: "run1"}
	r.keepUnpublished("exec1", want)

	got, ok := r.takeUnpublished("exec1")
	if !ok || got.Workflow == nil || got.Workflow.RunID != "run1" {
		t.Fatalf("takeUnpublished = %+v, %v; want the kept result", got, ok)
	}
	if _, ok := r.takeUnpublished("exec1"); ok {
		t.Fatal("a kept result must be taken once")
	}
	if _, ok := r.takeUnpublished(""); ok {
		t.Fatal("an empty execution id has nothing kept")
	}
}

func TestUnpublishedResultsAreBounded(t *testing.T) {
	r := &Runner{}
	for i := 0; i < maxUnpublishedResults+5; i++ {
		r.keepUnpublished(fmt.Sprintf("exec%d", i), message.Message{})
	}
	if _, ok := r.takeUnpublished("exec0"); ok {
		t.Fatal("the oldest kept result must be dropped beyond the bound")
	}
	if _, ok := r.takeUnpublished(fmt.Sprintf("exec%d", maxUnpublishedResults+4)); !ok {
		t.Fatal("the newest kept result must still be there")
	}
}
