package message

import (
	"slices"
	"testing"
)

func TestRunnerScope_ResultSubjects(t *testing.T) {
	s := &MessageService{resultSubject: "result_uat"}
	if got := s.resultPublishSubject(); got != "result_uat" {
		t.Fatalf("unscoped publish subject = %q", got)
	}
	if got := s.resultStreamSubjects(); !slices.Equal(got, []string{"result_uat"}) {
		t.Fatalf("unscoped stream subjects = %v", got)
	}

	s.SetRunnerScope(" 4f6c2a1e-9b7d-4c3a-8e21-5d0f7a9b3c11 ")
	if got := s.resultPublishSubject(); got != "result_uat.4f6c2a1e-9b7d-4c3a-8e21-5d0f7a9b3c11" {
		t.Fatalf("scoped publish subject = %q", got)
	}
	// The same subjects Zeus gives the stream, so a stream created here takes scoped results.
	if got := s.resultStreamSubjects(); !slices.Equal(got, []string{"result_uat", "result_uat.*"}) {
		t.Fatalf("scoped stream subjects = %v", got)
	}
}
