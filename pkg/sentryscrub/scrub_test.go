package sentryscrub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
)

// hl7 is a synthetic message. No part of it may reach Sentry.
const hl7 = "MSH|^~\\&|SENDER|FAC|RECV|FAC|20260101120000||ADT^A01|MSG0001|P|2.4\rPID|1||9990000018^^^NHS||TESTPATIENT^ALEX"

// leaks reports whether any recognisable piece of hl7 is in s.
func leaks(s string) bool {
	for _, piece := range []string{"MSH|", "PID|", "TESTPATIENT", "9990000018", "ADT^A01"} {
		if strings.Contains(s, piece) {
			return true
		}
	}
	return false
}

func TestInternalErrorMessage(t *testing.T) {
	cases := []struct {
		name, metaData, code, want string
	}{
		{"code only", "", "SCHEMA_FETCH_ERROR", "Code: SCHEMA_FETCH_ERROR"},
		{"metadata and code", "service=zeus run_id=7f3c", "RUN_FAILED", "MetaData: service=zeus run_id=7f3c\nCode: RUN_FAILED"},
		{"code in the metadata", "REPO_ERROR", "", "MetaData: REPO_ERROR\nCode: "},
		{"metadata with HL7 is dropped", hl7, "PARSE_ERROR", "Code: PARSE_ERROR"},
		{"metadata with JSON is dropped", `{"family":"Test"}`, "PARSE_ERROR", "Code: PARSE_ERROR"},
		{"metadata with a line break is dropped", "run_id=1\nPID|1", "PARSE_ERROR", "Code: PARSE_ERROR"},
		{"code that is not an identifier", "", "failed on PID|1", Withheld},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := InternalErrorMessage(tc.metaData, tc.code); got != tc.want {
				t.Fatalf("InternalErrorMessage(%q, %q) = %q, want %q", tc.metaData, tc.code, got, tc.want)
			}
		})
	}
}

func TestScrubMessage(t *testing.T) {
	cases := []struct {
		name, message, want string
	}{
		{
			name:    "legacy internal error",
			message: "Message: failed to parse message: " + hl7 + "\nCode: PARSE_ERROR\nError: unexpected segment in " + hl7,
			want:    "Code: PARSE_ERROR",
		},
		{
			name:    "legacy internal error with metadata",
			message: "MetaData: nats_client=elysium\nMessage: publish failed\nCode: NATS_PUBLISH_FAILED\nError: " + hl7,
			want:    "MetaData: nats_client=elysium\nCode: NATS_PUBLISH_FAILED",
		},
		{
			name:    "legacy internal error with a nil cause",
			message: "Message: Concept service is not configured\nCode: \nError: <nil>",
			want:    "Code: ",
		},
		{
			name:    "legacy message over several lines",
			message: "Message: transform failed:\n" + strings.ReplaceAll(hl7, "\r", "\n") + "\nCode: TRANSFORM_FAILED\nError: x",
			want:    "Code: TRANSFORM_FAILED",
		},
		{
			name: "a code line inside the cause is not used",
			// The earliest code line followed by an error line is NewInternalError's own.
			message: "Message: m\nCode: REAL_CODE\nError: bad input\nCode: PID|1\nError: again",
			want:    "Code: REAL_CODE",
		},
		{
			name:    "legacy message with HL7 in place of a code",
			message: "Message: m\nCode: " + hl7 + "\nError: x",
			want:    Withheld,
		},
		{"current shape is kept", "MetaData: run_id=1\nCode: RUN_FAILED", "MetaData: run_id=1\nCode: RUN_FAILED"},
		{"current shape without metadata", "Code: RUN_FAILED", "Code: RUN_FAILED"},
		{"free text", "caller stopped waiting after 30s", Withheld},
		{"free text holding a message", "could not route " + hl7, Withheld},
		{"metadata line alone", "MetaData: run_id=1", Withheld},
		{"empty", "", ""},
		{"withheld stays withheld", Withheld, Withheld},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := scrubMessage(tc.message)
			if got != tc.want {
				t.Fatalf("scrubMessage(%q) = %q, want %q", tc.message, got, tc.want)
			}
			if leaks(got) {
				t.Fatalf("scrubMessage kept message content: %q", got)
			}
			if again := scrubMessage(got); again != got {
				t.Fatalf("scrubMessage is not idempotent: %q then %q", got, again)
			}
		})
	}
}

func TestScrubEvent(t *testing.T) {
	handled := false
	event := &sentry.Event{
		Message: "free text " + hl7,
		Exception: []sentry.Exception{
			{Type: "*fmt.wrapError", Value: "parse failed: " + hl7, Stacktrace: &sentry.Stacktrace{Frames: []sentry.Frame{{Function: "parse"}}}},
			{Type: "*errors.errorString", Value: hl7, Mechanism: &sentry.Mechanism{Type: "generic", Handled: &handled, Description: hl7, Data: map[string]any{"input": hl7}}},
		},
		Request:     &sentry.Request{URL: "https://api.example/runs", Data: hl7},
		User:        sentry.User{ID: "u1", Email: "someone@example.com"},
		Breadcrumbs: []*sentry.Breadcrumb{{Message: hl7}},
		Extra:       map[string]any{"payload": hl7},
		Tags:        map[string]string{"run_id": "7f3c"},
		Contexts:    map[string]sentry.Context{"sync_wait": {"waited_seconds": 30.0}},
		Fingerprint: []string{"hermes-sync-canceled"},
	}

	got := Scrub(event, nil)

	if got != event {
		t.Fatal("Scrub must return the event it was given")
	}
	if got.Message != Withheld {
		t.Errorf("Message = %q, want %q", got.Message, Withheld)
	}
	for i, e := range got.Exception {
		if e.Value != "" {
			t.Errorf("Exception[%d].Value = %q, want empty", i, e.Value)
		}
	}
	if got.Exception[0].Type != "*fmt.wrapError" || got.Exception[0].Stacktrace == nil {
		t.Errorf("exception type or stack trace lost: %+v", got.Exception[0])
	}
	if m := got.Exception[1].Mechanism; m.Type != "generic" || m.Handled == nil || m.Description != "" || m.Data != nil {
		t.Errorf("mechanism = %+v, want its type and handled kept, description and data removed", m)
	}
	if got.Request != nil || !reflect.DeepEqual(got.User, sentry.User{}) || got.Breadcrumbs != nil || got.Extra != nil {
		t.Errorf("request, user, breadcrumbs or extra kept: %+v %+v %+v %+v", got.Request, got.User, got.Breadcrumbs, got.Extra)
	}
	if got.Tags["run_id"] != "7f3c" || got.Contexts["sync_wait"]["waited_seconds"] != 30.0 || got.Fingerprint[0] != "hermes-sync-canceled" {
		t.Errorf("tags, contexts or fingerprint changed: %+v %+v %+v", got.Tags, got.Contexts, got.Fingerprint)
	}

	if Scrub(nil, nil) != nil {
		t.Error("Scrub(nil) must return nil")
	}
}

// recordingTransport keeps the events a client would send.
type recordingTransport struct {
	mu     sync.Mutex
	events []*sentry.Event
}

func (r *recordingTransport) Configure(sentry.ClientOptions)        {}
func (r *recordingTransport) Flush(time.Duration) bool              { return true }
func (r *recordingTransport) FlushWithContext(context.Context) bool { return true }
func (r *recordingTransport) Close()                                {}
func (r *recordingTransport) SendEvent(event *sentry.Event) {
	r.mu.Lock()
	r.events = append(r.events, event)
	r.mu.Unlock()
}
func (r *recordingTransport) sent() []*sentry.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.events
}

// TestClientOptionsEndToEnd sends events through a client built from ClientOptions, the way a
// service does, and checks the serialised events that would leave the process.
func TestClientOptionsEndToEnd(t *testing.T) {
	transport := &recordingTransport{}
	opts := ClientOptions("https://public@sentry.example/1", "UAT")
	opts.Transport = transport
	// ContextifyFrames adds the source lines around each stack frame, read from disk. That is code,
	// not data, and a service's image ships no source, but here the source is this file, whose
	// lines name the test values. Without it the checks below see only what the event carries.
	opts.Integrations = func(integrations []sentry.Integration) []sentry.Integration {
		kept := integrations[:0]
		for _, i := range integrations {
			if i.Name() != "ContextifyFrames" {
				kept = append(kept, i)
			}
		}
		return kept
	}
	client, err := sentry.NewClient(opts)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	hub := sentry.NewHub(client, sentry.NewScope())

	hub.AddBreadcrumb(&sentry.Breadcrumb{Message: "received " + hl7}, nil)
	hub.ConfigureScope(func(scope *sentry.Scope) {
		scope.SetUser(sentry.User{Email: "someone@example.com"})
		scope.SetExtra("payload", hl7)
		scope.SetTag("run_id", "7f3c")
	})
	// What NewInternalError sent before this module, from a plugin that failed on a message.
	hub.CaptureMessage(fmt.Sprintf("Message: %s\nCode: %s\nError: %v",
		"embedded node processing failed: TypeError: no ward for "+hl7, "PROCESSING_ERROR", errors.New(hl7)))
	hub.CaptureException(fmt.Errorf("parse failed on %q: %w", hl7, errors.New("bad segment")))
	hub.CaptureMessage("could not route " + hl7)

	events := transport.sent()
	if len(events) != 3 {
		t.Fatalf("sent %d events, want 3", len(events))
	}
	for i, event := range events {
		raw, err := json.Marshal(event)
		if err != nil {
			t.Fatalf("marshal event %d: %v", i, err)
		}
		if leaks(string(raw)) {
			t.Errorf("event %d carries message content: %s", i, raw)
		}
		if strings.Contains(string(raw), "someone@example.com") {
			t.Errorf("event %d carries the user: %s", i, raw)
		}
		if event.Tags["run_id"] != "7f3c" {
			t.Errorf("event %d lost its tag: %+v", i, event.Tags)
		}
	}

	if got := events[0].Message; got != "Code: PROCESSING_ERROR" {
		t.Errorf("internal error message = %q, want the code alone", got)
	}
	if len(events[0].Threads) == 0 || events[0].Threads[0].Stacktrace == nil {
		t.Error("a message event must carry a stack trace, so a scrubbed event still says where it came from")
	}
	// sentry-go lists the chain innermost first and puts the stack trace on the outermost error.
	chain := events[1].Exception
	if len(chain) != 2 || chain[0].Type != "*errors.errorString" || chain[1].Type != "*fmt.wrapError" || chain[1].Stacktrace == nil {
		t.Errorf("exception event lost its types or stack trace: %+v", chain)
	}
	if got := events[2].Message; got != Withheld {
		t.Errorf("free text message = %q, want %q", got, Withheld)
	}
}

func TestClientOptionsDropTransactions(t *testing.T) {
	opts := ClientOptions("", "UAT")
	if opts.BeforeSendTransaction(&sentry.Event{Type: "transaction"}, nil) != nil {
		t.Error("transactions must be dropped")
	}
	if opts.MaxBreadcrumbs >= 0 || opts.SendDefaultPII || !opts.AttachStacktrace {
		t.Errorf("options = %+v, want breadcrumbs off, no default PII and stack traces on", opts)
	}
}
