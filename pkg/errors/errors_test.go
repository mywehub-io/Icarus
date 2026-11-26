package errors

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestToGRPCErrorCarriesTheCode(t *testing.T) {
	err := NewConflictError("tag v1 already exists", "TAG_EXISTS", nil).ToGRPCError()
	st, _ := status.FromError(err)
	if st.Code() != codes.AlreadyExists {
		t.Fatalf("code = %v, want AlreadyExists", st.Code())
	}
	if st.Message() != "tag v1 already exists" {
		t.Fatalf("message = %q", st.Message())
	}
	if got := CodeFromGRPC(err); got != "TAG_EXISTS" {
		t.Fatalf("CodeFromGRPC = %q, want TAG_EXISTS", got)
	}
}

func TestToGRPCErrorWithoutACodeHasNoDetail(t *testing.T) {
	err := NewBadRequestError("bad", "", nil).ToGRPCError()
	st, _ := status.FromError(err)
	if len(st.Details()) != 0 {
		t.Fatalf("details = %v, want none", st.Details())
	}
	if got := CodeFromGRPC(err); got != "" {
		t.Fatalf("CodeFromGRPC = %q, want empty", got)
	}
}

func TestTimeoutIsDeadlineExceeded(t *testing.T) {
	err := NewTimeoutError("timed out waiting", "WAIT_TIMEOUT", nil).ToGRPCError()
	st, _ := status.FromError(err)
	if st.Code() != codes.DeadlineExceeded {
		t.Fatalf("code = %v, want DeadlineExceeded", st.Code())
	}
	if got := CodeFromGRPC(err); got != "WAIT_TIMEOUT" {
		t.Fatalf("CodeFromGRPC = %q, want WAIT_TIMEOUT", got)
	}
}

// Type is serialised as its number, so a new type must not renumber the old ones.
func TestErrorTypeNumbersAreStable(t *testing.T) {
	want := map[ErrorType]int{
		Internal: 0, NotFound: 1, BadRequest: 2, Unauthorized: 3, Conflict: 4,
		ValidationFailed: 5, PermissionDenied: 6, FailedPrecondition: 7, Timeout: 8,
	}
	for typ, n := range want {
		if int(typ) != n {
			t.Errorf("type %d, want %d", typ, n)
		}
	}
}

func TestCodeFromGRPCOnAPlainError(t *testing.T) {
	if got := CodeFromGRPC(status.Error(codes.Internal, "x")); got != "" {
		t.Fatalf("CodeFromGRPC = %q", got)
	}
}

// sentryRecorder keeps the events a Sentry client would send.
type sentryRecorder struct {
	mu     sync.Mutex
	events []*sentry.Event
}

func (r *sentryRecorder) Configure(sentry.ClientOptions)        {}
func (r *sentryRecorder) Flush(time.Duration) bool              { return true }
func (r *sentryRecorder) FlushWithContext(context.Context) bool { return true }
func (r *sentryRecorder) Close()                                {}
func (r *sentryRecorder) SendEvent(event *sentry.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
}

func TestNewInternalErrorSendsSentryNoMessageContent(t *testing.T) {
	recorder := &sentryRecorder{}
	client, err := sentry.NewClient(sentry.ClientOptions{Dsn: "https://public@sentry.example/1", Transport: recorder})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	previous := sentry.CurrentHub().Client()
	sentry.CurrentHub().BindClient(client)
	t.Cleanup(func() { sentry.CurrentHub().BindClient(previous) })

	segment := "PID|1||9990000018^^^NHS||TESTPATIENT^ALEX"
	cause := errors.New("unexpected segment " + segment)
	withMeta := NewInternalError("service=elysium run_id=7f3c", "failed to parse "+segment, "PARSE_ERROR", cause)
	withoutMeta := NewInternalError("", "failed to parse "+segment, "PARSE_ERROR", cause)

	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	want := []string{"MetaData: service=elysium run_id=7f3c\nCode: PARSE_ERROR", "Code: PARSE_ERROR"}
	if len(recorder.events) != len(want) {
		t.Fatalf("sent %d events, want %d", len(recorder.events), len(want))
	}
	for i, event := range recorder.events {
		if event.Message != want[i] {
			t.Errorf("event %d message = %q, want %q", i, event.Message, want[i])
		}
	}
	// The error itself keeps everything, for the service's logs.
	for _, e := range []*AppError{withMeta, withoutMeta} {
		if e.Message != "failed to parse "+segment || e.Err != cause || e.Code != "PARSE_ERROR" {
			t.Errorf("AppError lost its message, cause or code: %+v", e)
		}
	}
}
