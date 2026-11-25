// Package httpaudit is net/http middleware with the same recording rules as grpcaudit, for a
// service that serves HTTP rather than gRPC.
//
// It records every response with status 401 or 403 (category DENIAL), a write at an entry
// (category WRITE) and a sensitive read at an entry (category SENSITIVE_READ, asynchronous).
// By default GET and HEAD are reads and every other method is a write, and a request is an entry
// when its path is not under /internal/ and the last x-forwarded-client-cert element names one
// of Config.EntryPrincipals.
package httpaudit

import (
	"bufio"
	"errors"
	"net"
	"net/http"
	"strings"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"

	"github.com/wehubfusion/Icarus/platform/audit"
	"github.com/wehubfusion/Icarus/platform/audit/auditpb"
	"github.com/wehubfusion/Icarus/platform/audit/grpcaudit"
	"github.com/wehubfusion/Icarus/platform/audit/internal/hook"
)

// Config configures the middleware. Every value comes from the service's code or configuration.
type Config struct {
	// Emitter publishes the events. Nil records nothing.
	Emitter *audit.Emitter
	// Service names the service in each action: <Service>.<METHOD> <route>.
	Service string
	// Identity returns the caller when the service's auth middleware did not record one with
	// audit.SetIdentity. Nil means such callers are unknown.
	Identity func(*http.Request) grpcaudit.Identity
	// Route returns the route template, such as /v1/runners/{id}. It is called after the
	// handler, so a router has filled in its match. Default: the request's net/http pattern
	// without its method and host, else the URL path.
	Route func(*http.Request) string
	// IsEntry reports whether a request came from outside the platform. Default: its path is
	// not under /internal/ and the last x-forwarded-client-cert element names one of
	// EntryPrincipals.
	IsEntry func(*http.Request) bool
	// EntryPrincipals are the SPIFFE ids, such as the ingress gateway's, whose requests are
	// entries under the default IsEntry.
	EntryPrincipals []string
	// Classify returns the class of a request. Default: GET and HEAD are reads, anything else
	// a write.
	Classify func(method, route string) grpcaudit.Class
	// Logger receives hook failures. Default: no logging.
	Logger *zap.Logger
}

// Middleware returns the audit middleware.
func Middleware(cfg Config) func(http.Handler) http.Handler {
	rec := hook.NewRecorder(cfg.Emitter, cfg.Logger)
	principals := hook.NewPrincipalSet(cfg.EntryPrincipals)
	if cfg.Route == nil {
		cfg.Route = defaultRoute
	}
	if cfg.Classify == nil {
		cfg.Classify = defaultClassify
	}
	if cfg.IsEntry == nil {
		cfg.IsEntry = func(r *http.Request) bool {
			return !underInternal(r.URL.Path) && principals.Contains(hook.LastXFCCURI(r.Header.Values("X-Forwarded-Client-Cert")))
		}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, col := audit.WithCollector(r.Context())
			r = r.WithContext(ctx)
			sw := &statusWriter{ResponseWriter: w}
			next.ServeHTTP(sw, r)
			record(cfg, rec, r, sw.Status(), col)
		})
	}
}

func record(cfg Config, rec *hook.Recorder, r *http.Request, status int, col *audit.Collector) {
	defer func() {
		if p := recover(); p != nil && cfg.Logger != nil {
			cfg.Logger.Error("audit http hook panicked", zap.Any("panic", p))
		}
	}()
	route := cfg.Route(r)
	class := cfg.Classify(r.Method, route)
	if class == grpcaudit.ClassIgnore {
		return
	}
	kind := kindOf(class)
	entry := cfg.IsEntry(r)
	denied := status == http.StatusUnauthorized || status == http.StatusForbidden
	if !hook.Recorded(kind, entry, denied) {
		return
	}
	var id grpcaudit.Identity
	switch snap := col.Snapshot(); {
	case snap.Identity != nil:
		// The service's own auth middleware recorded the caller with audit.SetIdentity.
		id = *snap.Identity
	case cfg.Identity != nil:
		id = cfg.Identity(r)
	}
	outcome, code := outcomeFor(status)
	method := r.Method + " " + route
	rec.Record(r.Context(), hook.Call{
		Kind:      kind,
		Entry:     entry,
		Denied:    denied,
		Outcome:   outcome,
		Code:      uint32(code),
		Action:    cfg.Service + "." + method,
		LimitKey:  method,
		ActorID:   id.ActorID,
		ActorType: id.ActorType,
		ClientID:  id.ClientID,
		RequestID: firstHeader(r, "Idempotency-Key", "X-Request-Id"),
		UserAgent: r.UserAgent(),
		SourceIP:  hook.SourceIP(r.Header.Get("X-Envoy-External-Address"), r.Header.Values("X-Forwarded-For"), r.RemoteAddr),
		Snapshot:  col.Snapshot(),
	})
}

func kindOf(c grpcaudit.Class) hook.Kind {
	switch c {
	case grpcaudit.ClassRead:
		return hook.KindRead
	case grpcaudit.ClassSensitiveRead:
		return hook.KindSensitiveRead
	}
	return hook.KindWrite
}

// outcomeFor maps an HTTP status to an event outcome and the matching gRPC code: 2xx and 3xx are
// OK; 401 and 403 are DENIED (Unauthenticated, PermissionDenied); 409, 412 and 423 are REFUSED;
// anything else is FAILED.
func outcomeFor(status int) (auditpb.Outcome, codes.Code) {
	switch {
	case status >= 200 && status < 400:
		return auditpb.Outcome_OUTCOME_OK, codes.OK
	case status == http.StatusUnauthorized:
		return auditpb.Outcome_OUTCOME_DENIED, codes.Unauthenticated
	case status == http.StatusForbidden:
		return auditpb.Outcome_OUTCOME_DENIED, codes.PermissionDenied
	case status == http.StatusConflict:
		return auditpb.Outcome_OUTCOME_REFUSED, codes.Aborted
	case status == http.StatusPreconditionFailed, status == http.StatusLocked:
		return auditpb.Outcome_OUTCOME_REFUSED, codes.FailedPrecondition
	}
	return auditpb.Outcome_OUTCOME_FAILED, failedCode(status)
}

func failedCode(status int) codes.Code {
	switch status {
	case http.StatusBadRequest:
		return codes.InvalidArgument
	case http.StatusNotFound:
		return codes.NotFound
	case http.StatusRequestTimeout, http.StatusGatewayTimeout:
		return codes.DeadlineExceeded
	case http.StatusTooManyRequests:
		return codes.ResourceExhausted
	case 499:
		return codes.Canceled
	case http.StatusNotImplemented:
		return codes.Unimplemented
	case http.StatusServiceUnavailable:
		return codes.Unavailable
	}
	if status >= 500 {
		return codes.Internal
	}
	return codes.Unknown
}

func defaultClassify(method, _ string) grpcaudit.Class {
	if method == http.MethodGet || method == http.MethodHead {
		return grpcaudit.ClassRead
	}
	return grpcaudit.ClassWrite
}

// defaultRoute returns the net/http pattern the request matched, without its method and host,
// else the URL path.
func defaultRoute(r *http.Request) string {
	p := r.Pattern
	if p == "" {
		return r.URL.Path
	}
	if i := strings.IndexByte(p, ' '); i >= 0 {
		p = strings.TrimSpace(p[i+1:])
	}
	if i := strings.IndexByte(p, '/'); i > 0 {
		p = p[i:]
	}
	return p
}

func underInternal(path string) bool {
	return path == "/internal" || strings.HasPrefix(path, "/internal/")
}

func firstHeader(r *http.Request, names ...string) string {
	for _, n := range names {
		if v := r.Header.Get(n); v != "" {
			return v
		}
	}
	return ""
}

// statusWriter records the final status a handler wrote. It keeps http.Flusher working and
// unwraps for http.ResponseController.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 && code >= 200 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

// Flush sends buffered data to the client when the underlying writer can.
func (w *statusWriter) Flush() {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Hijack hands over the connection when the underlying writer allows it.
func (w *statusWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := w.ResponseWriter.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, errors.New("httpaudit: response writer does not support hijacking")
}

// Unwrap returns the underlying writer, for http.ResponseController.
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// Status returns the status written, or 200 when the handler wrote nothing.
func (w *statusWriter) Status() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}
