package httpclient_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/embedded/processors/httpclient"
	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
)

// getInputURL runs a GET of a URL the item supplies (url.source=input).
func getInputURL(t *testing.T, u string) runtime.ProcessOutput {
	t.Helper()
	rawCfg, _ := json.Marshal(map[string]interface{}{
		"label":  "test",
		"url":    map[string]interface{}{"source": "input"},
		"method": "GET",
	})
	input, _ := withRun(runtime.ProcessInput{
		Ctx: context.Background(), RawConfig: rawCfg, NodeId: "node1",
		Data: map[string]interface{}{"url": u},
	})
	return createTestNode(t, "node1").Process(input)
}

// A failed request names the scheme and host it was sent to, not the path or query, which can
// carry identifiers or tokens. The identifiers are synthetic.
func TestProcess_RequestFailureDoesNotEchoPathOrQuery(t *testing.T) {
	server := newServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic(http.ErrAbortHandler) // closes the connection: the client sees EOF
	}))
	defer server.Close()

	out := getInputURL(t, server.URL+"/patients/SYNTHPATH4410?nhs=SYNTHQUERY5520&token=SYNTHTOKEN6630")
	if out.Error == nil {
		t.Fatal("expected the request to fail")
	}
	msg := out.Error.Error()
	for _, s := range []string{"SYNTHPATH4410", "SYNTHQUERY5520", "SYNTHTOKEN6630", "/patients"} {
		if strings.Contains(msg, s) {
			t.Errorf("error %q echoes %q from the URL", msg, s)
		}
	}
	if !strings.Contains(msg, server.URL) {
		t.Errorf("error %q should name the scheme and host %s", msg, server.URL)
	}
	var httpErr *httpclient.HTTPError
	if !errors.As(out.Error, &httpErr) {
		t.Fatalf("expected an HTTPError, got %T", out.Error)
	}
	if strings.Contains(httpErr.Err.Error(), "SYNTHTOKEN6630") {
		t.Errorf("wrapped error %q echoes the query", httpErr.Err)
	}
}

// A URL that does not parse is not echoed, nor is the parse error, which quotes a piece of it.
func TestProcess_InvalidURLDoesNotEchoURL(t *testing.T) {
	out := getInputURL(t, "http://example.test/SYNTHPATH4410%zz?token=SYNTHTOKEN6630")
	if out.Error == nil {
		t.Fatal("expected an invalid URL error")
	}
	msg := out.Error.Error()
	for _, s := range []string{"SYNTHPATH4410", "SYNTHTOKEN6630", "%zz", "example.test"} {
		if strings.Contains(msg, s) {
			t.Errorf("error %q echoes %q from the URL", msg, s)
		}
	}
	if !strings.Contains(msg, "invalid URL") {
		t.Errorf("error %q should say the URL is invalid", msg)
	}
	if errors.Unwrap(out.Error) != nil {
		t.Errorf("error wraps %v, which quotes the URL", errors.Unwrap(out.Error))
	}
}

// The redacted error is still a *url.Error (a net.Error), so Timeout and transient
// classification behave as before.
func TestProcess_RedactedRequestErrorStaysNetError(t *testing.T) {
	server := newServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic(http.ErrAbortHandler)
	}))
	defer server.Close()
	out := getInputURL(t, server.URL+"/x?token=SYNTHTOKEN6630")
	var ne net.Error
	if !errors.As(out.Error, &ne) {
		t.Fatalf("expected the cause to remain a net.Error (*url.Error), got %T", out.Error)
	}
}
