package httpclient_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/embedded/processors/httpclient"
	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
	"github.com/wehubfusion/Icarus/pkg/message"
)

var (
	allowedMu    sync.Mutex
	allowedPorts []uint16
)

// newServer starts a test server on 127.0.0.1 and permits its port, as a host-run local estate
// permits its test doubles'; every other loopback port stays refused.
func newServer(h http.Handler) *httptest.Server {
	s := httptest.NewServer(h)
	allowedMu.Lock()
	defer allowedMu.Unlock()
	allowedPorts = append(allowedPorts, netip.MustParseAddrPort(s.Listener.Addr().String()).Port())
	httpclient.AllowLoopbackPorts(allowedPorts)
	return s
}

// getURL runs a GET of url through the node; the response body is written to the run's files.
func getURL(t *testing.T, url string) runtime.ProcessOutput {
	t.Helper()
	rawCfg, _ := json.Marshal(map[string]interface{}{
		"label":      "test",
		"connection": map[string]interface{}{"url": url, "method": "GET"},
	})
	input, _ := withRun(runtime.ProcessInput{Ctx: context.Background(), RawConfig: rawCfg, NodeId: "node1"})
	return createTestNode(t, "node1").Process(input)
}

func requireRefused(t *testing.T, out runtime.ProcessOutput, addr string) {
	t.Helper()
	if out.Error == nil {
		t.Fatalf("expected the dial to %s to be refused, got data %v", addr, out.Data)
	}
	var egress *httpclient.EgressError
	if !errors.As(out.Error, &egress) {
		t.Fatalf("expected an EgressError, got %v", out.Error)
	}
	var cfgErr *httpclient.ConfigError
	if !errors.As(out.Error, &cfgErr) {
		t.Fatalf("a refused destination should be a configuration error, not retried: got %T", out.Error)
	}
	if !strings.Contains(out.Error.Error(), "blocked by SSRF egress policy: "+addr) {
		t.Fatalf("message: got %q", out.Error.Error())
	}
}

func TestEgress_LoopbackPortNotAllowedIsRefused(t *testing.T) {
	// A listening sibling on loopback, never permitted: what a pod's sidecars and a host's other
	// services look like to the node.
	sibling := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("the refused server was reached")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer sibling.Close()
	requireRefused(t, getURL(t, sibling.URL+"/health"), "127.0.0.1")
}

func TestEgress_AllowedLoopbackPortIsReached(t *testing.T) {
	s := newServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	defer s.Close()
	if out := getURL(t, s.URL); out.Error != nil {
		t.Fatalf("allowed port refused: %v", out.Error)
	}
}

func TestEgress_RedirectToLoopbackIsRefused(t *testing.T) {
	sibling := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		t.Error("the redirect target was reached")
	}))
	defer sibling.Close()
	front := newServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, sibling.URL+"/health", http.StatusFound)
	}))
	defer front.Close()
	requireRefused(t, getURL(t, front.URL), "127.0.0.1")
}

func TestEgress_MetadataAndUnspecifiedAreRefused(t *testing.T) {
	for _, tc := range []struct{ url, addr string }{
		{"http://169.254.169.254/metadata/instance", "169.254.169.254"},
		{"http://0.0.0.0:1/", "0.0.0.0"},
		{"http://[::1]:1/", "::1"},
		{"http://[::ffff:127.0.0.1]:1/", "127.0.0.1"}, // Go dials a mapped address as IPv4
	} {
		t.Run(tc.addr, func(t *testing.T) { requireRefused(t, getURL(t, tc.url), tc.addr) })
	}
}

func TestEgress_RefusalIsNotRetried(t *testing.T) {
	out := getURL(t, "http://169.254.169.254/")
	if out.Error == nil || message.IsTransientError(out.Error) {
		t.Fatalf("a refused destination must be permanent: %v", out.Error)
	}
}
