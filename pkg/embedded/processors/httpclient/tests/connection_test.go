package httpclient_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
	"github.com/wehubfusion/Icarus/pkg/fileref"
	"github.com/wehubfusion/Icarus/pkg/filestore"
	"github.com/wehubfusion/Icarus/pkg/filestore/memfs"
)

// runConn runs the node with cfg against data and returns its output. Every value here is synthetic.
func runConn(t *testing.T, cfg map[string]interface{}, data map[string]interface{}) runtime.ProcessOutput {
	t.Helper()
	if data == nil {
		data = map[string]interface{}{}
	}
	rawCfg, _ := json.Marshal(cfg)
	input, _ := withRun(runtime.ProcessInput{Ctx: context.Background(), Data: data, RawConfig: rawCfg, NodeId: "node1"})
	return createTestNode(t, "node1").Process(input)
}

// recorder answers 200 and keeps the last request's headers, method and URL.
type recorder struct {
	header http.Header
	method string
	uri    string
	calls  int
}

func (r *recorder) server() *httptest.Server {
	return newServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.header, r.method, r.uri = req.Header.Clone(), req.Method, req.URL.RequestURI()
		r.calls++
		w.WriteHeader(http.StatusOK)
	}))
}

func TestConnection_SendsEachAuthType(t *testing.T) {
	for _, tc := range []struct {
		name   string
		conn   map[string]interface{}
		header string
		want   string
	}{
		{"bearer", map[string]interface{}{"auth_type": "bearer", "bearer_token": "synthetic-token"}, "Authorization", "Bearer synthetic-token"},
		// base64("synthetic-user:synthetic-pass")
		{"basic", map[string]interface{}{"auth_type": "basic", "basic_username": "synthetic-user", "basic_password": "synthetic-pass"}, "Authorization", "Basic c3ludGhldGljLXVzZXI6c3ludGhldGljLXBhc3M="},
		{"apikey", map[string]interface{}{"auth_type": "apikey", "api_key_header": "X-API-Key", "api_key_value": "synthetic-key"}, "X-API-Key", "synthetic-key"},
		{"auth_type in capitals", map[string]interface{}{"auth_type": "APIKEY", "api_key_header": "X-API-Key", "api_key_value": "synthetic-key"}, "X-API-Key", "synthetic-key"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &recorder{}
			s := rec.server()
			defer s.Close()
			conn := map[string]interface{}{"url": s.URL, "method": "GET"}
			for k, v := range tc.conn {
				conn[k] = v
			}
			out := runConn(t, map[string]interface{}{"label": "t", "connection": conn}, nil)
			if out.Error != nil {
				t.Fatal(out.Error)
			}
			if got := rec.header.Get(tc.header); got != tc.want {
				t.Fatalf("%s: got %q, want %q", tc.header, got, tc.want)
			}
		})
	}
}

func TestConnection_NoneSendsNoCredential(t *testing.T) {
	for _, authType := range []string{"", "none"} {
		rec := &recorder{}
		s := rec.server()
		out := runConn(t, map[string]interface{}{"label": "t", "connection": map[string]interface{}{
			"url": s.URL, "method": "GET", "auth_type": authType, "bearer_token": "synthetic-token",
		}}, nil)
		s.Close()
		if out.Error != nil {
			t.Fatal(out.Error)
		}
		if got := rec.header.Get("Authorization"); got != "" {
			t.Fatalf("auth_type %q sent Authorization %q", authType, got)
		}
	}
}

// A connection whose auth_type names a credential it does not have is refused before any request,
// rather than sent unauthenticated; the error names the missing field and never a value.
func TestConnection_MissingCredentialIsRefused(t *testing.T) {
	for _, tc := range []struct {
		name    string
		conn    map[string]interface{}
		missing string
	}{
		{"bearer", map[string]interface{}{"auth_type": "bearer"}, "bearer_token"},
		{"basic without user", map[string]interface{}{"auth_type": "basic", "basic_password": "synthetic-pass"}, "basic_username"},
		{"basic without password", map[string]interface{}{"auth_type": "basic", "basic_username": "synthetic-user"}, "basic_password"},
		{"apikey without header", map[string]interface{}{"auth_type": "apikey", "api_key_value": "synthetic-key"}, "api_key_header"},
		{"apikey without value", map[string]interface{}{"auth_type": "apikey", "api_key_header": "X-API-Key"}, "api_key_value"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &recorder{}
			s := rec.server()
			defer s.Close()
			conn := map[string]interface{}{"url": s.URL, "method": "GET"}
			for k, v := range tc.conn {
				conn[k] = v
			}
			out := runConn(t, map[string]interface{}{"label": "t", "connection": conn}, nil)
			if out.Error == nil || !strings.Contains(out.Error.Error(), tc.missing) {
				t.Fatalf("want an error naming %s, got %v", tc.missing, out.Error)
			}
			for _, secret := range []string{"synthetic-pass", "synthetic-key", "synthetic-user"} {
				if strings.Contains(out.Error.Error(), secret) {
					t.Fatalf("error quotes a credential: %v", out.Error)
				}
			}
			if rec.calls != 0 {
				t.Fatal("a request was sent")
			}
		})
	}
}

func TestConnection_RefusesBadAuthSettings(t *testing.T) {
	for _, tc := range []struct {
		name string
		conn map[string]interface{}
	}{
		{"unknown auth_type", map[string]interface{}{"auth_type": "digest"}},
		{"api key header is not a header name", map[string]interface{}{"auth_type": "apikey", "api_key_header": "X API Key", "api_key_value": "synthetic-key"}},
		{"custom headers not JSON", map[string]interface{}{"custom_headers": "{not json"}},
		{"custom header not a string", map[string]interface{}{"custom_headers": `{"X-Tenant-Id": 7}`}},
		{"custom header name invalid", map[string]interface{}{"custom_headers": `{"X Tenant": "a"}`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &recorder{}
			s := rec.server()
			defer s.Close()
			conn := map[string]interface{}{"url": s.URL, "method": "GET"}
			for k, v := range tc.conn {
				conn[k] = v
			}
			out := runConn(t, map[string]interface{}{"label": "t", "connection": conn}, nil)
			if out.Error == nil {
				t.Fatal("want an error")
			}
			if strings.Contains(out.Error.Error(), "synthetic-key") {
				t.Fatalf("error quotes a credential: %v", out.Error)
			}
			if rec.calls != 0 {
				t.Fatal("a request was sent")
			}
		})
	}
}

// A value no header can carry is refused as a config error naming the field, not left to the transport
// to fail as a network fault. The commonest case is a secret pasted with a line break at its end.
func TestConnection_RefusesValueWithLineBreak(t *testing.T) {
	for _, tc := range []struct {
		name  string
		conn  map[string]interface{}
		field string
	}{
		{"bearer token", map[string]interface{}{"auth_type": "bearer", "bearer_token": "synthetic-secret\n"}, "bearer_token"},
		{"api key", map[string]interface{}{"auth_type": "apikey", "api_key_header": "X-API-Key", "api_key_value": "synthetic-secret\r\n"}, "api_key_value"},
		{"custom header", map[string]interface{}{"custom_headers": `{"X-Tenant-Id": "synthetic-secret\r\nX-Injected: 1"}`}, "X-Tenant-Id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &recorder{}
			s := rec.server()
			defer s.Close()
			conn := map[string]interface{}{"url": s.URL, "method": "GET"}
			for k, v := range tc.conn {
				conn[k] = v
			}
			out := runConn(t, map[string]interface{}{"label": "t", "connection": conn}, nil)
			if out.Error == nil || !strings.Contains(out.Error.Error(), "config error") || !strings.Contains(out.Error.Error(), tc.field) {
				t.Fatalf("want a config error naming %s, got %v", tc.field, out.Error)
			}
			if strings.Contains(out.Error.Error(), "synthetic-secret") {
				t.Fatalf("error quotes the value: %v", out.Error)
			}
			if rec.calls != 0 {
				t.Fatal("a request was sent")
			}
		})
	}

	// A basic credential goes base64 encoded, so any character in it is sent.
	rec := &recorder{}
	s := rec.server()
	defer s.Close()
	out := runConn(t, map[string]interface{}{"label": "t", "connection": map[string]interface{}{
		"url": s.URL, "method": "GET", "auth_type": "basic", "basic_username": "synthetic-user", "basic_password": "synthetic\tpass",
	}}, nil)
	if out.Error != nil || rec.calls != 1 {
		t.Fatalf("basic with a tab: err %v, calls %d", out.Error, rec.calls)
	}
}

// Order: the connection's custom headers, then the node's headers, then the credential. A node header
// replaces a custom header but never the credential.
func TestConnection_HeaderOrder(t *testing.T) {
	for _, customHeaders := range []interface{}{
		`{"X-Tenant-Id": "synthetic-tenant", "X-Trace": "from-connection"}`,
		map[string]interface{}{"X-Tenant-Id": "synthetic-tenant", "X-Trace": "from-connection"},
	} {
		rec := &recorder{}
		s := rec.server()
		out := runConn(t, map[string]interface{}{
			"label": "t",
			"connection": map[string]interface{}{
				"url": s.URL, "method": "GET", "auth_type": "bearer", "bearer_token": "synthetic-token",
				"custom_headers": customHeaders,
			},
			"headers": map[string]interface{}{"source": "config", "value": map[string]interface{}{
				"X-Trace": "from-node", "Authorization": "Bearer from-node",
			}},
		}, nil)
		s.Close()
		if out.Error != nil {
			t.Fatal(out.Error)
		}
		if got := rec.header.Get("X-Tenant-Id"); got != "synthetic-tenant" {
			t.Errorf("custom header: got %q", got)
		}
		if got := rec.header.Get("X-Trace"); got != "from-node" {
			t.Errorf("a node header should replace a custom header: got %q", got)
		}
		if got := rec.header.Get("Authorization"); got != "Bearer synthetic-token" {
			t.Errorf("a node header must not replace the credential: got %q", got)
		}
	}
}

// With a URL from input, nothing from the connection goes with the request.
func TestConnection_NothingSentWithURLFromInput(t *testing.T) {
	rec := &recorder{}
	s := rec.server()
	defer s.Close()
	out := runConn(t, map[string]interface{}{
		"label":  "t",
		"url":    map[string]interface{}{"source": "input", "inputKey": "url"},
		"method": "GET",
		"connection": map[string]interface{}{
			"url": "http://connection.invalid", "method": "GET", "auth_type": "apikey",
			"api_key_header": "X-API-Key", "api_key_value": "synthetic-key", "custom_headers": `{"X-Tenant-Id": "synthetic-tenant"}`,
		},
	}, map[string]interface{}{"url": s.URL})
	if out.Error != nil {
		t.Fatal(out.Error)
	}
	if rec.header.Get("X-API-Key") != "" || rec.header.Get("X-Tenant-Id") != "" {
		t.Fatalf("connection headers went to a URL from input: %v", rec.header)
	}
}

// A redirect off the connection's host loses every header the connection set, Authorization
// included (Go would keep it between two ports of one host). One on the same host keeps them.
func TestConnection_RedirectOffHostDropsConnectionHeaders(t *testing.T) {
	other := &recorder{}
	otherServer := other.server()
	defer otherServer.Close()

	same := &recorder{}
	var origin *httptest.Server
	origin = newServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/away":
			http.Redirect(w, r, otherServer.URL+"/landed", http.StatusFound)
		case "/here":
			http.Redirect(w, r, "/landed", http.StatusFound)
		default:
			same.header, same.calls = r.Header.Clone(), same.calls+1
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer origin.Close()

	for _, tc := range []struct {
		path   string
		landed *recorder
		keep   bool
	}{
		{"away", other, false},
		{"here", same, true},
	} {
		for _, conn := range []map[string]interface{}{
			{"auth_type": "apikey", "api_key_header": "X-API-Key", "api_key_value": "synthetic-key"},
			{"auth_type": "bearer", "bearer_token": "synthetic-token"},
		} {
			conn["url"], conn["method"], conn["custom_headers"] = origin.URL, "GET", `{"X-Tenant-Id": "synthetic-tenant"}`
			out := runConn(t, map[string]interface{}{"label": "t", "connection": conn, "path_from_input": true}, map[string]interface{}{"path": tc.path})
			if out.Error != nil {
				t.Fatal(out.Error)
			}
			if status := out.Data["status"]; status != 200 {
				t.Fatalf("%s: status %v", tc.path, status)
			}
			got := tc.landed.header
			for _, h := range []string{"X-API-Key", "Authorization", "X-Tenant-Id"} {
				if _, set := conn["api_key_header"]; h == "X-API-Key" && !set {
					continue
				}
				if _, set := conn["bearer_token"]; h == "Authorization" && !set {
					continue
				}
				if present := got.Get(h) != ""; present != tc.keep {
					t.Errorf("redirect %s: %s present=%v, want %v", tc.path, h, present, tc.keep)
				}
			}
		}
	}
}

// A request with a body is not redirected: the node reports the redirect's own status rather than the
// status of a body-less GET it never meant to send.
func TestConnection_RedirectAfterBodyIsNotFollowed(t *testing.T) {
	landed := 0
	var s *httptest.Server
	s = newServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/landed" {
			landed++
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Redirect(w, r, "/landed", http.StatusFound)
	}))
	defer s.Close()

	content := "synthetic"
	b := memfs.New()
	ref := fileref.FileRef{Path: fileref.RunPrefix("w", "r") + "t/payload.json", Size: int64(len(content))}
	b.Put(ref.Path, []byte(content))
	store, _ := filestore.New(b, "w", "r")
	rawCfg, _ := json.Marshal(map[string]interface{}{"label": "t", "connection": map[string]interface{}{"url": s.URL + "/post", "method": "POST"}})
	out := createTestNode(t, "node1").Process(runtime.ProcessInput{
		Ctx: context.Background(), RawConfig: rawCfg, NodeId: "node1", Files: store, WorkflowID: "w", RunID: "r",
		Data:       map[string]interface{}{"payload": fileref.Value(ref)},
		ByteFields: map[string]bool{"payload": true},
	})
	if out.Error != nil {
		t.Fatal(out.Error)
	}
	if out.Data["status"] != http.StatusFound || landed != 0 {
		t.Fatalf("status %v, landed %d: the redirect was followed", out.Data["status"], landed)
	}
}

// connection_method wins over the connection's method; the method setting, which belongs to a URL from
// config or input, is never read for a connection's step.
func TestConnection_Method(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  map[string]interface{}
		want string
	}{
		{"the connection's", map[string]interface{}{}, "PUT"},
		{"connection_method wins", map[string]interface{}{"connection_method": "get"}, "GET"},
		{"method is ignored", map[string]interface{}{"method": "DELETE"}, "PUT"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := &recorder{}
			s := rec.server()
			defer s.Close()
			cfg := map[string]interface{}{"label": "t", "connection": map[string]interface{}{"url": s.URL, "method": "PUT"}}
			for k, v := range tc.cfg {
				cfg[k] = v
			}
			if out := runConn(t, cfg, nil); out.Error != nil {
				t.Fatal(out.Error)
			}
			if rec.method != tc.want {
				t.Fatalf("method: got %s, want %s", rec.method, tc.want)
			}
		})
	}
}

// path_from_input appends the input path after the connection's URL (which already holds any
// connection_path), and refuses one that could leave it. The refusal never quotes the path.
func TestConnection_PathFromInput(t *testing.T) {
	rec := &recorder{}
	s := rec.server()
	defer s.Close()
	cfg := map[string]interface{}{"label": "t", "path_from_input": true,
		"connection": map[string]interface{}{"url": s.URL + "/openehr/v1/ehr?tenant=a", "method": "POST"}}

	if out := runConn(t, cfg, map[string]interface{}{"path": "synthetic-ehr/composition?format=FLAT&templateId=HSE%20-%20Lab"}); out.Error != nil {
		t.Fatal(out.Error)
	}
	if want := "/openehr/v1/ehr/synthetic-ehr/composition?tenant=a&format=FLAT&templateId=HSE%20-%20Lab"; rec.uri != want {
		t.Fatalf("uri: got %q, want %q", rec.uri, want)
	}
	// An encoded slash inside a segment is an identifier, not a separator.
	if out := runConn(t, cfg, map[string]interface{}{"path": "SYN%2F1"}); out.Error != nil {
		t.Fatal(out.Error)
	}
	if want := "/openehr/v1/ehr/SYN%2F1?tenant=a"; rec.uri != want {
		t.Fatalf("uri: got %q, want %q", rec.uri, want)
	}

	for _, bad := range []string{
		"", "../admin", "a/%2e%2e/admin", "a%2F..%2F..%2Fadmin", "a%5c..", "//evil.example/x",
		"https://evil.example/x", "user@evil.example", "a b", "a#frag", `a\b`,
	} {
		calls := rec.calls
		out := runConn(t, cfg, map[string]interface{}{"path": bad})
		if out.Error == nil {
			t.Errorf("path %q: want an error", bad)
			continue
		}
		if bad != "" && strings.Contains(out.Error.Error(), bad) {
			t.Errorf("error quotes the path: %v", out.Error)
		}
		if rec.calls != calls {
			t.Errorf("path %q: a request was sent", bad)
		}
	}

	// A path that is not text says so, rather than that it is missing.
	calls := rec.calls
	out := runConn(t, cfg, map[string]interface{}{"path": 12345})
	if out.Error == nil || !strings.Contains(out.Error.Error(), "must be text") {
		t.Fatalf("numeric path: want a must-be-text error, got %v", out.Error)
	}
	if rec.calls != calls {
		t.Fatal("numeric path: a request was sent")
	}
	// No path at all is still "required".
	if out := runConn(t, cfg, map[string]interface{}{}); out.Error == nil || !strings.Contains(out.Error.Error(), "required") {
		t.Fatalf("missing path: want a required error, got %v", out.Error)
	}
}

func TestConnection_PathFromInputNeedsTheConnectionsURL(t *testing.T) {
	out := runConn(t, map[string]interface{}{
		"label": "t", "path_from_input": true, "method": "GET",
		"url": map[string]interface{}{"source": "config", "value": "http://example.invalid"},
	}, map[string]interface{}{"path": "x"})
	if out.Error == nil || !strings.Contains(out.Error.Error(), "path_from_input") {
		t.Fatalf("want a path_from_input error, got %v", out.Error)
	}
}
