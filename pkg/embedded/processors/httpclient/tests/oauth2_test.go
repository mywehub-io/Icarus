package httpclient_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
	"github.com/wehubfusion/Icarus/pkg/fileref"
	"github.com/wehubfusion/Icarus/pkg/filestore"
	"github.com/wehubfusion/Icarus/pkg/filestore/memfs"
	"github.com/wehubfusion/Icarus/pkg/message"
)

// Every credential here is synthetic.
const (
	testUser     = "integration@example.test"
	testPassword = "synthetic-password-1"
	testSecret   = "synthetic-client-secret-1"
)

// tokenServer is an OAuth2 token endpoint: each successful sign-in issues tok-1, tok-2, ...
type tokenServer struct {
	mu        sync.Mutex
	calls     int32
	forms     []map[string]string
	expiresIn interface{} // what expires_in says; nil leaves it out
	status    int         // a refusal status, 0 for success
	errCode   string
	delay     time.Duration
	srv       *httptest.Server
}

func newTokenServer(t *testing.T) *tokenServer {
	t.Helper()
	ts := &tokenServer{expiresIn: 300}
	ts.srv = newServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&ts.calls, 1)
		_ = r.ParseForm()
		form := map[string]string{}
		for k := range r.PostForm {
			form[k] = r.PostForm.Get(k)
		}
		ts.mu.Lock()
		ts.forms = append(ts.forms, form)
		status, errCode, expires, delay := ts.status, ts.errCode, ts.expiresIn, ts.delay
		ts.mu.Unlock()
		if delay > 0 {
			time.Sleep(delay)
		}
		w.Header().Set("Content-Type", "application/json")
		if status != 0 {
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": errCode,
				"error_description": "Invalid user credentials for " + form["username"]})
			return
		}
		answer := map[string]interface{}{"access_token": fmt.Sprintf("tok-%d", n), "token_type": "Bearer"}
		if expires != nil {
			answer["expires_in"] = expires
		}
		_ = json.NewEncoder(w).Encode(answer)
	}))
	t.Cleanup(ts.srv.Close)
	return ts
}

func (ts *tokenServer) set(f func(ts *tokenServer)) { ts.mu.Lock(); f(ts); ts.mu.Unlock() }

func (ts *tokenServer) lastForm() map[string]string {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.forms[len(ts.forms)-1]
}

// apiServer accepts only tokens in valid; it records what it saw.
type apiServer struct {
	mu      sync.Mutex
	valid   map[string]bool
	seen    []string
	bodies  []string
	srv     *httptest.Server
	refused int32
}

func newAPIServer(t *testing.T, valid ...string) *apiServer {
	t.Helper()
	a := &apiServer{valid: map[string]bool{}}
	for _, v := range valid {
		a.valid[v] = true
	}
	a.srv = newServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		auth := r.Header.Get("Authorization")
		a.mu.Lock()
		a.seen = append(a.seen, auth)
		a.bodies = append(a.bodies, string(body))
		ok := a.valid[strings.TrimPrefix(auth, "Bearer ")]
		a.mu.Unlock()
		if !ok {
			atomic.AddInt32(&a.refused, 1)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(a.srv.Close)
	return a
}

func (a *apiServer) setValid(tokens ...string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.valid = map[string]bool{}
	for _, v := range tokens {
		a.valid[v] = true
	}
}

// oauthConn is an oauth2 connection to api, signing in at ts.
func oauthConn(connectionID string, ts *tokenServer, api *apiServer, grant string) map[string]interface{} {
	conn := map[string]interface{}{
		"url": api.srv.URL, "method": "GET", "auth_type": "oauth2", "oauth2_grant": grant,
		"oauth2_token_url": ts.srv.URL + "/realms/test/protocol/openid-connect/token", "oauth2_client_id": "test-client",
	}
	if grant == "password" {
		conn["oauth2_username"], conn["oauth2_password"] = testUser, testPassword
	} else {
		conn["oauth2_client_secret"] = testSecret
	}
	return map[string]interface{}{"label": "t", "connection_id": connectionID, "connection": conn}
}

func status(t *testing.T, out runtime.ProcessOutput) int {
	t.Helper()
	if out.Error != nil {
		t.Fatalf("unexpected error: %v", out.Error)
	}
	return out.Data["status"].(int)
}

// The password grant sends the user's credentials with the client id, and the token goes on the request.
// A second request uses the same token without signing in again.
func TestOAuth2_PasswordGrantSignsInOnceAndReusesTheToken(t *testing.T) {
	ts := newTokenServer(t)
	api := newAPIServer(t, "tok-1")
	cfg := oauthConn(t.Name(), ts, api, "password")

	if got := status(t, runConn(t, cfg, nil)); got != http.StatusOK {
		t.Fatalf("status %d", got)
	}
	form := ts.lastForm()
	want := map[string]string{"grant_type": "password", "client_id": "test-client", "username": testUser,
		"password": testPassword}
	for k, v := range want {
		if form[k] != v {
			t.Fatalf("token request %s = %q, want %q", k, form[k], v)
		}
	}
	if _, ok := form["client_secret"]; ok {
		t.Fatal("a public client sent a client_secret")
	}
	if api.seen[0] != "Bearer tok-1" {
		t.Fatalf("Authorization %q", api.seen[0])
	}
	if got := status(t, runConn(t, cfg, nil)); got != http.StatusOK || atomic.LoadInt32(&ts.calls) != 1 {
		t.Fatalf("second request: status %d, sign-ins %d, want 200 and 1", got, ts.calls)
	}
}

// The client credentials grant sends the client id and secret, and no user.
func TestOAuth2_ClientCredentialsGrant(t *testing.T) {
	ts := newTokenServer(t)
	api := newAPIServer(t, "tok-1")
	cfg := oauthConn(t.Name(), ts, api, "client_credentials")
	cfg["connection"].(map[string]interface{})["oauth2_scope"] = "api.read api.write"

	if got := status(t, runConn(t, cfg, nil)); got != http.StatusOK {
		t.Fatalf("status %d", got)
	}
	form := ts.lastForm()
	if form["grant_type"] != "client_credentials" || form["client_secret"] != testSecret ||
		form["scope"] != "api.read api.write" {
		t.Fatalf("token request %v", form)
	}
	if _, ok := form["username"]; ok {
		t.Fatal("client credentials sent a username")
	}
}

// A token is renewed shortly before it expires, without waiting for a refusal.
func TestOAuth2_RenewsATokenBeforeItExpires(t *testing.T) {
	ts := newTokenServer(t)
	ts.set(func(ts *tokenServer) { ts.expiresIn = "1" }) // some providers send it as a string
	api := newAPIServer(t, "tok-1", "tok-2")
	cfg := oauthConn(t.Name(), ts, api, "password")

	status(t, runConn(t, cfg, nil))
	time.Sleep(1100 * time.Millisecond)
	status(t, runConn(t, cfg, nil))
	if atomic.LoadInt32(&ts.calls) != 2 || api.seen[1] != "Bearer tok-2" {
		t.Fatalf("sign-ins %d, second Authorization %q; want 2 and tok-2", ts.calls, api.seen[1])
	}
}

// A cached token the service refuses is dropped, and the request is sent once more with a new token and
// the same body, read again from its file.
func TestOAuth2_RefusedCachedTokenIsRenewedAndTheRequestSentAgain(t *testing.T) {
	ts := newTokenServer(t)
	api := newAPIServer(t, "tok-1")
	cfg := oauthConn(t.Name(), ts, api, "password")
	cfg["connection"].(map[string]interface{})["method"] = "POST"
	status(t, runConn(t, cfg, nil)) // caches tok-1

	api.setValid("tok-2") // the service revoked tok-1 early
	content := `{"synthetic":"body"}`
	b := memfs.New()
	ref := fileref.FileRef{Path: fileref.RunPrefix("w", "r") + "t/payload.json", Size: int64(len(content))}
	b.Put(ref.Path, []byte(content))
	store, _ := filestore.New(b, "w", "r")
	rawCfg, _ := json.Marshal(cfg)
	out := createTestNode(t, "node1").Process(runtime.ProcessInput{
		Ctx: context.Background(), RawConfig: rawCfg, NodeId: "node1", Files: store, WorkflowID: "w", RunID: "r",
		Data: map[string]interface{}{"payload": fileref.Value(ref)}, ByteFields: map[string]bool{"payload": true},
	})
	if got := status(t, out); got != http.StatusOK {
		t.Fatalf("status %d, want 200 after the retry", got)
	}
	n := len(api.seen)
	if api.seen[n-2] != "Bearer tok-1" || api.seen[n-1] != "Bearer tok-2" {
		t.Fatalf("Authorization headers %v", api.seen)
	}
	if api.bodies[n-2] != content || api.bodies[n-1] != content {
		t.Fatalf("bodies %q and %q, want the payload twice", api.bodies[n-2], api.bodies[n-1])
	}
}

// A 401 to a token fetched for this very request is the service's answer: no second sign-in, no resend,
// and the 401 is the node's status, as for any other status.
func TestOAuth2_RefusalOfAFreshTokenIsNotRetried(t *testing.T) {
	ts := newTokenServer(t)
	api := newAPIServer(t) // accepts nothing
	cfg := oauthConn(t.Name(), ts, api, "password")

	if got := status(t, runConn(t, cfg, nil)); got != http.StatusUnauthorized {
		t.Fatalf("status %d, want 401", got)
	}
	if atomic.LoadInt32(&ts.calls) != 1 || atomic.LoadInt32(&api.refused) != 1 {
		t.Fatalf("sign-ins %d, requests %d; want 1 and 1", ts.calls, api.refused)
	}
}

// Steps that need the same token at the same time sign in once.
func TestOAuth2_ConcurrentStepsSignInOnce(t *testing.T) {
	ts := newTokenServer(t)
	ts.set(func(ts *tokenServer) { ts.delay = 200 * time.Millisecond })
	api := newAPIServer(t, "tok-1")
	cfg := oauthConn(t.Name(), ts, api, "password")

	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out := runConn(t, cfg, nil)
			if out.Error != nil {
				errs <- out.Error
			} else if out.Data["status"] != http.StatusOK {
				errs <- fmt.Errorf("status %v", out.Data["status"])
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&ts.calls); got != 1 {
		t.Fatalf("%d sign-ins, want 1", got)
	}
}

// A refused sign-in fails the step, is not retried, names the token endpoint's host and the OAuth2 error
// code, and quotes no secret and nothing of the provider's description (which can carry the username).
func TestOAuth2_RefusedSignInIsNotTransientAndQuotesNoSecret(t *testing.T) {
	ts := newTokenServer(t)
	ts.set(func(ts *tokenServer) { ts.status, ts.errCode = http.StatusUnauthorized, "invalid_grant" })
	api := newAPIServer(t)
	out := runConn(t, oauthConn(t.Name(), ts, api, "password"), nil)

	if out.Error == nil {
		t.Fatal("a refused sign-in succeeded")
	}
	msg := out.Error.Error()
	if !strings.Contains(msg, "invalid_grant") || !strings.Contains(msg, strings.TrimPrefix(ts.srv.URL, "http://")) {
		t.Fatalf("message %q does not name the code and host", msg)
	}
	for _, secret := range []string{testPassword, testUser, "/realms/test", "Invalid user credentials"} {
		if strings.Contains(msg, secret) {
			t.Fatalf("message %q quotes %q", msg, secret)
		}
	}
	if message.IsTransientError(out.Error) {
		t.Fatal("a refused sign-in is marked transient: the runner would retry a wrong password")
	}
	if len(api.seen) != 0 {
		t.Fatal("the request was sent without a token")
	}
}

// A token endpoint that is down or overloaded is transient: the runner tries the step again later.
func TestOAuth2_TokenEndpointDownIsTransient(t *testing.T) {
	for _, code := range []int{http.StatusServiceUnavailable, http.StatusTooManyRequests} {
		ts := newTokenServer(t)
		ts.set(func(ts *tokenServer) { ts.status = code })
		out := runConn(t, oauthConn(fmt.Sprintf("%s-%d", t.Name(), code), ts, newAPIServer(t), "password"), nil)
		if out.Error == nil || !message.IsTransientError(out.Error) {
			t.Fatalf("%d: error %v, want a transient failure", code, out.Error)
		}
	}
}

// A field the grant needs is refused by name before any sign-in.
func TestOAuth2_MissingFieldsAreRefusedByName(t *testing.T) {
	for _, tc := range []struct {
		grant, remove, want string
	}{
		{"password", "oauth2_username", "oauth2_username"},
		{"password", "oauth2_password", "oauth2_password"},
		{"client_credentials", "oauth2_client_secret", "oauth2_client_secret"},
		{"password", "oauth2_client_id", "oauth2_client_id"},
		{"password", "oauth2_token_url", "oauth2_token_url"},
		{"password", "oauth2_grant", "oauth2_grant"},
	} {
		ts := newTokenServer(t)
		cfg := oauthConn(t.Name()+tc.remove, ts, newAPIServer(t), tc.grant)
		delete(cfg["connection"].(map[string]interface{}), tc.remove)
		out := runConn(t, cfg, nil)
		if out.Error == nil || !strings.Contains(out.Error.Error(), tc.want) {
			t.Fatalf("%s without %s: error %v", tc.grant, tc.remove, out.Error)
		}
		if atomic.LoadInt32(&ts.calls) != 0 {
			t.Fatalf("%s without %s: signed in anyway", tc.grant, tc.remove)
		}
	}
}

// An unknown grant is refused.
func TestOAuth2_UnknownGrantIsRefused(t *testing.T) {
	cfg := oauthConn(t.Name(), newTokenServer(t), newAPIServer(t), "password")
	cfg["connection"].(map[string]interface{})["oauth2_grant"] = "implicit"
	out := runConn(t, cfg, nil)
	if out.Error == nil || !strings.Contains(out.Error.Error(), "unsupported oauth2_grant") {
		t.Fatalf("error %v", out.Error)
	}
}

// The token URL passes the egress policy like any other address: a loopback port the policy does not
// allow is refused, and nothing is sent there.
func TestOAuth2_TokenURLPassesTheEgressPolicy(t *testing.T) {
	var hit int32
	closed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&hit, 1)
	})) // not through newServer, so its port is not allowed
	defer closed.Close()
	api := newAPIServer(t)
	cfg := oauthConn(t.Name(), newTokenServer(t), api, "password")
	cfg["connection"].(map[string]interface{})["oauth2_token_url"] = closed.URL + "/token"

	out := runConn(t, cfg, nil)
	if out.Error == nil || !strings.Contains(out.Error.Error(), "egress") {
		t.Fatalf("error %v, want the egress policy's refusal", out.Error)
	}
	if atomic.LoadInt32(&hit) != 0 {
		t.Fatal("the password reached a refused address")
	}
}

// A redirect from the token endpoint is not followed: it would carry the password elsewhere.
func TestOAuth2_TokenEndpointRedirectIsNotFollowed(t *testing.T) {
	var landed int32
	elsewhere := newServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&landed, 1)
	}))
	defer elsewhere.Close()
	redirector := newServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, elsewhere.URL+"/token", http.StatusTemporaryRedirect)
	}))
	defer redirector.Close()
	cfg := oauthConn(t.Name(), newTokenServer(t), newAPIServer(t), "password")
	cfg["connection"].(map[string]interface{})["oauth2_token_url"] = redirector.URL + "/token"

	if out := runConn(t, cfg, nil); out.Error == nil {
		t.Fatal("signed in through a redirect")
	}
	if atomic.LoadInt32(&landed) != 0 {
		t.Fatal("the redirect was followed")
	}
}

// An answer without an access token, or with a token type other than Bearer, fails the step.
func TestOAuth2_UnusableAnswersFail(t *testing.T) {
	for name, answer := range map[string]string{
		"no access_token": `{"token_type":"Bearer"}`,
		"not Bearer":      `{"access_token":"x","token_type":"MAC"}`,
		"not JSON":        `<html>sign in</html>`,
	} {
		srv := newServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, answer) }))
		cfg := oauthConn(t.Name()+name, newTokenServer(t), newAPIServer(t), "password")
		cfg["connection"].(map[string]interface{})["oauth2_token_url"] = srv.URL + "/token"
		if out := runConn(t, cfg, nil); out.Error == nil {
			t.Fatalf("%s: the step succeeded", name)
		}
		srv.Close()
	}
}

// A changed password misses the cache: the old token is not used for new credentials.
func TestOAuth2_ChangedCredentialSignsInAgain(t *testing.T) {
	ts := newTokenServer(t)
	api := newAPIServer(t, "tok-1", "tok-2")
	cfg := oauthConn(t.Name(), ts, api, "password")
	status(t, runConn(t, cfg, nil))
	cfg["connection"].(map[string]interface{})["oauth2_password"] = "synthetic-password-2"
	status(t, runConn(t, cfg, nil))
	if atomic.LoadInt32(&ts.calls) != 2 || ts.lastForm()["password"] != "synthetic-password-2" {
		t.Fatalf("sign-ins %d, want 2 with the new password", ts.calls)
	}
}
