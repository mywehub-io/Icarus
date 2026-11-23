package httpclient

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// OAuth2 sign-in for a connection with auth_type "oauth2" (Nyx nyx/scripts/connection-schemas/http.sql).
//
// The node gets an access token from the connection's token URL with the connection's grant, keeps it
// in this process until shortly before it expires, and sends it as "Authorization: Bearer <token>".
// Two grants: "password" (a user's username and password, which some providers still require) and
// "client_credentials" (the application's own client id and secret).
//
// Nothing secret leaves this file: no error quotes a secret, the token, the token endpoint's answer, or
// the token URL's path and query; the cache is keyed by a hash.

const (
	grantPassword          = "password"
	grantClientCredentials = "client_credentials"

	// defaultTokenLifetime applies when the token endpoint gives no expires_in.
	defaultTokenLifetime = 300 * time.Second
	// maxRenewMargin is the most a token's life is cut short, so a request never leaves with a token
	// about to expire on the way.
	maxRenewMargin = 60 * time.Second
	// tokenTimeout bounds a whole sign-in: dial, TLS, request and answer.
	tokenTimeout = 30 * time.Second
	// maxTokenAnswer is the most of a token endpoint's answer read.
	maxTokenAnswer = 1 << 20
)

// oauthSettings are a connection's OAuth2 fields.
type oauthSettings struct {
	grant        string
	tokenURL     string
	clientID     string
	clientSecret string
	username     string
	password     string
	scope        string
}

// readOAuthSettings reads and checks a connection's OAuth2 fields. A field the grant needs is refused by
// name when missing; Nyx cannot make it required, since a field there can depend on one other field only.
func readOAuthSettings(nodeID string, conn map[string]interface{}) (oauthSettings, error) {
	s := oauthSettings{
		grant:        strings.ToLower(strings.TrimSpace(getString(conn, "oauth2_grant", ""))),
		tokenURL:     strings.TrimSpace(getString(conn, "oauth2_token_url", "")),
		clientID:     strings.TrimSpace(getString(conn, "oauth2_client_id", "")),
		clientSecret: getString(conn, "oauth2_client_secret", ""),
		username:     getString(conn, "oauth2_username", ""),
		password:     getString(conn, "oauth2_password", ""),
		scope:        strings.TrimSpace(getString(conn, "oauth2_scope", "")),
	}
	switch s.grant {
	case grantPassword:
		if s.username == "" {
			return s, missingCredential(nodeID, "oauth2 (password)", "oauth2_username")
		}
		if s.password == "" {
			return s, missingCredential(nodeID, "oauth2 (password)", "oauth2_password")
		}
	case grantClientCredentials:
		if s.clientSecret == "" {
			return s, missingCredential(nodeID, "oauth2 (client credentials)", "oauth2_client_secret")
		}
	case "":
		return s, missingCredential(nodeID, "oauth2", "oauth2_grant")
	default:
		return s, NewConfigError(nodeID, "connection.oauth2_grant",
			fmt.Sprintf("unsupported oauth2_grant %q: use password or client_credentials", s.grant), nil)
	}
	if s.clientID == "" {
		return s, missingCredential(nodeID, "oauth2", "oauth2_client_id")
	}
	u, err := url.Parse(s.tokenURL)
	if s.tokenURL == "" || err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		return s, NewConfigError(nodeID, "connection.oauth2_token_url",
			"the connection's oauth2_token_url must be an absolute http or https URL", nil)
	}
	return s, nil
}

// key identifies a token: the connection and every value its sign-in uses, hashed. A changed password,
// secret or token URL misses the cache, and nothing secret is kept as the key.
func (s oauthSettings) key(connectionID string) string {
	h := sha256.New()
	for _, part := range []string{connectionID, s.grant, s.tokenURL, s.clientID, s.clientSecret, s.username,
		s.password, s.scope} {
		h.Write([]byte(part))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// host is the token URL's scheme and host, the only part of it an error names.
func (s oauthSettings) host() string { return redactURL(s.tokenURL) }

// OAuthError is a failed OAuth2 sign-in. Transient says whether trying again later can help: a refused
// password or client (400, 401, 403) cannot, a token endpoint that is down or overloaded can.
type OAuthError struct {
	NodeID    string
	Message   string
	transient bool
	Err       error
}

func (e *OAuthError) Error() string {
	return fmt.Sprintf("node %s: OAuth2 sign-in failed: %s", e.NodeID, e.Message)
}

func (e *OAuthError) Unwrap() error { return e.Err }

// Transient implements message.TransientClassifier.
func (e *OAuthError) Transient() bool { return e.transient }

// oauthUse is what a request's credential came from, so a 401 can drop exactly that token.
type oauthUse struct {
	key   string
	token string
	fresh bool // fetched for this request rather than taken from the cache
}

type cachedToken struct {
	value   string
	renewAt time.Time
}

// flight is one sign-in in progress; others wanting the same key wait for it.
type flight struct {
	done  chan struct{}
	token string
	err   error
}

// tokenCache keeps tokens per process. Concurrent steps that need the same token sign in once.
type tokenCache struct {
	mu      sync.Mutex
	tokens  map[string]cachedToken
	flights map[string]*flight
	now     func() time.Time
}

var tokens = &tokenCache{tokens: map[string]cachedToken{}, flights: map[string]*flight{}, now: time.Now}

// get returns a token for key: the cached one while it is fresh, else a new one from fetch.
func (c *tokenCache) get(ctx context.Context, key string, fetch func(context.Context) (string, time.Duration, error)) (string, bool, error) {
	c.mu.Lock()
	if t, ok := c.tokens[key]; ok && c.now().Before(t.renewAt) {
		c.mu.Unlock()
		return t.value, false, nil
	}
	if f, ok := c.flights[key]; ok {
		c.mu.Unlock()
		select {
		case <-f.done:
			return f.token, true, f.err
		case <-ctx.Done():
			return "", false, ctx.Err()
		}
	}
	f := &flight{done: make(chan struct{})}
	c.flights[key] = f
	c.mu.Unlock()

	token, lifetime, err := fetch(ctx)
	c.mu.Lock()
	delete(c.flights, key)
	if err == nil {
		c.tokens[key] = cachedToken{value: token, renewAt: c.now().Add(lifetime - renewMargin(lifetime))}
	}
	c.mu.Unlock()
	f.token, f.err = token, err
	close(f.done)
	return token, true, err
}

// drop forgets key's token if it is still token, after the service refused it.
func (c *tokenCache) drop(key, token string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if t, ok := c.tokens[key]; ok && t.value == token {
		delete(c.tokens, key)
	}
}

// renewMargin is how much earlier than its expiry a token is renewed: a tenth of its life, at most a minute.
func renewMargin(lifetime time.Duration) time.Duration {
	m := lifetime / 10
	if m > maxRenewMargin {
		m = maxRenewMargin
	}
	return m
}

// applyOAuth sets "Authorization: Bearer <token>" for an oauth2 connection.
func applyOAuth(ctx context.Context, nodeID, connectionID string, req *http.Request, conn map[string]interface{}) (*oauthUse, error) {
	s, err := readOAuthSettings(nodeID, conn)
	if err != nil {
		return nil, err
	}
	key := s.key(connectionID)
	token, fresh, err := tokens.get(ctx, key, func(ctx context.Context) (string, time.Duration, error) {
		return fetchToken(ctx, nodeID, s)
	})
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	return &oauthUse{key: key, token: token, fresh: fresh}, nil
}

// tokenClient signs in: the egress policy on every dial, no redirects (a redirect would carry the
// password elsewhere, or turn the POST into a GET), and a bound on the whole exchange.
func tokenClient() *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.ResponseHeaderTimeout = tokenTimeout
	t.DialContext = (&net.Dialer{Timeout: tokenTimeout, KeepAlive: 30 * time.Second, Control: guardDial}).DialContext
	return &http.Client{Transport: t, Timeout: tokenTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

// fetchToken asks the token URL for an access token with the connection's grant. The client
// authenticates in the body (client_secret_post), which Keycloak and most providers accept.
func fetchToken(ctx context.Context, nodeID string, s oauthSettings) (string, time.Duration, error) {
	form := url.Values{"grant_type": {s.grant}, "client_id": {s.clientID}}
	if s.clientSecret != "" {
		form.Set("client_secret", s.clientSecret)
	}
	if s.grant == grantPassword {
		form.Set("username", s.username)
		form.Set("password", s.password)
	}
	if s.scope != "" {
		form.Set("scope", s.scope)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", 0, NewConfigError(nodeID, "connection.oauth2_token_url", "invalid oauth2_token_url", nil)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := tokenClient().Do(req)
	if err != nil {
		var egress *EgressError
		if errors.As(err, &egress) {
			return "", 0, NewConfigError(nodeID, "connection.oauth2_token_url", egress.Error(), egress)
		}
		// Unreachable, refused or timed out: the token endpoint may be back later.
		return "", 0, &OAuthError{NodeID: nodeID, transient: true, Err: redactURLError(err),
			Message: fmt.Sprintf("%s did not answer: %v", s.host(), redactURLError(err))}
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxTokenAnswer))

	if resp.StatusCode != http.StatusOK {
		code := oauthErrorCode(body)
		msg := fmt.Sprintf("%s answered %d", s.host(), resp.StatusCode)
		if code != "" {
			msg += ": " + code
		}
		transient := resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests
		return "", 0, &OAuthError{NodeID: nodeID, Message: msg, transient: transient}
	}

	var answer struct {
		AccessToken string          `json:"access_token"`
		TokenType   string          `json:"token_type"`
		ExpiresIn   json.RawMessage `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &answer); err != nil || answer.AccessToken == "" {
		return "", 0, &OAuthError{NodeID: nodeID, Message: fmt.Sprintf("%s answered without an access_token", s.host())}
	}
	if answer.TokenType != "" && !strings.EqualFold(answer.TokenType, "bearer") {
		return "", 0, &OAuthError{NodeID: nodeID,
			Message: fmt.Sprintf("%s gave a %q token; only Bearer tokens are supported", s.host(), answer.TokenType)}
	}
	if !validHeaderValue(answer.AccessToken) {
		return "", 0, &OAuthError{NodeID: nodeID, Message: fmt.Sprintf("%s gave a token no header can carry", s.host())}
	}
	return answer.AccessToken, tokenLifetime(answer.ExpiresIn), nil
}

// oauthErrorCode is the OAuth2 error code of a refusal, such as invalid_grant, and only when it is one
// word: the description is not quoted, since a provider can echo the username in it.
func oauthErrorCode(body []byte) string {
	var refusal struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(body, &refusal) != nil {
		return ""
	}
	code := strings.TrimSpace(refusal.Error)
	if code == "" || len(code) > 64 || strings.IndexFunc(code, func(r rune) bool {
		return !(r == '_' || r == '-' || r == '.' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9'))
	}) >= 0 {
		return ""
	}
	return code
}

// tokenLifetime reads expires_in, a number of seconds that some providers send as a string.
func tokenLifetime(raw json.RawMessage) time.Duration {
	s := strings.Trim(strings.TrimSpace(string(raw)), `"`)
	if s == "" || s == "null" {
		return defaultTokenLifetime
	}
	n, err := strconv.ParseFloat(s, 64)
	if err != nil || n <= 0 {
		return defaultTokenLifetime
	}
	return time.Duration(n * float64(time.Second))
}
