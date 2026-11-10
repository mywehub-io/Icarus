package httpclient

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"unicode"
)

// What a connection puts on a request. A Nyx HTTP connection (nyx/scripts/connection-schemas/http.sql)
// carries url, method, auth_type, the credential fields of each auth_type, and custom_headers. The
// node sends them only when the URL comes from the connection: a URL from config or input could point
// anywhere, so the connection's credentials never go with it.

const maxRedirects = 10

// applyConnectionHeaders sets the connection's custom_headers on req and returns their names. They go on
// first, so the node's own headers can replace them; auth goes on last (applyAuth).
func applyConnectionHeaders(nodeID string, req *http.Request, conn map[string]interface{}) ([]string, error) {
	headers, err := connectionCustomHeaders(nodeID, conn)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(headers))
	for k := range headers {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		req.Header.Set(k, headers[k])
	}
	return names, nil
}

// connectionCustomHeaders reads custom_headers: a JSON object of header names to string values, which
// Nyx stores as text from a textarea, or the same object already decoded. Values are never quoted in an
// error, since a header can carry a credential.
func connectionCustomHeaders(nodeID string, conn map[string]interface{}) (map[string]string, error) {
	raw, ok := conn["custom_headers"]
	if !ok || raw == nil {
		return nil, nil
	}
	var obj map[string]interface{}
	switch v := raw.(type) {
	case string:
		if strings.TrimSpace(v) == "" {
			return nil, nil
		}
		if err := json.Unmarshal([]byte(v), &obj); err != nil || obj == nil {
			return nil, NewConfigError(nodeID, "connection.custom_headers", "custom headers must be a JSON object of header names to values", nil)
		}
	case map[string]interface{}:
		obj = v
	default:
		return nil, NewConfigError(nodeID, "connection.custom_headers", "custom headers must be a JSON object of header names to values", nil)
	}
	out := make(map[string]string, len(obj))
	for k, val := range obj {
		s, ok := val.(string)
		if !ok {
			return nil, NewConfigError(nodeID, "connection.custom_headers", fmt.Sprintf("custom header %q must be a string", k), nil)
		}
		if !validHeaderName(k) {
			return nil, NewConfigError(nodeID, "connection.custom_headers", fmt.Sprintf("custom header %q is not a valid header name", k), nil)
		}
		if !validHeaderValue(s) {
			return nil, NewConfigError(nodeID, "connection.custom_headers", fmt.Sprintf("custom header %q has a line break or control character in its value", k), nil)
		}
		out[k] = s
	}
	return out, nil
}

// applyAuth sets the connection's credential for its auth_type and returns the header it set, or "" for
// none. An auth_type whose credential is missing is refused rather than sent unauthenticated: the
// connection says the service needs it, so a request without it can only fail, and a 401 is not an error
// to this node.
func applyAuth(nodeID string, req *http.Request, conn map[string]interface{}) (string, error) {
	authType := strings.ToLower(strings.TrimSpace(getString(conn, "auth_type", "")))
	switch authType {
	case "", "none":
		return "", nil
	case "bearer":
		token := getString(conn, "bearer_token", "")
		if token == "" {
			return "", missingCredential(nodeID, authType, "bearer_token")
		}
		if !validHeaderValue(token) {
			return "", badCredential(nodeID, "bearer_token")
		}
		req.Header.Set("Authorization", "Bearer "+token)
		return "Authorization", nil
	case "basic":
		user, pass := getString(conn, "basic_username", ""), getString(conn, "basic_password", "")
		if user == "" {
			return "", missingCredential(nodeID, authType, "basic_username")
		}
		if pass == "" {
			return "", missingCredential(nodeID, authType, "basic_password")
		}
		req.SetBasicAuth(user, pass)
		return "Authorization", nil
	case "apikey":
		name := strings.TrimSpace(getString(conn, "api_key_header", ""))
		value := getString(conn, "api_key_value", "")
		if name == "" {
			return "", missingCredential(nodeID, authType, "api_key_header")
		}
		if value == "" {
			return "", missingCredential(nodeID, authType, "api_key_value")
		}
		if !validHeaderName(name) {
			return "", NewConfigError(nodeID, "connection.api_key_header", fmt.Sprintf("%q is not a valid header name", name), nil)
		}
		if !validHeaderValue(value) {
			return "", badCredential(nodeID, "api_key_value")
		}
		req.Header.Set(name, value)
		return name, nil
	default:
		return "", NewConfigError(nodeID, "connection.auth_type", fmt.Sprintf("unsupported auth_type %q", authType), nil)
	}
}

func missingCredential(nodeID, authType, key string) error {
	return NewConfigError(nodeID, "connection."+key,
		fmt.Sprintf("the connection's auth_type is %s but it has no %s", authType, key), nil)
}

// badCredential refuses a credential no header can carry, most often one pasted with a line break at
// its end. Go would refuse it as a transport fault naming only the header; this names the field, and
// never the value. Basic credentials need no check: they go base64 encoded.
func badCredential(nodeID, key string) error {
	return NewConfigError(nodeID, "connection."+key,
		fmt.Sprintf("the connection's %s has a line break or control character in it; check for one pasted at its end", key), nil)
}

// redirectPolicy is the client's CheckRedirect.
//
// A request that carried a body is not followed: Go turns a 301, 302 or 303 after a POST into a GET
// without the body, and cannot resend a streamed body for a 307 or 308, so following would report the
// status of a request the workflow never meant to make. The redirect response itself is returned, and
// its status is what the node reports.
//
// The connection's own headers (connHeaders: custom headers and the credential) are removed when a
// redirect leaves the connection's scheme and host. Go removes Authorization only when the host stops
// being the same domain or a subdomain, and never removes an API key header or a custom one; the
// connection's credentials are for its own host and nothing else.
func redirectPolicy(connHeaders []string, hadBody bool) func(*http.Request, []*http.Request) error {
	return func(req *http.Request, via []*http.Request) error {
		if hadBody {
			return http.ErrUseLastResponse
		}
		if len(via) >= maxRedirects {
			return errors.New("stopped after 10 redirects")
		}
		first := via[0].URL
		if !strings.EqualFold(req.URL.Scheme, first.Scheme) || !strings.EqualFold(req.URL.Host, first.Host) {
			for _, h := range connHeaders {
				req.Header.Del(h)
			}
		}
		return nil
	}
}

// joinInputPath appends a path from the run's data to the connection's URL (which already carries any
// connection_path Elysium joined). It keeps the connection's scheme and host whatever the path says.
// The path is never quoted in an error: it comes from the run's data and can carry identifiers.
func joinInputPath(nodeID, base, p string) (string, error) {
	if p == "" {
		return "", NewConfigError(nodeID, "path", "path is required in input when path_from_input is on", nil)
	}
	if reason := relativePathProblem(p); reason != "" {
		return "", NewConfigError(nodeID, "path", "path from input is not a path after the connection's URL: "+reason, nil)
	}
	u, err := url.Parse(base)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", NewConfigError(nodeID, "connection.url", "connection url is not an absolute URL, so a path cannot be joined to it", nil)
	}
	path, query := p, ""
	if i := strings.IndexByte(p, '?'); i >= 0 {
		path, query = p[:i], p[i+1:]
	}
	if path != "" {
		joined := strings.TrimSuffix(u.EscapedPath(), "/") + "/" + strings.TrimPrefix(path, "/")
		parsed, err := url.Parse(joined)
		if err != nil {
			return "", NewConfigError(nodeID, "path", "path from input is not a valid path", nil)
		}
		u.Path, u.RawPath = parsed.Path, parsed.RawPath
	}
	if query != "" {
		if u.RawQuery != "" {
			u.RawQuery += "&" + query
		} else {
			u.RawQuery = query
		}
	}
	out := u.String()
	if check, err := url.Parse(out); err != nil || check.Host != u.Host || check.Scheme != u.Scheme {
		return "", NewConfigError(nodeID, "path", "path from input would change the connection's host", nil)
	}
	return out, nil
}

// relativePathProblem mirrors apollo graphops.RelativePathProblem and elysium's copy
// (elysium/internal/plugins/shared/connectionpath.go). Keep the three in step.
func relativePathProblem(p string) string {
	if strings.TrimSpace(p) != p {
		return "it has leading or trailing spaces"
	}
	for _, r := range p {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return "it holds whitespace or a control character"
		}
	}
	path := p
	if i := strings.IndexByte(path, '?'); i >= 0 {
		path = path[:i]
	}
	switch {
	case strings.Contains(p, "://") || hasScheme(path):
		return "it names a scheme"
	case strings.HasPrefix(path, "//"):
		return "it names a host"
	case strings.Contains(path, "@"):
		return "it holds user information"
	case strings.Contains(p, "#"):
		return "it holds a fragment"
	case strings.Contains(p, "\\"):
		return "it holds a backslash"
	}
	// %2F and %5C count as separators for the ".." check: allowed inside a segment (an identifier
	// holding a slash), but a server that decodes them before routing would let "a%2F..%2Fadmin"
	// climb out.
	for _, enc := range []string{"%2f", "%2F", "%5c", "%5C"} {
		path = strings.ReplaceAll(path, enc, "/")
	}
	for _, seg := range strings.Split(path, "/") {
		if seg == ".." || strings.EqualFold(seg, "%2e%2e") || strings.EqualFold(seg, ".%2e") || strings.EqualFold(seg, "%2e.") {
			return "it climbs out of the connection's path"
		}
	}
	return ""
}

func hasScheme(path string) bool {
	colon := strings.IndexByte(path, ':')
	if colon <= 0 {
		return false
	}
	if slash := strings.IndexByte(path, '/'); slash >= 0 && slash < colon {
		return false
	}
	for i, r := range path[:colon] {
		if !(unicode.IsLetter(r) || (i > 0 && (unicode.IsDigit(r) || r == '+' || r == '-' || r == '.'))) {
			return false
		}
	}
	return true
}

// validHeaderValue reports whether s can be sent as a header value: no control character but a tab
// (RFC 9110 field-value, as Go's transport checks it).
func validHeaderValue(s string) bool {
	for i := 0; i < len(s); i++ {
		if c := s[i]; (c < ' ' && c != '\t') || c == 0x7f {
			return false
		}
	}
	return true
}

// validHeaderName reports whether s is an RFC 9110 field name (a token).
func validHeaderName(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case strings.IndexByte("!#$%&'*+-.^_`|~", c) >= 0:
		default:
			return false
		}
	}
	return true
}
