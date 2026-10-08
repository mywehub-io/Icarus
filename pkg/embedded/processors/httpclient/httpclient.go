package httpclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
)

const (
	// streamIdleTimeout ends a streamed exchange that moves no bytes for this long, and
	// responseHeaderTimeout bounds the wait for response headers once the request is sent. A
	// streamed body has no overall limit, because a large file on a slow link is not a fault.
	streamIdleTimeout     = 30 * time.Second
	responseHeaderTimeout = 30 * time.Second
)

// streamingClient has no overall timeout; dial, TLS and the response headers are still bounded,
// and idleReader bounds the body. Every dial passes the egress policy (egress.go).
func streamingClient() *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.ResponseHeaderTimeout = responseHeaderTimeout
	// http.DefaultTransport's dialer, plus the policy.
	t.DialContext = (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second, Control: guardDial}).DialContext
	return &http.Client{Transport: t}
}

// idleReader cancels its context when no Read returns for idle, so a stalled peer fails the
// request instead of holding the unit for ever. Every Read that returns resets the timer.
type idleReader struct {
	r      io.Reader
	timer  *time.Timer
	idle   time.Duration
	cancel context.CancelFunc
}

func newIdleReader(r io.Reader, idle time.Duration, cancel context.CancelFunc) *idleReader {
	ir := &idleReader{r: r, idle: idle, cancel: cancel}
	ir.timer = time.AfterFunc(idle, cancel)
	return ir
}

func (ir *idleReader) Read(p []byte) (int, error) {
	n, err := ir.r.Read(p)
	if err != nil {
		// End of the body, or a failure: nothing is moving, and nothing is waiting on this reader.
		ir.timer.Stop()
		return n, err
	}
	ir.timer.Reset(ir.idle)
	return n, err
}

// stop ends the idle watch; call it when the body is done.
func (ir *idleReader) stop() { ir.timer.Stop() }

// HTTPClientNode implements HTTP client requests for embedded processing.
type HTTPClientNode struct {
	runtime.BaseNode

	// cfgCache holds the parsed configuration, read-only, shared by every worker of the unit.
	cfgCache runtime.ConfigCache[Config]
}

// NewHTTPClientNode creates a new HTTP client node.
func NewHTTPClientNode(config runtime.EmbeddedNodeConfig) (runtime.EmbeddedNode, error) {
	if config.PluginType != "plugin-http-client" {
		return nil, fmt.Errorf("invalid plugin type: expected 'plugin-http-client', got '%s'", config.PluginType)
	}
	return &HTTPClientNode{BaseNode: runtime.NewBaseNode(config)}, nil
}

// Prepare implements runtime.ConfigPreparer: the configuration is parsed once for the unit.
func (n *HTTPClientNode) Prepare(rawConfig json.RawMessage) {
	_, _ = n.cfgCache.Get(rawConfig, runtime.ParseJSON[Config])
}

// Process executes the HTTP request.
func (n *HTTPClientNode) Process(input runtime.ProcessInput) runtime.ProcessOutput {
	cfgp, err := n.cfgCache.Get(input.RawConfig, runtime.ParseJSON[Config])
	if err != nil {
		return runtime.ErrorOutput(NewConfigError(n.NodeId(), "configuration", fmt.Sprintf("failed to parse configuration: %v", err), err))
	}
	cfg := *cfgp

	if err := cfg.Validate(n.NodeId()); err != nil {
		return runtime.ErrorOutput(err)
	}

	// A file a BYTE mapping delivered to "payload" is streamed as the request body, with its
	// length.
	var fileBody io.ReadCloser
	var fileSize int64
	if ref, ok := input.TrustedFile("payload"); ok {
		if input.Files == nil {
			return runtime.ErrorOutput(NewConfigError(n.NodeId(), "payload", "payload is a file but file storage is not configured", nil))
		}
		rc, openErr := input.Files.Open(input.Ctx, ref)
		if openErr != nil {
			return runtime.ErrorOutput(NewHTTPError(n.NodeId(), "failed to open payload file: "+openErr.Error(), openErr, 0, nil))
		}
		defer rc.Close()
		fileBody, fileSize = rc, ref.Size
	} else if v, present := input.Data["payload"]; present && v != nil {
		return runtime.ErrorOutput(NewConfigError(n.NodeId(), "payload", "payload must be a file: map a byte port (a file reference) into payload", nil))
	}
	if !input.WritesFile("body") {
		return runtime.ErrorOutput(NewConfigError(n.NodeId(), "body", "the response body is written as a file but file storage is not configured", nil))
	}

	// A streamed exchange has no overall timeout, only an idle one.
	ctx, cancel := context.WithCancel(input.Ctx)
	defer cancel()
	var reqBody io.Reader
	if fileBody != nil {
		ir := newIdleReader(fileBody, streamIdleTimeout, cancel)
		defer ir.stop()
		reqBody = ir
	}

	// Build request
	req, err := n.buildRequest(ctx, &cfg, input.Data, reqBody, fileSize)
	if err != nil {
		return runtime.ErrorOutput(err)
	}

	// Execute request
	client := streamingClient()
	resp, err := client.Do(req)
	if err != nil {
		// A refused destination is the workflow's configuration, not a fault a retry clears.
		var egress *EgressError
		if errors.As(err, &egress) {
			return runtime.ErrorOutput(NewConfigError(n.NodeId(), "url", egress.Error(), egress))
		}
		return runtime.ErrorOutput(NewHTTPError(n.NodeId(), "request failed: "+err.Error(), err, 0, nil))
	}
	defer resp.Body.Close()

	// The response streams into a file with its Content-Type; nothing is read into memory.
	ct := resp.Header.Get("Content-Type")
	if ct == "" {
		ct = "application/octet-stream"
	}
	respReader := newIdleReader(resp.Body, streamIdleTimeout, cancel)
	defer respReader.stop()
	ref, werr := input.WriteOutputFile("body", ct, respReader)
	if werr != nil {
		return runtime.ErrorOutput(NewHTTPError(n.NodeId(), "failed to store response body: "+werr.Error(), werr, resp.StatusCode, nil))
	}
	return runtime.SuccessOutput(map[string]interface{}{"status": resp.StatusCode, "body": ref})
}

func (n *HTTPClientNode) buildRequest(ctx context.Context, cfg *Config, data map[string]interface{}, fileBody io.Reader, fileSize int64) (*http.Request, error) {
	urlStr, usingConnection, conn, err := n.resolveURL(cfg, data)
	if err != nil {
		return nil, err
	}

	method, err := n.resolveMethod(cfg, data, usingConnection, conn)
	if err != nil {
		return nil, err
	}

	var body io.Reader
	if method != "GET" && method != "HEAD" {
		if fileBody != nil {
			body = fileBody
		}
	}

	req, err := http.NewRequestWithContext(ctx, method, urlStr, body)
	if err != nil {
		return nil, NewConfigError(n.NodeId(), "url", "invalid URL: "+err.Error(), err)
	}
	if fileBody != nil && body != nil {
		req.ContentLength = fileSize
	}

	if err := n.applyHeaders(req, cfg, data); err != nil {
		return nil, err
	}

	// Apply auth only when using connection
	if usingConnection {
		n.applyAuth(req, conn)
	}

	return req, nil
}

func (n *HTTPClientNode) resolveURL(
	cfg *Config,
	data map[string]interface{},
) (urlStr string, usingConnection bool, conn map[string]interface{}, err error) {
	urlSource := "connection"
	if cfg.URL != nil && strings.TrimSpace(cfg.URL.Source) != "" {
		urlSource = strings.TrimSpace(cfg.URL.Source)
	}

	switch urlSource {
	case "connection":
		conn = cfg.Connection
		urlStr, _ = conn["url"].(string)
		if strings.TrimSpace(urlStr) == "" {
			return "", true, conn, NewConfigError(n.NodeId(), "connection.url", "connection url must be a non-empty string", nil)
		}
		return urlStr, true, conn, nil
	case "config":
		if cfg.URL == nil || strings.TrimSpace(cfg.URL.Value) == "" {
			return "", false, nil, NewConfigError(n.NodeId(), "url.value", "url.value is required when url.source=config", nil)
		}
		return strings.TrimSpace(cfg.URL.Value), false, nil, nil
	case "input":
		if data == nil {
			return "", false, nil, NewConfigError(n.NodeId(), "url", "url is required in input when url.source=input", nil)
		}
		v, ok := data["url"]
		if !ok || v == nil {
			return "", false, nil, NewConfigError(n.NodeId(), "url", "url is required in input when url.source=input", nil)
		}
		s, ok := v.(string)
		if !ok || strings.TrimSpace(s) == "" {
			return "", false, nil, NewConfigError(n.NodeId(), "url", "input url must be a non-empty string when url.source=input", nil)
		}
		return strings.TrimSpace(s), false, nil, nil
	default:
		return "", false, nil, NewConfigError(n.NodeId(), "url.source", fmt.Sprintf("unsupported url.source %q", urlSource), nil)
	}
}

func (n *HTTPClientNode) resolveMethod(
	cfg *Config,
	_ map[string]interface{},
	usingConnection bool,
	conn map[string]interface{},
) (string, error) {
	if usingConnection {
		method := strings.ToUpper(strings.TrimSpace(getString(conn, "method", "POST")))
		if method == "" {
			return "POST", nil
		}
		return method, nil
	}

	method := strings.ToUpper(strings.TrimSpace(cfg.Method))
	if method == "" {
		return "", NewConfigError(n.NodeId(), "method", "method is required when url.source is config or input", nil)
	}
	return method, nil
}

func (n *HTTPClientNode) applyHeaders(req *http.Request, cfg *Config, data map[string]interface{}) error {
	headersSource := "config"
	if cfg.Headers != nil && strings.TrimSpace(cfg.Headers.Source) != "" {
		headersSource = strings.TrimSpace(cfg.Headers.Source)
	}

	configHeaders, err := parseConfigHeaders(n.NodeId(), cfg)
	if err != nil {
		return err
	}

	inputHeaders, err := buildInputHeaders(n.NodeId(), cfg, data, headersSource)
	if err != nil {
		return err
	}

	final := make(map[string]string)
	switch headersSource {
	case "config":
		for k, v := range configHeaders {
			final[k] = v
		}
	case "input":
		for k, v := range inputHeaders {
			final[k] = v
		}
	case "merge":
		for k, v := range configHeaders {
			final[k] = v
		}
		for k, v := range inputHeaders {
			final[k] = v
		}
	default:
		return NewConfigError(n.NodeId(), "headers.source", fmt.Sprintf("unsupported headers.source %q", headersSource), nil)
	}

	for k, v := range final {
		if strings.TrimSpace(k) == "" {
			continue
		}
		req.Header.Set(k, v)
	}
	return nil
}

func parseConfigHeaders(nodeID string, cfg *Config) (map[string]string, error) {
	// Prefer explicit headers.value when present; otherwise fall back to legacy manual_inputs key/value entries.
	var value interface{}
	if cfg.Headers != nil && cfg.Headers.Value != nil {
		value = cfg.Headers.Value
	} else {
		legacy := make([]HeaderItem, 0)
		for _, mi := range cfg.ManualInputs {
			if strings.TrimSpace(mi.Key) != "" {
				legacy = append(legacy, HeaderItem{Key: mi.Key, Value: mi.Value})
			}
		}
		if len(legacy) > 0 {
			value = legacy
		}
	}

	result := make(map[string]string)
	if value == nil {
		return result, nil
	}

	switch v := value.(type) {
	case map[string]interface{}:
		for k, raw := range v {
			s, ok := raw.(string)
			if !ok {
				return nil, NewConfigError(nodeID, "headers.value", fmt.Sprintf("header %q must be a string value", k), nil)
			}
			result[k] = s
		}
		return result, nil
	case []interface{}:
		for _, item := range v {
			m, ok := item.(map[string]interface{})
			if !ok {
				continue
			}
			k, _ := m["key"].(string)
			val, _ := m["value"].(string)
			if strings.TrimSpace(k) != "" {
				result[k] = val
			}
		}
		return result, nil
	case []HeaderItem:
		for _, h := range v {
			if strings.TrimSpace(h.Key) != "" {
				result[h.Key] = h.Value
			}
		}
		return result, nil
	default:
		// Handle json.Unmarshal decoding into map[string]any or []any; any other type is unexpected.
		return nil, NewConfigError(nodeID, "headers.value", fmt.Sprintf("unsupported headers.value type %T", value), nil)
	}
}

func buildInputHeaders(nodeID string, cfg *Config, data map[string]interface{}, headersSource string) (map[string]string, error) {
	result := make(map[string]string)
	if headersSource != "input" && headersSource != "merge" {
		return result, nil
	}
	for _, mi := range cfg.ManualInputs {
		name := strings.TrimSpace(mi.Name)
		if name == "" {
			continue
		}
		if data == nil {
			return nil, NewConfigError(nodeID, "manual_inputs", fmt.Sprintf("header %q is required in input but input is empty", name), nil)
		}
		raw, ok := data[name]
		if !ok || raw == nil {
			return nil, NewConfigError(nodeID, "manual_inputs", fmt.Sprintf("header %q is required in input", name), nil)
		}
		s, ok := raw.(string)
		if !ok {
			return nil, NewConfigError(nodeID, "manual_inputs", fmt.Sprintf("header %q must be a string", name), nil)
		}
		result[name] = s
	}
	return result, nil
}

func (n *HTTPClientNode) applyAuth(req *http.Request, conn map[string]interface{}) {
	// Only Bearer auth supported (matches Nyx HTTP schema: url, method, auth_type, bearer_token)
	if strings.ToLower(getString(conn, "auth_type", "")) != "bearer" {
		return
	}
	if token := getString(conn, "bearer_token", ""); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
}

func getString(m map[string]interface{}, key, def string) string {
	if m == nil {
		return def
	}
	v, ok := m[key]
	if !ok || v == nil {
		return def
	}
	if s, ok := v.(string); ok {
		return s
	}
	return def
}
