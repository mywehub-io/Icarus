package httpclient

import (
	"encoding/json"
	"fmt"
	"strings"
)

// HeaderItem represents a key-value header pair.
type HeaderItem struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type BindableText struct {
	Source   string `json:"source"`
	Value    string `json:"value"`
	InputKey string `json:"inputKey"`
}

type BindableObject struct {
	Source   string      `json:"source"`
	Value    interface{} `json:"value"`
	InputKey string      `json:"inputKey"`
}

// ManualInput represents either a legacy key/value header entry or a dynamic header input selector.
// - Legacy: {"key":"X-Foo","value":"bar"}
// - Dynamic: {"name":"X-Foo"}
type ManualInput struct {
	Name  string `json:"name,omitempty"`
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

func (m *ManualInput) UnmarshalJSON(data []byte) error {
	// Try dynamic form first.
	var dynamic struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(data, &dynamic); err == nil && dynamic.Name != "" {
		m.Name = dynamic.Name
		return nil
	}

	// Fall back to legacy key/value.
	var legacy HeaderItem
	if err := json.Unmarshal(data, &legacy); err != nil {
		return err
	}
	m.Key = legacy.Key
	m.Value = legacy.Value
	return nil
}

// Config defines the configuration for the HTTP client processor.
type Config struct {
	Label        string `json:"label"`
	ConnectionID string `json:"connection_id"`

	// URL is optional for backward compatibility; when omitted, URL is assumed to come from connection.
	URL *BindableText `json:"url,omitempty"`
	// Method is required when URL comes from config or input; it is ignored when URL comes from connection.
	Method string `json:"method,omitempty"`
	// ConnectionMethod is the method when URL comes from connection; empty uses the connection's own.
	ConnectionMethod string `json:"connection_method,omitempty"`
	// PathFromInput appends the input "path" to the connection's URL, after any connection_path.
	// Only with URL from connection; the path may not name a scheme or host or climb out.
	PathFromInput bool `json:"path_from_input,omitempty"`
	// Headers is optional; when omitted, legacy manual_inputs key/value entries are treated as static headers.
	Headers *BindableObject `json:"headers,omitempty"`

	ManualInputs []ManualInput `json:"manual_inputs,omitempty"`
	// Connection is populated by Elysium enrichment (connection_id -> connection details from Nyx):
	// url, method, auth_type, bearer_token, basic_username, basic_password, api_key_header,
	// api_key_value and custom_headers, all of which the node sends (connection.go).
	Connection map[string]interface{} `json:"connection"`
}

// UnmarshalJSON supports both raw config (with connection_id) and enriched config (with connection).
func (c *Config) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	if v, ok := raw["label"]; ok {
		_ = json.Unmarshal(v, &c.Label)
	}
	if v, ok := raw["connection_id"]; ok {
		_ = json.Unmarshal(v, &c.ConnectionID)
	}
	if v, ok := raw["url"]; ok {
		var url BindableText
		if err := json.Unmarshal(v, &url); err == nil {
			c.URL = &url
		}
	}
	if v, ok := raw["method"]; ok {
		_ = json.Unmarshal(v, &c.Method)
	}
	if v, ok := raw["connection_method"]; ok {
		_ = json.Unmarshal(v, &c.ConnectionMethod)
	}
	if v, ok := raw["path_from_input"]; ok {
		_ = json.Unmarshal(v, &c.PathFromInput)
	}
	if v, ok := raw["headers"]; ok {
		var headers BindableObject
		if err := json.Unmarshal(v, &headers); err == nil {
			c.Headers = &headers
		}
	}
	if v, ok := raw["manual_inputs"]; ok {
		_ = json.Unmarshal(v, &c.ManualInputs)
	}
	if v, ok := raw["connection"]; ok {
		var conn map[string]interface{}
		if err := json.Unmarshal(v, &conn); err == nil {
			c.Connection = conn
		}
	}

	return nil
}

// Validate checks if the configuration is valid.
func (c *Config) Validate(nodeID string) error {
	urlSource := "connection"
	if c.URL != nil && strings.TrimSpace(c.URL.Source) != "" {
		urlSource = strings.TrimSpace(c.URL.Source)
	}

	switch urlSource {
	case "connection":
		if c.Connection == nil {
			if c.ConnectionID == "" {
				return NewConfigError(nodeID, "connection_id", "connection_id is required when url.source=connection", nil)
			}
			return NewConfigError(nodeID, "connection", "connection was not enriched (connection_id present but connection missing); ensure Elysium enrichment runs before processing", nil)
		}

		urlVal, ok := c.Connection["url"]
		if !ok || urlVal == nil {
			return NewConfigError(nodeID, "connection.url", "connection must contain url", nil)
		}
		urlStr, ok := urlVal.(string)
		if !ok || strings.TrimSpace(urlStr) == "" {
			return NewConfigError(nodeID, "connection.url", "connection url must be a non-empty string", nil)
		}
	case "config":
		if c.URL == nil || strings.TrimSpace(c.URL.Value) == "" {
			return NewConfigError(nodeID, "url.value", "url.value is required when url.source=config", nil)
		}
		if strings.TrimSpace(c.Method) == "" {
			return NewConfigError(nodeID, "method", "method is required when url.source=config", nil)
		}
	case "input":
		// URL value comes from runtime input; validate method requirement here.
		if c.URL != nil && c.URL.InputKey != "" && c.URL.InputKey != "url" {
			return NewConfigError(nodeID, "url.inputKey", fmt.Sprintf("url.inputKey must be \"url\" (got %q)", c.URL.InputKey), nil)
		}
		if strings.TrimSpace(c.Method) == "" {
			return NewConfigError(nodeID, "method", "method is required when url.source=input", nil)
		}
	default:
		return NewConfigError(nodeID, "url.source", fmt.Sprintf("unsupported url.source %q", urlSource), nil)
	}

	// A path from input is joined to the connection's URL, whose host it can never change. With a URL
	// from config or input there is no such host to keep, so the setting is refused rather than ignored.
	if c.PathFromInput && urlSource != "connection" {
		return NewConfigError(nodeID, "path_from_input", "path_from_input needs url.source=connection", nil)
	}

	return nil
}
