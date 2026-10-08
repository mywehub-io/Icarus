package httpclient_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/embedded/processors/httpclient"
	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
	"github.com/wehubfusion/Icarus/pkg/fileref"
	"github.com/wehubfusion/Icarus/pkg/filestore"
	"github.com/wehubfusion/Icarus/pkg/filestore/memfs"
)

// withRun gives input the run's files, as the runner does, on a fresh in-memory backend.
func withRun(input runtime.ProcessInput) (runtime.ProcessInput, *memfs.Backend) {
	b := memfs.New()
	store, _ := filestore.New(b, "w", "r")
	input.Files, input.WorkflowID, input.RunID, input.ParentNodeID = store, "w", "r", "p"
	return input, b
}

func createTestNode(t *testing.T, nodeID string) *httpclient.HTTPClientNode {
	config := runtime.EmbeddedNodeConfig{
		NodeId:     nodeID,
		Label:      "test-http-client",
		PluginType: "plugin-http-client",
		Embeddable: true,
		Depth:      0,
	}
	node, err := httpclient.NewHTTPClientNode(config)
	if err != nil {
		t.Fatalf("failed to create test node: %v", err)
	}
	return node.(*httpclient.HTTPClientNode)
}

func TestNewHTTPClientNode_InvalidPluginType(t *testing.T) {
	config := runtime.EmbeddedNodeConfig{
		NodeId:     "node1",
		PluginType: "plugin-other",
		Embeddable: true,
	}
	_, err := httpclient.NewHTTPClientNode(config)
	if err == nil {
		t.Fatal("expected error for invalid plugin type")
	}
}

func TestProcess_InvalidJSONConfig(t *testing.T) {
	node := createTestNode(t, "node1")
	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{"payload": "hello"},
		RawConfig: json.RawMessage(`{invalid json`),
		NodeId:    "node1",
	}
	output := node.Process(input)
	if output.Error == nil {
		t.Fatal("expected error for invalid JSON config")
	}
	if output.Data != nil {
		t.Fatal("expected nil data on error")
	}
}

func TestProcess_MissingConnection(t *testing.T) {
	node := createTestNode(t, "node1")
	rawCfg := `{"label":"test","connection_id":"conn-1","manual_inputs":[]}`
	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{"payload": "{}"},
		RawConfig: json.RawMessage(rawCfg),
		NodeId:    "node1",
	}
	output := node.Process(input)
	if output.Error == nil {
		t.Fatal("expected error when connection not enriched")
	}
}

func TestProcess_Success(t *testing.T) {
	server := newServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method: got %s", r.Method)
		}
		if v := r.Header.Get("X-Custom"); v != "custom-value" {
			t.Errorf("X-Custom header: got %q", v)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	cfg := map[string]interface{}{
		"label": "test",
		"connection": map[string]interface{}{
			"url":    server.URL,
			"method": "POST",
		},
		"manual_inputs": []map[string]string{{"key": "X-Custom", "value": "custom-value"}},
	}
	rawCfg, _ := json.Marshal(cfg)

	node := createTestNode(t, "node1")
	payload := fileref.FileRef{Path: fileref.RunPrefix("w", "r") + "t/payload.json", Size: int64(len(`{"data":"test"}`))}
	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{"payload": fileref.Value(payload)},
		RawConfig: rawCfg,
		NodeId:    "node1",
	}
	input, backend := withRun(input)
	backend.Put(payload.Path, []byte(`{"data":"test"}`))
	input.ByteFields = map[string]bool{"payload": true}
	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("process failed: %v", output.Error)
	}
	if output.Data == nil {
		t.Fatal("expected non-nil data")
	}
	if status, ok := output.Data["status"].(int); !ok || status != 200 {
		t.Errorf("status: got %v", output.Data["status"])
	}
	bodyRef, ok := fileref.Parse(output.Data["body"])
	if !ok {
		t.Fatalf("body is not a file reference: got %T", output.Data["body"])
	}
	if got := string(backend.Blobs[bodyRef.Path]); got != `{"ok":true}` {
		t.Errorf("body: got %q", got)
	}
}

func TestProcess_GETWithNoPayload(t *testing.T) {
	server := newServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method: got %s", r.Method)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	cfg := map[string]interface{}{
		"label": "test",
		"connection": map[string]interface{}{
			"url":    server.URL,
			"method": "GET",
		},
		"manual_inputs": []map[string]string{},
	}
	rawCfg, _ := json.Marshal(cfg)

	node := createTestNode(t, "node1")
	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: rawCfg,
		NodeId:    "node1",
	}
	input, _ = withRun(input)
	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("process failed: %v", output.Error)
	}
	if status, ok := output.Data["status"].(int); !ok || status != 204 {
		t.Errorf("status: got %v", output.Data["status"])
	}
}

func TestProcess_BearerAuth(t *testing.T) {
	server := newServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth != "Bearer secret-token-123" {
			t.Errorf("Authorization: got %q", auth)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := map[string]interface{}{
		"label": "test",
		"connection": map[string]interface{}{
			"url":          server.URL,
			"method":       "GET",
			"auth_type":    "bearer",
			"bearer_token": "secret-token-123",
		},
		"manual_inputs": []map[string]string{},
	}
	rawCfg, _ := json.Marshal(cfg)

	node := createTestNode(t, "node1")
	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: rawCfg,
		NodeId:    "node1",
	}
	input, _ = withRun(input)
	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("process failed: %v", output.Error)
	}
}

func TestProcess_URLFromInput_OverridesConnection(t *testing.T) {
	inputServer := newServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.String() != "/" {
			// sanity: still hitting our server
		}
		if r.Method != http.MethodGet {
			t.Errorf("method: got %s", r.Method)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`ok`))
	}))
	defer inputServer.Close()

	otherServer := newServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("should not have used connection url")
	}))
	defer otherServer.Close()

	cfg := map[string]interface{}{
		"label":  "test",
		"url":    map[string]interface{}{"source": "input", "inputKey": "url"},
		"method": "GET",
		"connection": map[string]interface{}{
			"url":    otherServer.URL,
			"method": "POST",
		},
	}
	rawCfg, _ := json.Marshal(cfg)

	node := createTestNode(t, "node1")
	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{"url": inputServer.URL},
		RawConfig: rawCfg,
		NodeId:    "node1",
	}
	input, _ = withRun(input)
	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("process failed: %v", output.Error)
	}
}

func TestProcess_URLFromInput_Missing_Fails(t *testing.T) {
	cfg := map[string]interface{}{
		"label":  "test",
		"url":    map[string]interface{}{"source": "input", "inputKey": "url"},
		"method": "GET",
	}
	rawCfg, _ := json.Marshal(cfg)

	node := createTestNode(t, "node1")
	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: rawCfg,
		NodeId:    "node1",
	}
	output := node.Process(input)
	if output.Error == nil {
		t.Fatal("expected error when url.source=input but input url missing")
	}
}

func TestProcess_URLFromConfig_Success(t *testing.T) {
	server := newServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method: got %s", r.Method)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := map[string]interface{}{
		"label":  "test",
		"url":    map[string]interface{}{"source": "config", "value": server.URL},
		"method": "POST",
	}
	rawCfg, _ := json.Marshal(cfg)

	node := createTestNode(t, "node1")
	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: rawCfg,
		NodeId:    "node1",
	}
	input, _ = withRun(input)
	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("process failed: %v", output.Error)
	}
}

func TestProcess_URLFromConfig_MissingMethod_Fails(t *testing.T) {
	cfg := map[string]interface{}{
		"label": "test",
		"url":   map[string]interface{}{"source": "config", "value": "https://example.com"},
	}
	rawCfg, _ := json.Marshal(cfg)

	node := createTestNode(t, "node1")
	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: rawCfg,
		NodeId:    "node1",
	}
	output := node.Process(input)
	if output.Error == nil {
		t.Fatal("expected error when url.source=config but method missing")
	}
}

func TestProcess_HeadersFromInput_DynamicKeys(t *testing.T) {
	server := newServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if v := r.Header.Get("X-Test"); v != "abc" {
			t.Errorf("X-Test header: got %q", v)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := map[string]interface{}{
		"label":   "test",
		"url":     map[string]interface{}{"source": "config", "value": server.URL},
		"method":  "GET",
		"headers": map[string]interface{}{"source": "input", "inputKey": "manual_inputs"},
		"manual_inputs": []map[string]string{
			{"name": "X-Test"},
		},
	}
	rawCfg, _ := json.Marshal(cfg)

	node := createTestNode(t, "node1")
	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{"X-Test": "abc"},
		RawConfig: rawCfg,
		NodeId:    "node1",
	}
	input, _ = withRun(input)
	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("process failed: %v", output.Error)
	}
}

func TestProcess_HeadersMerge_InputOverridesConfig(t *testing.T) {
	server := newServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if v := r.Header.Get("X-Test"); v != "from-input" {
			t.Errorf("X-Test header: got %q", v)
		}
		if v := r.Header.Get("X-Only"); v != "cfg" {
			t.Errorf("X-Only header: got %q", v)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := map[string]interface{}{
		"label":  "test",
		"url":    map[string]interface{}{"source": "config", "value": server.URL},
		"method": "GET",
		"headers": map[string]interface{}{
			"source": "merge",
			"value": map[string]interface{}{
				"X-Test": "from-config",
				"X-Only": "cfg",
			},
		},
		"manual_inputs": []map[string]string{
			{"name": "X-Test"},
		},
	}
	rawCfg, _ := json.Marshal(cfg)

	node := createTestNode(t, "node1")
	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{"X-Test": "from-input"},
		RawConfig: rawCfg,
		NodeId:    "node1",
	}
	input, _ = withRun(input)
	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("process failed: %v", output.Error)
	}
}

func TestProcess_ConfigHeaders_AsArray(t *testing.T) {
	server := newServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if v := r.Header.Get("X-Arr"); v != "1" {
			t.Errorf("X-Arr header: got %q", v)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := map[string]interface{}{
		"label":  "test",
		"url":    map[string]interface{}{"source": "config", "value": server.URL},
		"method": "GET",
		"headers": map[string]interface{}{
			"source": "config",
			"value": []map[string]string{
				{"key": "X-Arr", "value": "1"},
			},
		},
	}
	rawCfg, _ := json.Marshal(cfg)

	node := createTestNode(t, "node1")
	input := runtime.ProcessInput{
		Ctx:       context.Background(),
		Data:      map[string]interface{}{},
		RawConfig: rawCfg,
		NodeId:    "node1",
	}
	input, _ = withRun(input)
	output := node.Process(input)
	if output.Error != nil {
		t.Fatalf("process failed: %v", output.Error)
	}
}
