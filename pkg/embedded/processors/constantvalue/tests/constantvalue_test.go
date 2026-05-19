package tests

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/embedded/processors/constantvalue"
	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
)

func createConstantValueNode(t *testing.T, nodeID string) *constantvalue.ConstantValueNode {
	t.Helper()
	cfg := runtime.EmbeddedNodeConfig{
		NodeId:     nodeID,
		Label:      "test-constant-value",
		PluginType: "plugin-constant-value-generator",
		Embeddable: true,
		Depth:      0,
		NodeConfig: runtime.NodeConfig{NodeId: nodeID},
	}
	node, err := constantvalue.NewConstantValueNode(cfg)
	if err != nil {
		t.Fatalf("failed to create ConstantValueNode: %v", err)
	}
	return node.(*constantvalue.ConstantValueNode)
}

func createProcessInput(nodeID string, rawConfig json.RawMessage) runtime.ProcessInput {
	return runtime.ProcessInput{
		Ctx:        context.Background(),
		Data:       map[string]interface{}{"ignored": "input"},
		RawConfig:  rawConfig,
		NodeId:     nodeID,
		PluginType: "plugin-constant-value-generator",
		Label:      "test-constant-value",
		ItemIndex:  -1,
	}
}

func TestNewConstantValueNode_InvalidPluginType(t *testing.T) {
	cfg := runtime.EmbeddedNodeConfig{
		NodeId:     "node-1",
		Label:      "test",
		PluginType: "plugin-not-constant",
	}
	if _, err := constantvalue.NewConstantValueNode(cfg); err == nil {
		t.Fatal("expected error for invalid plugin type")
	}
}

func TestProcess_EmitsTypedConstants(t *testing.T) {
	node := createConstantValueNode(t, "node-constants")

	cfg := constantvalue.Config{
		Constants: []constantvalue.ConstantEntry{
			{Name: "greeting", Type: constantvalue.DataTypeString, Value: "hello"},
			{Name: "count", Type: constantvalue.DataTypeNumber, Value: "42"},
			{Name: "enabled", Type: constantvalue.DataTypeBoolean, Value: "true"},
		},
	}
	rawCfg, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("failed to marshal config: %v", err)
	}

	out := node.Process(createProcessInput("node-constants", rawCfg))
	if out.Error != nil {
		t.Fatalf("expected success, got error: %v", out.Error)
	}
	if out.Skipped {
		t.Fatal("expected not skipped")
	}

	if out.Data["greeting"] != "hello" {
		t.Errorf("greeting: got %v, want hello", out.Data["greeting"])
	}
	if f, ok := out.Data["count"].(float64); !ok || f != 42 {
		t.Errorf("count: got %v, want 42", out.Data["count"])
	}
	if b, ok := out.Data["enabled"].(bool); !ok || !b {
		t.Errorf("enabled: got %v, want true", out.Data["enabled"])
	}
}

func TestProcess_InvalidConfigJSON(t *testing.T) {
	node := createConstantValueNode(t, "node-invalid-json")
	out := node.Process(createProcessInput("node-invalid-json", json.RawMessage(`{invalid`)))
	if out.Error == nil {
		t.Fatal("expected error for invalid config JSON")
	}
	if !strings.Contains(out.Error.Error(), "failed to parse configuration") {
		t.Errorf("unexpected error: %v", out.Error)
	}
}

func TestProcess_EmptyConstants(t *testing.T) {
	node := createConstantValueNode(t, "node-empty")
	rawCfg, _ := json.Marshal(constantvalue.Config{Constants: []constantvalue.ConstantEntry{}})
	out := node.Process(createProcessInput("node-empty", rawCfg))
	if out.Error == nil {
		t.Fatal("expected error for empty constants")
	}
}

func TestProcess_EmptyName(t *testing.T) {
	node := createConstantValueNode(t, "node-empty-name")
	cfg := constantvalue.Config{
		Constants: []constantvalue.ConstantEntry{
			{Name: "", Type: constantvalue.DataTypeString, Value: "x"},
		},
	}
	rawCfg, _ := json.Marshal(cfg)
	out := node.Process(createProcessInput("node-empty-name", rawCfg))
	if out.Error == nil {
		t.Fatal("expected error for empty name")
	}
}

func TestProcess_InvalidType(t *testing.T) {
	node := createConstantValueNode(t, "node-bad-type")
	cfg := constantvalue.Config{
		Constants: []constantvalue.ConstantEntry{
			{Name: "bad", Type: "unknown", Value: "x"},
		},
	}
	rawCfg, _ := json.Marshal(cfg)
	out := node.Process(createProcessInput("node-bad-type", rawCfg))
	if out.Error == nil {
		t.Fatal("expected error for invalid type")
	}
}
