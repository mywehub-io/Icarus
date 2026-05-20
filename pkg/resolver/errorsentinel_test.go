package resolver

import (
	"encoding/json"
	"testing"

	embeddedrt "github.com/wehubfusion/Icarus/pkg/embedded/runtime"
	"github.com/wehubfusion/Icarus/pkg/message"
)

// ── EnsureFlatErrorSentinels ──────────────────────────────────────────────────

func TestEnsureFlatErrorSentinels_StampsAbsentKeys(t *testing.T) {
	flat := map[string]interface{}{
		"nodeA-/data": "hello",
	}
	EnsureFlatErrorSentinels(flat, "nodeA")

	if v, ok := flat["nodeA-/error"]; !ok || v != false {
		t.Errorf("expected nodeA-/error == false, got %v (present=%v)", v, ok)
	}
	if v, ok := flat["nodeA-/errorDescription"]; !ok || v != "" {
		t.Errorf("expected nodeA-/errorDescription == \"\", got %v (present=%v)", v, ok)
	}
}

func TestEnsureFlatErrorSentinels_DoesNotOverwriteErrorTrue(t *testing.T) {
	flat := map[string]interface{}{
		"nodeA-/error":            true,
		"nodeA-/errorDescription": "something went wrong",
	}
	EnsureFlatErrorSentinels(flat, "nodeA")

	if v := flat["nodeA-/error"]; v != true {
		t.Errorf("existing error:true must not be overwritten, got %v", v)
	}
	if v := flat["nodeA-/errorDescription"]; v != "something went wrong" {
		t.Errorf("existing errorDescription must not be overwritten, got %v", v)
	}
}

func TestEnsureFlatErrorSentinels_MultipleNodes(t *testing.T) {
	flat := map[string]interface{}{
		"parent-/data":    "x",
		"embedded-/value": 42,
	}
	EnsureFlatErrorSentinels(flat, "parent", "embedded")

	for _, nodeID := range []string{"parent", "embedded"} {
		if v, ok := flat[nodeID+"-/error"]; !ok || v != false {
			t.Errorf("%s-/error expected false, got %v (present=%v)", nodeID, v, ok)
		}
		if v, ok := flat[nodeID+"-/errorDescription"]; !ok || v != "" {
			t.Errorf("%s-/errorDescription expected \"\", got %v (present=%v)", nodeID, v, ok)
		}
	}
}

// ── EnsureFlatErrorSentinelsForAllNodes ───────────────────────────────────────

func TestEnsureFlatErrorSentinelsForAllNodes_StampsAllNodes(t *testing.T) {
	flat := map[string]interface{}{
		"nodeA-/data":  "hello",
		"nodeB-/value": 99,
	}
	EnsureFlatErrorSentinelsForAllNodes(flat)

	for _, nodeID := range []string{"nodeA", "nodeB"} {
		if v, ok := flat[nodeID+"-/error"]; !ok || v != false {
			t.Errorf("%s-/error expected false, got %v (present=%v)", nodeID, v, ok)
		}
	}
}

func TestEnsureFlatErrorSentinelsForAllNodes_PreservesExistingError(t *testing.T) {
	flat := map[string]interface{}{
		"nodeA-/error":            true,
		"nodeA-/errorDescription": "boom",
		"nodeA-/data":             nil,
	}
	EnsureFlatErrorSentinelsForAllNodes(flat)

	if v := flat["nodeA-/error"]; v != true {
		t.Errorf("error:true must not be overwritten, got %v", v)
	}
}

func TestEnsureFlatErrorSentinelsForAllNodes_EmptyMap(t *testing.T) {
	flat := map[string]interface{}{}
	EnsureFlatErrorSentinelsForAllNodes(flat) // must not panic
	if len(flat) != 0 {
		t.Errorf("empty map should remain empty, got %v", flat)
	}
}

// ── Cross-unit resolver synthesises /error defaults at read time ──────────────

// TestBuildInputFromMappings_PluginErrorFalseViaFlatKey verifies that when the
// upstream flat output has no /error key (normal success path — the node never
// writes /error), a downstream standalone simple-condition node reading from the
// pluginError section still receives false for the error destination field.
// The synthesis happens in buildInputFromMappings for absent pluginError keys.
func TestBuildInputFromMappings_PluginErrorFalseViaFlatKey(t *testing.T) {
	// Success path: upstream node wrote data but did NOT write /error.
	sourceFlat := map[string]interface{}{
		"xlsx-node-/data": []interface{}{},
	}

	params := BuildInputParams{
		UnitNodeID: "condition-node",
		FieldMappings: []message.FieldMapping{
			{
				SourceNodeID:         "xlsx-node",
				SourceSectionId:      embeddedrt.SectionPluginError,
				SourceEndpoint:       "/" + embeddedrt.ErrorOutputKeyError,
				DestinationEndpoints: []string{"/joberror"},
				DataType:             "EVENT",
				IsEventTrigger:       false,
			},
		},
		SourceResults: map[string]*SourceResult{
			"xlsx-node": {
				NodeID:          "xlsx-node",
				Status:          "success",
				ProjectedFields: map[string]map[string]interface{}{},
				RawFlatKeys:     sourceFlat,
			},
		},
	}

	result, err := buildInputFromMappings(params)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var data map[string]interface{}
	if err := json.Unmarshal(result, &data); err != nil {
		t.Fatalf("output not valid JSON: %v", err)
	}
	if v, ok := data["joberror"]; !ok {
		t.Errorf("expected joberror key in output, got keys %v", data)
	} else if v != false {
		t.Errorf("expected joberror == false, got %v", v)
	}
}

// TestBuildInputFromMappings_PluginErrorTrueViaFlatKey verifies the failure path:
// when errorAsSuccess wrote error:true into the flat output, the downstream
// simple-condition node reads it as true (synthesis must not overwrite a real error).
func TestBuildInputFromMappings_PluginErrorTrueViaFlatKey(t *testing.T) {
	sourceFlat := map[string]interface{}{
		"blob-node-/error":            true,
		"blob-node-/errorDescription": "upload failed",
	}

	params := BuildInputParams{
		UnitNodeID: "condition-node",
		FieldMappings: []message.FieldMapping{
			{
				SourceNodeID:         "blob-node",
				SourceSectionId:      embeddedrt.SectionPluginError,
				SourceEndpoint:       "/" + embeddedrt.ErrorOutputKeyError,
				DestinationEndpoints: []string{"/persisterror"},
				DataType:             "EVENT",
				IsEventTrigger:       false,
			},
		},
		SourceResults: map[string]*SourceResult{
			"blob-node": {
				NodeID:          "blob-node",
				Status:          "success",
				ProjectedFields: map[string]map[string]interface{}{},
				RawFlatKeys:     sourceFlat,
			},
		},
	}

	result, err := buildInputFromMappings(params)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var data map[string]interface{}
	if err := json.Unmarshal(result, &data); err != nil {
		t.Fatalf("output not valid JSON: %v", err)
	}
	if v, ok := data["persisterror"]; !ok {
		t.Errorf("expected persisterror key in output, got keys %v", data)
	} else if v != true {
		t.Errorf("expected persisterror == true, got %v", v)
	}
}
