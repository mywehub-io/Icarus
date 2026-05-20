package resolver

import (
	"strings"

	embeddedrt "github.com/wehubfusion/Icarus/pkg/embedded/runtime"
)

// EnsureFlatErrorSentinels stamps `nodeID-/error: false` and
// `nodeID-/errorDescription: ""` into flat when either key is absent for
// the given node IDs. Never overwrites an existing `nodeID-/error` key
// (preserves error:true from errorAsSuccess payloads).
func EnsureFlatErrorSentinels(flat map[string]interface{}, nodeIDs ...string) {
	for _, nodeID := range nodeIDs {
		errorKey := nodeID + "-/" + embeddedrt.ErrorOutputKeyError
		if _, exists := flat[errorKey]; !exists {
			flat[errorKey] = false
			descKey := nodeID + "-/" + embeddedrt.ErrorOutputKeyDescription
			if _, exists := flat[descKey]; !exists {
				flat[descKey] = ""
			}
		}
	}
}

// EnsureFlatErrorSentinelsForAllNodes scans a flat StandardUnitOutput map for
// every distinct node ID prefix (pattern "nodeId-/...") and calls
// EnsureFlatErrorSentinels for each. This ensures cross-unit consumers never
// see absent error keys when an upstream node succeeds without producing an
// explicit /error field.
func EnsureFlatErrorSentinelsForAllNodes(flat map[string]interface{}) {
	if len(flat) == 0 {
		return
	}
	seen := make(map[string]bool)
	for key := range flat {
		if idx := strings.Index(key, "-/"); idx > 0 {
			seen[key[:idx]] = true
		}
	}
	for nodeID := range seen {
		EnsureFlatErrorSentinels(flat, nodeID)
	}
}
