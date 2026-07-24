package runtime

import (
	"fmt"
	"strings"
)

// RootArrayKey is the reserved key for root-as-array. When input/root is an array,
// it is stored under this key only. Paths starting with "//" are equivalent to "/$items//...".
const RootArrayKey = "$items"

// ItemPlaceholderKey is the reserved trailing path segment meaning "the current
// array element itself" rather than a literal field name. It is equivalent to a
// bare trailing "//" (e.g. "/names//$item" behaves the same as "/names//") but
// can be used when a source endpoint needs an explicit terminal segment.
const ItemPlaceholderKey = "$item"

// NormalizeRootArrayEndpoint converts //field notation to /$items//field for root-array access.
// This ensures consistent key construction when the root of the input is an array.
// Examples:
//   - "//name" → "/$items//name"
//   - "///nested" → "/$items///nested" (preserves extra slashes)
//   - "/data//name" → "/data//name" (unchanged, not root-array)
//   - "/name" → "/name" (unchanged, no array traversal)
func NormalizeRootArrayEndpoint(endpoint string) string {
	// Check if path starts with // (root-is-array notation)
	// Trim at most one leading / then check for another /
	trimmed := strings.TrimPrefix(endpoint, "/")
	if strings.HasPrefix(trimmed, "/") {
		// This is //something pattern - normalize to /$items//something
		return "/" + RootArrayKey + "/" + trimmed
	}
	return endpoint
}

// FlattenMap flattens a nested map with nodeId prefix.
// Example: {"name": "david", "age": 19} with nodeId "abc" becomes:
// {"abc-/name": "david", "abc-/age": 19}
// Additionally stores the complete structure at root key "nodeId-/" when basePath is empty,
// enabling "" → "" field mappings to access the entire node output.
func FlattenMap(data map[string]interface{}, nodeId, basePath string) map[string]interface{} {
	result := make(map[string]interface{})

	// Store complete structure at root key when at base level
	// This enables "" → "" field mappings to access the entire node output
	if basePath == "" {
		rootKey := nodeId + "-/"
		result[rootKey] = data
	}

	for key, value := range data {
		var fullPath string
		if basePath == "" {
			fullPath = "/" + key
		} else {
			fullPath = basePath + "/" + key
		}

		flatKey := nodeId + "-" + fullPath

		switch v := value.(type) {
		case map[string]interface{}:
			// Recursively flatten nested objects
			nested := FlattenMap(v, nodeId, fullPath)
			for nk, nv := range nested {
				result[nk] = nv
			}
		default:
			result[flatKey] = value
		}
	}

	return result
}

// UnflattenMap reconstructs the original structure from flattened keys.
// This provides a fallback mechanism when the root key is not available.
//
// Example input (flattened):
//
//	"nodeId-/name": "alex"
//	"nodeId-/contact/email": "alex@example.com"
//
// Example output (reconstructed):
//
//	{"name": "alex", "contact": {"email": "alex@example.com"}}
func UnflattenMap(flatKeys map[string]interface{}, nodeId string) map[string]interface{} {
	prefix := nodeId + "-/"

	// First check if root key exists with complete structure
	if rootVal, exists := flatKeys[prefix]; exists {
		if rootMap, ok := rootVal.(map[string]interface{}); ok {
			return rootMap
		}
	}

	// Reconstruct from individual flattened keys
	result := make(map[string]interface{})

	for key, value := range flatKeys {
		if !strings.HasPrefix(key, prefix) {
			continue
		}

		// Skip keys with array indices - those represent iteration results
		if strings.Contains(key, "[") {
			continue
		}

		// Extract path after prefix (e.g., "name" or "contact/email")
		path := strings.TrimPrefix(key, prefix)
		if path == "" {
			continue
		}

		// Build nested structure from path
		SetNestedValue(result, path, value)
	}

	return result
}

// FlattenWithIndex flattens a map and adds an index suffix to all keys.
// Example: {"name": "david"} with index 0 becomes: {"nodeId-/name[0]": "david"}
func FlattenWithIndex(data map[string]interface{}, nodeId string, index int) map[string]interface{} {
	flat := FlattenMap(data, nodeId, "")
	result := make(map[string]interface{}, len(flat))

	for k, v := range flat {
		indexedKey := fmt.Sprintf("%s[%d]", k, index)
		result[indexedKey] = v
	}

	return result
}

// FlattenWithNestedIndices flattens a map and adds multiple index suffixes.
// Example: {"name": "david"} with indices [0, 1] becomes: {"nodeId-/name[0][1]": "david"}
func FlattenWithNestedIndices(data map[string]interface{}, nodeId string, indices []int) map[string]interface{} {
	flat := FlattenMap(data, nodeId, "")
	result := make(map[string]interface{}, len(flat))

	// Build index suffix like [0][1][2]
	var indexSuffix string
	for _, idx := range indices {
		indexSuffix += fmt.Sprintf("[%d]", idx)
	}

	for k, v := range flat {
		indexedKey := k + indexSuffix
		result[indexedKey] = v
	}

	return result
}

// GenerateOutputKey creates a standardized key for the flat output.
// Example: GenerateOutputKey("node-123", "name") -> "node-123-/name"
func GenerateOutputKey(nodeId, path string) string {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return nodeId + "-" + path
}

// BuildIndexedKey creates a key with index notation.
// Example: BuildIndexedKey("node-123", "/name", []int{0, 1}) -> "node-123-/name[0][1]"
func BuildIndexedKey(nodeId, path string, indices []int) string {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	key := nodeId + "-" + path
	for _, idx := range indices {
		key += fmt.Sprintf("[%d]", idx)
	}
	return key
}

// ParseIndexedKey parses a key to extract nodeId, path, and indices.
// Returns: nodeId, path, indices, error
// Example: "node-123-/name[0][1]" -> "node-123", "/name", [0, 1]
func ParseIndexedKey(key string) (nodeId, path string, indices []int, err error) {
	idx := strings.Index(key, "-/")
	if idx == -1 {
		return "", "", nil, fmt.Errorf("%w: invalid key format: %s", ErrInvalidInput, key)
	}

	nodeId = key[:idx]
	pathAndIndices := key[idx+1:] // includes leading /

	// Check for indices
	if bracketIdx := strings.Index(pathAndIndices, "["); bracketIdx >= 0 {
		path = pathAndIndices[:bracketIdx]

		// Parse all [n] patterns
		remaining := pathAndIndices[bracketIdx:]
		for {
			if len(remaining) == 0 || remaining[0] != '[' {
				break
			}

			endIdx := strings.Index(remaining, "]")
			if endIdx < 0 {
				break
			}

			var index int
			if _, parseErr := fmt.Sscanf(remaining[1:endIdx], "%d", &index); parseErr == nil {
				indices = append(indices, index)
			}

			remaining = remaining[endIdx+1:]
		}
	} else {
		path = pathAndIndices
	}

	return nodeId, path, indices, nil
}

// GetNestedValue gets a value from a nested map using a path.
// Path format: "field" or "nested/field"
func GetNestedValue(data map[string]interface{}, path string) interface{} {
	if path == "" {
		return data
	}

	parts := strings.Split(path, "/")
	current := interface{}(data)

	for _, part := range parts {
		if part == "" {
			continue
		}
		m, ok := current.(map[string]interface{})
		if !ok {
			return nil
		}
		current = m[part]
		if current == nil {
			return nil
		}
	}

	return current
}

// SetNestedValue sets a value at a nested path in the map.
// Creates intermediate maps as needed.
// When path is empty, merges the value (if it's a map) into data.
// When path starts with "//", treats as root-array: sets data[RootArrayKey] as array of maps with the field.
func SetNestedValue(data map[string]interface{}, path string, value interface{}) {
	path = strings.TrimPrefix(path, "/")

	// Root-is-array: //name means set data[$items][i][name] = value[i]; merge into existing array if present
	if strings.HasPrefix(path, "//") {
		rest := path[2:]
		if rest == "" {
			return
		}
		existing, _ := data[RootArrayKey].([]interface{})
		if valueArray, ok := value.([]interface{}); ok && len(valueArray) > 0 {
			if len(existing) == len(valueArray) {
				for i, v := range valueArray {
					if m, ok := existing[i].(map[string]interface{}); ok {
						m[rest] = v
					}
				}
			} else {
				items := make([]interface{}, len(valueArray))
				for i, v := range valueArray {
					items[i] = map[string]interface{}{rest: v}
				}
				data[RootArrayKey] = items
			}
		} else {
			if len(existing) == 1 {
				if m, ok := existing[0].(map[string]interface{}); ok {
					m[rest] = value
				}
			} else {
				data[RootArrayKey] = []interface{}{map[string]interface{}{rest: value}}
			}
		}
		return
	}

	// Handle // in path (not at start) - use only the part after //
	if idx := strings.Index(path, "//"); idx >= 0 {
		path = path[idx+2:]
	}

	// Handle empty path - merge value into data root
	if path == "" {
		if valueMap, ok := value.(map[string]interface{}); ok {
			for k, v := range valueMap {
				data[k] = v
			}
		}
		return
	}

	parts := strings.Split(path, "/")
	current := data

	for i, part := range parts {
		if part == "" {
			continue
		}
		if i == len(parts)-1 {
			current[part] = value
		} else {
			if _, exists := current[part]; !exists {
				current[part] = make(map[string]interface{})
			}
			if next, ok := current[part].(map[string]interface{}); ok {
				current = next
			} else {
				return
			}
		}
	}
}

// ExtractArrayPath extracts the array path from a source endpoint.
// Example: "/data//name" -> "data", "name"
// Example: "//name" -> "$items", "name" (root-is-array)
func ExtractArrayPath(endpoint string) (arrayPath, fieldPath string, hasArray bool) {
	endpoint = strings.TrimPrefix(endpoint, "/")

	if strings.HasPrefix(endpoint, "//") {
		return RootArrayKey, endpoint[2:], true
	}
	if idx := strings.Index(endpoint, "//"); idx >= 0 {
		return endpoint[:idx], endpoint[idx+2:], true
	}

	return "", endpoint, false
}

// FilterRootKeys removes root keys (ending with "-/") from output.
// Root keys are used internally for "" → "" field mappings but shouldn't appear in final output.
func FilterRootKeys(output map[string]interface{}) {
	for key := range output {
		// Remove keys that end with "-/" (root keys with complete node structure)
		if strings.HasSuffix(key, "-/") {
			delete(output, key)
		}
	}
}

// ExtractFieldFromDestination extracts the final field name from a destination path.
// Example: "/data//deleted" -> "deleted"
// Example: "/field" -> "field"
func ExtractFieldFromDestination(dest string) string {
	dest = strings.TrimPrefix(dest, "/")

	if idx := strings.Index(dest, "//"); idx >= 0 {
		return dest[idx+2:]
	}

	parts := strings.Split(dest, "/")
	return parts[len(parts)-1]
}

// MergeMaps merges source into target, overwriting existing keys.
func MergeMaps(target, source map[string]interface{}) {
	for k, v := range source {
		target[k] = v
	}
}

// CloneMap creates a shallow copy of a map.
func CloneMap(source map[string]interface{}) map[string]interface{} {
	if source == nil {
		return nil
	}
	result := make(map[string]interface{}, len(source))
	for k, v := range source {
		result[k] = v
	}
	return result
}

// DeepCloneMap creates a deep copy of a map.
func DeepCloneMap(source map[string]interface{}) map[string]interface{} {
	if source == nil {
		return nil
	}
	result := make(map[string]interface{}, len(source))
	for k, v := range source {
		switch val := v.(type) {
		case map[string]interface{}:
			result[k] = DeepCloneMap(val)
		case []interface{}:
			result[k] = deepCloneSlice(val)
		default:
			result[k] = v
		}
	}
	return result
}

// deepCloneSlice creates a deep copy of a slice.
func deepCloneSlice(source []interface{}) []interface{} {
	if source == nil {
		return nil
	}
	result := make([]interface{}, len(source))
	for i, v := range source {
		switch val := v.(type) {
		case map[string]interface{}:
			result[i] = DeepCloneMap(val)
		case []interface{}:
			result[i] = deepCloneSlice(val)
		default:
			result[i] = v
		}
	}
	return result
}

// FlattenMapWithIndex flattens a nested map like FlattenMap but injects an index
// into the path before the field. This is used when flattening per-item outputs
// so shared output can reflect indexed items (e.g., "/0/name").
func FlattenMapWithIndex(data map[string]interface{}, nodeId, basePath string, index int) map[string]interface{} {
	result := make(map[string]interface{})

	for key, value := range data {
		var fullPath string
		// insert index into path
		if basePath == "" {
			fullPath = fmt.Sprintf("/%d/%s", index, key)
		} else {
			fullPath = basePath + fmt.Sprintf("/%d/%s", index, key)
		}

		flatKey := nodeId + "-" + fullPath

		switch v := value.(type) {
		case map[string]interface{}:
			// Recursively flatten nested objects
			nested := FlattenMapWithIndex(v, nodeId, fullPath, index)
			for nk, nv := range nested {
				result[nk] = nv
			}
		default:
			result[flatKey] = value
		}
	}

	return result
}

// ArrayPathSegment represents a segment in a nested array path
type ArrayPathSegment struct {
	Path    string // Field path (e.g., "data", "assignments")
	IsArray bool   // True if this segment has // notation
}

// ParseNestedArrayPath parses a path like "/data//assignments//topics//name"
// For root-array paths like "//name", it inserts $items as the first segment.
func ParseNestedArrayPath(path string) []ArrayPathSegment {
	if path == "" {
		return nil
	}

	// Check for root-array pattern BEFORE trimming
	// "//name" or "//" means root is array
	isRootArray := strings.HasPrefix(path, "//")

	path = strings.TrimPrefix(path, "/")
	if path == "" {
		return nil
	}

	var segments []ArrayPathSegment

	// If root is array, add $items as first segment
	if isRootArray {
		segments = append(segments, ArrayPathSegment{
			Path:    RootArrayKey,
			IsArray: true, // This is the root array
		})
		// Remove the leading / that remains after the first trim
		// "//name" -> "/name" after first trim, now remove the second /
		path = strings.TrimPrefix(path, "/")
		if path == "" {
			return segments
		}
	}

	// Split remaining path by //
	parts := strings.Split(path, "//")

	for i, part := range parts {
		if part == "" {
			continue
		}

		// Check for trailing slash - means iterate over this array (for primitive arrays)
		// e.g., "//chapters/" means iterate over chapters array and extract each element
		// vs "//chapters" which gives the whole array
		trailingSlash := strings.HasSuffix(part, "/")
		if trailingSlash {
			part = strings.TrimSuffix(part, "/")
		}

		seg := ArrayPathSegment{
			Path:    part,
			IsArray: i < len(parts)-1 || trailingSlash, // Array if not last OR has trailing slash
		}
		segments = append(segments, seg)
	}

	return segments
}
