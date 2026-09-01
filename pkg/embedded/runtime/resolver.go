package runtime

import (
	"fmt"
	"strings"
)

// DefaultOutputResolver is the default implementation of OutputResolver.
type DefaultOutputResolver struct{}

// NewOutputResolver creates a new output resolver.
func NewOutputResolver() *DefaultOutputResolver {
	return &DefaultOutputResolver{}
}

// ResolveValue finds a value in the output based on source mapping.
func (r *DefaultOutputResolver) ResolveValue(
	output StandardUnitOutput,
	sourceNodeId string,
	sourceEndpoint string,
) (interface{}, error) {
	// Normalize //field to /$items//field for root-array access
	normalizedEndpoint := NormalizeRootArrayEndpoint(sourceEndpoint)
	baseKey := sourceNodeId + "-" + normalizedEndpoint

	// Check for indexed values (iteration)
	var values []interface{}
	for i := 0; ; i++ {
		indexedKey := fmt.Sprintf("%s[%d]", baseKey, i)
		if val, exists := output[indexedKey]; exists {
			values = append(values, val)
		} else {
			break
		}
	}

	if len(values) > 0 {
		return values, nil
	}

	// Fallback to non-indexed key
	if val, exists := output[baseKey]; exists {
		return val, nil
	}

	return nil, fmt.Errorf("%w: %s not found in output", ErrKeyNotFound, baseKey)
}

// BuildInputForUnit builds the complete input for a unit based on its field mappings.
func (r *DefaultOutputResolver) BuildInputForUnit(
	previousOutput StandardUnitOutput,
	unit ExecutionUnit,
) (map[string]interface{}, error) {
	destStructure := r.analyzeDestinationStructure(unit.FieldMappings)

	if destStructure.HasArrayDest {
		return r.buildArrayInput(previousOutput, unit, destStructure)
	}

	return r.buildSingleInput(previousOutput, unit)
}

// analyzeDestinationStructure determines if the unit's input should be built as an array.
//
// A destination path's own "//" is still read as path shape when present — it names the array
// boundary precisely, so ArrayPath comes from it. But a mapping's resolved iterate flag is the
// actual authority on whether this unit fans out at all: a mapping can have Iterate:true with a
// destination path that carries no "//" of its own (graph-authored-cut phases/
// 05-elysium-icarus-cutover.md item 8's acceptance test), and that must still trigger the array
// path here — ArrayPath is left empty in that case; buildArrayInput's own "data" default applies.
func (r *DefaultOutputResolver) analyzeDestinationStructure(mappings []FieldMapping) DestinationStructure {
	ds := DestinationStructure{}

	for _, mapping := range mappings {
		for _, dest := range mapping.DestinationEndpoints {
			if strings.Contains(dest, "//") {
				ds.HasArrayDest = true
				// Normalize leading // to /$items// so root-array dest is consistent
				destNorm := dest
				if strings.HasPrefix(strings.TrimPrefix(dest, "/"), "//") {
					destNorm = "/" + RootArrayKey + dest
				}
				parts := strings.Split(strings.TrimPrefix(destNorm, "/"), "//")
				if len(parts) > 0 && parts[0] != "" {
					ds.ArrayPath = parts[0]
				}
				return ds
			}
		}
		if mapping.Iterate && !mapping.IsEvent() {
			ds.HasArrayDest = true
			return ds
		}
	}

	return ds
}

// buildSingleInput builds input as a single object.
func (r *DefaultOutputResolver) buildSingleInput(
	output StandardUnitOutput,
	unit ExecutionUnit,
) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	for _, mapping := range unit.FieldMappings {
		// Normalize source endpoint: //field -> /$items//field
		normalizedSourceEndpoint := NormalizeRootArrayEndpoint(mapping.SourceEndpoint)

		// Check for root-as-array with empty sourceEndpoint (pass-through case)
		if mapping.SourceNodeId != "" && mapping.SourceEndpoint == "" {
			itemsKey := mapping.SourceNodeId + "-/" + RootArrayKey
			if items, ok := output[itemsKey]; ok {
				// Pass the raw array directly to destination endpoints
				for _, dest := range mapping.DestinationEndpoints {
					if dest == "" {
						result["input"] = items
					} else {
						SetNestedValue(result, dest, items)
					}
				}
				continue
			}
		}

		// Handle empty source endpoint - pass entire node output
		// Supports all relationships: parent→embedded, embedded→embedded, embedded→parent
		if mapping.SourceNodeId != "" && (mapping.SourceEndpoint == "" || mapping.SourceEndpoint == "/") {
			// Use UnflattenMap to reconstruct complete source structure
			sourceStructure := UnflattenMap(output, mapping.SourceNodeId)

			if len(sourceStructure) > 0 {
				for _, dest := range mapping.DestinationEndpoints {
					if dest == "" || dest == "/" {
						// Destination is root - merge complete structure into result
						// Special case: if source has a single key containing an array,
						// and destination is root, place the array under $items for root-array handling
						if len(sourceStructure) == 1 {
							foundArray := false
							for _, v := range sourceStructure {
								if arr, isArr := v.([]interface{}); isArr {
									result[RootArrayKey] = arr
									foundArray = true
									break
								}
							}
							if foundArray {
								continue
							}
						}
						for k, v := range sourceStructure {
							result[k] = v
						}
					} else {
						// Destination is specific path - set complete structure there
						SetNestedValue(result, dest, sourceStructure)
					}
				}
				continue
			}
		}

		// Original skip logic for truly empty mappings
		if mapping.SourceNodeId == "" || normalizedSourceEndpoint == "" {
			continue
		}

		value, err := r.ResolveValue(output, mapping.SourceNodeId, normalizedSourceEndpoint)
		if err != nil {
			continue // Skip missing values
		}

		for _, dest := range mapping.DestinationEndpoints {
			SetNestedValue(result, dest, value)
		}
	}

	return result, nil
}

// buildArrayInput builds input with array structure.
func (r *DefaultOutputResolver) buildArrayInput(
	output StandardUnitOutput,
	unit ExecutionUnit,
	destStructure DestinationStructure,
) (map[string]interface{}, error) {
	// Check if there are indexed values to determine array length
	length := output.Len()
	if length == 1 && !output.HasIteration() {
		// No iteration - wrap single object into array
		single, err := r.buildSingleInput(output, unit)
		if err != nil {
			return nil, err
		}
		arrayPath := destStructure.ArrayPath
		if arrayPath == "" {
			arrayPath = "data"
		}
		return map[string]interface{}{
			arrayPath: []interface{}{single},
		}, nil
	}

	// Build array of objects matching indexed items
	resultArray := make([]map[string]interface{}, length)
	for i := range resultArray {
		resultArray[i] = make(map[string]interface{})
	}

	for _, mapping := range unit.FieldMappings {
		if mapping.SourceNodeId == "" || mapping.SourceEndpoint == "" {
			continue
		}

		// Normalize source endpoint: //field -> /$items//field
		normalizedEndpoint := NormalizeRootArrayEndpoint(mapping.SourceEndpoint)
		baseKey := mapping.SourceNodeId + "-" + normalizedEndpoint

		// Get value from each indexed key
		for i := 0; i < length; i++ {
			indexedKey := fmt.Sprintf("%s[%d]", baseKey, i)
			if val, exists := output[indexedKey]; exists {
				for _, dest := range mapping.DestinationEndpoints {
					fieldName := ExtractFieldFromDestination(dest)
					resultArray[i][fieldName] = val
				}
			}
		}
	}

	// Wrap in the array path
	arrayPath := destStructure.ArrayPath
	if arrayPath == "" {
		arrayPath = "data"
	}

	return map[string]interface{}{
		arrayPath: resultArray,
	}, nil
}

// GetAllKeysForNode returns all output keys belonging to a specific node.
func (r *DefaultOutputResolver) GetAllKeysForNode(output StandardUnitOutput, nodeId string) []string {
	prefix := nodeId + "-"
	var keys []string

	for key := range output {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}

	return keys
}

// GetNodeIdsInOutput returns all unique node IDs present in the output.
func (r *DefaultOutputResolver) GetNodeIdsInOutput(output StandardUnitOutput) []string {
	nodeIds := make(map[string]struct{})

	for key := range output {
		if idx := strings.Index(key, "-/"); idx > 0 {
			nodeIds[key[:idx]] = struct{}{}
		}
	}

	result := make([]string, 0, len(nodeIds))
	for id := range nodeIds {
		result = append(result, id)
	}
	return result
}

// Ensure DefaultOutputResolver implements OutputResolver
var _ OutputResolver = (*DefaultOutputResolver)(nil)
