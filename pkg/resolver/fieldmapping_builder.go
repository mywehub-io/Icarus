package resolver

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
	"github.com/wehubfusion/Icarus/pkg/message"
)

// hasFlatKeyData merges RawFlatKeys from all SourceResults into a single map.
// This ensures flat-key extraction works in multi-blob scenarios where different
// sources contribute different flat keys.
func hasFlatKeyData(sourceResults map[string]*SourceResult) map[string]interface{} {
	var merged map[string]interface{}
	for _, result := range sourceResults {
		if len(result.RawFlatKeys) > 0 {
			if merged == nil {
				merged = make(map[string]interface{}, len(result.RawFlatKeys))
			}
			for k, v := range result.RawFlatKeys {
				merged[k] = v
			}
		}
	}
	return merged
}

// keyInfo holds information about a flat key match
type keyInfo struct {
	fullKey string
	indices []int // Array indices extracted from path
	value   interface{}
}

// extractFromFlatKeys extracts values directly from flat-key format
// Returns extracted data matching the destination structure, or nil if not found
func extractFromFlatKeys(
	flatKeys map[string]interface{},
	sourceNodeID string,
	sourceEndpoint string,
	destEndpoint string,
	iterate bool,
) interface{} {
	prefix := sourceNodeID + "-/"

	// Handle empty source endpoint - extract entire node output
	if sourceEndpoint == "" || sourceEndpoint == "/" {
		// Look for root key "nodeId-/" which contains the full output
		rootKey := sourceNodeID + "-/"
		if rootValue, exists := flatKeys[rootKey]; exists {
			return rootValue
		}
		// Fall through to check for other patterns
	}

	// Root-is-array: //field notation means root-level array access — no longer supported
	trimmedEndpoint := strings.TrimPrefix(sourceEndpoint, "/")
	if strings.HasPrefix(trimmedEndpoint, "/") {
		return nil
	}

	normalizedEndpoint := sourceEndpoint

	// Handle collection traversal (/path//field): look up parent path, extract field from each element
	if strings.Contains(normalizedEndpoint, "//") {
		parts := strings.SplitN(normalizedEndpoint, "//", 2)
		if len(parts) == 2 {
			collectionPath := strings.Trim(parts[0], "/")
			fieldPath := strings.Trim(parts[1], "/")
			if collectionPath != "" && fieldPath != "" {
				collectionKey := prefix + collectionPath
				if collectionData, exists := flatKeys[collectionKey]; exists {
					if arr, ok := collectionData.([]interface{}); ok {
						result := make([]interface{}, len(arr))
						for i, item := range arr {
							if m, ok := item.(map[string]interface{}); ok {
								// If fieldPath has nested // (e.g. "assignments//title"),
								// use collection traversal instead of simple navigation
								if strings.Contains(fieldPath, "//") {
									result[i] = extractWithCollectionTraversal(m, fieldPath)
								} else {
									result[i] = navigateMap(m, fieldPath)
								}
							}
						}
						return result
					}
				}
			}
		}
	}

	// Check for complete nested structure stored at "nodeId-/path" (without indices)
	// This handles nodes like 71bf0d05 where entire arrays are stored
	fullPathKey := prefix + strings.TrimPrefix(normalizedEndpoint, "/")
	if fullPathData, exists := flatKeys[fullPathKey]; exists {
		// Found complete nested structure
		// If destination has collection traversal (//), extract from nested
		if strings.Contains(destEndpoint, "//") {
			return extractFromNestedStructure(fullPathData, normalizedEndpoint, destEndpoint)
		}
		// Otherwise return directly
		return fullPathData
	}

	// Collect matching flat keys with their indices
	var matches []keyInfo

	for key, value := range flatKeys {
		if !strings.HasPrefix(key, prefix) {
			continue
		}

		pathPart := key[len(prefix):]

		// Extract indices from path like "/data[0]/assignments[1]/details/topics[0]/courseName[0]"
		indices := extractArrayIndices(pathPart)

		if len(indices) > 0 {
			matches = append(matches, keyInfo{
				fullKey: key,
				indices: indices,
				value:   value,
			})
		}
	}

	if len(matches) == 0 {
		return nil
	}

	// Handle iterate flag: return just the values array
	if iterate {
		// Find max index to size the result array
		maxIdx := -1
		for _, match := range matches {
			if len(match.indices) > 0 && match.indices[0] > maxIdx {
				maxIdx = match.indices[0]
			}
		}

		if maxIdx < 0 {
			return nil
		}

		// Place values at their correct indices.
		// When multiple matches share the same first index (nested paths),
		// build a map from remaining indices instead of overwriting.
		result := make([]interface{}, maxIdx+1)
		for _, match := range matches {
			if len(match.indices) > 0 {
				idx := match.indices[0]
				if result[idx] == nil {
					result[idx] = match.value
				} else {
					// Conflict: same first index — merge into map if possible
					if existingMap, ok := result[idx].(map[string]interface{}); ok {
						if newMap, ok := match.value.(map[string]interface{}); ok {
							for k, v := range newMap {
								existingMap[k] = v
							}
						}
						// If new value is not a map, keep existing (first write wins)
					}
					// If existing is not a map, keep existing (first write wins)
				}
			}
		}
		return result
	}

	// Build nested structure based on destination path and indices
	destParts := parsePathSegments(destEndpoint)
	return buildStructureFromFlatKeys(matches, destParts)
}

// extractFromNestedStructure extracts values from a complete nested data structure
// Used when flat keys contain full nested arrays instead of indexed flat keys
// Example: "71bf0d05.../data" contains full array, need to extract "/data//chapters"
// When destEndpoint starts with "//", data is treated as the root array (e.g. from $items).
// Trailing slash (e.g., "//chapters/") means iterate over a primitive array and return elements directly.
func extractFromNestedStructure(data interface{}, sourceEndpoint string, destEndpoint string) interface{} {
	if !strings.Contains(destEndpoint, "//") {
		return data
	}

	destPathClean := strings.TrimPrefix(destEndpoint, "/")

	// Leading //: data IS the collection (root-as-array), rest is path in each item
	if strings.HasPrefix(destPathClean, "//") {
		collection, ok := data.([]interface{})
		if !ok {
			return nil
		}
		restPath := destPathClean[2:]

		// Check for trailing slash (primitive array iteration)
		hasTrailingSlash := strings.HasSuffix(restPath, "/")
		restPath = strings.TrimSuffix(restPath, "/")

		if strings.Contains(restPath, "//") {
			result := make([]interface{}, len(collection))
			for i, item := range collection {
				result[i] = extractFromNestedStructure(item, "", "/"+restPath)
			}
			return result
		}

		// If restPath is empty (from trailing slash like "///"), return collection as-is
		if restPath == "" {
			return collection
		}

		result := make([]interface{}, len(collection))
		for i, item := range collection {
			if itemMap, ok := item.(map[string]interface{}); ok {
				val := navigateMap(itemMap, restPath)
				// If result is an array and we have trailing slash, flatten it
				if hasTrailingSlash {
					if arr, ok := val.([]interface{}); ok {
						// Return the array elements directly
						result[i] = arr
					} else {
						result[i] = val
					}
				} else {
					result[i] = val
				}
			} else if hasTrailingSlash {
				// Primitive item with trailing slash - return as is
				result[i] = item
			}
		}
		return result
	}

	// Handle multi-level collection traversal recursively
	// "/data//assignments//title" means: for each item in data, for each assignment, get title
	sourcePathClean := strings.Trim(sourceEndpoint, "/")

	// Find the first // to split at
	firstDoubleSlash := strings.Index(destPathClean, "//")
	if firstDoubleSlash < 0 {
		return data
	}

	// Split into: collection path + rest
	collectionPath := destPathClean[:firstDoubleSlash]
	restPath := destPathClean[firstDoubleSlash+2:] // Skip the //

	// Navigate to the collection from current data
	var collection []interface{}

	if collectionPath == sourcePathClean || collectionPath == "" {
		// Data IS the collection
		var ok bool
		collection, ok = data.([]interface{})
		if !ok {
			return nil
		}
	} else {
		// Need to navigate to collection
		if dataMap, ok := data.(map[string]interface{}); ok {
			collectionVal := navigateMap(dataMap, collectionPath)
			collection, ok = collectionVal.([]interface{})
			if !ok {
				return nil
			}
		} else {
			return nil
		}
	}

	// Check if there are more // levels in restPath
	if strings.Contains(restPath, "//") {
		// Recursive case: more collection levels to traverse
		result := make([]interface{}, len(collection))
		for i, item := range collection {
			// Recursively extract from each item
			subResult := extractFromNestedStructure(item, "", "/"+restPath)
			result[i] = subResult
		}
		return result
	}

	// Base case: extract final field from each item
	result := make([]interface{}, len(collection))
	for i, item := range collection {
		if itemMap, ok := item.(map[string]interface{}); ok {
			result[i] = navigateMap(itemMap, restPath)
		}
	}

	return result
}

// extractArrayIndices parses indices from a flat key path
// "/data[0]/assignments[1]/details/topics[0]/courseName[0]" -> [0, 1, 0]
func extractArrayIndices(path string) []int {
	var indices []int
	segments := strings.Split(path, "/")

	for _, segment := range segments {
		if segment == "" {
			continue
		}

		// Find array index in segment like "field[123]"
		bracketIdx := strings.IndexByte(segment, '[')
		if bracketIdx > 0 {
			closeBracket := strings.IndexByte(segment, ']')
			if closeBracket > bracketIdx {
				idxStr := segment[bracketIdx+1 : closeBracket]
				if idx, err := strconv.Atoi(idxStr); err == nil {
					indices = append(indices, idx)
				}
			}
		}
	}

	return indices
}

// parsePathSegments splits a path into segments, preserving empty strings for //
// "/data//assignments//details/topics//courseName" -> ["data", "", "assignments", "", "details", "topics", "", "courseName"]
func parsePathSegments(path string) []string {
	path = strings.TrimPrefix(path, "/")
	return strings.Split(path, "/")
}

// buildStructureFromFlatKeys constructs the data structure that setFieldAtPath expects
// For a destination like "/data//nameUpper", this should return an array ["ALEX", "JORDAN"]
// For a destination like "/data//assignments//details/topics//courseUpper", this needs more complex nesting
func buildStructureFromFlatKeys(matches []keyInfo, destParts []string) interface{} {
	// Count array levels in destination (number of empty strings marking //)
	arrayLevels := 0
	arrayPositions := []int{} // Track positions of array markers
	for i, part := range destParts {
		if part == "" {
			arrayLevels++
			arrayPositions = append(arrayPositions, i)
		}
	}

	if arrayLevels == 0 {
		// No arrays - return single value
		if len(matches) > 0 {
			return matches[0].value
		}
		return nil
	}

	// For a single array level like "/data//nameUpper"
	// destParts = ["data", "", "nameUpper"]
	// We want to return an array indexed by the first index in matches
	if arrayLevels == 1 {
		// Build array indexed by first index from matches
		maxIdx := -1
		for _, match := range matches {
			if len(match.indices) > 0 && match.indices[0] > maxIdx {
				maxIdx = match.indices[0]
			}
		}

		if maxIdx < 0 {
			return nil
		}

		result := make([]interface{}, maxIdx+1)
		for _, match := range matches {
			if len(match.indices) > 0 {
				idx := match.indices[0]
				result[idx] = match.value
			}
		}
		return result
	}

	// For multiple array levels like "/data//assignments//details/topics//courseUpper"
	// We need to build nested structure
	// Group by first index
	indexGroups := make(map[int][]keyInfo)
	maxIdx := -1
	for _, match := range matches {
		if len(match.indices) > 0 {
			firstIdx := match.indices[0]
			if firstIdx > maxIdx {
				maxIdx = firstIdx
			}
			// Strip the first index for recursive call
			strippedMatch := keyInfo{
				fullKey: match.fullKey,
				indices: match.indices[1:],
				value:   match.value,
			}
			indexGroups[firstIdx] = append(indexGroups[firstIdx], strippedMatch)
		}
	}

	if maxIdx < 0 {
		return nil
	}

	// Build array with nested structures
	result := make([]interface{}, maxIdx+1)

	// Remove first array level from destParts for recursive processing
	// Find position of first "" and skip it plus the part before it
	firstArrayPos := arrayPositions[0]
	remainingParts := destParts[firstArrayPos+1:]

	for idx, group := range indexGroups {
		if len(group) == 0 {
			continue
		}

		// Recursively build structure for remaining parts
		if len(remainingParts) == 1 && remainingParts[0] != "" {
			// Simple field at this level - just use first match value
			result[idx] = group[0].value
		} else {
			// More complex structure - recurse
			subResult := buildStructureFromFlatKeys(group, remainingParts)
			result[idx] = subResult
		}
	}

	return result
}

// unwrapSingleFieldObject extracts values from single-key wrapper objects.
// Embedded processors wrap their output in single-key maps like {"name": "ALICE"}.
// This function strips that wrapper to get the inner value.
//
// Only ONE level of unwrapping is performed to avoid destroying legitimate
// nested structures. For example {"config": {"key": "val"}} -> {"key": "val"}
// (one level), NOT recursively to "val".
//
// When called on an array, each element is individually unwrapped (one level).
func unwrapSingleFieldObject(v interface{}) interface{} {
	// Handle arrays - unwrap each element
	if arr, isArr := v.([]interface{}); isArr {
		unwrapped := make([]interface{}, len(arr))
		for i, item := range arr {
			unwrapped[i] = unwrapSingleFieldObject(item)
		}
		return unwrapped
	}

	m, ok := v.(map[string]interface{})
	if !ok || len(m) != 1 {
		return v
	}

	// Single-key object - unwrap one level only (no recursion)
	for _, val := range m {
		return val
	}
	return v
}

// getMapKeys returns the keys of a map for debugging
func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// deepCopyValue creates a deep copy of a value to avoid aliasing
func deepCopyValue(v interface{}) interface{} {
	switch val := v.(type) {
	case map[string]interface{}:
		return deepCopyMap(val)
	case []interface{}:
		return deepCopyArray(val)
	default:
		return v
	}
}

// deepCopyMap creates a deep copy of a map
func deepCopyMap(m map[string]interface{}) map[string]interface{} {
	if m == nil {
		return nil
	}
	result := make(map[string]interface{}, len(m))
	for k, v := range m {
		result[k] = deepCopyValue(v)
	}
	return result
}

// deepCopyArray creates a deep copy of an array
func deepCopyArray(arr []interface{}) []interface{} {
	if arr == nil {
		return nil
	}
	result := make([]interface{}, len(arr))
	for i, v := range arr {
		result[i] = deepCopyValue(v)
	}
	return result
}

// getSourceNodeIDs returns the node IDs from source results map
func getSourceNodeIDs(sourceResults map[string]*SourceResult) []string {
	ids := make([]string, 0, len(sourceResults))
	for k := range sourceResults {
		ids = append(ids, k)
	}
	return ids
}

// BuildInputParams contains the data required to build unit input from field mappings.
type BuildInputParams struct {
	UnitNodeID    string
	FieldMappings []message.FieldMapping
	SourceResults map[string]*SourceResult
	TriggerData   []byte
}

func buildInputFromMappings(params BuildInputParams) ([]byte, error) {
	if len(params.FieldMappings) == 0 {
		if len(params.TriggerData) > 0 {
			return params.TriggerData, nil
		}
		return []byte("{}"), nil
	}

	inputData := make(map[string]interface{})

	// Check if we have flat-key data available for direct extraction
	flatKeyData := hasFlatKeyData(params.SourceResults)

	arraySources := make(map[string]int)
	collectionSources := make(map[string]interface{})

	type sourceInfo struct {
		count                int
		sourceCollectionPath string
	}
	collectionPathSources := make(map[string]map[string]*sourceInfo)

	for _, mapping := range params.FieldMappings {
		if mapping.IsEventTrigger {
			continue
		}

		var sourceCollectionPath string
		if strings.Contains(mapping.SourceEndpoint, "//") {
			parts := strings.SplitN(mapping.SourceEndpoint, "//", 2)
			if len(parts) == 2 {
				sourceCollectionPath = strings.Trim(parts[0], "/")
			}
		}

		for _, destEndpoint := range mapping.DestinationEndpoints {
			if strings.Contains(destEndpoint, "//") {
				parts := strings.SplitN(destEndpoint, "//", 2)
				if len(parts) == 2 {
					collectionPath := strings.Trim(parts[0], "/")
					// Empty collectionPath means root array destination (//fieldname) — no longer supported
					if collectionPath == "" {
						continue // skip: root-level array paths are not supported
					}
					if collectionPathSources[collectionPath] == nil {
						collectionPathSources[collectionPath] = make(map[string]*sourceInfo)
					}

					if info := collectionPathSources[collectionPath][mapping.SourceNodeID]; info == nil {
						collectionPathSources[collectionPath][mapping.SourceNodeID] = &sourceInfo{
							count:                1,
							sourceCollectionPath: sourceCollectionPath,
						}
					} else {
						info.count++
						if info.sourceCollectionPath == "" && sourceCollectionPath != "" {
							info.sourceCollectionPath = sourceCollectionPath
						}
					}
				}
			}
		}
	}

	for collectionPath, sourceInfos := range collectionPathSources {
		var primarySourceID string
		var primarySourceCollectionPath string
		maxCount := 0
		for sourceID, info := range sourceInfos {
			if info.count > maxCount || (info.count == maxCount && (primarySourceID == "" || sourceID < primarySourceID)) {
				maxCount = info.count
				primarySourceID = sourceID
				primarySourceCollectionPath = info.sourceCollectionPath
			}
		}

		if primarySourceID == "" {
			continue
		}

		var sourceResult *SourceResult
		if result, exists := params.SourceResults[primarySourceID]; exists {
			sourceResult = result
		} else {
			for _, potential := range params.SourceResults {
				if potential.ProjectedFields != nil {
					if _, hasNode := potential.ProjectedFields[primarySourceID]; hasNode {
						sourceResult = potential
						break
					}
				}
			}
		}

		if sourceResult != nil && sourceResult.ProjectedFields != nil {
			if nodeFields, hasNode := sourceResult.ProjectedFields[primarySourceID]; hasNode {
				sourceCollectionPath := primarySourceCollectionPath
				if sourceCollectionPath == "" {
					sourceCollectionPath = "data"
				}
				arrayField := extractFromPath(nodeFields, "/"+sourceCollectionPath)
				if arr, isArr := arrayField.([]interface{}); isArr {
					collectionSources[collectionPath] = arr
				}
			}
		}
	}

	for collectionPath, sourceArray := range collectionSources {
		// Deep-copy the source array to avoid aliasing with source ProjectedFields
		if arr, ok := sourceArray.([]interface{}); ok {
			copied := deepCopyArray(arr)
			setFieldAtPath(inputData, collectionPath, copied)
		} else {
			setFieldAtPath(inputData, collectionPath, sourceArray)
		}
	}

	// Track failures for detailed error messages
	failedMappings := make([]string, 0)

	// OPTIMIZATION: Group field mappings by source node and collection path for batch extraction
	type batchKey struct {
		sourceNodeID     string
		collectionPath   string
		sourceCollection string
	}
	batchedExtractions := make(map[batchKey][]struct {
		mapping   message.FieldMapping
		fieldPath string
	})

	for _, mapping := range params.FieldMappings {
		if mapping.IsEventTrigger {
			continue
		}

		// Check if this is a collection traversal mapping (has //)
		if strings.Contains(mapping.SourceEndpoint, "//") {
			parts := strings.SplitN(mapping.SourceEndpoint, "//", 2)
			if len(parts) == 2 {
				sourceCollectionPath := strings.Trim(parts[0], "/")
				fieldPath := strings.Trim(parts[1], "/")

				// Determine destination collection path
				for _, destEndpoint := range mapping.DestinationEndpoints {
					if strings.Contains(destEndpoint, "//") {
						destParts := strings.SplitN(destEndpoint, "//", 2)
						if len(destParts) == 2 {
							destCollectionPath := strings.Trim(destParts[0], "/")
							// Empty destCollectionPath means root array destination (//fieldname)
							if destCollectionPath == "" {
								destCollectionPath = runtime.RootArrayKey
							}
							key := batchKey{
								sourceNodeID:     mapping.SourceNodeID,
								collectionPath:   destCollectionPath,
								sourceCollection: sourceCollectionPath,
							}
							batchedExtractions[key] = append(batchedExtractions[key], struct {
								mapping   message.FieldMapping
								fieldPath string
							}{mapping, fieldPath})
							break
						}
					}
				}
			}
		}
	}

	// Detect and group mappings with simple destination paths (no //)
	// These will be converted to array-of-objects format
	type simpleDestKey struct {
		sourceNodeID     string
		sourceCollection string
	}
	simpleDestGroups := make(map[simpleDestKey][]struct {
		mapping   message.FieldMapping
		fieldPath string
		destPath  string
	})

	for _, mapping := range params.FieldMappings {
		if mapping.IsEventTrigger {
			continue
		}

		// Check if source has array traversal (//)
		if strings.Contains(mapping.SourceEndpoint, "//") {
			parts := strings.SplitN(mapping.SourceEndpoint, "//", 2)
			if len(parts) == 2 {
				sourceCollectionPath := strings.Trim(parts[0], "/")
				fieldPath := strings.Trim(parts[1], "/")

				// Check if ALL destination endpoints are simple paths (no //)
				allSimple := true
				for _, destEndpoint := range mapping.DestinationEndpoints {
					if strings.Contains(destEndpoint, "//") {
						allSimple = false
						break
					}
				}

				if allSimple && len(mapping.DestinationEndpoints) > 0 {
					key := simpleDestKey{
						sourceNodeID:     mapping.SourceNodeID,
						sourceCollection: sourceCollectionPath,
					}
					// Add all destination endpoints
					for _, destEndpoint := range mapping.DestinationEndpoints {
						simpleDestGroups[key] = append(simpleDestGroups[key], struct {
							mapping   message.FieldMapping
							fieldPath string
							destPath  string
						}{mapping, fieldPath, destEndpoint})
					}
				}
			}
		}
	}

	// Process simple destination groups to create array-of-objects
	type mappingKey struct {
		sourceNodeID   string
		sourceEndpoint string
	}
	batchProcessedMappings := make(map[mappingKey]bool)
	var arrayOfObjectsResult []map[string]interface{}
	var hasArrayOfObjects bool
	var referenceArrayLength int

	// Process all simple destination groups and merge them
	for key, group := range simpleDestGroups {
		if len(group) == 0 {
			continue
		}

		// Get source result
		var sourceResult *SourceResult
		if result, exists := params.SourceResults[key.sourceNodeID]; exists {
			sourceResult = result
		} else {
			for _, potential := range params.SourceResults {
				if potential.ProjectedFields != nil {
					if _, hasNode := potential.ProjectedFields[key.sourceNodeID]; hasNode {
						sourceResult = potential
						break
					}
				}
			}
		}

		if sourceResult == nil || sourceResult.ProjectedFields == nil {
			continue
		}

		// Resolve nodeFields using the same fallback logic as the main mapping loop.
		// In real executions, ProjectedFields might be keyed by the node ID, an empty
		// string, or contain a single entry with all fields. We need to handle all
		// of these cases so that array-of-objects detection works with real data.
		var nodeFields map[string]interface{}
		var hasNode bool

		if nodeFields, hasNode = sourceResult.ProjectedFields[key.sourceNodeID]; !hasNode {
			if sourceResult.NodeID == key.sourceNodeID {
				// The direct lookup already failed above, so try fallbacks
				if allFields, ok := sourceResult.ProjectedFields[""]; ok {
					nodeFields = allFields
					hasNode = true
				} else if len(sourceResult.ProjectedFields) == 1 {
					for _, fields := range sourceResult.ProjectedFields {
						nodeFields = fields
						hasNode = true
						break
					}
				}
			}
		}

		if !hasNode || nodeFields == nil {
			continue
		}

		// Get the source collection array
		// Try multiple possible paths: the specified collection path, "data", "$items", "result"
		sourceCollPath := key.sourceCollection
		if sourceCollPath == "" {
			sourceCollPath = "data"
		}

		var collection interface{}
		var collectionArray []interface{}
		var ok bool

		// Try the specified path first
		collection = navigateMap(nodeFields, sourceCollPath)
		if collection != nil {
			if arr, isArr := collection.([]interface{}); isArr {
				collectionArray = arr
				ok = true
			}
		}

		// Fallback to common paths if not found
		if !ok {
			for _, fallbackPath := range []string{"data", "result"} {
				if fallbackPath == sourceCollPath {
					continue // Already tried
				}
				collection = navigateMap(nodeFields, fallbackPath)
				if collection != nil {
					if arr, isArr := collection.([]interface{}); isArr {
						collectionArray = arr
						ok = true
						break
					}
				}
			}
		}

		if !ok {
			continue
		}

		arrayLen := len(collectionArray)
		if arrayLen == 0 {
			continue
		}

		// Initialize array-of-objects if this is the first group
		if !hasArrayOfObjects {
			arrayOfObjectsResult = make([]map[string]interface{}, arrayLen)
			for i := 0; i < arrayLen; i++ {
				arrayOfObjectsResult[i] = make(map[string]interface{})
			}
			hasArrayOfObjects = true
			referenceArrayLength = arrayLen
		} else if arrayLen != referenceArrayLength {
			// Array length mismatch - skip this group and let it be processed normally
			continue
		}

		// Extract all fields in one pass and merge into existing objects
		for i, item := range collectionArray {
			if itemMap, ok := item.(map[string]interface{}); ok {
				for _, g := range group {
					fieldValue := navigateMap(itemMap, g.fieldPath)
					// Remove leading slash from destination path
					destKey := strings.TrimPrefix(g.destPath, "/")
					arrayOfObjectsResult[i][destKey] = fieldValue
				}
			}
		}

		// Mark mappings as processed
		for _, g := range group {
			batchProcessedMappings[mappingKey{
				sourceNodeID:   g.mapping.SourceNodeID,
				sourceEndpoint: g.mapping.SourceEndpoint,
			}] = true
		}
	}

	// Process batched extractions for complex destinations (with //)

	for key, batch := range batchedExtractions {
		if len(batch) <= 1 {
			continue // No benefit from batching
		}

		// Get source result
		var sourceResult *SourceResult
		if result, exists := params.SourceResults[key.sourceNodeID]; exists {
			sourceResult = result
		}

		if sourceResult == nil || sourceResult.ProjectedFields == nil {
			continue
		}

		nodeFields, hasNode := sourceResult.ProjectedFields[key.sourceNodeID]
		if !hasNode {
			continue
		}

		// Get the source collection array
		sourceCollPath := key.sourceCollection
		if sourceCollPath == "" {
			sourceCollPath = "data"
		}
		collection := navigateMap(nodeFields, sourceCollPath)
		if collection == nil {
			continue
		}

		collectionArray, ok := collection.([]interface{})
		if !ok {
			continue
		}

		arrayLen := len(collectionArray)

		// Extract all fields in ONE pass
		fieldResults := make(map[string][]interface{})
		for _, b := range batch {
			fieldResults[b.fieldPath] = make([]interface{}, arrayLen)
		}

		for i, item := range collectionArray {
			if itemMap, ok := item.(map[string]interface{}); ok {
				for _, b := range batch {
					fieldValue := navigateMap(itemMap, b.fieldPath)
					fieldResults[b.fieldPath][i] = fieldValue
				}
			}
		}

		// Now set the results for each mapping and mark as processed
		// Write to ALL destination endpoints (both // and simple) so that
		// mixed-destination mappings don't silently lose simple destinations.
		for _, b := range batch {
			result := fieldResults[b.fieldPath]
			for _, destEndpoint := range b.mapping.DestinationEndpoints {
				setFieldAtPath(inputData, destEndpoint, result)
			}
			batchProcessedMappings[mappingKey{
				sourceNodeID:   b.mapping.SourceNodeID,
				sourceEndpoint: b.mapping.SourceEndpoint,
			}] = true
		}
	}

	for _, mapping := range params.FieldMappings {
		if mapping.IsEventTrigger {
			continue
		}

		// Skip if already processed in batch
		if batchProcessedMappings[mappingKey{
			sourceNodeID:   mapping.SourceNodeID,
			sourceEndpoint: mapping.SourceEndpoint,
		}] {
			continue
		}

		// Try flat-key extraction first if available
		if flatKeyData != nil && len(mapping.DestinationEndpoints) > 0 {
			result := extractFromFlatKeys(
				flatKeyData,
				mapping.SourceNodeID,
				mapping.SourceEndpoint,
				mapping.DestinationEndpoints[0],
				mapping.Iterate,
			)

			if result != nil {
				// Success - apply to all destination endpoints
				for _, destEndpoint := range mapping.DestinationEndpoints {
					setFieldAtPath(inputData, destEndpoint, result)
				}
				continue // Skip fallback
			}

			// The key is absent from the flat output. For pluginError section
			// mappings that is the normal success path — the upstream node
			// completed without writing /error — so synthesise the implicit
			// "no error" value rather than leaving the destination unset.
			if mapping.SourceSectionId == runtime.SectionPluginError {
				if def := pluginErrorSectionDefault(mapping.SourceEndpoint); def != nil {
					for _, destEndpoint := range mapping.DestinationEndpoints {
						setFieldAtPath(inputData, destEndpoint, def)
					}
					continue
				}
			}
		}

		var sourceResult *SourceResult

		if result, exists := params.SourceResults[mapping.SourceNodeID]; exists {
			sourceResult = result
		} else {
			for _, potential := range params.SourceResults {
				if potential.ProjectedFields != nil {
					if _, hasNode := potential.ProjectedFields[mapping.SourceNodeID]; hasNode {
						sourceResult = potential
						break
					}
				}
			}
		}

		if sourceResult == nil {
			failedMappings = append(failedMappings, fmt.Sprintf("source node '%s' not found in source results (available nodes: %v)",
				mapping.SourceNodeID, getSourceNodeIDs(params.SourceResults)))
			continue
		}

		if strings.ToLower(sourceResult.Status) != "success" {
			failedMappings = append(failedMappings, fmt.Sprintf("source node '%s' has status '%s' (expected 'success')",
				mapping.SourceNodeID, sourceResult.Status))
			continue
		}

		var sourceData interface{}
		var hasIterationContext bool
		var arrayLength int
		var arrayPath string
		normalizedSourceEndpoint := mapping.SourceEndpoint

		if sourceResult.ProjectedFields != nil {
			var nodeFields map[string]interface{}
			var hasNode bool

			if nodeFields, hasNode = sourceResult.ProjectedFields[mapping.SourceNodeID]; !hasNode {
				if sourceResult.NodeID == mapping.SourceNodeID {
					if selfFields, ok := sourceResult.ProjectedFields[mapping.SourceNodeID]; ok {
						nodeFields = selfFields
						hasNode = true
					} else if allFields, ok := sourceResult.ProjectedFields[""]; ok {
						nodeFields = allFields
						hasNode = true
					} else if len(sourceResult.ProjectedFields) == 1 {
						for _, fields := range sourceResult.ProjectedFields {
							nodeFields = fields
							hasNode = true
							break
						}
					}
				}
			}

			if hasNode && nodeFields != nil {
				if sourceResult.IterationMetadata != nil {
					if iterCtx, exists := sourceResult.IterationMetadata[mapping.SourceNodeID]; exists && iterCtx != nil {
						hasIterationContext = iterCtx.IsArray
						arrayLength = iterCtx.ArrayLength
						arrayPath = iterCtx.ArrayPath
						// Only use arrayPath shortcut if source endpoint doesn't have field traversal (//)
						// If it has //, we need to extract the specific field from each array item
						if arrayPath != "" && !strings.Contains(mapping.SourceEndpoint, "//") {
							sourceData = extractFromPath(nodeFields, arrayPath)
						}
					}
				}

				// Detect //field notation (root-array access) — no longer supported
				{
					trimmed := strings.TrimPrefix(mapping.SourceEndpoint, "/")
					if strings.HasPrefix(trimmed, "/") {
						// This is //field notation — root-level arrays are no longer supported
						// Let extraction proceed; the $items error guard below will catch it
						// if the data actually contains root-array content
					}
				}

				var usedFallbackExtraction bool

				if sourceData == nil {
					sourceData = extractFromPath(nodeFields, normalizedSourceEndpoint)

					// Fallback: If sourceData is nil and nodeFields has $items (root array output),
					// return error — root-level arrays are no longer supported
					if sourceData == nil {
						if _, hasItems := nodeFields[runtime.RootArrayKey]; hasItems {
							return nil, fmt.Errorf("root-level array data ($items) is not supported: root of input data must be an object, source node %s", mapping.SourceNodeID)
						}
					}

					// Fallback: If sourceData is nil, try to find the field inside /data or /result array
					// This handles embedded processors that output arrays stored under "data" key
					// when the field mapping expects /field but data is at /data//field
					if sourceData == nil {
						sourcePath := strings.Trim(normalizedSourceEndpoint, "/")
						if sourcePath != "" && !strings.Contains(sourcePath, "/") {
							// Simple field path like /auth - try to extract from /data array first
							dataArray := extractFromPath(nodeFields, "/data")
							if arr, isArr := dataArray.([]interface{}); isArr && len(arr) > 0 {
								// Check if first item has the field we're looking for
								if firstItem, ok := arr[0].(map[string]interface{}); ok {
									if _, hasField := firstItem[sourcePath]; hasField {
										traversalPath := "/data//" + sourcePath
										sourceData = extractFromPath(nodeFields, traversalPath)
										usedFallbackExtraction = true
									}
								}
							}

							// Try /result array if /data didn't work
							if sourceData == nil {
								resultArray := extractFromPath(nodeFields, "/result")
								if arr, isArr := resultArray.([]interface{}); isArr && len(arr) > 0 {
									if firstItem, ok := arr[0].(map[string]interface{}); ok {
										if _, hasField := firstItem[sourcePath]; hasField {
											traversalPath := "/result//" + sourcePath
											sourceData = extractFromPath(nodeFields, traversalPath)
											usedFallbackExtraction = true
										}
									}
								}
							}

							// Try $items array as well — root-level arrays are no longer supported,
							// so return error if found
							if sourceData == nil {
								itemsArray := extractFromPath(nodeFields, "/"+runtime.RootArrayKey)
								if itemsArray != nil {
									return nil, fmt.Errorf("root-level array data ($items) is not supported: root of input data must be an object, source node %s", mapping.SourceNodeID)
								}
							}
						} else if strings.Contains(normalizedSourceEndpoint, "//") {
							// Already a traversal path - try as-is
							sourceData = extractFromPath(nodeFields, normalizedSourceEndpoint)
						}
					}

					// Special case: If sourceData is an array of objects where each object has
					// a single field matching the sourcePath, extract those values.
					// This handles /data returning [{data:0}, {data:0}] when we want [0, 0]
					// ONLY applies when data came from a /data or /result fallback path,
					// to avoid destroying legitimate user data.
					if sourceData != nil && usedFallbackExtraction {
						sourcePath := strings.Trim(normalizedSourceEndpoint, "/")
						if arr, isArr := sourceData.([]interface{}); isArr && len(arr) > 0 {
							if firstItem, ok := arr[0].(map[string]interface{}); ok {
								// Check if items have a field matching the path we looked for
								if _, hasField := firstItem[sourcePath]; hasField {
									// Extract the nested field from each item
									extracted := make([]interface{}, len(arr))
									allExtracted := true
									for i, item := range arr {
										if itemMap, ok := item.(map[string]interface{}); ok {
											if v, exists := itemMap[sourcePath]; exists {
												extracted[i] = v
											} else {
												allExtracted = false
												break
											}
										} else {
											// Item is not a map, might be the value itself
											extracted[i] = item
										}
									}
									if allExtracted {
										sourceData = extracted
									}
								}
							}
						}
					}
				}
			} else {
				// Node fields not found
				availableNodeIDs := make([]string, 0, len(sourceResult.ProjectedFields))
				for k := range sourceResult.ProjectedFields {
					availableNodeIDs = append(availableNodeIDs, k)
				}
				failedMappings = append(failedMappings, fmt.Sprintf("source node '%s' has no projected fields (available node IDs in projected fields: %v)",
					mapping.SourceNodeID, availableNodeIDs))
			}
		} else {
			// Source result has no projected fields
			failedMappings = append(failedMappings, fmt.Sprintf("source node '%s' result has no projected fields",
				mapping.SourceNodeID))
		}

		if sourceData == nil {
			// The source field is absent from the upstream output — treat as an optional field
			// and skip silently. The plugin will fall back to its nodeConfig defaults
			// (e.g. an SFTP node whose path was not overridden will use its configured
			// default path). Only structural failures (source node not found, non-success
			// status) are recorded in failedMappings and cause a hard error.
			continue
		}

		// Unwrap arrays of single-field objects to arrays of scalar values.
		// This handles embedded processor outputs like [{"name": "ALEX"}] -> ["ALEX"].
		// Only one level of unwrapping is performed to avoid destroying nested structures.
		if arr, isArr := sourceData.([]interface{}); isArr && len(arr) > 0 {
			sourceData = unwrapSingleFieldObject(arr)
		}

		if mapping.Iterate && hasIterationContext && arrayLength > 0 {
			if !isArray(sourceData) {
				continue
			}
			if existingLength, exists := arraySources[mapping.SourceNodeID]; exists {
				if existingLength != arrayLength {
					return nil, fmt.Errorf("array length mismatch for source %s", mapping.SourceNodeID)
				}
			} else {
				arraySources[mapping.SourceNodeID] = arrayLength
			}
		} else if mapping.Iterate && isArray(sourceData) {
			// Legacy behaviour: allow iterate flag without metadata
		}

		for _, destEndpoint := range mapping.DestinationEndpoints {
			if destEndpoint == "" || destEndpoint == "/" {
				// Empty destination: merge at root
				if sourceMap, ok := sourceData.(map[string]interface{}); ok {
					for k, v := range sourceMap {
						inputData[k] = v
					}
				} else if sourceArray, ok := sourceData.([]interface{}); ok {
					// Root-level array destination is no longer supported
					_ = sourceArray
					return nil, fmt.Errorf("root-level array data is not supported as destination: root of input data must be an object")
				}
			} else {
				// Deep copy to prevent aliasing with source ProjectedFields
				setFieldAtPath(inputData, destEndpoint, deepCopyValue(sourceData))
			}
		}
	}

	if len(arraySources) > 1 {
		var expectedLength int
		var firstSource string
		mismatch := make([]string, 0)
		for sourceID, length := range arraySources {
			if firstSource == "" {
				firstSource = sourceID
				expectedLength = length
			} else if length != expectedLength {
				mismatch = append(mismatch,
					fmt.Sprintf("%s has length %d (expected %d from %s)",
						sourceID, length, expectedLength, firstSource))
			}
		}

		if len(mismatch) > 0 {
			return nil, fmt.Errorf("array length mismatch in iterate sources for unit %s: %s",
				params.UnitNodeID, strings.Join(mismatch, "; "))
		}
	}

	// If we have array-of-objects result, prioritize it and merge other mappings
	if hasArrayOfObjects {
		// Merge inputData fields into each object in arrayOfObjectsResult
		// For scalar values, repeat them for each object
		// For array values, distribute them across objects (should be same length)
		for i := range arrayOfObjectsResult {
			for k, v := range inputData {
				// If value is an array, take the i-th element
				if arr, ok := v.([]interface{}); ok {
					if i < len(arr) {
						arrayOfObjectsResult[i][k] = arr[i]
					} else {
						arrayOfObjectsResult[i][k] = nil
					}
				} else if vMap, ok := v.(map[string]interface{}); ok {
					// If value is a map, merge its fields into the object
					for mk, mv := range vMap {
						arrayOfObjectsResult[i][mk] = mv
					}
				} else {
					// Scalar value - repeat for each object
					arrayOfObjectsResult[i][k] = v
				}
			}
		}
		result, err := json.Marshal(arrayOfObjectsResult)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal array-of-objects: %w", err)
		}
		return result, nil
	}

	if len(inputData) == 0 {
		if len(failedMappings) > 0 {
			// Structural failures: source node not found or had a non-success status.
			return nil, fmt.Errorf("field mapping resolution failed: no data could be extracted. Failures: %s",
				strings.Join(failedMappings, "; "))
		}
		// inputData is empty but there are no structural failures. This happens when:
		//   - all source fields were absent from upstream output (optional fields), or
		//   - all mappings were event triggers, or
		//   - there were no mappings at all.
		// Return an empty object so the plugin can apply its nodeConfig defaults.
		return []byte("{}"), nil
	}

	result, err := json.Marshal(inputData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal mapped input: %w", err)
	}

	return result, nil
}

func setFieldAtPath(data map[string]interface{}, path string, value interface{}) {
	if path == "" {
		return
	}

	// Root-is-array notation (//field) is no longer supported
	// Check BEFORE trimming prefix to correctly detect double-slash
	if strings.HasPrefix(path, "//") {
		// Skip silently — root-level array paths are not supported
		return
	}

	path = strings.TrimPrefix(path, "/")
	// Also check after trimming one slash (e.g., "///foo" → "//foo" after trim)
	if strings.HasPrefix(path, "//") {
		// Skip silently — root-level array paths are not supported
		return
	}

	if path == "" {
		// Only merge at root when value is a map; no hardcoded key
		if valueMap, ok := value.(map[string]interface{}); ok {
			for k, v := range valueMap {
				data[k] = v
			}
		}
		return
	}

	if strings.Contains(path, "//") {
		setFieldAtPathWithCollectionTraversal(data, path, value)
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
			if nextMap, ok := current[part].(map[string]interface{}); ok {
				current = nextMap
			} else {
				return
			}
		}
	}
}

func setFieldAtPathWithCollectionTraversal(data map[string]interface{}, path string, value interface{}) {
	parts := strings.SplitN(path, "//", 2)
	if len(parts) != 2 {
		setFieldAtPath(data, path, value)
		return
	}

	collectionPath := strings.Trim(parts[0], "/")
	fieldPath := strings.Trim(parts[1], "/")

	if collectionPath == "" || fieldPath == "" {
		return
	}

	// Root-is-array: $items collection path is no longer supported
	if collectionPath == runtime.RootArrayKey {
		// Root-level array paths are not supported — skip silently
		return
	}

	collectionParts := strings.Split(collectionPath, "/")
	current := data

	for i, part := range collectionParts {
		if part == "" {
			continue
		}
		isLast := i == len(collectionParts)-1

		if _, exists := current[part]; !exists {
			// Collection doesn't exist - create it if we have array value to determine size
			if isLast {
				if valueArray, ok := value.([]interface{}); ok && len(valueArray) > 0 {
					items := make([]interface{}, len(valueArray))
					for j := range valueArray {
						items[j] = make(map[string]interface{})
					}
					current[part] = items
				} else {
					// Can't create collection without knowing size
					return
				}
			} else {
				// Intermediate path doesn't exist - create it
				current[part] = make(map[string]interface{})
			}
		}

		if isLast {
			if collection, ok := current[part].([]interface{}); ok {
				if len(collection) > 0 {
					setFieldInEachItem(collection, fieldPath, value)
				}
			}
			return
		}

		if nextMap, ok := current[part].(map[string]interface{}); ok {
			current = nextMap
		} else {
			return
		}
	}
}

func setFieldInEachItem(collection []interface{}, fieldPath string, value interface{}) {
	// Check if fieldPath contains nested array marker (//)
	// If so, we need to handle nested iteration
	if strings.Contains(fieldPath, "//") {
		parts := strings.SplitN(fieldPath, "//", 2)
		nestedCollectionPath := strings.Trim(parts[0], "/")
		nestedFieldPath := strings.Trim(parts[1], "/")

		if valueArray, ok := value.([]interface{}); ok && len(valueArray) == len(collection) {
			// Value is an array matching collection length - distribute to each item
			for i, item := range collection {
				if itemMap, ok := item.(map[string]interface{}); ok {
					itemValue := valueArray[i]

					// Navigate/create the path to the nested collection
					// nestedCollectionPath might be "assignments" or "details/topics"
					var nestedCollection []interface{}
					var collectionParent map[string]interface{}
					var collectionKey string

					if strings.Contains(nestedCollectionPath, "/") {
						// Path has multiple components like "details/topics"
						pathParts := strings.Split(nestedCollectionPath, "/")
						current := itemMap
						for j := 0; j < len(pathParts)-1; j++ {
							part := pathParts[j]
							if part == "" {
								continue
							}
							if _, exists := current[part]; !exists {
								current[part] = make(map[string]interface{})
							}
							if nextMap, ok := current[part].(map[string]interface{}); ok {
								current = nextMap
							} else {
								break
							}
						}
						collectionParent = current
						collectionKey = pathParts[len(pathParts)-1]
					} else {
						collectionParent = itemMap
						collectionKey = nestedCollectionPath
					}

					// Get existing collection or prepare to create it
					if existing, exists := collectionParent[collectionKey]; exists {
						if existingArr, ok := existing.([]interface{}); ok {
							nestedCollection = existingArr
						}
					}

					// If itemValue is an array, it contains values for each element in the nested collection
					if itemValueArray, isArr := itemValue.([]interface{}); isArr {
						// Ensure nested collection is large enough
						for len(nestedCollection) < len(itemValueArray) {
							nestedCollection = append(nestedCollection, make(map[string]interface{}))
						}
						collectionParent[collectionKey] = nestedCollection

						// Recursively set field in each nested item
						setFieldInEachItem(nestedCollection, nestedFieldPath, itemValueArray)
					} else {
						// Single value - set in first item of nested collection
						if len(nestedCollection) == 0 {
							nestedCollection = append(nestedCollection, make(map[string]interface{}))
							collectionParent[collectionKey] = nestedCollection
						}
						if nestedItemMap, ok := nestedCollection[0].(map[string]interface{}); ok {
							setFieldAtPath(nestedItemMap, nestedFieldPath, itemValue)
						}
					}
					collection[i] = itemMap
				}
			}
		}
		return
	}

	// Simple case - no nested arrays in fieldPath
	if valueArray, ok := value.([]interface{}); ok && len(valueArray) > 0 {
		if len(valueArray) != len(collection) {
			minLen := len(collection)
			if len(valueArray) < minLen {
				minLen = len(valueArray)
			}
			for i := 0; i < minLen; i++ {
				if itemMap, ok := collection[i].(map[string]interface{}); ok {
					// Unwrap single-field objects before setting — one level only
					unwrappedValue := unwrapSingleFieldObject(valueArray[i])
					setFieldAtPath(itemMap, fieldPath, unwrappedValue)
					collection[i] = itemMap
				}
			}
		} else {
			for i, item := range collection {
				if itemMap, ok := item.(map[string]interface{}); ok {
					// Unwrap single-field objects before setting — one level only
					unwrappedValue := unwrapSingleFieldObject(valueArray[i])
					setFieldAtPath(itemMap, fieldPath, unwrappedValue)
					collection[i] = itemMap
				}
			}
		}
	} else {
		for i, item := range collection {
			if itemMap, ok := item.(map[string]interface{}); ok {
				setFieldAtPath(itemMap, fieldPath, value)
				collection[i] = itemMap
			}
		}
	}
}

func isArray(value interface{}) bool {
	switch value.(type) {
	case []interface{}, []string, []int, []float64, []bool:
		return true
	default:
		return false
	}
}

func extractFromPath(fields map[string]interface{}, path string) interface{} {
	if fields == nil {
		return nil
	}

	originalPath := path
	path = strings.TrimPrefix(path, "/")
	if path == "" {
		return fields
	}

	if strings.Contains(path, "//") {
		return extractWithCollectionTraversal(fields, path)
	}

	// First, try direct key lookup (for projected fields stored with leading slash keys, e.g., "/payload")
	// This handles the case where the map key is exactly the path (with or without leading slash)
	if value, exists := fields[originalPath]; exists {
		return value
	}
	if originalPath != path {
		if value, exists := fields[path]; exists {
			return value
		}
	}

	// Special namespaces are accessed directly
	if strings.HasPrefix(path, "_meta") ||
		strings.HasPrefix(path, "_events") ||
		strings.HasPrefix(path, "_error") {
		return navigateMap(fields, path)
	}

	// Data is spread at root level by sourceResultsFromResultFile()
	// Field mappings like /data//hire_date find "data" directly at root
	return navigateMap(fields, path)
}

func extractWithCollectionTraversal(fields map[string]interface{}, path string) interface{} {
	// NOTE: Cache is disabled because it used path-only keys which caused
	// incorrect results when the same path was used with different data.
	// The cache is now cleared at the start of each buildInputFromMappings call,
	// but we still can't safely cache here because the same path may be used
	// with different source data within a single mapping operation.

	parts := strings.SplitN(path, "//", 2)
	if len(parts) != 2 {
		return extractFromPath(fields, path)
	}

	collectionPath := strings.Trim(parts[0], "/")
	fieldPath := strings.Trim(parts[1], "/")

	if collectionPath == "" || fieldPath == "" {
		return nil
	}

	// Data is spread at root level - access collection directly
	collection := navigateMap(fields, collectionPath)
	if collection == nil {
		return nil
	}

	collectionArray, ok := collection.([]interface{})
	if !ok {
		return nil
	}

	arrayLen := len(collectionArray)
	if arrayLen == 0 {
		return []interface{}{}
	}

	result := make([]interface{}, arrayLen)
	hasAnyItem := false

	for i, item := range collectionArray {
		if itemMap, ok := item.(map[string]interface{}); ok {
			hasAnyItem = true
			var fieldValue interface{}

			// Check if fieldPath contains another // (nested collection traversal)
			if strings.Contains(fieldPath, "//") {
				// Recursively extract from nested collection
				fieldValue = extractWithCollectionTraversal(itemMap, fieldPath)
			} else {
				// Simple field navigation
				fieldValue = navigateMap(itemMap, fieldPath)
			}

			// Always set the result, even if nil, to maintain array index alignment
			result[i] = fieldValue
		}
	}

	// Return nil only if no items were processable maps
	if !hasAnyItem {
		return nil
	}

	return result
}

func navigateMap(data map[string]interface{}, path string) interface{} {
	if data == nil {
		return nil
	}

	parts := strings.Split(path, "/")
	current := interface{}(data)

	for _, part := range parts {
		if part == "" {
			continue
		}

		if m, ok := current.(map[string]interface{}); ok {
			var exists bool
			current, exists = m[part]
			if !exists {
				return nil
			}
			continue
		}
		return nil
	}

	return current
}

// pluginErrorSectionDefault returns the implicit "no error" value for a given
// endpoint read from the pluginError section when the key is absent from the
// flat StandardUnitOutput. An absent key means the upstream node succeeded
// without explicitly writing /error, so /error defaults to false and
// /errorDescription defaults to "".
func pluginErrorSectionDefault(sourceEndpoint string) interface{} {
	endpoint := strings.TrimPrefix(sourceEndpoint, "/")
	switch endpoint {
	case runtime.ErrorOutputKeyError:
		return false
	case runtime.ErrorOutputKeyDescription:
		return ""
	}
	return nil
}
