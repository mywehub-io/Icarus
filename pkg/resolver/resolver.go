package resolver

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"go.uber.org/zap"

	"github.com/wehubfusion/Icarus/pkg/archive"
	"github.com/wehubfusion/Icarus/pkg/message"
	"github.com/wehubfusion/Icarus/pkg/storage"
)

// DefaultMaxInlineBytes defines the default threshold (500KB) for inline payloads.
// This is set below NATS 1MB limit to leave room for message metadata and headers.
const DefaultMaxInlineBytes = 500 * 1024 // 500KB

// DefaultMaxConcurrentBlobDownloads bounds how many source blobs a single resolve
// pulls at once.
//
// Resolution previously started one goroutine per required file with no limit, so a
// unit fanning in from many blob-backed sources downloaded all of them simultaneously
// and held every raw payload until the last one landed. Peak memory was therefore the
// sum of every source payload, which is a live contributor to the OOM behaviour on the
// Elysium runner regardless of any format change.
const DefaultMaxConcurrentBlobDownloads = 4

// ResultMeta contains metadata required to build deterministic blob paths.
type ResultMeta struct {
	WorkflowID  string
	RunID       string
	NodeID      string
	ExecutionID string
}

// Result describes the outcome of CreateResult.
type Result struct {
	InlineData    []byte
	BlobReference *message.BlobReference
	UsedBlob      bool
}

// Service wraps blob-related helpers so plugins stay blob-agnostic.
type Service struct {
	blobClient         storage.BlobStorageClient
	maxInlineBytes     int
	maxConcurrentBlobs int
	logger             *zap.Logger
	// recordsMaterialiseMax caps building a records file in memory (see WithRecordsMaterialiseMax).
	recordsMaterialiseMax int64
}

// NewService builds a resolver service. If blobClient is nil, only inline resolution works.
func NewService(blobClient storage.BlobStorageClient, maxInlineBytes int) *Service {
	if maxInlineBytes <= 0 {
		maxInlineBytes = DefaultMaxInlineBytes
	}
	return &Service{
		blobClient:         blobClient,
		maxInlineBytes:     maxInlineBytes,
		maxConcurrentBlobs: DefaultMaxConcurrentBlobDownloads,
	}
}

// WithMaxConcurrentBlobDownloads bounds parallel source-blob downloads during one
// resolve. A non-positive value restores the default.
func (s *Service) WithMaxConcurrentBlobDownloads(n int) *Service {
	if n <= 0 {
		n = DefaultMaxConcurrentBlobDownloads
	}
	s.maxConcurrentBlobs = n
	return s
}

// WithLogger attaches a logger. Without one the resolver stays silent, which is what
// existing callers get.
func (s *Service) WithLogger(l *zap.Logger) *Service {
	s.logger = l
	return s
}

func (s *Service) blobDownloadLimit() int {
	if s.maxConcurrentBlobs <= 0 {
		return DefaultMaxConcurrentBlobDownloads
	}
	return s.maxConcurrentBlobs
}

// ResolveInput returns inline data or downloads it from blob storage when a reference is provided.
func (s *Service) ResolveInput(ctx context.Context, inline []byte, blobRef *message.BlobReference) ([]byte, error) {
	if len(inline) > 0 {
		return inline, nil
	}

	if blobRef == nil || blobRef.URL == "" {
		return nil, fmt.Errorf("resolver: no inline data or blob reference provided")
	}

	if s.blobClient == nil {
		return nil, fmt.Errorf("resolver: blob client not configured for blob reference resolution")
	}

	return s.downloadPayload(ctx, blobRef.URL)
}

// FieldMappingParams describes how resolver should apply field mappings.
type FieldMappingParams struct {
	FieldMappings    []message.FieldMapping
	SourceResults    map[string]*SourceResult
	TriggerData      []byte
	BlobSourceNodeID string
	ConsumerGraph    *ConsumerGraph // Optional: consumer graph for multi-blob downloads
}

// SourceResult contains the minimal data required to evaluate field mappings.
type SourceResult struct {
	NodeID            string
	Status            string
	ProjectedFields   map[string]map[string]interface{}
	IterationMetadata map[string]*IterationContext
	RawFlatKeys       map[string]interface{} // Direct access to flat keys from StandardUnitOutput format
}

// IterationContext mirrors array metadata used for coordinated iteration.
type IterationContext struct {
	IsArray     bool
	ArrayPath   string
	ArrayLength int
}

// ResolveMappedInput resolves the payload (inline or blob) and applies field mappings when provided.
// Priority: BlobRef > Inline
func (s *Service) ResolveMappedInput(
	ctx context.Context,
	inline []byte,
	blobRef *message.BlobReference,
	params *FieldMappingParams,
) ([]byte, error) {
	var base []byte
	var err error

	// Resolve input from blob reference or inline data
	if len(inline) > 0 || (blobRef != nil && blobRef.URL != "") {
		base, err = s.ResolveInput(ctx, inline, blobRef)
		if err != nil {
			return nil, err
		}
	}

	return s.buildInputFromFieldMappings(ctx, base, params)
}

// ResolveMappedInputWithConsumerGraph resolves input using consumer graph to download multiple blob files.
// This method handles cases where source nodes are in different blob files (e.g., Unit N-2, embedded nodes).
// If consumerGraph is nil, it falls back to ResolveMappedInput behavior.
func (s *Service) ResolveMappedInputWithConsumerGraph(
	ctx context.Context,
	inline []byte,
	blobRef *message.BlobReference,
	params *FieldMappingParams,
	consumerGraph *ConsumerGraph,
) ([]byte, error) {
	// If no consumer graph provided, fall back to standard behavior
	if consumerGraph == nil {
		return s.ResolveMappedInput(ctx, inline, blobRef, params)
	}

	// If we have inline data or blob ref, and no field mappings, use standard resolution
	if (len(inline) > 0 || (blobRef != nil && blobRef.URL != "")) && (params == nil || len(params.FieldMappings) == 0) {
		return s.ResolveMappedInput(ctx, inline, blobRef, params)
	}

	// If no field mappings but we have a consumer graph, try to use it
	if params == nil || len(params.FieldMappings) == 0 {
		// No field mappings - check if consumer graph has files we can use
		if len(consumerGraph.RequiredFiles) > 0 {
			// If there's exactly one file, use it directly
			if len(consumerGraph.RequiredFiles) == 1 {
				var requiredFile *RequiredBlobFile
				for _, file := range consumerGraph.RequiredFiles {
					requiredFile = file
					break
				}

				if requiredFile != nil && requiredFile.BlobURL != "" {
					if s.blobClient == nil {
						return nil, fmt.Errorf("resolver: blob client not configured")
					}

					data, err := s.downloadPayload(ctx, requiredFile.BlobURL)
					if err != nil {
						return nil, fmt.Errorf("resolver: failed to download blob file from consumer graph: %w", err)
					}
					return data, nil
				}
			}

			// Multiple files but no field mappings - can't determine which to use
			return nil, fmt.Errorf("resolver: consumer graph has %d files but no field mappings to determine which to use", len(consumerGraph.RequiredFiles))
		}

		// No files in consumer graph - fall back to standard resolution
		return s.ResolveMappedInput(ctx, inline, blobRef, params)
	}

	// Determine which blob files are needed
	requiredFiles := consumerGraph.DetermineRequiredFiles(params.FieldMappings)

	// Download and parse required blob files (if any)
	sourceResults := make(map[string]*SourceResult)
	if len(requiredFiles) > 0 {
		var err error
		sourceResults, err = s.downloadAndParseBlobFiles(ctx, requiredFiles, params.FieldMappings)
		if err != nil {
			return nil, fmt.Errorf("resolver: failed to download and parse blob files: %w", err)
		}
	}

	// Merge with any existing source results
	if params.SourceResults == nil {
		params.SourceResults = make(map[string]*SourceResult)
	}
	for nodeID, result := range sourceResults {
		params.SourceResults[nodeID] = result
	}

	// Check ResultLocations for nodes with inline data that aren't in source results yet
	if consumerGraph.ResultLocations != nil {
		// Extract source node IDs from field mappings
		sourceNodeIDs := make(map[string]bool)
		for _, mapping := range params.FieldMappings {
			if !mapping.IsEventTrigger && mapping.SourceNodeID != "" {
				sourceNodeIDs[mapping.SourceNodeID] = true
			}
		}

		for nodeID := range sourceNodeIDs {
			// Skip if we already have this node in source results
			if _, exists := params.SourceResults[nodeID]; exists {
				continue
			}

			// Check if this node has inline data in ResultLocations
			if location, exists := consumerGraph.ResultLocations[nodeID]; exists && location != nil {
				if location.HasInlineData && len(location.InlineData) > 0 {
					// Parse inline data
					var inlineData map[string]interface{}
					if err := json.Unmarshal(location.InlineData, &inlineData); err == nil {
						var nodeFields map[string]interface{}
						var rawFlatKeys map[string]interface{}

						// Check if this is StandardUnitOutput format (flat map with "nodeId-/" prefixed keys)
						isStandardUnitOutput := isStandardUnitOutputFormat(inlineData)

						if isStandardUnitOutput {
							// Store raw flat keys for direct extraction in field mapping
							rawFlatKeys = inlineData

							// Extract and restructure data from StandardUnitOutput format
							nodeFields = extractNodeDataFromStandardOutputFlat(inlineData, nodeID)
							// Handle empty extraction case
							if len(nodeFields) == 0 {
								if len(inlineData) == 0 {
									// Empty StandardUnitOutput - use inlineData as fallback
									nodeFields = inlineData
								} else {
									// Has data but no keys for this nodeID
									nodeFields = make(map[string]interface{})
								}
							}
						} else {
							// Not StandardUnitOutput - use as-is
							nodeFields = inlineData
						}

						// Create SourceResult with extracted/restructured data
						params.SourceResults[nodeID] = &SourceResult{
							NodeID:            nodeID,
							Status:            "success", // Assume success for inline data
							ProjectedFields:   map[string]map[string]interface{}{nodeID: nodeFields},
							IterationMetadata: nil,
							RawFlatKeys:       rawFlatKeys,
						}
					}
				}
			}
		}
	}

	// Build input using field mappings with all source results; records references become the
	// arrays they hold, for this (non-streaming) consumer.
	buildParams := BuildInputParams{
		UnitNodeID:    params.BlobSourceNodeID,
		FieldMappings: params.FieldMappings,
		SourceResults: params.SourceResults,
		TriggerData:   params.TriggerData,
	}
	return s.materialiseAndBuild(ctx, buildParams)
}

// downloadAndParseBlobFiles downloads multiple blob files in parallel and extracts SourceResults.
func (s *Service) downloadAndParseBlobFiles(
	ctx context.Context,
	requiredFiles []*RequiredBlobFile,
	fieldMappings []message.FieldMapping,
) (map[string]*SourceResult, error) {
	if s.blobClient == nil {
		return nil, fmt.Errorf("resolver: blob client not configured for multi-blob download")
	}

	// Extract all source node IDs we need
	sourceNodeIDs := make(map[string]bool)
	for _, mapping := range fieldMappings {
		if !mapping.IsEventTrigger && mapping.SourceNodeID != "" {
			sourceNodeIDs[mapping.SourceNodeID] = true
		}
	}

	// Download with bounded concurrency, and parse inside each worker so the raw
	// payload becomes garbage as soon as its source results are extracted. Holding
	// every raw blob until the last download finished doubled peak memory for no
	// benefit: the parsed form is what the caller needs, and the raw bytes were only
	// ever an intermediate.
	type downloadResult struct {
		file   *RequiredBlobFile
		parsed map[string]*SourceResult
		err    error
	}

	var wg sync.WaitGroup
	results := make([]downloadResult, len(requiredFiles))
	sem := make(chan struct{}, s.blobDownloadLimit())

	for i, file := range requiredFiles {
		wg.Add(1)
		go func(idx int, f *RequiredBlobFile) {
			defer wg.Done()

			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				results[idx] = downloadResult{file: f, err: ctx.Err()}
				return
			}

			parsed, err := s.fetchSourceResults(ctx, f, sourceNodeIDs, fieldMappings)
			if err != nil {
				results[idx] = downloadResult{file: f, err: err}
				return
			}
			results[idx] = downloadResult{file: f, parsed: parsed}
		}(i, file)
	}

	wg.Wait()

	// Merge in requiredFiles order. Where two files carry the same node, the later
	// file wins, which is what the previous sequential loop did — concurrency must not
	// be allowed to make that outcome depend on completion order.
	allSourceResults := make(map[string]*SourceResult)
	for _, result := range results {
		if result.err != nil {
			return nil, fmt.Errorf("resolver: failed to download blob file %s: %w", result.file.BlobURL, result.err)
		}
		for nodeID, sourceResult := range result.parsed {
			allSourceResults[nodeID] = sourceResult
		}
	}

	return allSourceResults, nil
}

// buildInputFromFieldMappings centralizes the logic for constructing unit inputs using field mappings.
func (s *Service) buildInputFromFieldMappings(
	ctx context.Context,
	base []byte,
	params *FieldMappingParams,
) ([]byte, error) {
	// If no field mappings, try to extract data from NodeExecutionResult format
	if params == nil || len(params.FieldMappings) == 0 {
		if len(base) > 0 {
			// Return blob data as-is when no field mappings
			return base, nil
		}
		// No field mappings - return empty object (don't fall back to trigger data)
		return []byte("{}"), nil
	}

	// Note: When using consumer graph, sourceResults should already be populated
	// from downloadAndParseBlobFiles. If not, we can't extract from base blob
	// without knowing which nodes are in the file (requires ContainsNodes).
	buildParams := BuildInputParams{
		UnitNodeID:    params.BlobSourceNodeID,
		FieldMappings: params.FieldMappings,
		SourceResults: params.SourceResults,
		TriggerData:   params.TriggerData,
	}
	return s.materialiseAndBuild(ctx, buildParams)
}

// sourceResultsFromContent builds SourceResults from a flat map.
//
// One file can hold several nodes, because embedded nodes are merged into the parent's
// output before the result is written; containsNodes names them, and a SourceResult is
// built for each that a mapping actually reads.
//
// The map arrives from the archive read path, assembled out of whatever entries the fetch
// planner selected rather than parsed from one document. That is precisely why the planner
// has to be conservative: extractNodeDataFromStandardOutputFlat below reconstructs a node
// from every key carrying its prefix, and silently produces less when given fewer.
func sourceResultsFromContent(blobContent map[string]interface{}, sourceNodeIDs map[string]bool, containsNodes []string) map[string]*SourceResult {
	if len(blobContent) == 0 || len(sourceNodeIDs) == 0 || len(containsNodes) == 0 {
		return nil
	}

	// Extract status if available (default to "success" if not found)
	status := "success"
	if s, ok := blobContent["status"].(string); ok && s != "" {
		status = s
	}

	// Check if blob is in StandardUnitOutput format (flat map with "nodeId-/" prefixed keys)
	isStandardUnitOutput := isStandardUnitOutputFormat(blobContent)

	// Build source results map for each node ID in containsNodes that matches sourceNodeIDs
	result := make(map[string]*SourceResult)
	for _, nodeID := range containsNodes {
		// Only create SourceResult for nodes that are in sourceNodeIDs
		if !sourceNodeIDs[nodeID] {
			continue
		}

		var nodeFields map[string]interface{}
		var rawFlatKeys map[string]interface{}

		if isStandardUnitOutput {
			// Store raw flat keys for direct extraction in field mapping
			rawFlatKeys = blobContent

			// Extract and restructure data from StandardUnitOutput format
			nodeFields = extractNodeDataFromStandardOutputFlat(blobContent, nodeID)
			// If extraction returned empty (no matching keys)
			if len(nodeFields) == 0 {
				if len(blobContent) == 0 {
					// Empty StandardUnitOutput - use blob content as fallback
					nodeFields = blobContent
				} else {
					// Has data but no keys for this nodeID
					nodeFields = make(map[string]interface{})
				}
			}
		} else {
			// Use blob content as-is (for other formats)
			nodeFields = blobContent
		}

		// Create SourceResult with the extracted/restructured data
		result[nodeID] = &SourceResult{
			NodeID:            nodeID,
			Status:            status,
			ProjectedFields:   map[string]map[string]interface{}{nodeID: nodeFields},
			IterationMetadata: nil,
			RawFlatKeys:       rawFlatKeys,
		}
	}

	if len(result) == 0 {
		return nil
	}

	return result
}

// isStandardUnitOutputFormat checks if a map is in StandardUnitOutput format
// by looking for keys with the pattern "nodeId-/path" or "nodeId-/path[index]"
func isStandardUnitOutputFormat(data map[string]interface{}) bool {
	// Check for at least one key with the StandardUnitOutput pattern
	for key := range data {
		// Look for keys with "-/" pattern (nodeId-/path format)
		if strings.Contains(key, "-/") {
			return true
		}
	}
	return false
}

// ExtractNodeDataFromFlatOutput is the canonical way to get per-node data from flat StandardUnitOutput.
// It extracts and restructures data for a given nodeID: keys like "nodeId-/path" or "nodeId-/path[0]"
// become nested structures and arrays. Consumers (e.g. Zeus) should use this instead of reimplementing
// flat-format parsing so that key-format or index-notation changes are handled in one place.
func ExtractNodeDataFromFlatOutput(flatOutput map[string]interface{}, nodeID string) map[string]interface{} {
	return extractNodeDataFromStandardOutputFlat(flatOutput, nodeID)
}

// BuildPriorUnitOutputsFromFlat converts flat StandardUnitOutput (keys like "nodeId-/path") into
// per-node outputs map[nodeID]output so a downstream unit's subflow can seed its store and reference
// prior unit node IDs in field mappings. Elysium embedded-unit-executor uses this when building
// priorUnitOutputs from ConsumerGraph ResultLocations.
func BuildPriorUnitOutputsFromFlat(flat map[string]interface{}) map[string]map[string]interface{} {
	if len(flat) == 0 {
		return nil
	}
	seen := make(map[string]bool)
	for k := range flat {
		if idx := strings.Index(k, "-/"); idx > 0 {
			seen[k[:idx]] = true
		}
	}
	if len(seen) == 0 {
		return nil
	}
	out := make(map[string]map[string]interface{}, len(seen))
	for nodeID := range seen {
		out[nodeID] = extractNodeDataFromStandardOutputFlat(flat, nodeID)
	}
	return out
}

// BuildPriorUnitOutputsFromConsumerGraph builds priorUnitOutputs from ConsumerGraph ResultLocations
// so a downstream unit's subflow can reference prior unit node IDs. For each ResultLocation with
// inline data, if the data is StandardUnitOutput flat form, extracts all node outputs; otherwise
// treats it as that single node's output.
func BuildPriorUnitOutputsFromConsumerGraph(cg *ConsumerGraph) map[string]map[string]interface{} {
	if cg == nil || cg.ResultLocations == nil {
		return nil
	}
	var prior map[string]map[string]interface{}
	for nodeID, loc := range cg.ResultLocations {
		if loc == nil || !loc.HasInlineData || len(loc.InlineData) == 0 {
			continue
		}
		var data map[string]interface{}
		if err := json.Unmarshal(loc.InlineData, &data); err != nil {
			continue
		}
		if isStandardUnitOutputFormat(data) {
			flatPrior := BuildPriorUnitOutputsFromFlat(data)
			if prior == nil {
				prior = make(map[string]map[string]interface{})
			}
			for nid, out := range flatPrior {
				prior[nid] = out
			}
		} else {
			if prior == nil {
				prior = make(map[string]map[string]interface{})
			}
			prior[nodeID] = data
		}
	}
	return prior
}

// extractNodeDataFromStandardOutputFlat extracts and restructures data from flat StandardUnitOutput format.
// Converts flattened keys like "nodeId-/path" or "nodeId-/path[0]" to nested structure.
// When the only key for the node is "nodeId-/" (root key, path ""), returns that value as the node data
// (map as-is, or wrapped as map[""] for arrays) so stage-object and similar single-blob outputs are correct.
func extractNodeDataFromStandardOutputFlat(flatOutput map[string]interface{}, nodeID string) map[string]interface{} {
	result := make(map[string]interface{})
	prefix := nodeID + "-/"

	// Root key "nodeId-/" (path "") holds the whole payload for single-output nodes (e.g. stage object).
	var rootValue interface{}

	// First pass: collect all keys and determine array sizes to pre-allocate
	arraySizes := make(map[string]int)
	keysToProcess := make([]struct {
		path  string
		value interface{}
	}, 0, len(flatOutput)/2)

	for key, value := range flatOutput {
		if !strings.HasPrefix(key, prefix) {
			continue
		}

		pathWithIndex := key[len(prefix):]
		if pathWithIndex == "" {
			rootValue = value
			continue
		}

		keysToProcess = append(keysToProcess, struct {
			path  string
			value interface{}
		}{pathWithIndex, value})

		// Detect array fields and track max index for pre-allocation (optimized)
		bracketIdx := -1
		for i := 0; i < len(pathWithIndex); i++ {
			if pathWithIndex[i] == '[' {
				bracketIdx = i
				break
			}
		}
		if bracketIdx > 0 {
			fieldName := pathWithIndex[:bracketIdx]
			// Find closing bracket
			closeBracketIdx := -1
			for i := bracketIdx + 1; i < len(pathWithIndex); i++ {
				if pathWithIndex[i] == ']' {
					closeBracketIdx = i
					break
				}
			}
			if closeBracketIdx > bracketIdx {
				// Parse index manually
				index := 0
				for i := bracketIdx + 1; i < closeBracketIdx; i++ {
					if pathWithIndex[i] < '0' || pathWithIndex[i] > '9' {
						index = -1
						break
					}
					index = index*10 + int(pathWithIndex[i]-'0')
				}
				if index >= 0 && index+1 > arraySizes[fieldName] {
					arraySizes[fieldName] = index + 1
				}
			}
		}
	}

	// Only root key "nodeId-/": return payload as node data (map as-is, or under "" for array/scalar).
	if rootValue != nil && len(keysToProcess) == 0 {
		if m, ok := rootValue.(map[string]interface{}); ok {
			return m
		}
		return map[string]interface{}{"": rootValue}
	}

	// Pre-allocate arrays to avoid O(n²) resizing
	for fieldName, size := range arraySizes {
		result[fieldName] = make([]interface{}, size)
	}

	// Second pass: set values using pre-allocated arrays
	for _, item := range keysToProcess {
		setValueWithIndices(result, item.path, item.value)
	}

	return result
}

// setValueWithIndices sets a value in a nested structure, handling array indices.
// Path format: "/data[0]/field" or "/field[0]" or "/field"
func setValueWithIndices(result map[string]interface{}, path string, value interface{}) {
	path = strings.TrimPrefix(path, "/")
	if path == "" {
		return
	}

	// Safety check: prevent infinite recursion
	if len(path) > 1000 {
		// Path too long, likely a bug
		return
	}

	// Find the first [index] notation (manual search - faster than strings.Index)
	bracketIdx := -1
	for i := 0; i < len(path); i++ {
		if path[i] == '[' {
			bracketIdx = i
			break
		}
	}

	if bracketIdx < 0 {
		// No index - simple nested path
		setNestedValue(result, "/"+path, value)
		return
	}

	// Safety: field name before bracket should not be empty
	if bracketIdx == 0 {
		return
	}

	// Extract the field name before the index
	fieldName := path[:bracketIdx]

	// Find the closing ] (manual search)
	closeBracketIdx := -1
	for i := bracketIdx + 1; i < len(path); i++ {
		if path[i] == ']' {
			closeBracketIdx = i
			break
		}
	}
	if closeBracketIdx < 0 {
		// Malformed - skip
		return
	}

	// Parse the index manually (faster than fmt.Sscanf)
	index := 0
	for i := bracketIdx + 1; i < closeBracketIdx; i++ {
		if path[i] < '0' || path[i] > '9' {
			return // Invalid index
		}
		index = index*10 + int(path[i]-'0')
	}

	// Get or create the array
	if result[fieldName] == nil {
		// Array wasn't pre-allocated, create it now (shouldn't happen often)
		result[fieldName] = make([]interface{}, index+1)
	}
	arr, ok := result[fieldName].([]interface{})
	if !ok {
		// Type conflict - overwrite
		result[fieldName] = make([]interface{}, index+1)
		arr = result[fieldName].([]interface{})
	}

	// Ensure array is large enough (only needed if pre-allocation missed this case)
	if index >= len(arr) {
		newArr := make([]interface{}, index+1)
		copy(newArr, arr)
		arr = newArr
		result[fieldName] = arr
	}

	// Check if there's more path after the index
	remainingPath := path[closeBracketIdx+1:]
	remainingPath = strings.TrimPrefix(remainingPath, "/")

	if remainingPath == "" {
		// No more path - set value directly
		arr[index] = value
		return
	}

	// Path like "data[0]/Actual_Termination_Date[0]" leaves remainingPath "Actual_Termination_Date[0]".
	// When that is a single segment "FieldName[index]" (redundant row index), set scalar on the item at arr[index].
	if strings.IndexByte(remainingPath, '/') < 0 {
		if idx := strings.Index(remainingPath, "["); idx > 0 {
			suffix := remainingPath[idx:]
			if len(suffix) >= 3 && suffix[len(suffix)-1] == ']' {
				// Ensure item at arr[index] is a map, then set scalar field on it
				if arr[index] == nil {
					arr[index] = make(map[string]interface{})
				}
				itemMap, ok := arr[index].(map[string]interface{})
				if !ok {
					itemMap = make(map[string]interface{})
					arr[index] = itemMap
				}
				itemMap[remainingPath[:idx]] = value
				return
			}
		}
	}

	// More path - recurse into object at this index
	if arr[index] == nil {
		arr[index] = make(map[string]interface{})
	}
	itemMap, ok := arr[index].(map[string]interface{})
	if !ok {
		itemMap = make(map[string]interface{})
		arr[index] = itemMap
	}
	setValueWithIndices(itemMap, remainingPath, value)
}

// setNestedValue sets a value in a nested map structure using a path like "/payload" or "/data/field".
// Creates intermediate maps as needed.
func setNestedValue(m map[string]interface{}, path string, value interface{}) {
	path = strings.TrimPrefix(path, "/")
	if path == "" {
		return
	}

	parts := strings.Split(path, "/")
	current := m

	for i, part := range parts {
		if part == "" {
			continue
		}

		if i == len(parts)-1 {
			// Last part - set the value
			current[part] = value
		} else {
			// Intermediate part - create nested map if needed
			if current[part] == nil {
				current[part] = make(map[string]interface{})
			}
			if nextMap, ok := current[part].(map[string]interface{}); ok {
				current = nextMap
			} else {
				// Path conflict - existing value is not a map, can't create nested structure
				// Overwrite with new map structure
				current[part] = make(map[string]interface{})
				current = current[part].(map[string]interface{})
			}
		}
	}
}

// CreateResult decides whether to return inline data or upload it to blob storage.
func (s *Service) CreateResult(ctx context.Context, data []byte, meta ResultMeta) (*Result, error) {
	if len(data) == 0 {
		return &Result{InlineData: data}, nil
	}

	if len(data) <= s.maxInlineBytes || s.blobClient == nil {
		return &Result{
			InlineData: data,
		}, nil
	}

	blobPath, metadata, err := resultBlobLocation(meta)
	if err != nil {
		return nil, err
	}

	// The inline decision above was made on the payload as given, before any format
	// choice, so an archive is only ever built on the blob branch. A payload sitting near
	// the threshold can never be pushed over it by the container's framing.
	//
	// Everything written here is an archive, and there is no branch left at this call
	// site. CreateResult is not only a node-output writer — Artemis's MLLP ingest offloads
	// a raw HL7 message through it and an HTTP trigger offloads whatever body arrived —
	// so BuildPayload decides between an addressable document archive and an opaque one
	// from the payload's own shape. Neither can fail on shape, which matters because the
	// trigger upload completes before the acknowledgement is written: a refusal here would
	// cost a message rather than a field.
	payload, stats, err := archive.BuildPayload(data)
	if err != nil {
		return nil, fmt.Errorf("resolver: failed to build result archive: %w", err)
	}

	blobURL, err := s.blobClient.UploadResult(ctx, blobPath, payload, metadata)
	if err != nil {
		return nil, fmt.Errorf("resolver: failed to upload result to blob: %w", err)
	}

	if s.logger != nil {
		s.logger.Info("result written to blob",
			zap.String("blob_url", blobURL),
			zap.Int("total_bytes", len(payload)),
			zap.Int("document_bytes", len(data)),
			zap.Bool("used_blob", true),
			// entry_count is what says whether the high-key-count shape is real in this
			// deployment. That shape costs heap on open and makes the archive larger than
			// the document it replaced; both are accepted rather than designed around, on
			// the basis that this number decides whether a remedy is ever needed.
			zap.Int("entry_count", stats.EntryCount),
			zap.Int("array_entry_count", stats.ArrayEntryCount),
			// An opaque write on a node-output path would mean the document failed the
			// flat-key shape test and silently lost selective fetch, so it is worth being
			// able to see the two apart in the logs.
			zap.Bool("opaque", stats.Opaque),
		)
	}

	return &Result{
		BlobReference: &message.BlobReference{
			URL: blobURL,
			// The size of what was actually written. Opening an archive over ranged reads
			// needs the blob's exact length, and a stale or wrong value here surfaces as a
			// hard 416 rather than a short read.
			SizeBytes: len(payload),
		},
		UsedBlob: true,
	}, nil
}

// resultBlobLocation is where a unit's result blob goes and the metadata it carries. Shared
// by CreateResult and CreateResultStream so both write to the same path for the same unit.
func resultBlobLocation(meta ResultMeta) (string, map[string]string, error) {
	if meta.WorkflowID == "" || meta.RunID == "" {
		return "", nil, fmt.Errorf("resolver: workflow metadata required for blob result")
	}

	nodeID := meta.NodeID
	if nodeID == "" {
		nodeID = "unknown-node"
	}

	execID := meta.ExecutionID
	if execID == "" {
		execID = nodeID
	}

	blobPath := fmt.Sprintf("results/%s/%s/%s%s",
		sanitizeBlobPathPart(meta.WorkflowID, "workflow"),
		sanitizeBlobPathPart(meta.RunID, "run"),
		sanitizeBlobPathPart(execID, "execution"),
		archive.Extension,
	)
	return blobPath, map[string]string{
		"workflow_id":  meta.WorkflowID,
		"run_id":       meta.RunID,
		"execution_id": execID,
		"node_id":      nodeID,
	}, nil
}

func sanitizeBlobPathPart(part string, fallback string) string {
	part = strings.TrimSpace(part)
	if part == "" {
		return fallback
	}
	part = strings.ReplaceAll(part, "/", "-")
	part = strings.ReplaceAll(part, "\\", "-")
	return part
}
