package runtime

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// collectResult holds the results from CollectResults for channel communication
type collectResult struct {
	results     []map[string]interface{}
	resultItems [][]map[string]interface{}
	err         error
}

// collectResultsWithContext collects results with context cancellation support
// It will return when all results are received, the channel is closed, or context is cancelled
func collectResultsWithContext(ctx context.Context, resultChan <-chan BatchResult, count int) ([]map[string]interface{}, [][]map[string]interface{}, error) {
	results := make([]map[string]interface{}, count)
	items := make([][]map[string]interface{}, count)
	var firstError error

	received := 0
	// Track which indices we've received to detect if we're stuck
	receivedIndices := make(map[int]bool)

	// Add a "no progress" timeout - adaptive based on batch size and concurrent load
	// For large batches or concurrent scenarios, allow more time before first result
	// Base timeout: 1 minute, but scale up significantly for large batches
	baseTimeout := 1 * time.Minute
	if count > 10000 {
		// For very large batches (10k+), allow up to 15 minutes before first result
		// This accounts for worker startup delays and heavy concurrent load
		baseTimeout = 15 * time.Minute
	} else if count > 1000 {
		// For medium batches (1k-10k), allow 10 minutes
		baseTimeout = 10 * time.Minute
	} else if count > 100 {
		// For small-medium batches (100-1k), allow 5 minutes
		baseTimeout = 5 * time.Minute
	}
	// After first result, use longer timeout (2 minutes) to detect stalls
	// Increased from 30s to handle slower processing under concurrent load
	initialTimeout := baseTimeout
	progressTimeout := 2 * time.Minute

	noProgressTimeout := time.NewTimer(initialTimeout)
	defer noProgressTimeout.Stop()
	lastResultTime := time.Now()
	firstResultReceived := false

	// Overall timeout - maximum time to wait for all results
	// Scale up significantly for large batches to account for processing time and concurrent load
	overallTimeoutDuration := 10 * time.Minute
	if count > 10000 {
		// For very large batches, allow up to 60 minutes total
		// This accounts for processing 10k+ items under heavy concurrent load
		overallTimeoutDuration = 60 * time.Minute
	} else if count > 1000 {
		// For medium batches, allow 30 minutes
		overallTimeoutDuration = 30 * time.Minute
	} else if count > 100 {
		// For small-medium batches, allow 20 minutes
		overallTimeoutDuration = 20 * time.Minute
	}
	overallTimeout := time.NewTimer(overallTimeoutDuration)
	defer overallTimeout.Stop()

	for {
		select {
		case <-ctx.Done():
			// Context cancelled - return partial results
			return results, items, fmt.Errorf("context cancelled during result collection: %w", ctx.Err())
		case <-overallTimeout.C:
			// Overall timeout - we've been waiting too long total
			return results, items, fmt.Errorf("overall timeout waiting for results: received %d/%d results", received, count)
		case <-noProgressTimeout.C:
			// No progress timeout - no results received within timeout period
			timeSinceLastResult := time.Since(lastResultTime)
			if received == 0 {
				return results, items, fmt.Errorf("no progress timeout: received 0/%d results, no results received within %v (workers may be resource constrained or blocked)", count, initialTimeout)
			}
			return results, items, fmt.Errorf("no progress timeout: received %d/%d results, last result %v ago", received, count, timeSinceLastResult)
		case result, ok := <-resultChan:
			if !ok {
				// Channel closed - return what we have
				if received < count {
					// We didn't get all results, but channel is closed
					return results, items, fmt.Errorf("result channel closed prematurely: received %d/%d results", received, count)
				}
				return results, items, firstError
			}
			if result.Error != nil && firstError == nil {
				firstError = result.Error
			}
			if result.Index >= 0 && result.Index < count {
				if result.Output != nil {
					results[result.Index] = result.Output
				}
				if len(result.Items) > 0 {
					items[result.Index] = result.Items
				}
				receivedIndices[result.Index] = true
			}
			received++
			lastResultTime = time.Now()
			// Reset no-progress timeout on each result
			// Use shorter timeout after first result is received
			if !noProgressTimeout.Stop() {
				<-noProgressTimeout.C
			}
			if !firstResultReceived {
				firstResultReceived = true
				// After first result, switch to shorter progress timeout
			}
			noProgressTimeout.Reset(progressTimeout)

			if received >= count {
				return results, items, firstError
			}
		}
	}
}

// EmbeddedProcessor handles processing of embedded nodes with concurrency support.
// Concurrency is controlled via an internal worker pool (no external limiter).
type EmbeddedProcessor struct {
	factory EmbeddedNodeFactory
	config  ProcessorConfig
	logger  Logger
}

// NewEmbeddedProcessor creates a new embedded processor with full configuration.
func NewEmbeddedProcessor(
	factory EmbeddedNodeFactory,
	config ProcessorConfig,
) *EmbeddedProcessor {
	config.Validate()

	logger := config.Logger
	if logger == nil {
		logger = &NoOpLogger{}
	}

	return &EmbeddedProcessor{
		factory: factory,
		config:  config,
		logger:  logger,
	}
}

// NewEmbeddedProcessorWithDefaults creates a processor with default configuration.
func NewEmbeddedProcessorWithDefaults(factory EmbeddedNodeFactory) *EmbeddedProcessor {
	return NewEmbeddedProcessor(factory, DefaultProcessorConfig())
}

// ProcessEmbeddedNodes processes all embedded nodes for a unit.
// It handles two iteration scenarios:
//
// 1. Parent-level iteration:
//   - Parent node outputs an array (e.g., {data: [...]})
//   - First embedded node has iterate:true from parent's array
//   - processWithConcurrency() handles parallel processing of array items
//   - Single contains parent's non-array fields (metadata, status, etc.)
//   - Array contains per-item results from all embedded nodes
//
// 2. Mid-flow iteration:
//   - Parent node outputs a single object
//   - Some embedded node mid-flow produces an array
//   - Later embedded node has iterate:true from that array
//   - processSingleObject() handles this via SubflowProcessor
//   - SubflowProcessor internally handles the per-item subflow execution
func (p *EmbeddedProcessor) ProcessEmbeddedNodes(
	ctx context.Context,
	parentOutput map[string]interface{},
	unit ExecutionUnit,
	priorUnitOutputs map[string]map[string]interface{},
) (StandardUnitOutput, EventOutputs, error) {
	p.logger.Debug("processing embedded nodes",
		Field{Key: "unit_id", Value: unit.NodeId},
		Field{Key: "unit_label", Value: unit.Label},
		Field{Key: "embedded_count", Value: len(unit.EmbeddedNodes)},
	)

	// Invariant: callers (plugins) must handle len(EmbeddedNodes)==0 themselves and never
	// invoke ProcessEmbeddedNodes in that case. Reaching here indicates a contract violation.
	if len(unit.EmbeddedNodes) == 0 {
		output, err := p.flattenParentOnly(parentOutput, unit.NodeId)
		if err != nil {
			return nil, nil, err
		}
		return output, make(EventOutputs), nil
	}

	// Analyze iteration context from field mappings
	iterCtx := p.analyzeIterationContext(unit)

	if iterCtx.IsArrayIteration {
		p.logger.Debug("processing with array iteration",
			Field{Key: "array_path", Value: iterCtx.ArrayPath},
		)
		output, err := p.processWithConcurrency(ctx, parentOutput, unit, iterCtx, priorUnitOutputs)
		if err != nil {
			return nil, nil, err
		}
		return output, extractEventOutputs(output, unit.NodeId), nil
	}

	p.logger.Debug("processing single object")
	output, err := p.processSingleObject(ctx, parentOutput, unit, priorUnitOutputs)
	if err != nil {
		return nil, nil, err
	}
	return output, extractEventOutputs(output, unit.NodeId), nil
}

// withImplicitNoErrorDefaults returns parentOutput unchanged when it already declares an
// `error` key (failure paths inject `{error: true, errorDescription: "..."}` themselves and
// must be preserved). Otherwise it returns a shallow copy with `error: false` and
// `errorDescription: ""` added so embedded nodes consuming /error from a parent's
// pluginError section see a deterministic "no error" event on the success path.
func withImplicitNoErrorDefaults(parentOutput map[string]interface{}) map[string]interface{} {
	if parentOutput == nil {
		return map[string]interface{}{
			ErrorOutputKeyError:       false,
			ErrorOutputKeyDescription: "",
		}
	}
	if _, hasErr := parentOutput[ErrorOutputKeyError]; hasErr {
		return parentOutput
	}
	enriched := make(map[string]interface{}, len(parentOutput)+2)
	for k, v := range parentOutput {
		enriched[k] = v
	}
	enriched[ErrorOutputKeyError] = false
	if _, hasDesc := enriched[ErrorOutputKeyDescription]; !hasDesc {
		enriched[ErrorOutputKeyDescription] = ""
	}
	return enriched
}

// extractEventOutputs keeps embedded-node outputs (and parent error flags) so
// schedulers can evaluate event triggers even when full unit output is blob-backed.
func extractEventOutputs(stdOut StandardUnitOutput, parentNodeID string) EventOutputs {
	events := make(EventOutputs)
	if len(stdOut) == 0 {
		return events
	}

	parentErrorKey := parentNodeID + "-/" + ErrorOutputKeyError
	parentErrorDescriptionKey := parentNodeID + "-/" + ErrorOutputKeyDescription

	for key, value := range stdOut {
		sepIdx := strings.Index(key, "-/")
		if sepIdx <= 0 {
			continue
		}
		sourceNodeID := key[:sepIdx]
		if sourceNodeID != parentNodeID {
			events[key] = value
			continue
		}
		if key == parentErrorKey || key == parentErrorDescriptionKey {
			events[key] = value
		}
	}
	return events
}

// analyzeIterationContext determines if parent-level array iteration is needed.
// This only detects iteration that starts from the parent node's output.
// Mid-flow iteration (where an embedded node produces an array) is handled
// separately by SubflowProcessor during processSingleObject().
//
// Returns IterationContext with:
// - IsArrayIteration: true if any embedded node has iterate:true from parent's array
// - ArrayPath: the path to the array in parent output (e.g., "data" from "/data//field")
func (p *EmbeddedProcessor) analyzeIterationContext(unit ExecutionUnit) IterationContext {
	ctx := IterationContext{}

	for _, node := range unit.EmbeddedNodes {
		for _, mapping := range node.FieldMappings {
			// Look for patterns like "/data//field" with iterate=true from parent
			// Note: "//field" (root-is-array) patterns are no longer supported
			if mapping.Iterate && mapping.SourceNodeId == unit.NodeId {
				endpoint := mapping.SourceEndpoint
				endpointTrimmed := strings.TrimPrefix(endpoint, "/")
				if strings.HasPrefix(endpointTrimmed, "//") {
					// Root-is-array (//field) is no longer supported — skip this mapping
					p.logger.Warn("root-level array iteration (//field) is no longer supported, skipping",
						Field{Key: "endpoint", Value: endpoint},
						Field{Key: "nodeId", Value: node.NodeId})
					continue
				}
				if idx := strings.Index(endpoint, "//"); idx > 0 {
					arrayPath := strings.TrimPrefix(endpoint[:idx], "/")
					if arrayPath != "" {
						ctx.IsArrayIteration = true
						ctx.ArrayPath = arrayPath
						return ctx
					}
				}
			}
		}
	}

	return ctx
}

// processSingleObject handles non-array parent output.
func (p *EmbeddedProcessor) processSingleObject(
	ctx context.Context,
	parentOutput map[string]interface{},
	unit ExecutionUnit,
	priorUnitOutputs map[string]map[string]interface{},
) (StandardUnitOutput, error) {
	// Check if parentOutput has transposed array fields (e.g., {name: ["alex", "jordan"], age: [30, 25]})
	// Note: $items reconstruction from transposed data is no longer supported since root-level arrays
	// are not allowed. The transposed data will be passed through as-is.
	enrichedParentOutput := withImplicitNoErrorDefaults(parentOutput)

	// Create subflow processor with concurrency support for mid-flow iteration
	subflowCfg := SubflowConfig{
		ParentNodeId:     unit.NodeId,
		NodeConfigs:      unit.EmbeddedNodes,
		Factory:          p.factory,
		ArrayPath:        "",
		Logger:           p.logger,
		WorkerPoolConfig: p.config.WorkerPool,
		Config:           p.config.NestedIteration,
		PriorUnitOutputs: priorUnitOutputs,
		WorkflowID:       unit.WorkflowId,
		RunID:            unit.RunId,
		ClientID:         unit.ClientId,
		ProjectID:        unit.ProjectId,
		LifecycleEmitter: p.config.LifecycleEmitter,
	}
	subflow, err := NewSubflowProcessor(subflowCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create subflow processor: %w", err)
	}

	// Process as single item with enriched parent data
	result := subflow.ProcessItem(ctx, BatchItem{
		Index: 0,
		Data:  enrichedParentOutput,
	})

	if result.Error != nil {
		return nil, result.Error
	}

	// Merge output and items into flat map.
	// If there are Items, their keys already include path-level indices; do not append [i] again.
	output := NewSingleOutput(result.Output)
	for _, item := range result.Items {
		for k, v := range item {
			output[k] = v
		}
	}

	// Parent is a single object. Emit it as flattened keys (object shape).
	flat := FlattenMap(enrichedParentOutput, unit.NodeId, "")
	FilterRootKeys(flat)
	for k, v := range flat {
		output[k] = v
	}
	return output, nil
}

// processWithConcurrency handles parent-level array iteration with worker pool.
// This is used when the parent node's output contains an array and embedded nodes
// have iterate:true mappings from that array.
//
// The function:
// 1. Extracts the array from parentOutput at iterCtx.ArrayPath
// 2. Preserves non-array fields from parent for the Single output
// 3. Creates a SubflowProcessor to run all embedded nodes for each item
// 4. Uses a worker pool for concurrent processing
// 5. Returns StandardUnitOutput as a flat map: shared keys plus per-item keys with [index] notation.
func (p *EmbeddedProcessor) processWithConcurrency(
	ctx context.Context,
	parentOutput map[string]interface{},
	unit ExecutionUnit,
	iterCtx IterationContext,
	priorUnitOutputs map[string]map[string]interface{},
) (StandardUnitOutput, error) {
	// Extract array from parent output
	arrayData, ok := parentOutput[iterCtx.ArrayPath]
	if !ok {
		return nil, fmt.Errorf("%w: path '%s' not found in parent output", ErrArrayNotFound, iterCtx.ArrayPath)
	}

	rawItems, ok := arrayData.([]interface{})
	if !ok {
		return nil, fmt.Errorf("%w: path '%s' is not an array", ErrNotAnArray, iterCtx.ArrayPath)
	}

	// Convert to map items
	items := make([]map[string]interface{}, 0, len(rawItems))
	for _, item := range rawItems {
		if m, ok := item.(map[string]interface{}); ok {
			items = append(items, m)
		}
	}

	if len(items) == 0 {
		p.logger.Debug("empty array, returning empty result")
		// Extract and flatten parent's non-array fields
		nonArrayFields := p.extractNonArrayFields(parentOutput, iterCtx.ArrayPath)
		flatSingle := FlattenMap(nonArrayFields, unit.NodeId, "")
		FilterRootKeys(flatSingle)
		return NewSingleOutput(flatSingle), nil
	}

	p.logger.Info("processing array items",
		Field{Key: "item_count", Value: len(items)},
		Field{Key: "workers", Value: p.config.WorkerPool.NumWorkers},
	)

	// Create subflow processor (implements ItemProcessor) with concurrency support
	subflowCfg := SubflowConfig{
		ParentNodeId:     unit.NodeId,
		NodeConfigs:      unit.EmbeddedNodes,
		Factory:          p.factory,
		ArrayPath:        iterCtx.ArrayPath,
		Logger:           p.logger,
		WorkerPoolConfig: p.config.WorkerPool,
		Config:           p.config.NestedIteration,
		PriorUnitOutputs: priorUnitOutputs,
		WorkflowID:       unit.WorkflowId,
		RunID:            unit.RunId,
		ClientID:         unit.ClientId,
		ProjectID:        unit.ProjectId,
		LifecycleEmitter: p.config.LifecycleEmitter,
	}
	subflow, err := NewSubflowProcessor(subflowCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create subflow processor: %w", err)
	}

	// Pre-initialize the IO accumulator for parent-level array iteration so that
	// each concurrent worker accumulates its per-item lifecycle events instead of
	// calling EmitNodeEnd once per row. Without this, all workers fire separate
	// NodeEnd events for the same embedded nodeId and Argus stores only the last
	// concurrent write — monitoring then shows a single (arbitrary) row instead of
	// all rows. A single aggregated event is emitted via flushAggregatedIterationIO
	// after all workers complete, matching the behaviour of mid-flow concurrent iteration.
	if p.config.LifecycleEmitter != nil {
		subflow.iterIOAccumulator = &iterationIOAccumulator{
			inputs:  make(map[string][]map[string]interface{}),
			outputs: make(map[string][]map[string]interface{}),
		}
		subflow.iterIOAccumulator.init(len(items))
		defer func() {
			subflow.flushAggregatedIterationIO(ctx)
			subflow.iterIOAccumulator = nil
		}()
	}

	// Create worker pool
	pool := NewWorkerPool(p.config.WorkerPool, subflow, p.logger)
	p.logger.Info("created worker pool", Field{Key: "num_workers", Value: p.config.WorkerPool.NumWorkers})

	// Start workers
	pool.Start(ctx)
	p.logger.Info("started worker pool")

	// Create batch items
	batchItems := CreateBatchItems(items)
	p.logger.Info("created batch items", Field{Key: "batch_count", Value: len(batchItems)})

	// Submit items and wait for completion in a goroutine
	// Use a separate context for the submission goroutine to ensure it completes
	submitDone := make(chan struct{})
	resultChanClosed := make(chan struct{}) // Track if result channel is closed
	go func() {
		defer close(submitDone)
		p.logger.Info("submitting batch items to worker pool")
		pool.SubmitAll(ctx, batchItems)
		p.logger.Info("submitted all batch items, waiting for workers to complete")
		// Wait for all workers to finish and close the result channel
		// This must happen after SubmitAll closes the job channel
		// Use a timeout to prevent indefinite blocking
		waitDone := make(chan struct{})
		go func() {
			defer close(waitDone)
			pool.Wait()
			close(resultChanClosed) // Signal that result channel is now closed
		}()
		// Wait with timeout - if context is cancelled or workers are stuck, don't wait forever
		waitTimeout := 5 * time.Minute // Match the result collection timeout
		select {
		case <-waitDone:
			// Pool wait completed normally
			p.logger.Info("pool.Wait() completed normally")
			p.logger.Info("pool.Wait() completed normally")
		case <-ctx.Done():
			// Context cancelled - workers should exit when they check context
			// Give them a short time to finish, then continue
			select {
			case <-waitDone:
			case <-time.After(5 * time.Second):
				// Workers didn't finish in time, but we continue anyway
				// Force close result channel if pool.Wait() is stuck
				select {
				case <-resultChanClosed:
					// Already closed
				default:
					// Force close by calling pool.Close() in a goroutine with timeout
					go func() {
						pool.Close()
					}()
				}
			}
		case <-time.After(waitTimeout):
			// Workers are taking too long - something is stuck
			p.logger.Warn("pool.Wait() timeout - workers may be stuck", Field{Key: "timeout", Value: waitTimeout})
			// Force close result channel if not already closed
			select {
			case <-resultChanClosed:
				// Already closed
			default:
				// Force close by calling pool.Close() in a goroutine
				// This will close the job channel and wait for workers, then close result channel
				go func() {
					pool.Close()
				}()
			}
		}
	}()

	// Collect results with context cancellation support
	// Use a select to handle both context cancellation and result collection
	p.logger.Info("starting result collection")
	resultsChan := make(chan collectResult, 1)
	collectDone := make(chan struct{})
	go func() {
		defer close(collectDone)
		p.logger.Info("collecting results from worker pool", Field{Key: "expected_count", Value: len(items)})
		collectedResults, collectedItems, collectErr := collectResultsWithContext(ctx, pool.Results(), len(items))
		p.logger.Info("finished collecting results",
			Field{Key: "results_count", Value: len(collectedResults)},
			Field{Key: "error", Value: collectErr != nil},
		)
		resultsChan <- collectResult{results: collectedResults, resultItems: collectedItems, err: collectErr}
	}()

	var results []map[string]interface{}
	var resultItems [][]map[string]interface{}
	var processErr error

	// Add overall timeout to prevent indefinite hanging
	// Use adaptive timeout based on batch size to match collectResultsWithContext
	overallTimeoutDuration := 10 * time.Minute
	if len(items) > 10000 {
		// For very large batches (10k+), allow up to 60 minutes
		overallTimeoutDuration = 60 * time.Minute
	} else if len(items) > 1000 {
		// For medium batches (1k-10k), allow 30 minutes
		overallTimeoutDuration = 30 * time.Minute
	} else if len(items) > 100 {
		// For small-medium batches (100-1k), allow 20 minutes
		overallTimeoutDuration = 20 * time.Minute
	}
	overallTimer := time.NewTimer(overallTimeoutDuration)
	defer overallTimer.Stop()
	p.logger.Info("waiting for results or timeout",
		Field{Key: "batch_size", Value: len(items)},
		Field{Key: "overall_timeout", Value: overallTimeoutDuration})

	select {
	case <-ctx.Done():
		// Context cancelled - close the pool and return error
		pool.Close()
		// Wait for both goroutines to finish with timeout
		select {
		case <-submitDone:
		case <-time.After(5 * time.Second):
		}
		select {
		case <-collectDone:
		case <-time.After(5 * time.Second):
		}
		return nil, fmt.Errorf("context cancelled during embedded node processing: %w", ctx.Err())
	case <-overallTimer.C:
		// Overall timeout - something is stuck, return error with partial results if available
		p.logger.Error("overall timeout reached - workflow is hanging",
			Field{Key: "timeout", Value: overallTimeoutDuration},
			Field{Key: "batch_size", Value: len(items)},
		)
		pool.Close()
		// Try to get partial results if available
		select {
		case result := <-resultsChan:
			results = result.results
			resultItems = result.resultItems
			if result.err != nil {
				processErr = fmt.Errorf("overall timeout after %v: %w", overallTimeoutDuration, result.err)
			} else {
				processErr = fmt.Errorf("overall timeout after %v: processing incomplete", overallTimeoutDuration)
			}
		default:
			// No results available yet
			processErr = fmt.Errorf("overall timeout after %v: no results received", overallTimeoutDuration)
		}
		// Wait for goroutines with timeout
		select {
		case <-submitDone:
		case <-time.After(2 * time.Second):
		}
		select {
		case <-collectDone:
		case <-time.After(2 * time.Second):
		}
		// Return partial results even on timeout so caller can see what was processed
		if processErr != nil {
			return nil, processErr
		}
	case result := <-resultsChan:
		p.logger.Info("received results from collection goroutine",
			Field{Key: "results_count", Value: len(result.results)},
			Field{Key: "has_error", Value: result.err != nil},
		)
		results = result.results
		resultItems = result.resultItems
		processErr = result.err
		// Ensure both goroutines complete with timeout
		select {
		case <-submitDone:
		case <-time.After(10 * time.Second):
			// Submit goroutine is taking too long, but continue anyway
		}
		select {
		case <-collectDone:
		case <-time.After(10 * time.Second):
			// Collect goroutine is taking too long, but continue anyway
		}
	}

	// Log stats
	processed, errors := pool.Stats()
	p.logger.Debug("processing complete",
		Field{Key: "processed", Value: processed},
		Field{Key: "errors", Value: errors},
	)

	if processErr != nil {
		return nil, processErr
	}

	// Build final items: if subflow produced per-item Items, prefer them; otherwise use results (per-item Output)
	finalItems := make([]map[string]interface{}, len(results))
	for i := range results {
		// merge resultItems[i] (which is []map[string]interface{}) into a single map if present
		if len(resultItems[i]) > 0 {
			// merge all maps in resultItems[i]
			merged := make(map[string]interface{})
			for _, m := range resultItems[i] {
				MergeMaps(merged, m)
			}
			// also merge results[i] on top
			MergeMaps(merged, results[i])
			finalItems[i] = merged
			continue
		}
		// fallback to results[i]
		finalItems[i] = results[i]
	}

	// Extract and flatten parent's non-array fields
	nonArrayFields := p.extractNonArrayFields(parentOutput, iterCtx.ArrayPath)
	flatShared := FlattenMap(nonArrayFields, unit.NodeId, "")
	FilterRootKeys(flatShared)

	output := NewSingleOutput(flatShared)

	var arrayKey string
	if iterCtx.ArrayPath == RootArrayKey {
		arrayKey = unit.NodeId + "-/" + RootArrayKey
	} else {
		arrayKey = unit.NodeId + "-/" + iterCtx.ArrayPath
	}

	// Unit output must be exactly a flat map of nodeId-/field keys (parent and embedded nodes treated the same).
	// Emit each node's output separately: parent array (no embedded fields) + per-embedded-node arrays.
	length := len(items)
	if len(finalItems) > length {
		length = len(finalItems)
	}
	parentOnlyArray := make([]interface{}, length)
	for i := 0; i < length; i++ {
		if i < len(items) && items[i] != nil {
			parentOnlyArray[i] = CloneMap(items[i])
		} else {
			parentOnlyArray[i] = map[string]interface{}{}
		}
	}
	output[arrayKey] = parentOnlyArray

	embedded := collectEmbeddedIndexedKeys(finalItems, unit.NodeId, iterCtx.ArrayPath, length)
	for k, v := range embedded {
		output[k] = v
	}
	// Emit aggregate keys (nodeId-/auth = [val0, val1, ...]) so field mapping with source "/auth"
	// finds fullPathKey "nodeId-/auth" and returns the array; resolver checks this before indexed keys.
	emitEmbeddedAggregateKeys(output, embedded, length)

	// Remove any raw embedded keys that still contain the parent array segment (e.g. nodeId-/data[0]/auth[0])
	// so the blob only has normalized keys (nodeId-/auth[0], nodeId-/auth[1], ...).
	scrubEmbeddedArrayPathKeys(output, unit.NodeId, iterCtx.ArrayPath, length)

	// Flatten array-valued keys into indexed scalar keys (nodeId-/auth[0], nodeId-/auth[1], ...)
	// so every row can be field-mapped; keep the main array key (parentId-/arrayPath) as-is.
	flattenArrayKeysInOutput(output, unit.NodeId, iterCtx.ArrayPath)

	return output, nil
}

// flattenParentOnly handles case with no embedded nodes.
// If parent has an array, emits one key nodeId-/arrayPath = full array plus any nodeId-/otherField.
// If parent is a single object, flattens to nodeId-/field keys without indices.
func (p *EmbeddedProcessor) flattenParentOnly(
	parentOutput map[string]interface{},
	parentNodeId string,
) (StandardUnitOutput, error) {
	// Check for array at top level
	for key, value := range parentOutput {
		if arr, ok := value.([]interface{}); ok {
			// Emit exactly nodeId-/field: one key for the full array, plus non-array fields.
			output := make(StandardUnitOutput)

			// Add non-array fields (nodeId-/otherField)
			nonArrayFields := p.extractNonArrayFields(parentOutput, key)
			flatShared := FlattenMap(nonArrayFields, parentNodeId, "")
			FilterRootKeys(flatShared)
			for k, v := range flatShared {
				output[k] = v
			}

			// Single key for the array: nodeId-/arrayPath = full array (no indexed keys)
			output[parentNodeId+"-/"+key] = arr
			return output, nil
		}
	}

	// Single object - flatten without indices
	flat := FlattenMap(parentOutput, parentNodeId, "")
	FilterRootKeys(flat)
	return NewSingleOutput(flat), nil
}

// extractNonArrayFields returns a copy of parentOutput without the specified array field.
// This preserves metadata and other shared fields when the parent outputs an array.
func (p *EmbeddedProcessor) extractNonArrayFields(parentOutput map[string]interface{}, arrayPath string) map[string]interface{} {
	result := make(map[string]interface{})
	for key, value := range parentOutput {
		if key == arrayPath {
			continue // Skip the array being iterated
		}
		result[key] = value
	}
	return result
}

// Config returns the processor configuration.
func (p *EmbeddedProcessor) Config() ProcessorConfig {
	return p.config
}

// Factory returns the node factory.
func (p *EmbeddedProcessor) Factory() EmbeddedNodeFactory {
	return p.factory
}

// emitEmbeddedAggregateKeys adds nodeId-/field = [val0, val1, ...] for each (nodeId, field)
// so field mapping with source "/auth" finds fullPathKey "nodeId-/auth" and returns the array.
func emitEmbeddedAggregateKeys(output StandardUnitOutput, embedded map[string]interface{}, length int) {
	// Group by (nodeId, baseField) -> values[i]
	type key struct{ nodeID, field string }
	arrays := make(map[key][]interface{})
	for k, v := range embedded {
		sep := strings.Index(k, "-/")
		if sep < 0 {
			continue
		}
		nodeID := k[:sep]
		pathPart := k[sep+2:]
		bracket := strings.IndexByte(pathPart, '[')
		if bracket <= 0 {
			continue
		}
		baseField := pathPart[:bracket]
		idxStr := pathPart[bracket+1:]
		closeB := strings.IndexByte(idxStr, ']')
		if closeB < 0 {
			continue
		}
		var idx int
		if _, err := fmt.Sscanf(idxStr[:closeB], "%d", &idx); err != nil || idx < 0 || idx >= length {
			continue
		}
		kk := key{nodeID, baseField}
		if arrays[kk] == nil {
			arrays[kk] = make([]interface{}, length)
		}
		arrays[kk][idx] = v
	}
	for kk, arr := range arrays {
		output[kk.nodeID+"-/"+kk.field] = arr
	}
}

// flattenArrayKeysInOutput replaces each array-valued key (except the main array key) with
// indexed scalar keys: nodeId-/auth = [v0,v1,...] becomes nodeId-/auth[0]=v0, nodeId-/auth[1]=v1, ...
// so every index can be field-mapped. The main array key (parentNodeId-/arrayPath) is left as-is.
func flattenArrayKeysInOutput(output StandardUnitOutput, parentNodeId, arrayPath string) {
	mainArrayKey := parentNodeId + "-/" + arrayPath
	var toFlatten []string
	for k, v := range output {
		if k == mainArrayKey {
			continue
		}
		if _, ok := v.([]interface{}); ok {
			toFlatten = append(toFlatten, k)
		}
	}
	for _, k := range toFlatten {
		arr, _ := output[k].([]interface{})
		delete(output, k)
		for i, v := range arr {
			output[k+fmt.Sprintf("[%d]", i)] = v
		}
	}
}

// scrubEmbeddedArrayPathKeys removes keys that still contain the parent array segment
// (e.g. nodeId-/data[0]/auth[0]) so the blob only has normalized keys (nodeId-/auth[0], ...).
// The array key (parentNodeId-/arrayPath) is left unchanged.
func scrubEmbeddedArrayPathKeys(output StandardUnitOutput, parentNodeId, arrayPath string, length int) {
	arrayKey := parentNodeId + "-/" + arrayPath
	segment := "-/" + arrayPath + "["
	var toDelete []string
	for key := range output {
		if key == arrayKey {
			continue
		}
		if strings.Contains(key, segment) {
			toDelete = append(toDelete, key)
		}
	}
	for _, k := range toDelete {
		delete(output, k)
	}
}

// baseFieldName returns the path with any array indices removed so we emit one key per
// (node, field, parentIndex). E.g. "auth[0]" -> "auth", "auth[0][10000]" -> "auth".
func baseFieldName(fieldPath string) string {
	if idx := strings.IndexByte(fieldPath, '['); idx > 0 {
		return fieldPath[:idx]
	}
	return fieldPath
}

// collectEmbeddedIndexedKeys extracts per-embedded-node output from finalItems and emits
// indexed flat keys (nodeId-/baseField[i] = scalar) so the blob looks like:
//
//	nodeId-/auth[0]=val0, nodeId-/auth[1]=val1, ...
//
// Keys from the subflow may look like nodeId-/data[0]/auth[0] or nodeId-/data[0]/auth[0][10000];
// we strip the parent array segment and normalize to base field name so the index is only
// the parent item index. When multiple keys map to the same blob key, we keep the value
// from the simpler path (fewer brackets, e.g. auth[0] over auth[0][10000]).
func collectEmbeddedIndexedKeys(
	finalItems []map[string]interface{},
	parentNodeId string,
	arrayPath string,
	length int,
) map[string]interface{} {
	out := make(map[string]interface{})
	brackets := make(map[string]int) // indexedKey -> bracket count of path we chose (lower = simpler)
	prefix := parentNodeId + "-/"
	indexSegmentBase := "/" + arrayPath + "["
	for i := 0; i < length; i++ {
		if i >= len(finalItems) || finalItems[i] == nil {
			continue
		}
		indexSegment := indexSegmentBase + fmt.Sprintf("%d]", i) + "/"
		// pathPart from FlattenMap has no leading slash (e.g. "data[0]/auth"), so match both forms
		indexSegmentNoLeading := arrayPath + fmt.Sprintf("[%d]", i) + "/"
		for key, value := range finalItems[i] {
			if strings.HasPrefix(key, prefix) {
				continue
			}
			sep := strings.Index(key, "-/")
			if sep < 0 {
				continue
			}
			nodeID := key[:sep]
			pathPart := key[sep+2:]
			var fieldPath string
			if idx := strings.Index(pathPart, indexSegment); idx >= 0 {
				fieldPath = strings.TrimPrefix(pathPart[idx+len(indexSegment):], "/")
			} else if idx := strings.Index(pathPart, indexSegmentNoLeading); idx >= 0 {
				fieldPath = strings.TrimPrefix(pathPart[idx+len(indexSegmentNoLeading):], "/")
			} else {
				fieldPath = strings.TrimPrefix(pathPart, "/")
			}
			if fieldPath == "" {
				continue
			}
			baseField := baseFieldName(fieldPath)
			if baseField == "" {
				continue
			}
			indexedKey := nodeID + "-/" + baseField + fmt.Sprintf("[%d]", i)
			n := strings.Count(fieldPath, "[")
			if prev, ok := brackets[indexedKey]; !ok || n < prev {
				out[indexedKey] = value
				brackets[indexedKey] = n
			}
		}
	}
	return out
}

// stripTrailingIndexFromPath removes a trailing [itemIndex] from the last path segment
// so merged row keys are plain (e.g. auth, result, true/date) for field mapping and CSV.
// Example: "auth[0]" with i=0 → "auth"; "true/date[0]" with i=0 → "true/date".
func stripTrailingIndexFromPath(fieldPath string, itemIndex int) string {
	if fieldPath == "" {
		return fieldPath
	}
	suffix := fmt.Sprintf("[%d]", itemIndex)
	lastSlash := strings.LastIndex(fieldPath, "/")
	var lastSeg string
	var prefix string
	if lastSlash >= 0 {
		prefix = fieldPath[:lastSlash+1]
		lastSeg = fieldPath[lastSlash+1:]
	} else {
		lastSeg = fieldPath
		prefix = ""
	}
	if strings.HasSuffix(lastSeg, suffix) {
		lastSeg = lastSeg[:len(lastSeg)-len(suffix)]
		return prefix + lastSeg
	}
	return fieldPath
}

// reconstructItemsFromTransposedData reconstructs a $items array from transposed raw data.
// This is similar to reconstructItemsFromTransposedOutput but works on raw parent data
// (without nodeId prefixes) before subflow processing.
//
// Example input (transposed):
//
//	{"name": ["alex", "jordan"], "age": [30, 25]}
//
// Example output (reconstructed array):
//
//	[{"name": "alex", "age": 30}, {"name": "jordan", "age": 25}]
func (p *EmbeddedProcessor) reconstructItemsFromTransposedData(
	data map[string]interface{},
) []interface{} {
	// First, find the array length by checking any array-valued field with primitive elements
	var arrayLength int
	for key, value := range data {
		// Skip $items if it already exists
		if key == RootArrayKey {
			continue
		}
		if arr, ok := value.([]interface{}); ok && len(arr) > 0 {
			// Check if this is a simple transposed array (array of primitives or simple objects)
			// vs a complex nested structure that shouldn't be transposed
			if isSimpleTransposedArray(arr) {
				arrayLength = len(arr)
				break
			}
		}
	}

	if arrayLength == 0 {
		return nil
	}

	// Build array of objects by transposing
	result := make([]interface{}, arrayLength)
	for i := 0; i < arrayLength; i++ {
		itemObj := make(map[string]interface{})

		for key, value := range data {
			// Skip $items key itself
			if key == RootArrayKey {
				continue
			}

			// Handle array-valued fields (transposed data)
			if arr, ok := value.([]interface{}); ok && len(arr) == arrayLength {
				// Only transpose if it's a simple transposed array
				if isSimpleTransposedArray(arr) {
					itemObj[key] = arr[i]
				} else {
					// Complex structure - keep as-is for the item
					itemObj[key] = arr[i]
				}
			} else if arr, ok := value.([]interface{}); ok && len(arr) != arrayLength {
				// Array length doesn't match - this might be a nested structure, keep as-is
				// but only for the first item (or distribute if we can figure out the mapping)
				if i == 0 {
					itemObj[key] = value
				}
			} else {
				// Non-array value - copy to all items
				itemObj[key] = value
			}
		}

		result[i] = itemObj
	}

	return result
}

// isSimpleTransposedArray checks if an array is a simple transposed array
// (array of primitives or simple single-key objects) vs a complex nested structure
func isSimpleTransposedArray(arr []interface{}) bool {
	if len(arr) == 0 {
		return false
	}

	// Check first element
	first := arr[0]

	// Primitive values - definitely transposed
	switch first.(type) {
	case string, int, int64, float64, bool, nil:
		return true
	}

	// Map - check if it's a simple single-key wrapper or complex nested structure
	if m, ok := first.(map[string]interface{}); ok {
		// If all elements are single-key maps with primitive values, it's transposed
		if len(m) == 1 {
			for _, v := range m {
				switch v.(type) {
				case string, int, int64, float64, bool, nil:
					return true
				}
			}
		}
		// Multi-key map or nested structure - not simple transposed
		return false
	}

	return true
}
