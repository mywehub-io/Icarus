package resolver

import (
	"encoding/json"

	"github.com/wehubfusion/Icarus/pkg/message"
)

// RequiredBlobFile represents a blob file that needs to be downloaded
// to access results from one or more nodes
type RequiredBlobFile struct {
	ExecutionID   string   `json:"executionId"`   // Execution ID that created this blob file
	BlobURL       string   `json:"blobUrl"`       // URL to download the blob file
	BlobPath      string   `json:"blobPath"`      // Blob storage path (for reference)
	ContainsNodes []string `json:"containsNodes"` // List of node IDs whose results are in this file
	// SizeBytes is the archive's length, from the result message. When set, the resolver opens
	// the archive without a HEAD request first. Zero means unknown: the resolver asks.
	SizeBytes int64 `json:"sizeBytes,omitempty"`
}

// ResultLocation tracks where a node's result is stored and includes inline data if available
type ResultLocation struct {
	NodeID        string          `json:"nodeId"`        // The node ID
	ExecutionID   string          `json:"executionId"`   // Which execution ID/file contains this result
	StorageType   string          `json:"storageType"`   // "inline" or "blob"
	BlobURL       string          `json:"blobUrl"`       // If blob, the URL to download
	BlobPath      string          `json:"blobPath"`      // If blob, the path
	HasInlineData bool            `json:"hasInlineData"` // Whether inline result is available
	IsEmbedded    bool            `json:"isEmbedded"`    // Whether this is an embedded node
	InlineData    json.RawMessage `json:"inlineData"`     // Inline data if available (for nodes with inline results)
}

// ConsumerGraph tracks which blob files are needed to access results from source nodes
// It maps execution IDs to blob file information and tracks result locations
type ConsumerGraph struct {
	// RequiredFiles maps execution ID to the blob file information
	// This tells the resolver which files to download to access specific node results
	RequiredFiles map[string]*RequiredBlobFile `json:"requiredFiles"`

	// ResultLocations maps node ID to where its result is stored
	// Includes inline data for nodes with inline results
	ResultLocations map[string]*ResultLocation `json:"resultLocations,omitempty"`
}

// DetermineRequiredFiles extracts which blob files are needed based on field mappings
// and the consumer graph. Returns a list of RequiredBlobFile entries.
func (cg *ConsumerGraph) DetermineRequiredFiles(fieldMappings []message.FieldMapping) []*RequiredBlobFile {
	if cg == nil || cg.RequiredFiles == nil {
		return nil
	}

	// Extract unique source node IDs from field mappings
	sourceNodeIDs := make(map[string]bool)
	for _, mapping := range fieldMappings {
		if !mapping.IsEventTrigger && mapping.SourceNodeID != "" {
			sourceNodeIDs[mapping.SourceNodeID] = true
		}
	}

	// Find which execution IDs contain these source nodes
	executionIDs := make(map[string]bool)
	for nodeID := range sourceNodeIDs {
		// Search through all required files to find which one contains this node
		for execID, file := range cg.RequiredFiles {
			for _, containsNode := range file.ContainsNodes {
				if containsNode == nodeID {
					executionIDs[execID] = true
					break
				}
			}
		}
	}

	// Build list of required files
	result := make([]*RequiredBlobFile, 0, len(executionIDs))
	for execID := range executionIDs {
		if file, exists := cg.RequiredFiles[execID]; exists {
			result = append(result, file)
		}
	}

	return result
}

// FindFileForNode returns the RequiredBlobFile that contains the specified node ID
// Returns nil if not found
func (cg *ConsumerGraph) FindFileForNode(nodeID string) *RequiredBlobFile {
	if cg == nil || cg.RequiredFiles == nil {
		return nil
	}

	for _, file := range cg.RequiredFiles {
		for _, containsNode := range file.ContainsNodes {
			if containsNode == nodeID {
				return file
			}
		}
	}

	return nil
}

// NewConsumerGraph creates a new empty consumer graph
func NewConsumerGraph() *ConsumerGraph {
	return &ConsumerGraph{
		RequiredFiles:   make(map[string]*RequiredBlobFile),
		ResultLocations: make(map[string]*ResultLocation),
	}
}

// AddRequiredFile adds or updates a required blob file entry
func (cg *ConsumerGraph) AddRequiredFile(executionID, blobURL, blobPath string, containsNodes []string) {
	if cg.RequiredFiles == nil {
		cg.RequiredFiles = make(map[string]*RequiredBlobFile)
	}

	cg.RequiredFiles[executionID] = &RequiredBlobFile{
		ExecutionID:   executionID,
		BlobURL:       blobURL,
		BlobPath:      blobPath,
		ContainsNodes: containsNodes,
	}
}
