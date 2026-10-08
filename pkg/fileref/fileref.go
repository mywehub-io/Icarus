// Package fileref is the one representation of a byte value: a reference to a blob holding the
// bytes in their native format, never the bytes themselves.
//
// In JSON a reference is an object with a single "$file" key:
//
//	{"$file": {"path": "results/<wf>/<run>/<node>/payload.csv", "size": 1048576,
//	           "contentType": "text/csv", "fileName": "export.csv"}}
//
// The key lets a reader tell a reference from data, but the key alone is never a reason to open
// one: a reference is trusted only when the plan types the value as a byte or records port and
// its path is under the current run's prefix (see InRun). A "$file" object that arrives inside
// user data is plain data.
package fileref

import (
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"strings"
)

// Key is the JSON key that marks a file reference.
const Key = "$file"

// ResultsPrefix is the root of every run's files in the results container.
const ResultsPrefix = "results/"

// FileRef points at one blob in the results container.
type FileRef struct {
	// Path is the blob path in the results container. Not a URL, and never signed.
	Path string `json:"path"`
	// Size is the file's byte length: the content, not an archive's or an encoding's.
	Size int64 `json:"size"`
	// ContentType is the MIME type of the native format.
	ContentType string `json:"contentType,omitempty"`
	// FileName is the name a person would save it as.
	FileName string `json:"fileName,omitempty"`
	// Records is the row count of a records (.ndjson) file; nil for any other file.
	Records *int64 `json:"records,omitempty"`
}

// IsRecords reports whether the file is a record stream.
func (r FileRef) IsRecords() bool {
	return BaseContentType(r.ContentType) == ContentTypeNDJSON
}

// Value returns the reference in its JSON value form, {"$file": {...}}.
func Value(ref FileRef) map[string]interface{} {
	inner := map[string]interface{}{
		"path": ref.Path,
		"size": ref.Size,
	}
	if ref.ContentType != "" {
		inner["contentType"] = ref.ContentType
	}
	if ref.FileName != "" {
		inner["fileName"] = ref.FileName
	}
	if ref.Records != nil {
		inner["records"] = *ref.Records
	}
	return map[string]interface{}{Key: inner}
}

// MarshalValue returns the reference's JSON value form as bytes.
func MarshalValue(ref FileRef) ([]byte, error) {
	return json.Marshal(struct {
		File FileRef `json:"$file"`
	}{ref})
}

// Parse reads a reference from a decoded JSON value. It accepts the value form
// ({"$file": {...}}) only, with a non-empty path and a non-negative size. It does not say the
// reference may be opened: see InRun.
func Parse(v interface{}) (FileRef, bool) {
	var inner map[string]interface{}
	switch t := v.(type) {
	case map[string]interface{}:
		if len(t) != 1 {
			return FileRef{}, false
		}
		m, ok := t[Key].(map[string]interface{})
		if !ok {
			return FileRef{}, false
		}
		inner = m
	case json.RawMessage:
		var decoded interface{}
		if err := json.Unmarshal(t, &decoded); err != nil {
			return FileRef{}, false
		}
		return Parse(decoded)
	default:
		return FileRef{}, false
	}

	p, ok := inner["path"].(string)
	if !ok || p == "" {
		return FileRef{}, false
	}
	size, ok := toInt64(inner["size"])
	if !ok || size < 0 {
		return FileRef{}, false
	}
	ref := FileRef{Path: p, Size: size}
	if ct, ok := inner["contentType"].(string); ok {
		ref.ContentType = ct
	}
	if fn, ok := inner["fileName"].(string); ok {
		ref.FileName = fn
	}
	if raw, present := inner["records"]; present && raw != nil {
		n, ok := toInt64(raw)
		if !ok || n < 0 {
			return FileRef{}, false
		}
		ref.Records = &n
	}
	return ref, true
}

// IsRef reports whether v has the shape of a reference. Shape only: see Parse and InRun.
func IsRef(v interface{}) bool {
	_, ok := Parse(v)
	return ok
}

func toInt64(v interface{}) (int64, bool) {
	switch n := v.(type) {
	case float64:
		if n != float64(int64(n)) {
			return 0, false
		}
		return int64(n), true
	case int64:
		return n, true
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case json.Number:
		i, err := n.Int64()
		return i, err == nil
	case string:
		i, err := strconv.ParseInt(n, 10, 64)
		return i, err == nil
	default:
		return 0, false
	}
}

// RunPrefix is the path prefix every file of one run lives under: results/{wf}/{run}/.
func RunPrefix(workflowID, runID string) string {
	return ResultsPrefix + SanitizePart(workflowID, "workflow") + "/" + SanitizePart(runID, "run") + "/"
}

// InRun reports whether ref's path is a clean path under the run's prefix. A path with "..",
// ".", an empty segment, a backslash or a leading slash is refused whatever its prefix.
func InRun(ref FileRef, workflowID, runID string) bool {
	if workflowID == "" || runID == "" {
		return false
	}
	p := ref.Path
	if p == "" || strings.ContainsAny(p, "\\\x00") || strings.HasPrefix(p, "/") {
		return false
	}
	for _, seg := range strings.Split(p, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return false
		}
	}
	if path.Clean(p) != p {
		return false
	}
	prefix := RunPrefix(workflowID, runID)
	return strings.HasPrefix(p, prefix) && len(p) > len(prefix)
}

// SanitizePart makes one path segment safe: trimmed, slashes replaced, and never empty, "." or
// "..". It matches the resolver's archive path rule, plus the dot segments.
func SanitizePart(part, fallback string) string {
	part = strings.TrimSpace(part)
	part = strings.ReplaceAll(part, "/", "-")
	part = strings.ReplaceAll(part, "\\", "-")
	part = strings.ReplaceAll(part, "\x00", "")
	if part == "" || part == "." || part == ".." {
		return fallback
	}
	return part
}

// PathOptions selects which of the D2 layouts PathFor builds.
type PathOptions struct {
	// ParentNodeID, when set, makes this an embedded node's file:
	// results/{wf}/{run}/{parentNodeId}/{nodeId}/{port}[/{index}].{ext}
	ParentNodeID string
	// Index, when non-nil, makes this an iterated item's file: .../{port}/{index}.{ext}
	Index *int
	// FileName, when set, makes this one file of a {files:[...]} list:
	// .../{port}/files/{index}-{fileName}. Index is required with it (0 when nil).
	FileName string
	// Ext is the extension, with or without the dot. Ignored for a files-list entry.
	Ext string
}

// PathFor builds a file's blob path under the run's prefix, following decisions D2.
func PathFor(workflowID, runID, nodeID, port string, opts PathOptions) string {
	var b strings.Builder
	b.WriteString(RunPrefix(workflowID, runID))
	if opts.ParentNodeID != "" {
		b.WriteString(SanitizePart(opts.ParentNodeID, "parent"))
		b.WriteByte('/')
	}
	b.WriteString(SanitizePart(nodeID, "node"))
	b.WriteByte('/')
	portPart := SanitizePart(strings.TrimPrefix(port, "/"), "payload")

	if opts.FileName != "" {
		idx := 0
		if opts.Index != nil {
			idx = *opts.Index
		}
		fmt.Fprintf(&b, "%s/files/%d-%s", portPart, idx, SanitizePart(opts.FileName, "file"))
		return b.String()
	}

	ext := strings.TrimPrefix(opts.Ext, ".")
	if ext == "" {
		ext = "bin"
	}
	if opts.Index != nil {
		fmt.Fprintf(&b, "%s/%d.%s", portPart, *opts.Index, ext)
		return b.String()
	}
	fmt.Fprintf(&b, "%s.%s", portPart, ext)
	return b.String()
}

// DefaultFileName is "<port>.<ext>", the name used when a value has no source file name.
func DefaultFileName(port, ext string) string {
	return SanitizePart(strings.TrimPrefix(port, "/"), "payload") + "." + strings.TrimPrefix(ext, ".")
}

// DocumentPath is where a node's monitoring document lives (decisions D9):
// results/{wf}/{run}/{nodeId}.{direction}.json, beside the node's folder so it never collides with
// a port of the node. direction is "input" or "output".
func DocumentPath(workflowID, runID, nodeID, direction string) string {
	return RunPrefix(workflowID, runID) + SanitizePart(nodeID, "node") + "." + SanitizePart(direction, "output") + ".json"
}
