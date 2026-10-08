package runtime

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/wehubfusion/Icarus/pkg/fileref"
	"github.com/wehubfusion/Icarus/pkg/message"
)

// byteFields returns the top-level input fields a mapping the plan marks BYTE delivers: the only
// fields whose file references a processor may open (raw payloads, D11). A root destination takes
// the source endpoint's last segment, as the resolver names it.
func byteFields(mappings []FieldMapping) map[string]bool {
	var out map[string]bool
	for _, m := range mappings {
		if m.ValueType != message.ValueTypeByte {
			continue
		}
		if out == nil {
			out = make(map[string]bool)
		}
		dests := m.DestinationEndpoints
		if len(dests) == 0 {
			dests = []string{path.Base(strings.TrimRight(m.SourceEndpoint, "/"))}
		}
		for _, d := range dests {
			key := strings.Trim(d, "/")
			if i := strings.Index(key, "/"); i >= 0 {
				key = key[:i]
			}
			if key != "" {
				out[key] = true
			}
		}
	}
	return out
}

// TrustedFile returns the file reference at an input field when a BYTE mapping delivered it. A
// "$file" object arriving any other way is data and is never opened.
func (in ProcessInput) TrustedFile(field string) (fileref.FileRef, bool) {
	if !in.ByteFields[field] {
		return fileref.FileRef{}, false
	}
	return fileref.Parse(in.Data[field])
}

// ReadTrustedFile reads the file at a trusted input field whole. isFile is false when the field
// is not a trusted file reference, and the processor reads the value as it always has. max > 0
// refuses a larger file.
func (in ProcessInput) ReadTrustedFile(field string, max int64) (data []byte, isFile bool, err error) {
	ref, ok := in.TrustedFile(field)
	if !ok {
		return nil, false, nil
	}
	if in.Files == nil {
		return nil, true, fmt.Errorf("input %s is a file but file storage is not configured", field)
	}
	if max > 0 && ref.Size > max {
		return nil, true, fmt.Errorf("input %s is %d bytes, above the %d byte limit", field, ref.Size, max)
	}
	rc, err := in.Files.Open(in.Ctx, ref)
	if err != nil {
		return nil, true, fmt.Errorf("open input %s file: %w", field, err)
	}
	defer rc.Close()
	data, err = io.ReadAll(io.LimitReader(rc, ref.Size+1))
	if err != nil {
		return nil, true, fmt.Errorf("read input %s file: %w", field, err)
	}
	if int64(len(data)) != ref.Size {
		return nil, true, fmt.Errorf("input %s file is %d bytes, reference says %d", field, len(data), ref.Size)
	}
	return data, true, nil
}

type fileOutputsKey struct{}

// WithFileOutputs records, for a unit's embedded nodes, which byte output ports every consumer
// reads as files (node id -> endpoints). Elysium sets it from unit metadata embedded_file_outputs;
// the runtime gives each node its own set in ProcessInput.FileOutputs.
func WithFileOutputs(ctx context.Context, byNode map[string][]string) context.Context {
	return context.WithValue(ctx, fileOutputsKey{}, byNode)
}

func fileOutputsFor(ctx context.Context, nodeID string) map[string]bool {
	byNode, _ := ctx.Value(fileOutputsKey{}).(map[string][]string)
	ports := byNode[nodeID]
	if len(ports) == 0 {
		return nil
	}
	out := make(map[string]bool, len(ports))
	for _, p := range ports {
		out["/"+strings.Trim(p, "/")] = true
	}
	return out
}

// WritesFile reports whether the node should write output port as a file.
func (in ProcessInput) WritesFile(port string) bool {
	return in.Files != nil && in.FileOutputs["/"+strings.Trim(port, "/")]
}

// WriteOutputFile writes one byte output of this embedded node under its parent unit
// (results/{wf}/{run}/{parent}/{node}/{port}[/{item}].{ext}) and returns the reference value.
func (in ProcessInput) WriteOutputFile(port, contentType string, body io.Reader) (map[string]interface{}, error) {
	if in.Files == nil {
		return nil, fmt.Errorf("output %s: no file store", port)
	}
	ext := fileref.ExtensionFor(contentType)
	opts := fileref.PathOptions{ParentNodeID: in.ParentNodeID, Ext: ext}
	if in.IsIteration && in.ItemIndex >= 0 {
		i := in.ItemIndex
		opts.Index = &i
	}
	path := fileref.PathFor(in.WorkflowID, in.RunID, in.NodeId, port, opts)
	w, err := in.Files.Create(in.Ctx, path, contentType, fileref.DefaultFileName(port, ext))
	if err != nil {
		return nil, fmt.Errorf("output %s: %w", port, err)
	}
	if _, err := io.Copy(w, body); err != nil {
		_ = w.Abort()
		return nil, fmt.Errorf("output %s: %w", port, err)
	}
	ref, err := w.Close()
	if err != nil {
		return nil, fmt.Errorf("output %s: %w", port, err)
	}
	return fileref.Value(ref), nil
}

// MaybeFileOutput writes data[key], a base64 byte output, as a file when the port is in
// FileOutputs; otherwise it leaves data alone.
func (in ProcessInput) MaybeFileOutput(data map[string]interface{}, key, contentType string) error {
	if data == nil || !in.WritesFile(key) {
		return nil
	}
	b64, ok := data[key].(string)
	if !ok {
		return nil
	}
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return fmt.Errorf("output %s: %w", key, err)
	}
	ref, err := in.WriteOutputFile(key, contentType, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	data[key] = ref
	return nil
}
