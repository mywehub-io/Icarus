package jsrunner

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync/atomic"

	"github.com/dop251/goja"

	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
	"github.com/wehubfusion/Icarus/pkg/fileref"
)

// DefaultMaxFileBytes caps one file a script reads or writes: a script holds the whole file in
// memory. A host overrides it with SetMaxFileBytes (Elysium: ICARUS_JS_MAX_FILE_BYTES).
const DefaultMaxFileBytes = 16 << 20

var maxFileBytes atomic.Int64

func init() { maxFileBytes.Store(DefaultMaxFileBytes) }

// SetMaxFileBytes sets the largest file a script may read or write; n <= 0 restores the default.
func SetMaxFileBytes(n int64) {
	if n <= 0 {
		n = DefaultMaxFileBytes
	}
	maxFileBytes.Store(n)
}

// registerFileHelpers sets the "file" global a script uses to read the files its inputs reference
// and to write files for its byte outputs (raw payloads, phase 5 step 5):
//
//	file.text(ref)   the file as a UTF-8 string
//	file.json(ref)   the file parsed as JSON; an .ndjson file is an array of its lines
//	file.bytes(ref)  the file as an array of byte values
//	file.write(name, contentType, content)   writes a file; returns its reference
//
// Only files in this run can be opened: the run's store refuses any other path. A run always has a
// store; the guard keeps a unit test without one from failing at registration.
func registerFileHelpers(vm *goja.Runtime, input runtime.ProcessInput) error {
	if input.Files == nil {
		return nil
	}
	read := func(v goja.Value) []byte {
		ref, ok := fileref.Parse(v.Export())
		if !ok {
			panic(vm.NewTypeError("file: argument is not a file reference"))
		}
		if limit := maxFileBytes.Load(); ref.Size > limit {
			panic(vm.NewTypeError(fmt.Sprintf("file: %d bytes is above the %d byte limit for a script", ref.Size, limit)))
		}
		rc, err := input.Files.Open(input.Ctx, ref)
		if err != nil {
			panic(vm.NewGoError(fmt.Errorf("file: %w", err)))
		}
		defer rc.Close()
		data, err := io.ReadAll(io.LimitReader(rc, ref.Size+1))
		if err != nil {
			panic(vm.NewGoError(fmt.Errorf("file: %w", err)))
		}
		return data
	}
	obj := vm.NewObject()
	_ = obj.Set("text", func(call goja.FunctionCall) goja.Value {
		return vm.ToValue(string(read(call.Argument(0))))
	})
	_ = obj.Set("bytes", func(call goja.FunctionCall) goja.Value {
		data := read(call.Argument(0))
		out := make([]interface{}, len(data))
		for i, b := range data {
			out[i] = int(b)
		}
		return vm.ToValue(out)
	})
	_ = obj.Set("json", func(call goja.FunctionCall) goja.Value {
		data := read(call.Argument(0))
		ref, _ := fileref.Parse(call.Argument(0).Export())
		if ref.IsRecords() {
			var rows []interface{}
			for _, line := range strings.Split(string(data), "\n") {
				if strings.TrimSpace(line) == "" {
					continue
				}
				var v interface{}
				if err := json.Unmarshal([]byte(line), &v); err != nil {
					panic(vm.NewGoError(fmt.Errorf("file.json: %w", err)))
				}
				rows = append(rows, v)
			}
			return vm.ToValue(rows)
		}
		var v interface{}
		if err := json.Unmarshal(data, &v); err != nil {
			panic(vm.NewGoError(fmt.Errorf("file.json: %w", err)))
		}
		return vm.ToValue(v)
	})
	var written int32
	_ = obj.Set("write", func(call goja.FunctionCall) goja.Value {
		name := strings.TrimSpace(call.Argument(0).String())
		contentType := strings.TrimSpace(call.Argument(1).String())
		if name == "" {
			panic(vm.NewTypeError("file.write: a file name is required"))
		}
		var content []byte
		switch v := call.Argument(2).Export().(type) {
		case string:
			content = []byte(v)
		case []interface{}:
			content = make([]byte, len(v))
			for i, x := range v {
				n, ok := x.(int64)
				if !ok {
					if f, isF := x.(float64); isF {
						n, ok = int64(f), true
					}
				}
				if !ok || n < 0 || n > 255 {
					panic(vm.NewTypeError("file.write: byte arrays hold values 0 to 255"))
				}
				content[i] = byte(n)
			}
		default:
			b, err := json.Marshal(v)
			if err != nil {
				panic(vm.NewGoError(fmt.Errorf("file.write: %w", err)))
			}
			content = b
		}
		if limit := maxFileBytes.Load(); int64(len(content)) > limit {
			panic(vm.NewTypeError(fmt.Sprintf("file.write: %d bytes is above the %d byte limit for a script", len(content), limit)))
		}
		if contentType == "" || contentType == "undefined" {
			contentType = fileref.ContentTypeFor(name)
		}
		// One script may write several files, and an iterated node runs once per item: the item
		// goes in the port segment, the write count in the index.
		index := int(atomic.AddInt32(&written, 1) - 1)
		port := "files"
		if input.ItemIndex >= 0 {
			port = fmt.Sprintf("files-%d", input.ItemIndex)
		}
		path := fileref.PathFor(input.WorkflowID, input.RunID, input.NodeId, port,
			fileref.PathOptions{ParentNodeID: input.ParentNodeID, Index: &index, FileName: name})
		w, err := input.Files.Create(input.Ctx, path, contentType, name)
		if err != nil {
			panic(vm.NewGoError(fmt.Errorf("file.write: %w", err)))
		}
		if _, err := w.Write(content); err != nil {
			_ = w.Abort()
			panic(vm.NewGoError(fmt.Errorf("file.write: %w", err)))
		}
		ref, err := w.Close()
		if err != nil {
			panic(vm.NewGoError(fmt.Errorf("file.write: %w", err)))
		}
		return vm.ToValue(fileref.Value(ref))
	})
	return vm.Set("file", obj)
}
