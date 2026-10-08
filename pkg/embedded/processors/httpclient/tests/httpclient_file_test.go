package httpclient_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
	"github.com/wehubfusion/Icarus/pkg/fileref"
	"github.com/wehubfusion/Icarus/pkg/filestore"
	"github.com/wehubfusion/Icarus/pkg/filestore/memfs"
)

// A payload file is streamed as the request body with its exact length (raw payloads).
func TestProcess_PayloadFileIsTheRequestBody(t *testing.T) {
	content := `{"resourceType":"Patient","id":"synthetic"}`
	var gotBody string
	var gotLength int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody, gotLength = string(b), r.ContentLength
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	b := memfs.New()
	ref := fileref.FileRef{Path: fileref.RunPrefix("w", "r") + "t/payload.json", Size: int64(len(content))}
	b.Put(ref.Path, []byte(content))
	store, _ := filestore.New(b, "w", "r")
	rawCfg, _ := json.Marshal(map[string]interface{}{"label": "t", "connection": map[string]interface{}{"url": server.URL, "method": "POST"}})

	out := createTestNode(t, "node1").Process(runtime.ProcessInput{
		Ctx: context.Background(), RawConfig: rawCfg, NodeId: "node1", Files: store, WorkflowID: "w", RunID: "r",
		Data:       map[string]interface{}{"payload": fileref.Value(ref)},
		ByteFields: map[string]bool{"payload": true},
	})
	if out.Error != nil {
		t.Fatal(out.Error)
	}
	if gotBody != content || gotLength != int64(len(content)) {
		t.Fatalf("server got %q (length %d)", gotBody, gotLength)
	}
}

// When every consumer of /body reads files, the response is streamed into a file.
func TestProcess_ResponseBodyIsAFile(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		_, _ = w.Write([]byte("a,b\n1,2\n"))
	}))
	defer server.Close()
	b := memfs.New()
	store, _ := filestore.New(b, "w", "r")
	rawCfg, _ := json.Marshal(map[string]interface{}{"label": "t", "connection": map[string]interface{}{"url": server.URL, "method": "GET"}})
	out := createTestNode(t, "node1").Process(runtime.ProcessInput{
		Ctx: context.Background(), RawConfig: rawCfg, NodeId: "node1", Files: store, WorkflowID: "w", RunID: "r", ParentNodeID: "p",
		Data: map[string]interface{}{}, ItemIndex: -1,
	})
	if out.Error != nil {
		t.Fatal(out.Error)
	}
	ref, ok := fileref.Parse(out.Data["body"])
	if !ok || string(b.Blobs[ref.Path]) != "a,b\n1,2\n" || ref.ContentType != "text/csv" {
		t.Fatalf("body file wrong: %#v", out.Data["body"])
	}
}

// A streamed exchange has no overall timeout: a response that keeps arriving for longer than the
// 30 s limit of an in-memory exchange is not cut off. Here the limit is shortened by sending a
// body in two parts with a gap, which an idle timeout of a few seconds would still allow.
func TestProcess_StreamedResponseIsNotCutByAnOverallTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("first,"))
		w.(http.Flusher).Flush()
		time.Sleep(300 * time.Millisecond)
		_, _ = w.Write([]byte("second"))
	}))
	defer server.Close()
	b := memfs.New()
	store, _ := filestore.New(b, "w", "r")
	rawCfg, _ := json.Marshal(map[string]interface{}{"label": "t", "connection": map[string]interface{}{"url": server.URL, "method": "GET"}})
	out := createTestNode(t, "node1").Process(runtime.ProcessInput{
		Ctx: context.Background(), RawConfig: rawCfg, NodeId: "node1", Files: store, WorkflowID: "w", RunID: "r", ParentNodeID: "p",
		Data: map[string]interface{}{}, ItemIndex: -1,
	})
	if out.Error != nil {
		t.Fatal(out.Error)
	}
	ref, ok := fileref.Parse(out.Data["body"])
	if !ok || ref.Size != int64(len("first,second")) {
		t.Fatalf("body = %v", out.Data["body"])
	}
}

// A payload that is not a file reference is refused: a byte value is a file, never text to decode.
func TestProcess_PayloadMustBeAFile(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("the request must not be sent")
	}))
	defer server.Close()
	b := memfs.New()
	store, _ := filestore.New(b, "w", "r")
	rawCfg, _ := json.Marshal(map[string]interface{}{"label": "t", "connection": map[string]interface{}{"url": server.URL, "method": "POST"}})
	out := createTestNode(t, "node1").Process(runtime.ProcessInput{
		Ctx: context.Background(), RawConfig: rawCfg, NodeId: "node1", Files: store, WorkflowID: "w", RunID: "r", ParentNodeID: "p",
		Data:       map[string]interface{}{"payload": "aGVsbG8="},
		ByteFields: map[string]bool{"payload": true},
	})
	if out.Error == nil {
		t.Fatal("a text payload was accepted")
	}
}
