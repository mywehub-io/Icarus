package resolver

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/archive"
	"github.com/wehubfusion/Icarus/pkg/message"
)

func rawValue(b []byte) archive.StreamedValue {
	return archive.StreamedValue{
		Size: int64(len(b)),
		Open: func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(b)), nil },
	}
}

// The streamed writer is only safe to adopt if nothing downstream can tell it apart from
// CreateResult: same inline-or-blob outcome, same path, same bytes in storage.
func TestCreateResultStreamMatchesCreateResult(t *testing.T) {
	small := map[string]json.RawMessage{"esr-/error": json.RawMessage(`null`)}
	meta := ResultMeta{WorkflowID: "wf", RunID: "run", NodeID: "esr", ExecutionID: "esr"}

	for _, n := range []int{10, 5_000, 100_000} {
		raw := bytes.Repeat([]byte("DUCK\x00\xff"), n/6+1)[:n]
		doc, err := json.Marshal(map[string]interface{}{
			"esr-/error":  nil,
			"esr-/output": base64.StdEncoding.EncodeToString(raw),
		})
		if err != nil {
			t.Fatal(err)
		}

		for _, threshold := range []int{1, 64 * 1024, 1 << 20} {
			oldFake := &fakeBlobClient{bodies: map[string]string{}, uploads: map[string][]byte{}}
			want, err := NewService(oldFake, threshold).CreateResult(context.Background(), doc, meta)
			if err != nil {
				t.Fatalf("n=%d t=%d: CreateResult: %v", n, threshold, err)
			}
			newFake := &fakeBlobClient{bodies: map[string]string{}, uploads: map[string][]byte{}}
			got, err := NewService(newFake, threshold).CreateResultStream(context.Background(), small,
				map[string]archive.StreamedValue{"esr-/output": rawValue(raw)}, meta)
			if err != nil {
				t.Fatalf("n=%d t=%d: CreateResultStream: %v", n, threshold, err)
			}

			if got.UsedBlob != want.UsedBlob {
				t.Fatalf("n=%d t=%d: UsedBlob %v, want %v", n, threshold, got.UsedBlob, want.UsedBlob)
			}
			if !want.UsedBlob {
				if !bytes.Equal(got.InlineData, want.InlineData) {
					t.Fatalf("n=%d t=%d: inline data differs", n, threshold)
				}
				continue
			}
			if got.BlobReference.URL != want.BlobReference.URL {
				t.Fatalf("n=%d t=%d: URL %s, want %s", n, threshold, got.BlobReference.URL, want.BlobReference.URL)
			}
			if got.BlobReference.SizeBytes != want.BlobReference.SizeBytes {
				t.Fatalf("n=%d t=%d: SizeBytes %d, want %d", n, threshold, got.BlobReference.SizeBytes, want.BlobReference.SizeBytes)
			}
			if !bytes.Equal(newFake.uploads[got.BlobReference.URL], oldFake.uploads[want.BlobReference.URL]) {
				t.Fatalf("n=%d t=%d: stored archive bytes differ", n, threshold)
			}
		}
	}
}

func TestCreateResultStreamReportsTheUploadFailure(t *testing.T) {
	fake := &fakeBlobClient{bodies: map[string]string{}, uploads: map[string][]byte{}, streamFailAfter: 100}
	_, err := NewService(fake, 1).CreateResultStream(context.Background(), nil,
		map[string]archive.StreamedValue{"esr-/output": rawValue(make([]byte, 1<<20))},
		ResultMeta{WorkflowID: "wf", RunID: "run", NodeID: "esr"})
	if err == nil || !strings.Contains(err.Error(), "upload interrupted") {
		t.Fatalf("expected the upload error, got %v", err)
	}
	if len(fake.uploads) != 0 {
		t.Fatal("a failed upload must not leave a stored result")
	}
}

func TestCreateResultStreamRefusesANonFlatKey(t *testing.T) {
	fake := &fakeBlobClient{bodies: map[string]string{}, uploads: map[string][]byte{}}
	_, err := NewService(fake, 1).CreateResultStream(context.Background(), nil,
		map[string]archive.StreamedValue{"output": rawValue(make([]byte, 100))},
		ResultMeta{WorkflowID: "wf", RunID: "run", NodeID: "esr"})
	if err == nil {
		t.Fatal("expected a non-flat key to be refused")
	}
}

const locateURL = "https://acct/c/results/wf/run/src.zip"

func locateFixture(t *testing.T, doc map[string]interface{}, node string) (*Service, *ConsumerGraph) {
	t.Helper()
	b, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	raw, _, err := archive.Build(b)
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakeBlobClient{bodies: map[string]string{locateURL: string(raw)}}
	cg := NewConsumerGraph()
	cg.AddRequiredFile("src", locateURL, "results/wf/run/src.zip", []string{node})
	return NewService(fake, 1), cg
}

func plainMapping(node, src, dst string) message.FieldMapping {
	return message.FieldMapping{SourceNodeID: node, SourceEndpoint: src, DestinationEndpoints: []string{dst}, DataType: "FIELD"}
}

// LocateEntry's contract is an equivalence: where it succeeds, the ordinary resolve would
// have produced {"payload": V} with V built from the located entry alone. This checks that
// claim against the real mapping engine for every shape LocateEntry accepts.
func TestLocateEntryAgreesWithTheOrdinaryResolve(t *testing.T) {
	files := []interface{}{
		map[string]interface{}{"file_data": "SERSfg==", "file_name": "a.dat"},
		map[string]interface{}{"file_data": "SERSfn4=", "file_name": "b.dat"},
	}
	cases := []struct {
		name     string
		doc      map[string]interface{}
		node     string
		mappings []message.FieldMapping
		rel      string
	}{
		{
			name:     "sftp files envelope",
			doc:      map[string]interface{}{"sftp-/payload/files": files, "sftp-/action": "Download Directory"},
			node:     "sftp",
			mappings: []message.FieldMapping{plainMapping("sftp", "/payload", "/payload")},
			rel:      "files",
		},
		{
			name:     "trigger body string",
			doc:      map[string]interface{}{"trig-/payload": "SERSfg=="},
			node:     "trig",
			mappings: []message.FieldMapping{plainMapping("trig", "/payload", "/payload")},
		},
		{
			name: "event mapping alongside",
			doc:  map[string]interface{}{"trig-/payload": "SERSfg==", "trig-/success": true},
			node: "trig",
			mappings: []message.FieldMapping{
				plainMapping("trig", "/payload", "/payload"),
				{SourceNodeID: "trig", SourceEndpoint: "/success", DestinationEndpoints: []string{"/trigger"}, DataType: "EVENT", IsEventTrigger: true},
			},
		},
		{
			name:     "esr database output",
			doc:      map[string]interface{}{"esr-/output": base64.StdEncoding.EncodeToString([]byte("DUCK"))},
			node:     "esr",
			mappings: []message.FieldMapping{plainMapping("esr", "/output", "/payload")},
		},
	}

	for _, tc := range cases {
		svc, cg := locateFixture(t, tc.doc, tc.node)
		loc, ok, err := svc.LocateEntry(context.Background(), tc.mappings, cg)
		if err != nil || !ok {
			t.Fatalf("%s: LocateEntry = %v, %v; want a locator", tc.name, ok, err)
		}
		if loc.RelPath != tc.rel {
			t.Fatalf("%s: RelPath %q, want %q", tc.name, loc.RelPath, tc.rel)
		}

		rr, err := loc.Open(context.Background(), svc.RangeSource(), 7)
		if err != nil {
			t.Fatal(err)
		}
		entry, err := io.ReadAll(rr)
		rr.Close()
		if err != nil {
			t.Fatalf("%s: stream entry: %v", tc.name, err)
		}
		var value interface{}
		if err := json.Unmarshal(entry, &value); err != nil {
			t.Fatalf("%s: entry is not JSON: %v", tc.name, err)
		}
		if loc.RelPath != "" {
			value = map[string]interface{}{loc.RelPath: value}
		}
		predicted := map[string]interface{}{"payload": value}

		full, err := svc.ResolveMappedInputWithConsumerGraph(context.Background(), nil, nil,
			&FieldMappingParams{FieldMappings: tc.mappings, ConsumerGraph: cg}, cg)
		if err != nil {
			t.Fatalf("%s: ordinary resolve: %v", tc.name, err)
		}
		var resolved interface{}
		if err := json.Unmarshal(full, &resolved); err != nil {
			t.Fatalf("%s: resolved input is not JSON: %v", tc.name, err)
		}
		if !reflect.DeepEqual(resolved, interface{}(predicted)) {
			t.Fatalf("%s: located entry does not reproduce the resolved input\nresolved  %s\npredicted %v", tc.name, full, predicted)
		}
	}
}

// Every shape outside the whitelist must decline, so the caller resolves the ordinary way.
func TestLocateEntryDeclinesEverythingElse(t *testing.T) {
	doc := map[string]interface{}{
		"n-/payload/files":    []interface{}{"x"},
		"n-/payload/other":    "y",
		"n-/single/files[0]":  "z",
		"n-/deep/a/b":         "w",
		"n-/payload2":         "v",
		"n-/data//name":       "u",
		"n-/collection":       []interface{}{map[string]interface{}{"name": "t"}},
	}
	svc, cg := locateFixture(t, doc, "n")

	cases := map[string][]message.FieldMapping{
		"two entries under the source":  {plainMapping("n", "/payload", "/payload")},
		"indexed entry under source":    {plainMapping("n", "/single", "/payload")},
		"deeper entry under source":     {plainMapping("n", "/deep", "/payload")},
		"other destination":             {plainMapping("n", "/payload2", "/data")},
		"two destinations":              {{SourceNodeID: "n", SourceEndpoint: "/payload2", DestinationEndpoints: []string{"/payload", "/x"}}},
		"two data mappings":             {plainMapping("n", "/payload2", "/payload"), plainMapping("n", "/payload2", "/payload")},
		"collection syntax":             {plainMapping("n", "/collection//name", "/payload")},
		"iterate":                       {{SourceNodeID: "n", SourceEndpoint: "/payload2", DestinationEndpoints: []string{"/payload"}, Iterate: true}},
		"whole node":                    {plainMapping("n", "/", "/payload")},
		"absent key":                    {plainMapping("n", "/missing", "/payload")},
		"source not in consumer graph":  {plainMapping("elsewhere", "/payload2", "/payload")},
		"no data mapping":               {{SourceNodeID: "n", SourceEndpoint: "/success", DestinationEndpoints: []string{"/t"}, IsEventTrigger: true}},
		"plugin error section":          {{SourceNodeID: "n", SourceEndpoint: "/payload2", SourceSectionId: "pluginError", DestinationEndpoints: []string{"/payload"}}},
	}
	for name, mappings := range cases {
		if _, ok, err := svc.LocateEntry(context.Background(), mappings, cg); ok || err != nil {
			t.Errorf("%s: LocateEntry = %v, %v; want a plain decline", name, ok, err)
		}
	}

	// An inline result for the source takes precedence over its file, so no locator.
	cg.ResultLocations["n"] = &ResultLocation{NodeID: "n", HasInlineData: true, InlineData: json.RawMessage(`{}`)}
	if _, ok, _ := svc.LocateEntry(context.Background(), []message.FieldMapping{plainMapping("n", "/payload2", "/payload")}, cg); ok {
		t.Error("inline source: want a decline")
	}
	if _, ok, _ := svc.LocateEntry(context.Background(), []message.FieldMapping{plainMapping("n", "/payload2", "/payload")}, nil); ok {
		t.Error("no consumer graph: want a decline")
	}
}
