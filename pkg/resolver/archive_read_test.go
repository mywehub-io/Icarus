package resolver

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/archive"
	"github.com/wehubfusion/Icarus/pkg/message"
)

// This is the differential harness in miniature, and it is the primary safety net for the
// whole change: the same payload, read once as a document and once as an archive, must
// produce identical SourceResults. Everything downstream — every array, every iterated
// fan-out, every reconstructed node — is a pure function of that map, so if the two agree
// here they agree everywhere.
//
// The assertion is strong precisely because the layout was chosen to make it so. The key
// set and each value's bytes are unchanged by the container, so any difference this
// reports is a container bug and never a disagreement about how data should be reshaped.

func buildFixtureDoc(t *testing.T) []byte {
	t.Helper()
	doc := map[string]interface{}{
		// Shape A: a whole array under one key, plus scalars and a nested leaf.
		"solo-/name":       "david",
		"solo-/rows":       []interface{}{map[string]interface{}{"name": "a"}, map[string]interface{}{"name": "b"}},
		"solo-/meta/count": float64(2),
		"solo-/blob":       largeString(30000),
		// Shape B: an indexed key family, the prefix-scan path.
		"iter-/rows[0]/id":   float64(0),
		"iter-/rows[0]/name": "item-0",
		"iter-/rows[1]/id":   float64(1),
		"iter-/rows[1]/name": "item-1",
		// An embedded node merged into the same file.
		"child-/value": "from-child",
	}
	b, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	return b
}

func serveArchive(t *testing.T, doc []byte) (*fakeBlobClient, string) {
	t.Helper()
	raw, _, err := archive.Build(doc)
	if err != nil {
		t.Fatalf("archive.Build: %v", err)
	}
	const url = "https://acct/c/results/wf/run/exec.zip"
	return &fakeBlobClient{bodies: map[string]string{url: string(raw)}}, url
}

func TestArchiveReadMatchesDocumentRead(t *testing.T) {
	doc := buildFixtureDoc(t)
	nodes := []string{"solo", "iter", "child"}

	// Every route through the completeness rule at once, so the comparison covers the
	// exact-key case and the three that force a whole-node read.
	mappings := []message.FieldMapping{
		{SourceNodeID: "solo", SourceEndpoint: "/name", DestinationEndpoints: []string{"/name"}},
		{SourceNodeID: "solo", SourceEndpoint: "/rows//name", DestinationEndpoints: []string{"/names"}},
		{SourceNodeID: "solo", SourceEndpoint: "/meta/count", DestinationEndpoints: []string{"/count"}},
		{SourceNodeID: "iter", SourceEndpoint: "/rows", DestinationEndpoints: []string{"/all"}, Iterate: true},
		{SourceNodeID: "child", SourceEndpoint: "", DestinationEndpoints: []string{"/whole"}},
	}

	sourceNodeIDs := map[string]bool{}
	for _, n := range nodes {
		sourceNodeIDs[n] = true
	}

	// The comparison is on the resolved input bytes, not on the intermediate map, and the
	// distinction is the whole point. Selective fetch means the archive path's map is
	// deliberately missing keys no mapping asked for — solo-/blob here, 30KB of payload
	// nobody reads. Asserting the maps match would forbid the optimisation by definition.
	// What must not change is what the plugin receives.
	wantResults := sourceResultsFromDocument(t, doc, sourceNodeIDs, nodes)
	if len(wantResults) != len(nodes) {
		t.Fatalf("document read produced %d source results, want %d", len(wantResults), len(nodes))
	}
	want := resolveVia(t, wantResults, mappings)

	// The archive path, over ranged reads against a fake that serves byte ranges.
	fake, url := serveArchive(t, doc)
	svc := NewService(fake, 0)
	files := []*RequiredBlobFile{{BlobURL: url, ContainsNodes: nodes}}

	gotResults, err := svc.downloadAndParseBlobFiles(context.Background(), files, mappings)
	if err != nil {
		t.Fatalf("archive read: %v", err)
	}
	got := resolveVia(t, gotResults, mappings)

	if !bytes.Equal(got, want) {
		t.Fatalf("archive read resolved different input bytes than the document read\n got %s\nwant %s", got, want)
	}

	// Guard against the comparison passing vacuously: the fixture must actually have
	// resolved the arrays and scalars it claims to cover.
	for _, needle := range []string{"david", "item-1", "from-child"} {
		if !bytes.Contains(got, []byte(needle)) {
			t.Fatalf("fixture did not resolve %q; the comparison is not covering what it claims: %s", needle, got)
		}
	}
}

// sourceResultsFromDocument is the reference side of the differential: it builds source
// results the way the read path did before the format changed, by parsing one whole
// document. Production no longer has this path — result files are always archives — so the
// reference lives here, deliberately, rather than as unused code in the package it tests.
func sourceResultsFromDocument(t *testing.T, doc []byte, sourceNodeIDs map[string]bool, nodes []string) map[string]*SourceResult {
	t.Helper()
	var content map[string]interface{}
	if err := json.Unmarshal(doc, &content); err != nil {
		t.Fatalf("parse reference document: %v", err)
	}
	return sourceResultsFromContent(content, sourceNodeIDs, nodes)
}

// resolveVia runs the real input builder, which is what turns source results into the
// bytes a plugin is handed.
func resolveVia(t *testing.T, results map[string]*SourceResult, mappings []message.FieldMapping) []byte {
	t.Helper()
	out, err := buildInputFromMappings(BuildInputParams{
		UnitNodeID:    "consumer",
		FieldMappings: mappings,
		SourceResults: results,
	})
	if err != nil {
		t.Fatalf("buildInputFromMappings: %v", err)
	}
	return out
}

// The point of the format: a narrow selection must move a fraction of the blob. Without
// this the read path could be correct and worthless at the same time.
func TestArchiveReadFetchesOnlyTheSelectedKeys(t *testing.T) {
	doc := buildFixtureDoc(t)
	fake, url := serveArchive(t, doc)

	svc := NewService(fake, 0)
	files := []*RequiredBlobFile{{BlobURL: url, ContainsNodes: []string{"solo", "iter", "child"}}}

	// One scalar from one node. solo-/blob is 30KB of the payload and must not be moved.
	mappings := []message.FieldMapping{
		{SourceNodeID: "solo", SourceEndpoint: "/name", DestinationEndpoints: []string{"/name"}},
	}

	got, err := svc.downloadAndParseBlobFiles(context.Background(), files, mappings)
	if err != nil {
		t.Fatalf("archive read: %v", err)
	}

	sr := got["solo"]
	if sr == nil {
		t.Fatal("no source result for solo")
	}
	if v := sr.RawFlatKeys["solo-/name"]; v != "david" {
		t.Fatalf("selected key resolved to %v, want \"david\"", v)
	}
	if _, present := sr.RawFlatKeys["solo-/blob"]; present {
		t.Fatal("the large unselected key was fetched; selective read is not working")
	}
}

// The end-to-end property of "everything in blob is an archive": whatever CreateResult is
// handed, what comes back out of downloadPayload is what went in.
//
// The two payload kinds fail in different ways if this is wrong, and neither is loud. A
// node output that came back as raw bytes would hand a plugin an empty document; an HL7
// message that came back re-serialised, or as a ZIP, would break trigger ingest — and the
// MLLP upload completes before the acknowledgement is written, so the cost is an MSA|AA
// never sent for a message whose body did become durable.
func TestCreateResultAndDownloadPayloadRoundTripEveryPayloadKind(t *testing.T) {
	doc := buildFixtureDoc(t)

	cases := []struct {
		name    string
		nodeID  string
		payload []byte
		// document is set where the payload is a StandardUnitOutput and so is compared as
		// JSON rather than byte for byte: its entries are re-marshalled into an object, and
		// key order and whitespace are then the writer's.
		document bool
	}{
		{name: "node output", nodeID: "up", payload: doc, document: true},
		{
			name:    "mllp hl7",
			nodeID:  "mllp-trigger",
			payload: []byte("MSH|^~\\&|SENDER|FAC|RECV|FAC|20260928||ADT^A01|MSG0001|P|2.5\rPID|1||12345^^^FAC^MR||SMITH^JOHN\r"),
		},
		// An HTTP trigger body is very often a JSON object, and it is not a node output. It
		// must survive verbatim: a plugin is handed the bytes the caller sent, so sorting
		// its keys or stripping its whitespace would silently change what every oversized
		// trigger delivers.
		{
			name:    "http json body",
			nodeID:  "http-trigger",
			payload: []byte("{ \"patient\" : { \"id\" : \"1\" },\n  \"zebra\": 1, \"apple\": 2 }"),
		},
		{name: "http csv body", nodeID: "http-trigger", payload: []byte("id,name\r\n1,david\r\n")},
	}

	for _, tc := range cases {
		fake := &fakeBlobClient{bodies: map[string]string{}, uploads: map[string][]byte{}}
		svc := NewService(fake, 1) // force the blob branch

		res, err := svc.CreateResult(context.Background(), tc.payload, ResultMeta{
			WorkflowID: "wf", RunID: "run", NodeID: tc.nodeID, ExecutionID: "exec-" + tc.name,
		})
		if err != nil {
			t.Fatalf("%s: CreateResult: %v", tc.name, err)
		}
		if res.BlobReference == nil {
			t.Fatalf("%s: payload did not go to blob", tc.name)
		}
		if !strings.HasSuffix(res.BlobReference.URL, archive.Extension) {
			t.Fatalf("%s: stored at %s, want an %s path", tc.name, res.BlobReference.URL, archive.Extension)
		}
		if stored := fake.uploads[res.BlobReference.URL]; !archive.IsArchive(stored) {
			t.Fatalf("%s: what reached storage is not an archive", tc.name)
		}
		if res.BlobReference.SizeBytes != len(fake.uploads[res.BlobReference.URL]) {
			t.Fatalf("%s: SizeBytes is not the length of what was written, so a ranged open would 416", tc.name)
		}

		got, err := svc.downloadPayload(context.Background(), res.BlobReference.URL)
		if err != nil {
			t.Fatalf("%s: downloadPayload: %v", tc.name, err)
		}

		if !tc.document {
			if !bytes.Equal(got, tc.payload) {
				t.Fatalf("%s: payload changed on the way through\n got %q\nwant %q", tc.name, got, tc.payload)
			}
			continue
		}

		var want, have map[string]interface{}
		if err := json.Unmarshal(tc.payload, &want); err != nil {
			t.Fatalf("%s: parse source document: %v", tc.name, err)
		}
		if err := json.Unmarshal(got, &have); err != nil {
			t.Fatalf("%s: archive did not re-materialise into a document: %v (%s)", tc.name, err, got)
		}
		if !reflect.DeepEqual(want, have) {
			t.Fatalf("%s: re-materialised document differs from the one the archive was built from", tc.name)
		}
	}
}

// One producer of a readable blob is not this package: Zeus offloads an oversized pruned
// consumer graph by calling the blob client directly, and Elysium reads it back through the
// same ResolveInput path. Archiving it would mean changing Zeus, which the design does not,
// so plain JSON has to keep working here.
//
// Anything that is neither an archive nor JSON is a stale or corrupt blob, and naming it is
// worth more than passing it to a plugin that will misparse it.
func TestDownloadPayloadAdmitsConsumerGraphJSONAndRejectsAnythingElse(t *testing.T) {
	const graphURL = "https://acct/c/consumer-graphs/wf/run/unit.json"
	const staleURL = "https://acct/c/results/wf/run/stale.json"

	graph := `{"nodeResults":{"n":{"storageType":"inline"}}}`
	fake := &fakeBlobClient{bodies: map[string]string{
		graphURL: graph,
		staleURL: "MSH|^~\\&|S|F|R|F|20260928||ADT^A01|1|P|2.5\r",
	}}
	svc := NewService(fake, 0)

	got, err := svc.downloadPayload(context.Background(), graphURL)
	if err != nil {
		t.Fatalf("downloadPayload(consumer graph): %v", err)
	}
	if string(got) != graph {
		t.Fatalf("consumer graph was altered on the way through:\n got %q\nwant %q", got, graph)
	}

	if _, err := svc.downloadPayload(context.Background(), staleURL); err == nil {
		t.Fatal("a blob that is neither an archive nor JSON was passed through silently")
	}
}

// An opaque archive has no key space, so every mapping against it resolves to nothing. That
// would run the unit on empty input with no error anywhere, which is the failure mode this
// whole design is most concerned with, so the read path refuses it by name instead.
func TestFieldMappingReadRefusesAnOpaqueArchive(t *testing.T) {
	opaque, _, err := archive.BuildOpaque([]byte("MSH|^~\\&|S|F|R|F|20260928||ADT^A01|1|P|2.5\r"))
	if err != nil {
		t.Fatalf("BuildOpaque: %v", err)
	}
	const url = "https://acct/c/results/wf/run/trigger.zip"
	fake := &fakeBlobClient{bodies: map[string]string{url: string(opaque)}}

	files := []*RequiredBlobFile{{BlobURL: url, ContainsNodes: []string{"mllp-trigger"}}}
	_, err = NewService(fake, 0).downloadAndParseBlobFiles(
		context.Background(), files, mappingsFor("mllp-trigger"))
	if err == nil {
		t.Fatal("an opaque archive resolved to an empty key set instead of failing")
	}
	if !strings.Contains(err.Error(), "opaque") {
		t.Fatalf("error does not identify the cause: %v", err)
	}
}
