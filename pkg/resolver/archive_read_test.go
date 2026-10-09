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
	svc := NewService(fake)
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

	svc := NewService(fake)
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

// CreateResult writes the unit's output document as an archive the field-mapping read path
// opens, and the document comes back out of it unchanged in JSON terms. A payload that is not a
// node output (not flat keys) is refused rather than written in a form nothing reads.
func TestCreateResultWritesAReadableArchive(t *testing.T) {
	doc := buildFixtureDoc(t)

	fake := &fakeBlobClient{bodies: map[string]string{}, uploads: map[string][]byte{}}
	svc := NewService(fake)
	res, err := svc.CreateResult(context.Background(), doc, ResultMeta{
		WorkflowID: "wf", RunID: "run", NodeID: "up", ExecutionID: "exec",
	})
	if err != nil {
		t.Fatalf("CreateResult: %v", err)
	}
	if res.BlobReference == nil {
		t.Fatal("the result did not go to blob")
	}
	if !strings.HasSuffix(res.BlobReference.URL, archive.Extension) {
		t.Fatalf("stored at %s, want an %s path", res.BlobReference.URL, archive.Extension)
	}
	stored := fake.uploads[res.BlobReference.URL]
	if !archive.IsArchive(stored) {
		t.Fatal("what reached storage is not an archive")
	}
	if res.BlobReference.SizeBytes != int64(len(stored)) {
		t.Fatal("SizeBytes is not the length of what was written, so a ranged open would 416")
	}

	r, err := archive.NewReader(bytes.NewReader(stored), int64(len(stored)))
	if err != nil {
		t.Fatalf("open archive: %v", err)
	}
	got, err := r.Document()
	if err != nil {
		t.Fatalf("re-materialise: %v", err)
	}
	var want, have map[string]interface{}
	if err := json.Unmarshal(doc, &want); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(got, &have); err != nil {
		t.Fatalf("archive did not re-materialise into a document: %v (%s)", err, got)
	}
	if !reflect.DeepEqual(want, have) {
		t.Fatal("re-materialised document differs from the one the archive was built from")
	}

	if _, err := svc.CreateResult(context.Background(), []byte("MSH|^~\\&|S"), ResultMeta{WorkflowID: "wf", RunID: "run", NodeID: "n"}); err == nil {
		t.Fatal("a payload that is not a JSON object was written as a result")
	}
	if _, err := NewService(nil).CreateResult(context.Background(), doc, ResultMeta{WorkflowID: "wf", RunID: "run"}); err == nil {
		t.Fatal("a result was accepted with no blob storage")
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
	svc := NewService(fake)

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
