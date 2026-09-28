package resolver

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/archive"
	"github.com/wehubfusion/Icarus/pkg/message"
)

// These tests exist to answer one question: does bounding concurrency and moving the
// parse inside the download worker change what the resolver hands to the array and
// iterate logic?
//
// None of the array or iterate code was touched. Everything that understands arrays —
// extractFromFlatKeys, extractArrayIndices, extractNodeDataFromStandardOutputFlat,
// buildInputFromMappings — is unchanged. The only way the change could alter their
// behaviour is by handing them a different map, so that is what is asserted here:
// against a reference implementation of the previous algorithm, over fixtures chosen
// to hit every branch those functions take.

// sequentialReference reimplements downloadAndParseBlobFiles as it behaved before the
// change: download every file, then parse and merge strictly in requiredFiles order.
//
// The previous code downloaded in parallel and parsed afterwards in slice order, so a
// sequential download produces the identical merge. Keeping the reference this simple
// is deliberate — a reference that shared the implementation's structure could share
// its bugs.
func sequentialReference(
	ctx context.Context,
	s *Service,
	requiredFiles []*RequiredBlobFile,
	fieldMappings []message.FieldMapping,
) (map[string]*SourceResult, error) {
	sourceNodeIDs := make(map[string]bool)
	for _, mapping := range fieldMappings {
		if !mapping.IsEventTrigger && mapping.SourceNodeID != "" {
			sourceNodeIDs[mapping.SourceNodeID] = true
		}
	}

	all := make(map[string]*SourceResult)
	for _, f := range requiredFiles {
		// The same per-file read the concurrent path performs, just one at a time. That is
		// what isolates the property under test: any difference between this and
		// downloadAndParseBlobFiles is concurrency, never format or planning.
		parsed, err := s.fetchSourceResults(ctx, f, sourceNodeIDs, fieldMappings)
		if err != nil {
			return nil, fmt.Errorf("resolver: failed to download blob file %s: %w", f.BlobURL, err)
		}
		for nodeID, sr := range parsed {
			all[nodeID] = sr
		}
	}
	return all, nil
}

// asArchive renders a flat document the way CreateResult now stores it.
func asArchive(t *testing.T, doc string) string {
	t.Helper()
	raw, _, err := archive.Build([]byte(doc))
	if err != nil {
		t.Fatalf("archive.Build(%s): %v", doc, err)
	}
	return string(raw)
}

// indexedFamily builds a Shape B flat-key family: one key per item per field, which is
// what an iterated node emits and what the prefix-scan extraction path consumes.
func indexedFamily(nodeID, field string, n int) map[string]interface{} {
	out := make(map[string]interface{}, n*2)
	for i := 0; i < n; i++ {
		out[fmt.Sprintf("%s-/%s[%d]/id", nodeID, field, i)] = float64(i)
		out[fmt.Sprintf("%s-/%s[%d]/name", nodeID, field, i)] = fmt.Sprintf("item-%d", i)
	}
	return out
}

func mustJSON(t *testing.T, v interface{}) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	return string(b)
}

// corpus returns blob bodies and required files covering every shape the extraction
// code branches on, including the two that truncate silently when under-fetched.
func corpus(t *testing.T) (map[string]string, []*RequiredBlobFile, []string) {
	t.Helper()

	// Shape B: an indexed key family, the prefix-scan path.
	iterated := indexedFamily("iter", "rows", 50)

	// Shape A: a whole array under a single key, plus scalars and a nested object.
	shapeA := map[string]interface{}{
		"solo-/name":       "david",
		"solo-/rows":       []interface{}{"a", "b", "c"},
		"solo-/meta/count": float64(3),
		"solo-/error":      "",
	}

	// A file holding two nodes, as an output with embedded nodes merged into it does.
	multi := map[string]interface{}{
		"parentA-/value": "from-parent",
		"childB-/value":  "from-child",
	}

	// The same node again in a later file: the later file must win.
	overlapEarly := map[string]interface{}{"dup-/value": "early"}
	overlapLate := map[string]interface{}{"dup-/value": "late"}

	// Not StandardUnitOutput at all — takes the blob-content-as-is branch.
	plain := map[string]interface{}{"anything": "goes", "n": float64(1)}

	bodies := map[string]string{
		"https://acct/c/iterated.zip": asArchive(t, mustJSON(t, iterated)),
		"https://acct/c/shapea.zip":   asArchive(t, mustJSON(t, shapeA)),
		"https://acct/c/multi.zip":    asArchive(t, mustJSON(t, multi)),
		"https://acct/c/dup1.zip":     asArchive(t, mustJSON(t, overlapEarly)),
		"https://acct/c/dup2.zip":     asArchive(t, mustJSON(t, overlapLate)),
		"https://acct/c/plain.zip":    asArchive(t, mustJSON(t, plain)),
		"https://acct/c/empty.zip":    asArchive(t, "{}"),
	}

	files := []*RequiredBlobFile{
		{BlobURL: "https://acct/c/iterated.zip", ContainsNodes: []string{"iter"}},
		{BlobURL: "https://acct/c/shapea.zip", ContainsNodes: []string{"solo"}},
		{BlobURL: "https://acct/c/multi.zip", ContainsNodes: []string{"parentA", "childB"}},
		{BlobURL: "https://acct/c/dup1.zip", ContainsNodes: []string{"dup"}},
		{BlobURL: "https://acct/c/dup2.zip", ContainsNodes: []string{"dup"}},
		{BlobURL: "https://acct/c/plain.zip", ContainsNodes: []string{"plainNode"}},
		{BlobURL: "https://acct/c/empty.zip", ContainsNodes: []string{"missingNode"}},
		// A node present in the file but never named by a mapping must be filtered out.
		{BlobURL: "https://acct/c/multi.zip", ContainsNodes: []string{"parentA", "unmapped"}},
	}

	nodes := []string{"iter", "solo", "parentA", "childB", "dup", "plainNode", "missingNode"}
	return bodies, files, nodes
}

// The parsed map handed to the array and iterate logic must be identical to what the
// previous algorithm produced. This is the load-bearing assertion: every array
// behaviour downstream is a pure function of this map.
func TestDownloadAndParseMatchesPreviousAlgorithm(t *testing.T) {
	bodies, files, nodes := corpus(t)

	// Repeat: a completion-order dependency would surface intermittently, and a single
	// pass could get lucky.
	for i := 0; i < 40; i++ {
		fake := &fakeBlobClient{bodies: bodies}
		svc := NewService(fake, 0).WithMaxConcurrentBlobDownloads(3)

		got, err := svc.downloadAndParseBlobFiles(context.Background(), files, mappingsFor(nodes...))
		if err != nil {
			t.Fatalf("iteration %d: new path: %v", i, err)
		}

		refFake := &fakeBlobClient{bodies: bodies}
		refSvc := NewService(refFake, 0)
		want, err := sequentialReference(context.Background(), refSvc, files, mappingsFor(nodes...))
		if err != nil {
			t.Fatalf("iteration %d: reference: %v", i, err)
		}

		if !reflect.DeepEqual(got, want) {
			t.Fatalf("iteration %d: parsed source results differ from the previous algorithm\n got keys: %v\nwant keys: %v",
				i, sortedKeys(got), sortedKeys(want))
		}
	}
}

func sortedKeys(m map[string]*SourceResult) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	// Small maps; insertion sort keeps the helper dependency-free.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// The specific failure this whole area risks is a shorter array with no error. Assert
// the full family survives the concurrent download path, by length and by content.
func TestIteratedArrayIsCompleteAfterConcurrentDownload(t *testing.T) {
	const items = 50
	bodies, files, nodes := corpus(t)

	for i := 0; i < 25; i++ {
		fake := &fakeBlobClient{bodies: bodies}
		svc := NewService(fake, 0).WithMaxConcurrentBlobDownloads(2)

		results, err := svc.downloadAndParseBlobFiles(context.Background(), files, mappingsFor(nodes...))
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}

		sr := results["iter"]
		if sr == nil {
			t.Fatalf("iteration %d: no source result for the iterated node", i)
		}

		// Whole-node reconstruction — the root-handle path, and what populates
		// ProjectedFields. Every indexed key must be present or the array comes back
		// short, which is the failure this area risks.
		node := extractNodeDataFromStandardOutputFlat(sr.RawFlatKeys, "iter")
		rows, ok := node["rows"].([]interface{})
		if !ok {
			t.Fatalf("iteration %d: reconstructed rows is %T, want []interface{}", i, node["rows"])
		}
		if len(rows) != items {
			t.Fatalf("iteration %d: reconstruction produced %d rows, want %d — truncated",
				i, len(rows), items)
		}
		for j, row := range rows {
			m, ok := row.(map[string]interface{})
			if !ok {
				t.Fatalf("iteration %d: row %d is %T, want an object", i, j, row)
			}
			if want := fmt.Sprintf("item-%d", j); m["name"] != want {
				t.Fatalf("iteration %d: row %d name = %v, want %q", i, j, m["name"], want)
			}
			if want := float64(j); m["id"] != want {
				t.Fatalf("iteration %d: row %d id = %v, want %v", i, j, m["id"], want)
			}
		}

		// Shape A: a whole array under one key resolves by exact lookup, so it is
		// deterministic and must come back complete and in order.
		solo := results["solo"]
		if solo == nil {
			t.Fatalf("iteration %d: no source result for the single-key node", i)
		}
		got := extractFromFlatKeys(solo.RawFlatKeys, "solo", "/rows", "/out", false)
		arr, ok := got.([]interface{})
		if !ok {
			t.Fatalf("iteration %d: whole-array extraction returned %T, want []interface{}", i, got)
		}
		if len(arr) != 3 || arr[0] != "a" || arr[1] != "b" || arr[2] != "c" {
			t.Fatalf("iteration %d: whole-array extraction = %v, want [a b c]", i, arr)
		}
	}
}

// extractFromFlatKeys' fallback prefix scan collects matches by ranging over a map, so
// before the keys were sorted, every conflict it resolved was decided by Go's randomised
// iteration order: identical input produced four distinct results over 300 runs.
//
// This pins the fix. It matters beyond the obvious, because the plan's primary safety net
// is a differential harness asserting byte-identical output — an input that cannot decide
// what it returns makes that harness flaky, and the tempting response, loosening the
// assertion, would disable the net for every other case too.
func TestFallbackPrefixScanIsDeterministic(t *testing.T) {
	build := func() map[string]interface{} {
		flat := map[string]interface{}{}
		for i := 0; i < 8; i++ {
			flat[fmt.Sprintf("iter-/rows[%d]/id", i)] = float64(i)
			flat[fmt.Sprintf("iter-/rows[%d]/name", i)] = fmt.Sprintf("item-%d", i)
			flat[fmt.Sprintf("iter-/rows[%d]/zeta", i)] = fmt.Sprintf("z-%d", i)
		}
		return flat
	}

	for _, tc := range []struct {
		name     string
		endpoint string
		iterate  bool
	}{
		{"collection path, iterating", "/rows//name", true},
		{"collection path, not iterating", "/rows//name", false},
		{"bare path, iterating", "/rows", true},
		{"root handle", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			distinct := map[string]bool{}
			for run := 0; run < 300; run++ {
				got := extractFromFlatKeys(build(), "iter", tc.endpoint, "/out", tc.iterate)
				b, err := json.Marshal(got)
				if err != nil {
					t.Fatalf("marshal: %v", err)
				}
				distinct[string(b)] = true
			}
			if len(distinct) != 1 {
				t.Fatalf("extraction returned %d distinct results for identical input: %v",
					len(distinct), keysOf(distinct))
			}
		})
	}
}

// Sorting settled which value wins a collision; it did not change that a collision
// happens at all. The fallback still ignores the endpoint path and gathers every indexed
// key under the node, so a record's sibling fields collide on the same slot and all but
// one are dropped — now dropped predictably rather than randomly.
//
// This is a separate, deeper defect from the ordering one, and fixing it means choosing
// between at least three defensible semantics for "/rows//name" over an indexed family:
// the name values, the whole records, or the current single-field-per-slot. That is a
// product decision, so the current behaviour is pinned here rather than guessed at.
func TestFallbackPrefixScanStillDropsSiblingFields_KnownGap(t *testing.T) {
	flat := map[string]interface{}{}
	for i := 0; i < 3; i++ {
		flat[fmt.Sprintf("iter-/rows[%d]/id", i)] = float64(i)
		flat[fmt.Sprintf("iter-/rows[%d]/name", i)] = fmt.Sprintf("item-%d", i)
	}

	got := extractFromFlatKeys(flat, "iter", "/rows//name", "/out", true)
	b, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// "id" sorts before "name", so the id wins every slot and the requested field is the
	// one discarded. Asserted so a future change to the scan is a deliberate, visible act.
	if want := `[0,1,2]`; string(b) != want {
		t.Fatalf("fallback extraction = %s, want %s — the scan's behaviour changed; if that "+
			"was intended, update this test and the completeness rule in the plan together",
			string(b), want)
	}
}

func keysOf(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// End to end through buildInputFromMappings: the bytes a plugin receives must be
// identical on every run, for both an iterated mapping and a whole-array one.
func TestResolvedInputBytesAreStableAcrossRuns(t *testing.T) {
	bodies, files, nodes := corpus(t)

	mappings := []message.FieldMapping{
		{SourceNodeID: "iter", SourceEndpoint: "/rows//name", DestinationEndpoints: []string{"/names"}},
		{SourceNodeID: "iter", SourceEndpoint: "/rows", DestinationEndpoints: []string{"/all"}, Iterate: true},
		{SourceNodeID: "solo", SourceEndpoint: "/rows", DestinationEndpoints: []string{"/wholeArray"}},
		{SourceNodeID: "solo", SourceEndpoint: "/name", DestinationEndpoints: []string{"/name"}},
		{SourceNodeID: "solo", SourceEndpoint: "/meta/count", DestinationEndpoints: []string{"/count"}},
		{SourceNodeID: "dup", SourceEndpoint: "/value", DestinationEndpoints: []string{"/dup"}},
	}
	_ = nodes

	var first string
	for i := 0; i < 30; i++ {
		fake := &fakeBlobClient{bodies: bodies}
		svc := NewService(fake, 0).WithMaxConcurrentBlobDownloads(3)

		results, err := svc.downloadAndParseBlobFiles(context.Background(), files, mappings)
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}

		out, err := buildInputFromMappings(BuildInputParams{
			UnitNodeID:    "consumer",
			FieldMappings: mappings,
			SourceResults: results,
		})
		if err != nil {
			t.Fatalf("iteration %d: buildInputFromMappings: %v", i, err)
		}

		if i == 0 {
			first = string(out)
			// Sanity: the fixtures must actually have produced the arrays, otherwise
			// this test would happily assert that nothing equals nothing.
			if !strings.Contains(first, "item-49") {
				t.Fatalf("fixture did not resolve the full iterated array: %s", truncate(first, 400))
			}
			if !strings.Contains(first, "david") {
				t.Fatalf("fixture did not resolve the scalar mapping: %s", truncate(first, 400))
			}
			continue
		}
		if string(out) != first {
			t.Fatalf("iteration %d: resolved input bytes changed between runs\nfirst: %s\n got: %s",
				i, truncate(first, 400), truncate(string(out), 400))
		}
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
