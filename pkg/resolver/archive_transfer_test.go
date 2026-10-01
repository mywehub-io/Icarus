package resolver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/message"
)

// transferCounter wraps fakeBlobClient and records every byte and request a resolve
// moves, by method. These tests pin what the read path transfers, not only what it
// returns: returning the right input while downloading the blob twice is the failure
// they exist to catch.
type transferCounter struct {
	*fakeBlobClient
	sizeCalls  int
	ranged     int
	wholeGets  int
	movedBytes int64
}

func (c *transferCounter) BlobSize(ctx context.Context, url string) (int64, error) {
	c.sizeCalls++
	return c.fakeBlobClient.BlobSize(ctx, url)
}

func (c *transferCounter) DownloadRange(ctx context.Context, url string, off, n int64) ([]byte, error) {
	b, err := c.fakeBlobClient.DownloadRange(ctx, url, off, n)
	c.ranged++
	c.movedBytes += int64(len(b))
	return b, err
}

func (c *transferCounter) DownloadFromURL(ctx context.Context, url string) ([]byte, error) {
	b, err := c.fakeBlobClient.DownloadFromURL(ctx, url)
	c.wholeGets++
	c.movedBytes += int64(len(b))
	return b, err
}

// rowsDoc is the shape an iterate fan-out reads: many small indexed keys, where the
// directory is a large share of the archive.
func rowsDoc(t *testing.T, node string, n int) []byte {
	t.Helper()
	doc := make(map[string]interface{}, 2*n)
	for i := 0; i < n; i++ {
		doc[fmt.Sprintf("%s-/rows[%d]/name", node, i)] = fmt.Sprintf("record-%06d", i)
		doc[fmt.Sprintf("%s-/rows[%d]/body", node, i)] = largeString(160)
	}
	b, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func readCounted(t *testing.T, doc []byte, nodes []string, mappings []message.FieldMapping) (*transferCounter, int64, []byte) {
	t.Helper()
	fake, url := serveArchive(t, doc)
	c := &transferCounter{fakeBlobClient: fake}
	svc := NewService(c, 0)

	got, err := svc.downloadAndParseBlobFiles(context.Background(),
		[]*RequiredBlobFile{{BlobURL: url, ContainsNodes: nodes}}, mappings)
	if err != nil {
		t.Fatalf("archive read: %v", err)
	}

	ids := map[string]bool{}
	for _, n := range nodes {
		ids[n] = true
	}
	want := resolveVia(t, sourceResultsFromDocument(t, doc, ids, nodes), mappings)
	resolved := resolveVia(t, got, mappings)
	if !bytes.Equal(resolved, want) {
		t.Fatalf("archive read resolved different input than the document read\n got %.200s\nwant %.200s", resolved, want)
	}
	return c, int64(len(fake.bodies[url])), resolved
}

// Every node in the file read whole: the answer is the whole blob, so it is fetched in
// one GET with no size lookup, directory or manifest read first. It used to move the
// directory twice: about 1.33x the blob in 15 requests for this shape.
func TestWholeNodeReadIsOneGet(t *testing.T) {
	doc := rowsDoc(t, "src", 2000)
	c, blob, resolved := readCounted(t, doc, []string{"src"}, []message.FieldMapping{
		{SourceNodeID: "src", SourceEndpoint: "/rows", DestinationEndpoints: []string{"/rows"}, Iterate: true},
	})

	if c.sizeCalls != 0 || c.ranged != 0 || c.wholeGets != 1 {
		t.Fatalf("size lookups %d, ranged %d, whole %d; want 0, 0, 1", c.sizeCalls, c.ranged, c.wholeGets)
	}
	if c.movedBytes != blob {
		t.Fatalf("moved %d bytes for a %d-byte blob", c.movedBytes, blob)
	}
	if !bytes.Contains(resolved, []byte("record-001999")) {
		t.Fatal("the last row did not resolve; the comparison is vacuous")
	}
}

// One node read whole and one by key: the shortcut must not fire, because the
// exact-key node may need a fraction of its keys.
func TestShortcutNeedsEveryNodeReadWhole(t *testing.T) {
	doc := buildFixtureDoc(t)
	c, _, _ := readCounted(t, doc, []string{"solo", "iter"}, []message.FieldMapping{
		{SourceNodeID: "iter", SourceEndpoint: "/rows", DestinationEndpoints: []string{"/all"}, Iterate: true},
		{SourceNodeID: "solo", SourceEndpoint: "/name", DestinationEndpoints: []string{"/name"}},
	})
	if c.sizeCalls != 1 || c.wholeGets != 0 {
		t.Fatalf("size lookups %d, whole gets %d; want the archive planned (1, 0)", c.sizeCalls, c.wholeGets)
	}
}

// The planner picks a whole-file read after opening the archive: a key lookup selecting
// most of the payload. The branch now fetches only the entries it does not hold yet, so
// the read moves no more than the blob. It used to download the whole blob on top of the
// directory it had already read.
func TestWholeFileBranchDoesNotRedownloadTheDirectory(t *testing.T) {
	doc := rowsDoc(t, "src", 2000)
	var m map[string]interface{}
	if err := json.Unmarshal(doc, &m); err != nil {
		t.Fatal(err)
	}
	mappings := make([]message.FieldMapping, 0, len(m))
	for k := range m {
		mappings = append(mappings, message.FieldMapping{
			SourceNodeID: "src", SourceEndpoint: k[len("src-"):], DestinationEndpoints: []string{"/" + k},
		})
	}

	c, blob, _ := readCounted(t, doc, []string{"src"}, mappings)
	if c.wholeGets != 0 {
		t.Fatalf("whole-blob GETs = %d, want 0", c.wholeGets)
	}
	// archive/zip probes the last 1 KiB for the end-of-directory record and the directory
	// scan then reads that tail again. That is the cost of opening any archive, not of
	// this branch, so it is the one overlap allowed.
	const zipTailProbe = 1024
	if c.movedBytes > blob+zipTailProbe {
		t.Fatalf("moved %d bytes for a %d-byte blob (%.3fx); the directory was fetched twice",
			c.movedBytes, blob, float64(c.movedBytes)/float64(blob))
	}
	if c.ranged > 8 {
		t.Fatalf("%d ranged requests; the entry region should be one", c.ranged)
	}
}

func TestEveryNodeReadWhole(t *testing.T) {
	iter := message.FieldMapping{SourceNodeID: "a", SourceEndpoint: "/rows", Iterate: true}
	root := message.FieldMapping{SourceNodeID: "b", SourceEndpoint: ""}
	slash := message.FieldMapping{SourceNodeID: "b", SourceEndpoint: "/"}
	exact := message.FieldMapping{SourceNodeID: "b", SourceEndpoint: "/v"}
	event := message.FieldMapping{SourceNodeID: "b", SourceEndpoint: "", IsEventTrigger: true}

	cases := []struct {
		name  string
		nodes []string
		maps  []message.FieldMapping
		want  bool
	}{
		{"iterate and root", []string{"a", "b"}, []message.FieldMapping{iter, root}, true},
		{"slash is root", []string{"b"}, []message.FieldMapping{slash}, true},
		{"root wins over an exact mapping on the same node", []string{"b"}, []message.FieldMapping{exact, root}, true},
		{"exact only", []string{"b"}, []message.FieldMapping{exact}, false},
		{"a node with no mapping", []string{"a", "b"}, []message.FieldMapping{iter}, false},
		{"event triggers do not count", []string{"b"}, []message.FieldMapping{event}, false},
		{"no nodes", nil, []message.FieldMapping{iter}, false},
	}
	for _, tc := range cases {
		if got := everyNodeReadWhole(tc.nodes, tc.maps); got != tc.want {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}
