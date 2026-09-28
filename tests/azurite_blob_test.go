package tests

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wehubfusion/Icarus/pkg/archive"
	"github.com/wehubfusion/Icarus/pkg/message"
	"github.com/wehubfusion/Icarus/pkg/resolver"
	"github.com/wehubfusion/Icarus/pkg/storage"
	"go.uber.org/zap"
)

// These tests run against Azurite, the Azure Storage emulator, rather than the
// byte-slice fake in blob_reader_at_test.go.
//
// The distinction matters more than it looks. The fake was written to match one
// reading of the Azure SDK, so it cannot disagree with that reading — it will happily
// confirm a wrong assumption about what Count: 0 means on the wire, or about what a
// range past the end of a blob returns. Azurite implements the real REST surface and
// can disagree. Everything asserted here is a property BlobReaderAt depends on and
// could not otherwise verify without a storage account.
//
// Bring the emulator up with:
//
//	docker compose -f Olympus/scripts/local/docker-compose.yml up -d azurite
//
// When it is not listening these tests skip, so `go test ./...` stays green without it.

const (
	azuriteEndpoint = "127.0.0.1:10000"
	azuriteAccount  = "localdev"
	azuriteKey      = "bG9jYWxkZXZrZXlsb2NhbGRldmtleWxvY2FsZGV2a2V5bG9jYWxkZXY="
	azuriteProbeTTL = 10 * time.Second
)

// azuriteConnectionString honours AZURITE_CONNECTION_STRING when set, and otherwise
// targets the olympus-azurite service as docker-compose.yml defines it. Note the
// /localdev suffix on the endpoint: the compose file overrides AZURITE_ACCOUNTS with a
// named account, so the default devstoreaccount1 path does not apply.
//
// The override exists because these tests are also run from inside a container joined
// to olympus-network, where the emulator answers to http://azurite:10000 instead.
func azuriteConnectionString() string {
	if s := os.Getenv("AZURITE_CONNECTION_STRING"); s != "" {
		return s
	}
	return fmt.Sprintf(
		"DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;BlobEndpoint=http://%s/%s;",
		azuriteAccount, azuriteKey, azuriteEndpoint, azuriteAccount,
	)
}

// newAzuriteClient returns a client against a fresh container, or skips when the
// emulator is not reachable.
//
// The reachability check is a real authenticated write, not a TCP dial. A dial is not
// good enough and that is not hypothetical: on the machine this was first run, Azurite
// accepted TCP connections on the published port while no request ever reached the
// service, so a dial-based gate reported the emulator as present and every test then
// hung until the suite timed out. A probe has to exercise the thing being probed.
func newAzuriteClient(t *testing.T) (*storage.AzureBlobClient, string) {
	t.Helper()

	// A container per test keeps runs independent and makes leftovers attributable.
	container := "icarus-test-" + strings.ToLower(uuid.NewString()[:8])

	client, err := storage.NewAzureBlobClient(azuriteConnectionString(), container, zap.NewNop())
	if err != nil {
		t.Fatalf("NewAzureBlobClient: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), azuriteProbeTTL)
	defer cancel()

	if _, err := client.UploadResult(ctx, "probe", []byte("{}"), nil); err != nil {
		// Unreachable means skip; reachable but refusing means fail, because that is a
		// real defect rather than a missing dependency.
		if isUnreachable(err) {
			t.Skipf("Azurite is not reachable; start it with "+
				"`docker compose -f Olympus/scripts/local/docker-compose.yml up -d azurite` (%v)", err)
		}
		t.Fatalf("Azurite is reachable but rejected a probe write: %v", err)
	}
	return client, container
}

// isUnreachable distinguishes "the emulator is not there" from "the emulator said no".
func isUnreachable(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}
	s := err.Error()
	return strings.Contains(s, "connection refused") ||
		strings.Contains(s, "no such host") ||
		strings.Contains(s, "context deadline exceeded") ||
		strings.Contains(s, "dial tcp")
}

func mustUpload(t *testing.T, c *storage.AzureBlobClient, path string, data []byte) string {
	t.Helper()
	url, err := c.UploadResult(context.Background(), path, data, nil)
	if err != nil {
		t.Fatalf("UploadResult(%s): %v", path, err)
	}
	return url
}

// DownloadRange's contract against the real REST surface. Each case here is a property
// BlobReaderAt relies on; the fake could only ever echo back the assumption.
func TestAzuriteDownloadRangeSemantics(t *testing.T) {
	client, _ := newAzuriteClient(t)
	ctx := context.Background()

	data := []byte("0123456789abcdefghijklmnopqrstuvwxyz") // 36 bytes
	url := mustUpload(t, client, "range/basic.bin", data)

	t.Run("exact interior range", func(t *testing.T) {
		got, err := client.DownloadRange(ctx, url, 10, 6)
		if err != nil {
			t.Fatalf("DownloadRange: %v", err)
		}
		if want := data[10:16]; !bytes.Equal(got, want) {
			t.Fatalf("got %q, want %q", got, want)
		}
	})

	// BlobReaderAt never issues this itself — fetchLocked always passes an explicit,
	// clamped count — but zero-means-to-the-end is the documented SDK behaviour and the
	// interface comment promises it, so it is asserted rather than assumed.
	t.Run("zero count reads to the end", func(t *testing.T) {
		got, err := client.DownloadRange(ctx, url, 30, 0)
		if err != nil {
			t.Fatalf("DownloadRange: %v", err)
		}
		if want := data[30:]; !bytes.Equal(got, want) {
			t.Fatalf("got %q, want %q", got, want)
		}
	})

	t.Run("zero offset zero count reads the whole blob", func(t *testing.T) {
		got, err := client.DownloadRange(ctx, url, 0, 0)
		if err != nil {
			t.Fatalf("DownloadRange: %v", err)
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("got %d bytes, want the whole %d byte blob", len(got), len(data))
		}
	})

	// A count running past the end must clamp rather than fail. BlobReaderAt clamps to
	// the size it was constructed with, so this only bites when that size is stale —
	// which is exactly the overwrite case the plan's R4 ETag guard is for. Knowing
	// whether the storage layer clamps or rejects decides whether that failure is a
	// short read or a hard error.
	t.Run("count past the end clamps to the blob", func(t *testing.T) {
		got, err := client.DownloadRange(ctx, url, 30, 1000)
		if err != nil {
			t.Fatalf("DownloadRange past end returned an error rather than clamping: %v", err)
		}
		if want := data[30:]; !bytes.Equal(got, want) {
			t.Fatalf("got %q, want %q", got, want)
		}
	})

	// An offset at or beyond the end is the genuinely ambiguous case: it may clamp to
	// empty or fail with 416 InvalidRange. archive/zip probes near the tail of a file,
	// so whichever it is has to be known rather than guessed. The assertion is
	// deliberately loose — it pins down that ONE of the two happens and records which,
	// rather than baking in a behaviour Azure might not share with Azurite.
	t.Run("offset at or past the end is empty or InvalidRange", func(t *testing.T) {
		for _, off := range []int64{int64(len(data)), int64(len(data)) + 50} {
			got, err := client.DownloadRange(ctx, url, off, 16)
			switch {
			case err != nil:
				if !strings.Contains(err.Error(), "InvalidRange") && !strings.Contains(err.Error(), "416") {
					t.Fatalf("offset %d: unexpected error %v; want InvalidRange/416 or an empty read", off, err)
				}
				t.Logf("offset %d past EOF -> error (%s)", off, firstLine(err.Error()))
			case len(got) != 0:
				t.Fatalf("offset %d: got %d bytes past the end of a %d byte blob", off, len(got), len(data))
			default:
				t.Logf("offset %d past EOF -> empty read, no error", off)
			}
		}
	})

	t.Run("rejects a negative offset without a round trip", func(t *testing.T) {
		if _, err := client.DownloadRange(ctx, url, -1, 4); err == nil {
			t.Fatal("expected an error for a negative offset")
		}
	})
}

// UploadStream has never had a caller, so this is the first thing to exercise its
// option shape at all. A wrong shape here would not surface until a plugin adopted it.
func TestAzuriteUploadStreamRoundTrip(t *testing.T) {
	client, _ := newAzuriteClient(t)
	ctx := context.Background()

	// Larger than the SDK's default block size so the multi-block path is the one tested.
	payload := bytes.Repeat([]byte("wehub-payload-"), 400_000) // ~5.6 MB

	url, err := client.UploadStream(ctx, "stream/large.zip", bytes.NewReader(payload),
		"application/zip", map[string]string{"node_id": "abc", "run_id": "r1"})
	if err != nil {
		t.Fatalf("UploadStream: %v", err)
	}
	if url == "" {
		t.Fatal("UploadStream returned an empty URL")
	}

	got, err := client.DownloadFromURL(ctx, url)
	if err != nil {
		t.Fatalf("DownloadFromURL: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("round trip mismatch: got %d bytes, want %d", len(got), len(payload))
	}

	// Ranged reads must work against a streamed upload exactly as against a buffered
	// one. If UploadStream ever produced something the range reader could not address,
	// the archive write path would be broken on the day it landed.
	mid, err := client.DownloadRange(ctx, url, 1_000_000, 14)
	if err != nil {
		t.Fatalf("DownloadRange over a streamed blob: %v", err)
	}
	if want := payload[1_000_000 : 1_000_000+14]; !bytes.Equal(mid, want) {
		t.Fatalf("ranged read of streamed blob = %q, want %q", mid, want)
	}

	if _, err := client.UploadStream(ctx, "stream/nil.bin", nil, "", nil); err == nil {
		t.Fatal("expected an error for a nil body")
	}
}

// ensureContainer used to check and set containerInit with no synchronisation, and one
// client is shared across concurrently executing units, so every first upload raced. This
// drives real concurrent uploads through a fresh client, which is what makes the race
// detector able to see it — a single-threaded upload test never could.
func TestAzuriteConcurrentFirstUploadsAreSafe(t *testing.T) {
	client, _ := newAzuriteClient(t)

	const writers = 12
	var wg sync.WaitGroup
	errs := make([]error, writers)

	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = client.UploadResult(context.Background(),
				fmt.Sprintf("concurrent/%d.json", idx),
				[]byte(fmt.Sprintf(`{"writer":%d}`, idx)), nil)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent upload %d failed: %v", i, err)
		}
	}
}

// The whole change end to end against real storage: a node result written through the
// resolver becomes an archive, and a downstream unit mapping two named fields out of it
// resolves the same values while moving a fraction of the blob.
//
// This is the acceptance test the design exists for. The in-memory tests prove the format
// and the fetch planner; only this proves they survive a real upload, a real central
// directory read and real ranged GETs.
func TestAzuriteResultArchiveRoundTripThroughTheResolver(t *testing.T) {
	client, _ := newAzuriteClient(t)
	ctx := context.Background()

	// Shape A: a few named fields beside one large value nothing downstream reads.
	doc := map[string]interface{}{
		"up-/name":       "david",
		"up-/meta/count": 2,
		"up-/rows":       []interface{}{map[string]interface{}{"name": "a"}, map[string]interface{}{"name": "b"}},
		"up-/payload":    strings.Repeat("A", 4<<20),
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// maxInlineBytes of 1 forces the blob branch regardless of payload size.
	svc := resolver.NewService(client, 1)

	res, err := svc.CreateResult(ctx, raw, resolver.ResultMeta{
		WorkflowID:  "wf1",
		RunID:       "run1",
		NodeID:      "up",
		ExecutionID: "exec1",
	})
	if err != nil {
		t.Fatalf("CreateResult: %v", err)
	}
	if res.BlobReference == nil {
		t.Fatal("result did not go to blob")
	}
	if !strings.HasSuffix(res.BlobReference.URL, ".zip") {
		t.Fatalf("result blob is %q, want an archive", res.BlobReference.URL)
	}

	// A downstream unit reading two named fields and the array's name column.
	mappings := []message.FieldMapping{
		{SourceNodeID: "up", SourceEndpoint: "/name", DestinationEndpoints: []string{"/name"}},
		{SourceNodeID: "up", SourceEndpoint: "/meta/count", DestinationEndpoints: []string{"/count"}},
		{SourceNodeID: "up", SourceEndpoint: "/rows//name", DestinationEndpoints: []string{"/names"}},
	}

	graph := resolver.NewConsumerGraph()
	graph.AddRequiredFile("exec1", res.BlobReference.URL, "", []string{"up"})

	out, err := svc.ResolveMappedInputWithConsumerGraph(ctx, nil, nil, &resolver.FieldMappingParams{
		FieldMappings: mappings,
		ConsumerGraph: graph,
	}, graph)
	if err != nil {
		t.Fatalf("resolve through consumer graph: %v", err)
	}

	// A "//" mapping whose destinations are all simple paths fans the result out into one
	// object per collection element, carrying the scalar mappings alongside. That shape is
	// existing buildInputFromMappings behaviour and none of this change touches it, so the
	// assertion is on the values rather than on the container they arrive in.
	if !json.Valid(out) {
		t.Fatalf("resolved input is not valid JSON: %s", truncateFor(out, 300))
	}
	for _, want := range []string{`"name":"david"`, `"count":2`, `"names":"a"`, `"names":"b"`} {
		if !bytes.Contains(out, []byte(want)) {
			t.Fatalf("resolved input is missing %s\nfull: %s", want, truncateFor(out, 600))
		}
	}

	// The point of the whole exercise: the 4MB value no mapping asked for must not appear
	// in the plugin's input, and must never have been fetched to get there.
	if bytes.Contains(out, []byte(strings.Repeat("A", 1024))) {
		t.Fatal("the unread 4MB value leaked into the resolved input")
	}
	if len(out) > 64*1024 {
		t.Fatalf("resolved input is %d bytes for four small fields; the large value was pulled in", len(out))
	}
}

// The trigger path, end to end against real storage: an MLLP message offloaded through
// CreateResult is an archive like everything else, and what a plugin is handed back is the
// message byte for byte.
//
// This is the case that makes "everything in blob is an archive" safe to say. Artemis
// uploads the body BEFORE writing the acknowledgement, precisely so no MSA|AA is sent for a
// message that is not yet durable, so a refusal or a corruption here costs a message rather
// than a field — and an HL7 message re-serialised as JSON, or handed over as a ZIP, is not
// something the parser downstream would report cleanly.
func TestAzuriteTriggerPayloadsRoundTripThroughTheArchive(t *testing.T) {
	client, _ := newAzuriteClient(t)
	ctx := context.Background()

	// A realistic oversized MLLP body: repeated OBX segments, carriage returns and all.
	var hl7 bytes.Buffer
	hl7.WriteString("MSH|^~\\&|SENDER|FAC|RECV|FAC|20260928||ORU^R01|MSG0001|P|2.5\r")
	hl7.WriteString("PID|1||12345^^^FAC^MR||SMITH^JOHN||19700101|M\r")
	for i := 0; i < 40000; i++ {
		fmt.Fprintf(&hl7, "OBX|%d|NM|GLU^Glucose||%d.2|mmol/L|||||F\r", i+1, i%20)
	}

	cases := []struct {
		name    string
		nodeID  string
		payload []byte
	}{
		{"mllp hl7", "mllp-trigger", hl7.Bytes()},
		// An HTTP trigger body is not a node output even when it is a JSON object, and its
		// bytes are the caller's: key order and whitespace must survive untouched.
		{"http json body", "http-trigger", []byte("{ \"zebra\" : 1,\n  \"apple\": 2, \"pad\": \"" + strings.Repeat("p", 1<<20) + "\" }")},
	}

	// maxInlineBytes of 1 forces the blob branch regardless of payload size.
	svc := resolver.NewService(client, 1)

	for _, tc := range cases {
		res, err := svc.CreateResult(ctx, tc.payload, resolver.ResultMeta{
			WorkflowID:  "wf-trigger",
			RunID:       "run-" + tc.nodeID,
			NodeID:      tc.nodeID,
			ExecutionID: "trigger-" + tc.nodeID,
		})
		if err != nil {
			t.Fatalf("%s: CreateResult: %v", tc.name, err)
		}
		if res.BlobReference == nil {
			t.Fatalf("%s: trigger payload did not go to blob", tc.name)
		}
		if !strings.HasSuffix(res.BlobReference.URL, ".zip") {
			t.Fatalf("%s: stored at %q, want an archive", tc.name, res.BlobReference.URL)
		}

		// What is actually in the container is a ZIP, not the bare payload.
		stored, err := client.DownloadResult(ctx, res.BlobReference.URL)
		if err != nil {
			t.Fatalf("%s: download stored blob: %v", tc.name, err)
		}
		if !archive.IsArchive(stored) {
			t.Fatalf("%s: what reached storage is not an archive", tc.name)
		}
		if len(stored) != res.BlobReference.SizeBytes {
			t.Fatalf("%s: SizeBytes %d but %d bytes stored; a ranged open would 416",
				tc.name, res.BlobReference.SizeBytes, len(stored))
		}

		// And what a plugin receives is the body the caller sent.
		got, err := svc.ResolveInput(ctx, nil, &message.BlobReference{
			URL:       res.BlobReference.URL,
			SizeBytes: res.BlobReference.SizeBytes,
		})
		if err != nil {
			t.Fatalf("%s: ResolveInput: %v", tc.name, err)
		}
		if !bytes.Equal(got, tc.payload) {
			t.Fatalf("%s: payload changed between write and read (%d bytes out, %d back)",
				tc.name, len(tc.payload), len(got))
		}
	}
}

func truncateFor(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "…"
}

// The thesis of the whole change, end to end against a real storage API: write an
// archive, open it over ranged GETs, pull one small entry out of a large one, and move
// a small fraction of the blob.
//
// blob_reader_at_test.go proves this against an in-memory fake. What that cannot prove
// is that it survives real HTTP range requests, real response framing and a real
// multi-block upload — which is the only form in which it will ever actually run.
func TestAzuriteArchivePointLookupOverRangedGets(t *testing.T) {
	client, _ := newAzuriteClient(t)
	ctx := context.Background()

	// Shape A: a few keys, one holding an enormous value.
	const bigSize = 8 << 20
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	entries := [][2]string{
		{"abc-/error", `""`},
		{"abc-/name", `"david"`},
		{"abc-/data//name", `"nested"`},
		{"abc-/rows", `"` + strings.Repeat("A", bigSize) + `"`},
	}
	for _, e := range entries {
		w, err := zw.CreateHeader(&zip.FileHeader{Name: e[0], Method: zip.Store})
		if err != nil {
			t.Fatalf("CreateHeader(%q): %v", e[0], err)
		}
		if _, err := io.WriteString(w, e[1]); err != nil {
			t.Fatalf("write %q: %v", e[0], err)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("zip close: %v", err)
	}
	raw := buf.Bytes()

	// Write it the way the format change will: streamed, not buffered.
	url, err := client.UploadStream(ctx, "archive/node.zip", bytes.NewReader(raw), "application/zip", nil)
	if err != nil {
		t.Fatalf("UploadStream: %v", err)
	}

	// Size comes from BlobReference.SizeBytes in production; here from what was written.
	// Either way no HEAD request is needed, which is the point.
	r, err := storage.NewBlobReaderAt(ctx, client, url, int64(len(raw)))
	if err != nil {
		t.Fatalf("NewBlobReaderAt: %v", err)
	}

	zr, err := zip.NewReader(r, int64(len(raw)))
	if err != nil {
		t.Fatalf("zip.NewReader over ranged GETs: %v", err)
	}

	// Entry names must come back byte-identical over the wire, including the "//" form
	// that some ZIP implementations normalise. Plan risk 9, now proven against real
	// storage rather than an in-memory buffer.
	if len(zr.File) != len(entries) {
		t.Fatalf("got %d entries, want %d", len(zr.File), len(entries))
	}
	for i, f := range zr.File {
		if f.Name != entries[i][0] {
			t.Errorf("entry %d name = %q, want %q", i, f.Name, entries[i][0])
		}
	}

	got := readEntry(t, zr, "abc-/name")
	if got != `"david"` {
		t.Fatalf("point lookup = %q, want %q", got, `"david"`)
	}
	if got := readEntry(t, zr, "abc-/data//name"); got != `"nested"` {
		t.Fatalf("double-slash key lookup = %q, want %q", got, `"nested"`)
	}

	fetched, requests := r.Stats()
	if fetched >= int64(len(raw)) {
		t.Fatalf("moved %d bytes of a %d byte archive — no better than a full download", fetched, len(raw))
	}
	if ratio := float64(fetched) / float64(len(raw)); ratio > 0.10 {
		t.Fatalf("moved %.2f%% of the archive (%d of %d bytes) in %d ranged GETs; expected well under 10%%",
			ratio*100, fetched, len(raw), requests)
	}
	t.Logf("two point lookups moved %d of %d bytes (%.3f%%) in %d ranged GETs against Azurite",
		fetched, len(raw), float64(fetched)/float64(len(raw))*100, requests)

	// And the capability the plan's Tier 2 acceptance test names: consume an entry far
	// larger than the read buffer without materialising it.
	streamed := streamEntry(t, zr, "abc-/rows", 32*1024)
	if streamed != bigSize+2 { // the value plus its two JSON quotes
		t.Fatalf("streamed %d bytes, want %d", streamed, bigSize+2)
	}
}

func readEntry(t *testing.T, zr *zip.Reader, name string) string {
	t.Helper()
	for _, f := range zr.File {
		if f.Name != name {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			t.Fatalf("open %q: %v", name, err)
		}
		defer rc.Close()
		b, err := io.ReadAll(rc)
		if err != nil {
			t.Fatalf("read %q: %v", name, err)
		}
		return string(b)
	}
	t.Fatalf("entry %q not found", name)
	return ""
}

// streamEntry consumes an entry through a fixed buffer and returns the byte count,
// verifying content as it goes so a silent truncation cannot pass.
func streamEntry(t *testing.T, zr *zip.Reader, name string, bufSize int) int {
	t.Helper()
	for _, f := range zr.File {
		if f.Name != name {
			continue
		}
		rc, err := f.Open()
		if err != nil {
			t.Fatalf("open %q: %v", name, err)
		}
		defer rc.Close()

		buf := make([]byte, bufSize)
		total := 0
		for {
			n, err := rc.Read(buf)
			for i := 0; i < n; i++ {
				if c := buf[i]; c != 'A' && c != '"' {
					t.Fatalf("corrupt byte %q at offset %d", c, total+i)
				}
			}
			total += n
			if err == io.EOF {
				return total
			}
			if err != nil {
				t.Fatalf("read %q: %v", name, err)
			}
		}
	}
	t.Fatalf("entry %q not found", name)
	return 0
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
