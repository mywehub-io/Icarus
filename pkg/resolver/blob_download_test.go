package resolver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wehubfusion/Icarus/pkg/message"
)

// fakeBlobClient records concurrency and serves canned bodies per URL.
type fakeBlobClient struct {
	bodies map[string]string
	errs   map[string]error
	// uploads records what UploadResult was given, so a test can assert on the bytes that
	// actually reached storage rather than on what the caller intended to write.
	uploads map[string][]byte

	inFlight atomic.Int32
	peak     atomic.Int32
	hold     time.Duration
}

// enter records one in-flight read and applies the configured hold. Every read method
// goes through it, so concurrency and cancellation are measured whichever read the
// resolver chooses: the archive path reaches a blob through DownloadRange as well as
// DownloadResult, and a fake that held only one of them would test the route, not the
// bound.
func (f *fakeBlobClient) enter(ctx context.Context) (func(), error) {
	cur := f.inFlight.Add(1)
	for {
		peak := f.peak.Load()
		if cur <= peak || f.peak.CompareAndSwap(peak, cur) {
			break
		}
	}
	leave := func() { f.inFlight.Add(-1) }

	if f.hold > 0 {
		select {
		case <-time.After(f.hold):
		case <-ctx.Done():
			leave()
			return nil, ctx.Err()
		}
	}
	return leave, nil
}

func (f *fakeBlobClient) DownloadResult(ctx context.Context, blobURL string) ([]byte, error) {
	leave, err := f.enter(ctx)
	if err != nil {
		return nil, err
	}
	defer leave()

	if err, ok := f.errs[blobURL]; ok {
		return nil, err
	}
	body, ok := f.bodies[blobURL]
	if !ok {
		return nil, fmt.Errorf("no body for %s", blobURL)
	}
	return []byte(body), nil
}

func (f *fakeBlobClient) UploadResult(_ context.Context, blobPath string, data []byte, _ map[string]string) (string, error) {
	url := "https://acct/c/" + blobPath
	if f.uploads != nil {
		stored := make([]byte, len(data))
		copy(stored, data)
		f.uploads[url] = stored
	}
	if f.bodies != nil {
		f.bodies[url] = string(data)
	}
	return url, nil
}
func (f *fakeBlobClient) DownloadFromURL(ctx context.Context, blobURL string) ([]byte, error) {
	return f.DownloadResult(ctx, blobURL)
}
func (f *fakeBlobClient) DownloadRange(ctx context.Context, blobURL string, offset, count int64) ([]byte, error) {
	leave, err := f.enter(ctx)
	if err != nil {
		return nil, err
	}
	defer leave()

	// Injected failures apply to every read method, not just the whole-object one. The
	// archive path reaches a blob through BlobSize and DownloadRange, so a fake that only
	// failed DownloadResult would report a "missing body" error instead of the one the
	// test injected, and an error-precedence assertion would be testing the fake.
	if err, ok := f.errs[blobURL]; ok {
		return nil, err
	}
	body, ok := f.bodies[blobURL]
	if !ok {
		return nil, fmt.Errorf("no body for %s", blobURL)
	}
	if offset >= int64(len(body)) {
		return nil, nil
	}
	end := offset + count
	if count == 0 || end > int64(len(body)) {
		end = int64(len(body))
	}
	return []byte(body[offset:end]), nil
}

func (f *fakeBlobClient) BlobSize(_ context.Context, blobURL string) (int64, error) {
	if err, ok := f.errs[blobURL]; ok {
		return 0, err
	}
	body, ok := f.bodies[blobURL]
	if !ok {
		return 0, fmt.Errorf("no body for %s", blobURL)
	}
	return int64(len(body)), nil
}
func (f *fakeBlobClient) UploadStream(context.Context, string, io.Reader, string, map[string]string) (string, error) {
	return "", errors.New("not used")
}

func mappingsFor(nodeIDs ...string) []message.FieldMapping {
	out := make([]message.FieldMapping, 0, len(nodeIDs))
	for _, n := range nodeIDs {
		out = append(out, message.FieldMapping{
			SourceNodeID:         n,
			SourceEndpoint:       "/v",
			DestinationEndpoints: []string{"/v"},
		})
	}
	return out
}

// A unit fanning in from many blob-backed sources must not open them all at once.
// Unbounded fan-out made peak memory the sum of every source payload.
func TestDownloadAndParseBlobFilesBoundsConcurrency(t *testing.T) {
	const files = 12
	const limit = 3

	fake := &fakeBlobClient{bodies: map[string]string{}, hold: 20 * time.Millisecond}
	required := make([]*RequiredBlobFile, 0, files)
	nodes := make([]string, 0, files)
	for i := 0; i < files; i++ {
		node := fmt.Sprintf("n%d", i)
		url := fmt.Sprintf("https://acct/c/%d.json", i)
		fake.bodies[url] = asArchive(t, fmt.Sprintf(`{"%s-/v":"x"}`, node))
		required = append(required, &RequiredBlobFile{BlobURL: url, ContainsNodes: []string{node}})
		nodes = append(nodes, node)
	}

	svc := NewService(fake, 0).WithMaxConcurrentBlobDownloads(limit)
	got, err := svc.downloadAndParseBlobFiles(context.Background(), required, mappingsFor(nodes...))
	if err != nil {
		t.Fatalf("downloadAndParseBlobFiles: %v", err)
	}
	if len(got) != files {
		t.Fatalf("got %d source results, want %d", len(got), files)
	}
	if peak := fake.peak.Load(); peak > limit {
		t.Fatalf("peak concurrent downloads = %d, limit was %d", peak, limit)
	}
	if peak := fake.peak.Load(); peak < 2 {
		t.Fatalf("peak concurrency = %d; downloads appear to have serialised, losing the parallelism", peak)
	}
}

// Two files carrying the same node must resolve the same way they did when downloads
// were sequential: later file wins. Concurrency must not make this depend on which
// download happens to finish first.
func TestDownloadAndParseBlobFilesMergeOrderIsStable(t *testing.T) {
	fake := &fakeBlobClient{bodies: map[string]string{
		"https://acct/c/first.json":  asArchive(t, `{"n-/v":"first"}`),
		"https://acct/c/second.json": asArchive(t, `{"n-/v":"second"}`),
	}}

	required := []*RequiredBlobFile{
		{BlobURL: "https://acct/c/first.json", ContainsNodes: []string{"n"}},
		{BlobURL: "https://acct/c/second.json", ContainsNodes: []string{"n"}},
	}

	svc := NewService(fake, 0)
	// Repeat: a completion-order dependency would show up intermittently.
	for i := 0; i < 50; i++ {
		got, err := svc.downloadAndParseBlobFiles(context.Background(), required, mappingsFor("n"))
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		sr := got["n"]
		if sr == nil {
			t.Fatalf("iteration %d: no source result for n", i)
		}
		if v := sr.RawFlatKeys["n-/v"]; v != "second" {
			t.Fatalf("iteration %d: merged value = %v, want the later file's value", i, v)
		}
	}
}

// The reported error must be the earliest failing file by position, not whichever
// goroutine lost the race, so the same input produces the same message every time.
func TestDownloadAndParseBlobFilesReportsLowestIndexError(t *testing.T) {
	fake := &fakeBlobClient{
		bodies: map[string]string{"https://acct/c/ok.json": asArchive(t, `{"n-/v":"x"}`)},
		errs: map[string]error{
			"https://acct/c/bad1.json": errors.New("first failure"),
			"https://acct/c/bad2.json": errors.New("second failure"),
		},
	}
	required := []*RequiredBlobFile{
		{BlobURL: "https://acct/c/ok.json", ContainsNodes: []string{"n"}},
		{BlobURL: "https://acct/c/bad1.json", ContainsNodes: []string{"n"}},
		{BlobURL: "https://acct/c/bad2.json", ContainsNodes: []string{"n"}},
	}

	svc := NewService(fake, 0)
	for i := 0; i < 25; i++ {
		_, err := svc.downloadAndParseBlobFiles(context.Background(), required, mappingsFor("n"))
		if err == nil {
			t.Fatalf("iteration %d: expected an error", i)
		}
		if got := err.Error(); !strings.Contains(got, "first failure") {
			t.Fatalf("iteration %d: error = %q, want the lowest-index failure", i, got)
		}
	}
}

func TestDownloadAndParseBlobFilesHonoursCancellation(t *testing.T) {
	fake := &fakeBlobClient{bodies: map[string]string{}, hold: time.Second}
	required := make([]*RequiredBlobFile, 0, 8)
	for i := 0; i < 8; i++ {
		url := fmt.Sprintf("https://acct/c/%d.json", i)
		fake.bodies[url] = asArchive(t, `{"n-/v":"x"}`)
		required = append(required, &RequiredBlobFile{BlobURL: url, ContainsNodes: []string{"n"}})
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	if _, err := NewService(fake, 0).WithMaxConcurrentBlobDownloads(2).
		downloadAndParseBlobFiles(ctx, required, mappingsFor("n")); err == nil {
		t.Fatal("expected cancellation to surface as an error")
	}
	wg.Wait()

	// Queued work must abandon on cancellation rather than each waiting its full turn.
	if elapsed := time.Since(start); elapsed > 900*time.Millisecond {
		t.Fatalf("took %v after cancel; queued downloads did not abandon", elapsed)
	}
}
