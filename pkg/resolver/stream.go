package resolver

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"

	"go.uber.org/zap"

	"github.com/wehubfusion/Icarus/pkg/archive"
	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
	"github.com/wehubfusion/Icarus/pkg/message"
	"github.com/wehubfusion/Icarus/pkg/storage"
)

// MaxInlineBytes is the threshold above which CreateResult offloads a result to blob.
func (s *Service) MaxInlineBytes() int { return s.maxInlineBytes }

// CreateResultStream is CreateResult for a document whose large values live on disk rather
// than in memory. small holds values already in JSON form; streamed holds raw byte values,
// written as base64 JSON strings exactly as the in-memory path would have encoded them.
//
// The outcome is the one CreateResult would give for the equivalent marshalled document:
// the same inline-or-blob decision, the same blob path, the same archive bytes. Only the
// memory profile differs, and only on the blob branch, where the archive is encoded straight
// into the upload instead of being built as one buffer.
//
// When the streamed values together fit within the inline threshold, the document is
// assembled in memory and handed to CreateResult unchanged — it is small by definition, and
// routing it through the one existing path keeps the inline outcome identical rather than
// approximately so.
func (s *Service) CreateResultStream(
	ctx context.Context,
	small map[string]json.RawMessage,
	streamed map[string]archive.StreamedValue,
	meta ResultMeta,
) (*Result, error) {
	var encodedLen int64
	for _, v := range streamed {
		encodedLen += v.EncodedLen()
	}

	if s.blobClient == nil || encodedLen <= int64(s.maxInlineBytes) {
		doc, err := materialiseDocument(small, streamed)
		if err != nil {
			return nil, err
		}
		return s.CreateResult(ctx, doc, meta)
	}

	// CreateResult would choose a document archive for this payload only if every key has
	// the flat-key shape; otherwise it would carry the bytes opaquely. A streamed write is
	// always a document, so a key outside that shape is refused rather than written in a
	// form CreateResult would never have produced.
	for key := range small {
		if !isFlatKey(key) {
			return nil, fmt.Errorf("resolver: streamed result key %q is not a flat node-output key", key)
		}
	}
	for key := range streamed {
		if !isFlatKey(key) {
			return nil, fmt.Errorf("resolver: streamed result key %q is not a flat node-output key", key)
		}
	}

	blobPath, metadata, err := resultBlobLocation(meta)
	if err != nil {
		return nil, err
	}

	pr, pw := io.Pipe()
	counted := &countingReader{r: pr}

	var (
		wg       sync.WaitGroup
		stats    archive.Stats
		writeErr error
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		stats, writeErr = archive.WriteDocument(pw, small, streamed)
		// A write failure reaches the upload as a read error, so a half-written archive is
		// never committed: the block list is only committed once the body ends cleanly.
		pw.CloseWithError(writeErr)
	}()

	blobURL, uploadErr := s.blobClient.UploadStream(ctx, blobPath, counted, archive.ContentType, metadata)
	// Unblock the writer if the upload stopped reading early, then wait for it, so neither
	// side outlives this call.
	pr.CloseWithError(fmt.Errorf("upload finished"))
	wg.Wait()

	// The upload error comes first: when it stops early, the writer then fails only because
	// its pipe was closed, which says nothing about the cause.
	if uploadErr != nil {
		return nil, fmt.Errorf("resolver: failed to upload result to blob: %w", uploadErr)
	}
	if writeErr != nil {
		return nil, fmt.Errorf("resolver: failed to build result archive: %w", writeErr)
	}

	if s.logger != nil {
		s.logger.Info("result written to blob",
			zap.String("blob_url", blobURL),
			zap.Int64("total_bytes", counted.n),
			zap.Bool("used_blob", true),
			zap.Bool("streamed", true),
			zap.Int("streamed_entry_count", len(streamed)),
			zap.Int("entry_count", stats.EntryCount),
			zap.Int("array_entry_count", stats.ArrayEntryCount),
		)
	}

	return &Result{
		BlobReference: &message.BlobReference{URL: blobURL, SizeBytes: int(counted.n)},
		UsedBlob:      true,
	}, nil
}

// materialiseDocument builds the marshalled document the streamed values stand for, for
// the inline branch, where they are small enough to hold.
func materialiseDocument(small map[string]json.RawMessage, streamed map[string]archive.StreamedValue) ([]byte, error) {
	doc := make(map[string]json.RawMessage, len(small)+len(streamed))
	for k, v := range small {
		doc[k] = v
	}
	for k, v := range streamed {
		if _, dup := doc[k]; dup {
			return nil, fmt.Errorf("resolver: key %q is both streamed and inline", k)
		}
		raw, err := readStreamed(v)
		if err != nil {
			return nil, fmt.Errorf("resolver: read streamed value %q: %w", k, err)
		}
		encoded, err := json.Marshal(base64.StdEncoding.EncodeToString(raw))
		if err != nil {
			return nil, err
		}
		doc[k] = encoded
	}
	return json.Marshal(doc)
}

func readStreamed(v archive.StreamedValue) ([]byte, error) {
	if v.Open == nil {
		return nil, fmt.Errorf("no source")
	}
	rc, err := v.Open()
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	raw, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) != v.Size {
		return nil, fmt.Errorf("yielded %d bytes, declared %d", len(raw), v.Size)
	}
	return raw, nil
}

func isFlatKey(key string) bool { return strings.Index(key, "-/") > 0 }

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// StreamDestination is the one destination endpoint LocateEntry serves. A unit mapping its
// input to anywhere else is resolved the ordinary way.
const StreamDestination = "/payload"

// EntryLocator says where, within a source blob, the single value a unit's input consists
// of sits, so the unit can stream it with ranged GETs instead of receiving it resolved.
//
// The input the ordinary resolve would build is {"payload": V}, where V is the entry's
// value when RelPath is empty, or {RelPath: value} when the mapping named the parent of the
// one entry beneath it. Callers rely on that equivalence; LocateEntry only succeeds where
// it holds.
type EntryLocator struct {
	BlobURL  string
	BlobSize int64
	Key      string
	RelPath  string
	Offset   int64
	Length   int64
}

// Open streams the entry's value bytes.
func (l *EntryLocator) Open(ctx context.Context, src storage.RangeDownloader, chunk int64) (*storage.RangeReader, error) {
	return storage.NewRangeReader(ctx, src, l.BlobURL, l.Offset, l.Length, chunk)
}

// RangeSource is the blob client as a RangeDownloader, for EntryLocator.Open.
func (s *Service) RangeSource() storage.RangeDownloader { return s.blobClient }

// LocateEntry reports whether a unit's resolved input would be exactly one archive entry
// placed at StreamDestination, and if so where that entry is.
//
// It succeeds only in the narrow case where that equivalence can be stated without running
// the mapping engine: one data mapping (event mappings contribute nothing to the input),
// one destination, which is StreamDestination, a plain path on both sides (no collection
// "//" syntax, no iteration, not a plugin-error section), and a source endpoint that names
// either one entry or the parent of exactly one entry with a plain segment name. The source
// must live in a blob file of the consumer graph. Everything else — inline sources, several
// mappings, collections, whole-node mappings, indexed families — returns false, and the
// caller resolves the ordinary way.
//
// An error means the shape qualified but the archive could not be opened; the caller may
// fall back or fail.
func (s *Service) LocateEntry(
	ctx context.Context,
	mappings []message.FieldMapping,
	cg *ConsumerGraph,
) (*EntryLocator, bool, error) {
	if s.blobClient == nil || cg == nil {
		return nil, false, nil
	}

	var m *message.FieldMapping
	for i := range mappings {
		if mappings[i].IsEventTrigger {
			continue
		}
		if m != nil {
			return nil, false, nil
		}
		m = &mappings[i]
	}
	if m == nil || m.SourceNodeID == "" || m.Iterate || m.SourceSectionId == runtime.SectionPluginError {
		return nil, false, nil
	}
	if len(m.DestinationEndpoints) != 1 || m.DestinationEndpoints[0] != StreamDestination {
		return nil, false, nil
	}
	if strings.Contains(m.SourceEndpoint, "//") {
		return nil, false, nil
	}
	path := strings.Trim(m.SourceEndpoint, "/")
	if path == "" {
		return nil, false, nil // the whole node, reconstructed from every key
	}

	files := cg.DetermineRequiredFiles([]message.FieldMapping{*m})
	if len(files) != 1 || files[0].BlobURL == "" {
		return nil, false, nil
	}
	if loc, ok := cg.ResultLocations[m.SourceNodeID]; ok && loc != nil && loc.HasInlineData {
		return nil, false, nil
	}
	file := files[0]

	size, err := s.blobClient.BlobSize(ctx, file.BlobURL)
	if err != nil {
		return nil, false, fmt.Errorf("resolver: archive size for %s: %w", file.BlobURL, err)
	}
	ra, err := storage.NewBlobReaderAt(ctx, s.blobClient, file.BlobURL, size)
	if err != nil {
		return nil, false, fmt.Errorf("resolver: open archive %s: %w", file.BlobURL, err)
	}
	reader, err := archive.NewReader(ra, size)
	if err != nil {
		return nil, false, fmt.Errorf("resolver: read archive %s: %w", file.BlobURL, err)
	}
	if reader.IsRaw() {
		return nil, false, nil
	}

	key := m.SourceNodeID + "-/" + path
	rel := ""
	if !reader.Has(key) {
		var under []string
		for _, name := range reader.Names() {
			if strings.HasPrefix(name, key+"/") {
				under = append(under, name)
			}
		}
		if len(under) != 1 {
			return nil, false, nil
		}
		rel = strings.TrimPrefix(under[0], key+"/")
		// One plain segment only. A deeper path or an indexed key is rebuilt by the mapping
		// engine into structure this locator does not describe.
		if rel == "" || strings.ContainsAny(rel, "/[]") {
			return nil, false, nil
		}
		key = under[0]
	}

	off, n, err := reader.EntryRange(key)
	if err != nil {
		return nil, false, fmt.Errorf("resolver: locate %q in %s: %w", key, file.BlobURL, err)
	}
	return &EntryLocator{
		BlobURL:  file.BlobURL,
		BlobSize: size,
		Key:      key,
		RelPath:  rel,
		Offset:   off,
		Length:   n,
	}, true, nil
}
