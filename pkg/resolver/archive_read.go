package resolver

import (
	"bytes"
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/wehubfusion/Icarus/pkg/archive"
	"github.com/wehubfusion/Icarus/pkg/message"
	"github.com/wehubfusion/Icarus/pkg/storage"
)

// fetchSourceResults reads one source file.
//
// Result files carrying node outputs are always addressable archives, so there is no
// document-reading branch here.
func (s *Service) fetchSourceResults(
	ctx context.Context,
	f *RequiredBlobFile,
	sourceNodeIDs map[string]bool,
	mappings []message.FieldMapping,
) (map[string]*SourceResult, error) {
	return s.readArchive(ctx, f, sourceNodeIDs, mappings)
}

// downloadPayload returns a blob's contents: the plain JSON a consumer graph offloaded by Zeus is
// the one readable blob that is not a result archive, so anything else is refused as a named
// failure rather than passed on to a plugin that would misparse it.
func (s *Service) downloadPayload(ctx context.Context, blobURL string) ([]byte, error) {
	data, err := s.blobClient.DownloadResult(ctx, blobURL)
	if err != nil {
		return nil, fmt.Errorf("resolver: failed to download input from blob: %w", err)
	}
	if archive.IsArchive(data) {
		return nil, fmt.Errorf("resolver: %s is a result archive, which is read by field mapping, not whole", blobURL)
	}
	if !looksLikeJSON(data) {
		return nil, fmt.Errorf("resolver: %s is neither an archive nor JSON", blobURL)
	}
	return data, nil
}

// looksLikeJSON reports whether data begins with a byte that can start a JSON value.
//
// A first-byte test rather than a parse: the check exists to reject a payload written in
// some other format, and parsing a multi-megabyte document to establish that would cost
// more than the whole-payload read it is guarding.
func looksLikeJSON(data []byte) bool {
	for _, b := range data {
		switch b {
		case ' ', '\t', '\r', '\n':
			continue
		case '{', '[', '"', '-', 't', 'f', 'n':
			return true
		default:
			return b >= '0' && b <= '9'
		}
	}
	return false
}

// readArchive implements the read protocol: open the archive over ranged reads, plan which
// entries are needed, fetch those, and assemble them into the same flat map a parsed
// document would have produced.
//
// The demand set travels already — the field mappings state exactly which keys the unit
// will read. The supply set is discovered from the archive itself, so nothing about which
// entries exist has to be propagated through Zeus, and no index rides on any message.
func (s *Service) readArchive(
	ctx context.Context,
	f *RequiredBlobFile,
	sourceNodeIDs map[string]bool,
	mappings []message.FieldMapping,
) (map[string]*SourceResult, error) {
	size := f.SizeBytes
	if size <= 0 {
		var err error
		size, err = s.blobClient.BlobSize(ctx, f.BlobURL)
		if err != nil {
			return nil, fmt.Errorf("resolver: archive size for %s: %w", f.BlobURL, err)
		}
	}

	ra, err := storage.NewBlobReaderAt(ctx, s.blobClient, f.BlobURL, size)
	if err != nil {
		return nil, fmt.Errorf("resolver: open archive %s: %w", f.BlobURL, err)
	}

	reader, err := archive.NewReader(ra, size)
	if err != nil {
		return nil, fmt.Errorf("resolver: read archive %s: %w", f.BlobURL, err)
	}
	plan := planArchiveFetch(reader, f.ContainsNodes, mappings)

	flat, err := s.materialise(ctx, f, reader, plan, size)
	if err != nil {
		return nil, err
	}

	moved, requests := ra.Stats()
	s.logArchiveRead(f, size, moved, requests, len(flat), plan)

	return sourceResultsFromContent(flat, sourceNodeIDs, f.ContainsNodes), nil
}

// materialise turns a fetch plan into the flat map, choosing between many ranged reads and
// one whole-object download.
//
// The whole-file branch is not a fallback for something going wrong; it is the right answer
// when the selection approaches the entire payload, where one large request beats a
// thousand small ones both in latency and in cost.
func (s *Service) materialise(
	ctx context.Context,
	f *RequiredBlobFile,
	reader *archive.Reader,
	plan fetchPlan,
	size int64,
) (map[string]interface{}, error) {
	if !plan.wholeFile {
		flat, err := reader.Flat(plan.keys)
		if err != nil {
			return nil, fmt.Errorf("resolver: read archive entries from %s: %w", f.BlobURL, err)
		}
		return flat, nil
	}

	data, err := s.blobClient.DownloadFromURL(ctx, f.BlobURL)
	if err != nil {
		return nil, fmt.Errorf("resolver: download archive %s: %w", f.BlobURL, err)
	}
	whole, err := archive.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("resolver: reopen archive %s: %w", f.BlobURL, err)
	}
	flat, err := whole.FlatAll()
	if err != nil {
		return nil, fmt.Errorf("resolver: read archive %s: %w", f.BlobURL, err)
	}
	return flat, nil
}

// logArchiveRead records the ratio this whole change exists to move: fields requested
// against bytes downloaded. Without it there is no way to tell whether a deployment is
// getting the benefit or quietly falling back to whole-file reads on every unit.
func (s *Service) logArchiveRead(f *RequiredBlobFile, size, moved, requests int64, fields int, plan fetchPlan) {
	if s.logger == nil {
		return
	}
	s.logger.Debug("archive read",
		zap.String("blob_url", f.BlobURL),
		zap.Int64("blob_size_bytes", size),
		zap.Int64("bytes_downloaded", moved),
		zap.Int64("ranged_requests", requests),
		zap.Int("fields_requested", len(plan.keys)),
		zap.Int("fields_resolved", fields),
		zap.Bool("whole_file", plan.wholeFile),
	)
}
