package resolver

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/wehubfusion/Icarus/pkg/fileref"
	"github.com/wehubfusion/Icarus/pkg/filestore"
	"github.com/wehubfusion/Icarus/pkg/message"
	"github.com/wehubfusion/Icarus/pkg/records"
)

// DefaultRecordsMaterialiseMaxBytes caps the records file a consumer that does not stream records
// may have built in memory (ICARUS_RECORDS_MATERIALISE_MAX_BYTES in the plan, D7).
const DefaultRecordsMaterialiseMaxBytes int64 = 64 << 20

// ErrRecordsTooLarge is returned when a records file is above the materialise cap.
var ErrRecordsTooLarge = errors.New("RECORDS_TOO_LARGE_TO_MATERIALISE")

// WithRecordsMaterialiseMax sets the cap for building a records file in memory (0 or less keeps
// the default).
func (s *Service) WithRecordsMaterialiseMax(n int64) *Service {
	if n > 0 {
		s.recordsMaterialiseMax = n
	}
	return s
}

func (s *Service) recordsCap() int64 {
	if s.recordsMaterialiseMax > 0 {
		return s.recordsMaterialiseMax
	}
	return DefaultRecordsMaterialiseMaxBytes
}

// DefaultRecordsBatchAboveBytes is the records file size above which a consumer that iterates over
// it reads it in batches, never holding it whole (ICARUS_RECORDS_BATCH_ABOVE_BYTES). Below it the
// array is built, as a small file costs less that way than the reader and the per-batch passes.
const DefaultRecordsBatchAboveBytes int64 = 4 << 20

// WithRecordsBatchAbove sets that size (0 or less keeps the default). It only matters for a context
// that allows batching (WithRecordsBatching).
func (s *Service) WithRecordsBatchAbove(n int64) *Service {
	if n > 0 {
		s.recordsBatchAboveMax = n
	}
	return s
}

func (s *Service) recordsBatchAbove() int64 {
	if s.recordsBatchAboveMax > 0 {
		return s.recordsBatchAboveMax
	}
	return DefaultRecordsBatchAboveBytes
}

type recordsStreamingKey struct{}

// WithRecordsStreaming marks the consumer resolved under ctx as one that reads a records file
// itself for the given input endpoints ("/data"): a RECORDS mapping of a whole record port into
// one of them keeps the reference (D7), and the consumer streams the file. Every other RECORDS
// mapping is still materialised.
func WithRecordsStreaming(ctx context.Context, destinations ...string) context.Context {
	set := map[string]bool{}
	for _, d := range destinations {
		set["/"+strings.Trim(d, "/")] = true
	}
	return context.WithValue(ctx, recordsStreamingKey{}, set)
}

// streamsWhole reports whether m delivers a whole record port into an input the consumer streams.
func streamsWhole(ctx context.Context, m message.FieldMapping) bool {
	set, _ := ctx.Value(recordsStreamingKey{}).(map[string]bool)
	if len(set) == 0 || len(m.DestinationEndpoints) != 1 {
		return false
	}
	if "/"+strings.Trim(m.SourceEndpoint, "/") != recordsPort(m.SourceEndpoint) {
		return false
	}
	return set["/"+strings.Trim(m.DestinationEndpoints[0], "/")]
}

// materialiseRecords replaces the records reference (an .ndjson FileRef) on every source port a
// RECORDS mapping reads with the array of its records, read from the run's files (raw payloads,
// D7). A consumer then sees the array it always has, and every mapping into records (/data//field)
// keeps its meaning. Only a port a RECORDS mapping names is opened (D11): a "$file" object reaching
// the consumer any other way is data. Without a run store nothing is changed. The size is checked
// before anything is read.
func (s *Service) materialiseRecords(ctx context.Context, mappings []message.FieldMapping, results map[string]*SourceResult) error {
	store := filestore.FromContext(ctx)
	if store == nil || len(results) == 0 {
		return nil
	}
	// An over-cap port does not stop the others: every port that fits is materialised, then the
	// first over-cap error is returned (a batching consumer reads that port in batches).
	var tooLarge error
	for _, m := range mappings {
		if m.ValueType != message.ValueTypeRecords || streamsWhole(ctx, m) {
			continue
		}
		r := results[m.SourceNodeID]
		if r == nil {
			continue
		}
		port := recordsPort(m.SourceEndpoint)
		if port == "" {
			continue
		}
		flatKey := m.SourceNodeID + "-" + port
		if v, ok := r.RawFlatKeys[flatKey]; ok {
			nv, err := s.materialiseRef(ctx, store, v)
			if errors.Is(err, ErrRecordsTooLarge) {
				if tooLarge == nil {
					tooLarge = err
				}
				continue
			}
			if err != nil {
				return err
			}
			r.RawFlatKeys[flatKey] = nv
		}
		if fields := r.ProjectedFields[m.SourceNodeID]; fields != nil {
			key := strings.TrimPrefix(port, "/")
			if v, ok := fields[key]; ok {
				nv, err := s.materialiseRef(ctx, store, v)
				if errors.Is(err, ErrRecordsTooLarge) {
					if tooLarge == nil {
						tooLarge = err
					}
					continue
				}
				if err != nil {
					return err
				}
				fields[key] = nv
			}
		}
	}
	return tooLarge
}

// recordsPort is the output port a mapping's source endpoint reads: "/data" for "/data" and for
// "/data//name".
func recordsPort(endpoint string) string {
	e := "/" + strings.TrimLeft(endpoint, "/")
	if i := strings.Index(e, "//"); i >= 0 {
		e = e[:i]
	}
	e = strings.TrimRight(e, "/")
	if e == "" {
		return ""
	}
	return e
}

// materialiseRef reads v when it is a records reference; any other value is returned unchanged.
func (s *Service) materialiseRef(ctx context.Context, store filestore.Store, v interface{}) (interface{}, error) {
	ref, ok := fileref.Parse(v)
	if !ok || !ref.IsRecords() {
		return v, nil
	}
	if ref.Size > s.recordsCap() {
		return nil, fmt.Errorf("%w: %s is %d bytes, above the %d byte limit for a consumer that does not stream records",
			ErrRecordsTooLarge, ref.Path, ref.Size, s.recordsCap())
	}
	items, err := records.ReadAll(ctx, store, ref, s.recordsCap())
	if err != nil {
		return nil, fmt.Errorf("resolver: read records %s: %w", ref.Path, err)
	}
	if items == nil {
		items = []interface{}{}
	}
	return items, nil
}
