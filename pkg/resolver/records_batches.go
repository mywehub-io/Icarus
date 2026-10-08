package resolver

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/wehubfusion/Icarus/pkg/fileref"
	"github.com/wehubfusion/Icarus/pkg/filestore"
	"github.com/wehubfusion/Icarus/pkg/message"
	"github.com/wehubfusion/Icarus/pkg/records"
)

// Records batching (raw payloads phase 6 step 4): a records file above the materialise cap that
// a consumer iterates over (a RECORDS mapping with Iterate) is not refused when the consumer runs
// per item anyway. With WithRecordsBatching on ctx, resolving returns a *RecordsBatches instead:
// each Next builds the consumer's input for the next batch of records with the same mapping code
// as a whole array would, so a batch's input is exactly that slice of the whole input. Below the
// cap, and for every other mapping, nothing changes.

type recordsBatchingKey struct{}

// WithRecordsBatching lets the consumer resolved under ctx take an over-cap iterated records input
// in batches of size records (see RecordsBatches).
func WithRecordsBatching(ctx context.Context, size int) context.Context {
	if size <= 0 {
		size = 1000
	}
	return context.WithValue(ctx, recordsBatchingKey{}, size)
}

func recordsBatchSize(ctx context.Context) int {
	n, _ := ctx.Value(recordsBatchingKey{}).(int)
	return n
}

// RecordsBatches is returned, as an error, by a resolve that batches its input. Use errors.As.
// Next returns the next batch's input, or io.EOF after the last. Close releases the file.
type RecordsBatches struct {
	// Ref is the records file being read.
	Ref    fileref.FileRef
	size   int
	reader records.Reader
	set    func(items []interface{})
	build  func() ([]byte, error)
	done   bool
}

func (b *RecordsBatches) Error() string {
	return fmt.Sprintf("resolver: records input %s is read in batches", b.Ref.Path)
}

// Next builds the next batch's input.
func (b *RecordsBatches) Next() ([]byte, error) {
	if b.done {
		return nil, io.EOF
	}
	items := make([]interface{}, 0, b.size)
	for len(items) < b.size {
		item, err := b.reader.Next()
		if err == io.EOF {
			b.done = true
			break
		}
		if err != nil {
			return nil, fmt.Errorf("resolver: read records %s: %w", b.Ref.Path, err)
		}
		items = append(items, item)
	}
	if len(items) == 0 {
		return nil, io.EOF
	}
	b.set(items)
	return b.build()
}

// Close releases the records file.
func (b *RecordsBatches) Close() error {
	if b.reader == nil {
		return nil
	}
	return b.reader.Close()
}

// materialiseAndBuild materialises the records the mappings read and builds the input. A records
// port read only by iterate mappings that is above the batching threshold, when ctx allows
// batching, becomes a *RecordsBatches error instead, so a large iterated input is read in batches
// rather than built whole; so does one above the materialise cap, which could not be built at all.
func (s *Service) materialiseAndBuild(ctx context.Context, params BuildInputParams) ([]byte, error) {
	if recordsBatchSize(ctx) > 0 {
		if batches, ok := s.batchRecordsAbove(ctx, params, s.recordsBatchAbove()); ok {
			return nil, batches
		}
	}
	err := s.materialiseRecords(ctx, params.FieldMappings, params.SourceResults)
	if err == nil {
		return buildInputFromMappings(params)
	}
	if !errors.Is(err, ErrRecordsTooLarge) || recordsBatchSize(ctx) == 0 {
		return nil, err
	}
	if batches, ok := s.batchRecordsAbove(ctx, params, s.recordsCap()); ok {
		return nil, batches
	}
	return nil, err
}

// batchRecordsAbove sets up batching for the one records port above threshold bytes, when the
// input is one batching can serve: every mapping that reads the port iterates, and no other port is
// batched. ok is false otherwise, and the caller builds the input whole or refuses it.
func (s *Service) batchRecordsAbove(ctx context.Context, params BuildInputParams, threshold int64) (*RecordsBatches, bool) {
	store := filestore.FromContext(ctx)
	if store == nil {
		return nil, false
	}
	type port struct{ node, port string }
	var big *port
	var bigRef fileref.FileRef
	for _, m := range params.FieldMappings {
		if m.ValueType != message.ValueTypeRecords {
			continue
		}
		r := params.SourceResults[m.SourceNodeID]
		if r == nil {
			continue
		}
		p := port{m.SourceNodeID, recordsPort(m.SourceEndpoint)}
		ref, ok := fileref.Parse(r.RawFlatKeys[p.node+"-"+p.port])
		if !ok || !ref.IsRecords() || ref.Size <= threshold {
			continue
		}
		if !m.Iterate {
			return nil, false // a whole-array reader needs it all
		}
		if big != nil && *big != p {
			return nil, false // only one batched port
		}
		big, bigRef = &p, ref
	}
	if big == nil {
		return nil, false
	}
	// Any whole-array reader of the same port rules batching out.
	for _, m := range params.FieldMappings {
		if m.ValueType == message.ValueTypeRecords && m.SourceNodeID == big.node && recordsPort(m.SourceEndpoint) == big.port && !m.Iterate {
			return nil, false
		}
	}
	reader, err := records.Open(ctx, store, bigRef)
	if err != nil {
		return nil, false
	}
	r := params.SourceResults[big.node]
	key := big.node + "-" + big.port
	field := big.port[1:]
	return &RecordsBatches{
		Ref:    bigRef,
		size:   recordsBatchSize(ctx),
		reader: reader,
		set: func(items []interface{}) {
			r.RawFlatKeys[key] = items
			if f := r.ProjectedFields[big.node]; f != nil {
				f[field] = items
			}
		},
		build: func() ([]byte, error) { return buildInputFromMappings(params) },
	}, true
}
