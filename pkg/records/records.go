// Package records moves large record arrays as .ndjson files, one JSON record per line, so a
// producer writes and a consumer reads them one record at a time (decisions D7).
//
// A records file is an ordinary fileref.FileRef with content type application/x-ndjson and
// Records set to the row count.
package records

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/wehubfusion/Icarus/pkg/fileref"
	"github.com/wehubfusion/Icarus/pkg/filestore"
)

// MaxLineBytes bounds one record. A line longer than this is an error, not an allocation.
var MaxLineBytes = 64 << 20

// Writer writes records to a .ndjson file, one per line.
type Writer struct {
	w     filestore.Writer
	buf   *bufio.Writer
	enc   *json.Encoder
	count int64
	done  bool
}

// Create starts a records file at path in the store.
func Create(ctx context.Context, store filestore.Store, path, fileName string) (*Writer, error) {
	fw, err := store.Create(ctx, path, fileref.ContentTypeNDJSON, fileName)
	if err != nil {
		return nil, err
	}
	return NewWriter(fw), nil
}

// NewWriter wraps a file writer.
func NewWriter(fw filestore.Writer) *Writer {
	buf := bufio.NewWriterSize(fw, 256<<10)
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	return &Writer{w: fw, buf: buf, enc: enc}
}

// Write encodes one record as one line. json.Encoder ends each value with a newline, and a JSON
// value never contains a raw newline, so one record is exactly one line.
func (w *Writer) Write(record interface{}) error {
	if w.done {
		return errors.New("records: write after close")
	}
	if err := w.enc.Encode(record); err != nil {
		return fmt.Errorf("records: encode record %d: %w", w.count, err)
	}
	w.count++
	return nil
}

// Count is the number of records written so far.
func (w *Writer) Count() int64 { return w.count }

// Close commits the file and returns its reference with Records set.
func (w *Writer) Close() (fileref.FileRef, error) {
	if w.done {
		return fileref.FileRef{}, errors.New("records: already closed")
	}
	w.done = true
	if err := w.buf.Flush(); err != nil {
		_ = w.w.Abort()
		return fileref.FileRef{}, fmt.Errorf("records: flush: %w", err)
	}
	ref, err := w.w.Close()
	if err != nil {
		return fileref.FileRef{}, err
	}
	n := w.count
	ref.Records = &n
	return ref, nil
}

// Abort discards the file.
func (w *Writer) Abort() error {
	if w.done {
		return nil
	}
	w.done = true
	return w.w.Abort()
}

// Reader yields one record at a time.
type Reader interface {
	// Next returns the next record, or io.EOF after the last.
	Next() (interface{}, error)
	Close() error
}

type lineReader struct {
	rc   io.Closer
	sc   *bufio.Scanner
	line int64
}

// Open opens a records file for reading.
func Open(ctx context.Context, store filestore.Store, ref fileref.FileRef) (Reader, error) {
	rc, err := store.Open(ctx, ref)
	if err != nil {
		return nil, err
	}
	return NewReader(rc), nil
}

// NewReader reads NDJSON from rc. Blank lines are skipped.
func NewReader(rc io.ReadCloser) Reader {
	sc := bufio.NewScanner(rc)
	sc.Buffer(make([]byte, 0, 64<<10), MaxLineBytes)
	return &lineReader{rc: rc, sc: sc}
}

func (r *lineReader) Next() (interface{}, error) {
	for r.sc.Scan() {
		r.line++
		b := bytes.TrimSpace(r.sc.Bytes())
		if len(b) == 0 {
			continue
		}
		var v interface{}
		dec := json.NewDecoder(bytes.NewReader(b))
		dec.UseNumber()
		if err := dec.Decode(&v); err != nil {
			return nil, fmt.Errorf("records: line %d: %w", r.line, err)
		}
		return normaliseNumbers(v), nil
	}
	if err := r.sc.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			return nil, fmt.Errorf("records: line %d is longer than %d bytes", r.line+1, MaxLineBytes)
		}
		return nil, err
	}
	return nil, io.EOF
}

func (r *lineReader) Close() error { return r.rc.Close() }

// normaliseNumbers turns json.Number into float64, as encoding/json would have, except where the
// value is an integer too large for a float64 to hold exactly: that stays an int64.
func normaliseNumbers(v interface{}) interface{} {
	switch t := v.(type) {
	case json.Number:
		if i, err := t.Int64(); err == nil && (i > 1<<53 || i < -(1<<53)) {
			return i
		}
		f, _ := t.Float64()
		return f
	case map[string]interface{}:
		for k, x := range t {
			t[k] = normaliseNumbers(x)
		}
		return t
	case []interface{}:
		for i, x := range t {
			t[i] = normaliseNumbers(x)
		}
		return t
	default:
		return v
	}
}

// SliceReader serves an in-memory array as a Reader, so a consumer handles a records file and an
// inline array with one code path.
func SliceReader(items []interface{}) Reader { return &sliceReader{items: items} }

type sliceReader struct {
	items []interface{}
	i     int
}

func (s *sliceReader) Next() (interface{}, error) {
	if s.i >= len(s.items) {
		return nil, io.EOF
	}
	v := s.items[s.i]
	s.i++
	return v, nil
}

func (s *sliceReader) Close() error { return nil }

// ErrTooLargeToMaterialise is returned by ReadAll when a records file is larger than the cap.
var ErrTooLargeToMaterialise = errors.New("RECORDS_TOO_LARGE_TO_MATERIALISE")

// ReadAll builds the array in memory for a consumer that cannot stream, refusing above maxBytes
// before any byte is read (the reference carries the size).
func ReadAll(ctx context.Context, store filestore.Store, ref fileref.FileRef, maxBytes int64) ([]interface{}, error) {
	if maxBytes > 0 && ref.Size > maxBytes {
		return nil, fmt.Errorf("%w: %s is %d bytes, the limit is %d", ErrTooLargeToMaterialise, ref.Path, ref.Size, maxBytes)
	}
	r, err := Open(ctx, store, ref)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var out []interface{}
	if ref.Records != nil && *ref.Records < 1<<20 {
		out = make([]interface{}, 0, *ref.Records)
	}
	for {
		v, err := r.Next()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
}
