package jsonops

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/wehubfusion/Icarus/pkg/records"
	"github.com/wehubfusion/Icarus/pkg/schema"
	"github.com/wehubfusion/Icarus/pkg/schema/contracts"
	schemajson "github.com/wehubfusion/Icarus/pkg/schema/json"
)

var (
	streamTransformer = schemajson.NewTransformer()
	streamValidator   = schemajson.NewValidator()
)

// itemSource yields the items of an array one at a time; ok is false after the last.
type itemSource func() (item interface{}, ok bool, err error)

// jsonArraySource reads the single JSON array dec holds, one item at a time. Anything but a single
// array is an error, as it is for json.Unmarshal against an ARRAY schema.
func jsonArraySource(dec *json.Decoder) (itemSource, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("invalid input JSON: %w", err)
	}
	if d, ok := tok.(json.Delim); !ok || d != '[' {
		return nil, fmt.Errorf("schema type is ARRAY but data is not a JSON array")
	}
	return func() (interface{}, bool, error) {
		if dec.More() {
			var item interface{}
			if err := dec.Decode(&item); err != nil {
				return nil, false, fmt.Errorf("invalid input JSON: %w", err)
			}
			return item, true, nil
		}
		if _, err := dec.Token(); err != nil {
			return nil, false, fmt.Errorf("invalid input JSON: %w", err)
		}
		if _, err := dec.Token(); err != io.EOF {
			return nil, false, fmt.Errorf("invalid input JSON: data after the array")
		}
		return nil, false, nil
	}, nil
}

// recordsSource yields the records of an .ndjson file as the items of an array.
func recordsSource(rd records.Reader) itemSource {
	return func() (interface{}, bool, error) {
		item, err := rd.Next()
		if err == io.EOF {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, fmt.Errorf("invalid input records: %w", err)
		}
		return item, true, nil
	}
}

// sliceSource yields the items of an array already in memory.
func sliceSource(items []interface{}) itemSource {
	i := 0
	return func() (interface{}, bool, error) {
		if i >= len(items) {
			return nil, false, nil
		}
		i++
		return items[i-1], true, nil
	}
}

// errStopFeeding ends the goroutine that feeds transformed items to the validator once it stops.
var errStopFeeding = errors.New("validation finished")

// streamItems processes an array whose schema root is ARRAY one item at a time, as
// ProcessWithSchema processes the whole array: each item gets defaults and structure, and with
// strict validation the transformed items are validated as a stream (ValidateStream), so array item
// rules and error paths (root[41].email) are the whole-array path's. emit receives each transformed
// item in order; with strict validation it stops at the first error, and the errors are returned.
// Without strict validation nothing is validated, because the verdict would decide nothing.
func streamItems(src itemSource, s *schema.Schema, cfg *Config, byteDefault schemajson.ByteDefaultFunc, emit func(item interface{}) error) ([]contracts.ValidationError, error) {
	transform := func(item interface{}) interface{} {
		return streamTransformer.TransformItemWith(item, s.Items, cfg.GetApplyDefaults(), cfg.GetStructureData(), byteDefault)
	}

	if !cfg.GetStrictValidation() {
		for {
			item, ok, err := src()
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, nil
			}
			if err := emit(transform(item)); err != nil {
				return nil, err
			}
		}
	}

	pr, pw := io.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		bw := bufio.NewWriter(pw)
		enc := json.NewEncoder(bw)
		err := bw.WriteByte('[')
		for first := true; err == nil; first = false {
			item, ok, srcErr := src()
			if srcErr != nil || !ok {
				err = srcErr
				break
			}
			if !first {
				if err = bw.WriteByte(','); err != nil {
					break
				}
			}
			err = enc.Encode(transform(item))
		}
		if err == nil {
			err = bw.WriteByte(']')
		}
		if err == nil {
			err = bw.Flush()
		}
		pw.CloseWithError(err)
	}()

	result, err := streamValidator.ValidateStream(json.NewDecoder(pr), s, schemajson.StreamOptions{
		OnItem: func(item interface{}, _ int64) error { return emit(item) },
	})
	pr.CloseWithError(errStopFeeding)
	<-done
	if err != nil {
		return nil, err
	}
	errs, _, _ := contracts.ApplyAndBucket(result.Errors, nil)
	return errs, nil
}
