package json

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/wehubfusion/Icarus/pkg/schema/contracts"
)

// StreamOptions controls ValidateStream.
type StreamOptions struct {
	// CollectAllErrors collects every error; when false, validation stops at the first.
	CollectAllErrors bool
	// RecordsPath names the array whose items go to OnItem instead of being kept: "" for a root
	// array, "data" for root.data, "payload.rows" for root.payload.rows. It is matched against the
	// error path without its "root" prefix, and must be reachable through objects only: array items
	// are decoded whole, so an array inside an item is never a records array.
	RecordsPath string
	// OnItem receives each item of the array at RecordsPath once it has been validated, valid or
	// not, as json.Unmarshal would produce it (the result says whether it was valid). An error
	// stops validation and is returned by ValidateStream. Nil means no records array.
	OnItem func(item interface{}, index int64) error
}

// errStreamStopped unwinds the walk once the first error is recorded and CollectAllErrors is false.
var errStreamStopped = errors.New("validation stopped at first error")

// ValidateStream validates the JSON value read from dec against a schema without holding the whole
// value in memory, and gives the same verdict and error paths as Validate on the json.Unmarshal of
// the same bytes. Errors are reported under the root path "root", as Validate does.
//
// It walks tokens with dec.Token. Objects, and arrays reached through objects, are walked; each
// scalar and each array item is decoded on its own and validated with the per-value code Validate
// uses (validateValueIntoState), so the rules have one source of truth. What is held:
//
//   - required: the set of schema keys seen in each open object, checked when the object closes;
//   - minItems, maxItems: a counter per open array; with CollectAllErrors false, maxItems fails as
//     soon as it is passed, with a message that cannot state the final length;
//   - uniqueItems: itemDigest (32 bytes) of each item, not the item;
//   - everything else: one value, or one array item.
//
// Differences from Validate, all documented here and in the tests:
//
//   - Error order: errors are in document order, and array rules (minItems, maxItems) are reported
//     when the array closes, after its items' errors; Validate reports them first. With
//     CollectAllErrors false, both stop at an error but not necessarily the same one.
//   - Duplicate keys in a walked object: every occurrence is validated, where json.Unmarshal keeps
//     only the last. Inside an array item (decoded whole) the last wins, as in Validate.
//   - Anything after the root value is not read; the caller decides whether it is allowed. With
//     CollectAllErrors false nothing after the first error is read either, so a later syntax
//     error is not reported.
//
// A decoder with UseNumber is accepted: numbers are validated and hashed as float64, as Validate
// sees them, and OnItem still receives json.Number.
//
// The error is non-nil when the stream is not valid JSON (where Validate's caller would have failed
// to unmarshal) or OnItem returned an error; the result then holds the errors found so far.
func (v *Validator) ValidateStream(dec *json.Decoder, s *Schema, opts StreamOptions) (*contracts.ValidationResult, error) {
	w := &streamWalker{
		v:     v,
		dec:   dec,
		opts:  opts,
		state: &validationState{collectAll: opts.CollectAllErrors},
	}
	prop := &Property{
		Type:       s.Type,
		Properties: s.Properties,
		Items:      s.Items,
	}
	err := w.walkValue(prop, "root")
	if errors.Is(err, errStreamStopped) {
		err = nil
	}
	return &contracts.ValidationResult{
		Valid:  len(w.state.errors) == 0,
		Errors: w.state.errors,
	}, err
}

// streamWalker holds one ValidateStream call.
type streamWalker struct {
	v     *Validator
	dec   *json.Decoder
	opts  StreamOptions
	state *validationState
}

// stop reports whether the walk must unwind.
func (w *streamWalker) stop() error {
	if w.state.shouldStop() {
		return errStreamStopped
	}
	return nil
}

// isRecords reports whether path is the array whose items go to OnItem.
func (w *streamWalker) isRecords(path string) bool {
	return w.opts.OnItem != nil && relativePath(path) == w.opts.RecordsPath
}

// mayHoldRecords reports whether the records array can be at or below path.
func (w *streamWalker) mayHoldRecords(path string) bool {
	if w.opts.OnItem == nil {
		return false
	}
	rel := relativePath(path)
	return rel == "" || w.opts.RecordsPath == rel || strings.HasPrefix(w.opts.RecordsPath, rel+".")
}

// relativePath strips the "root" prefix: "root" -> "", "root.data" -> "data".
func relativePath(path string) string {
	return strings.TrimPrefix(strings.TrimPrefix(path, "root"), ".")
}

// walkValue reads the next value and validates it against prop. A nil prop validates nothing and is
// used to reach a records array the schema does not describe.
func (w *streamWalker) walkValue(prop *Property, path string) error {
	tok, err := w.dec.Token()
	if err != nil {
		return syntaxErr(err)
	}
	delim, isDelim := tok.(json.Delim)
	if !isDelim {
		val, err := scalarValue(tok)
		if err != nil {
			return err
		}
		if prop != nil {
			w.v.validateValueIntoState(val, prop, path, w.state)
		}
		return w.stop()
	}

	switch {
	case delim == '{' && prop != nil && prop.Type == TypeObject:
		return w.walkObject(prop, path)
	case delim == '[' && prop != nil && prop.Type == TypeArray:
		return w.walkArray(prop, path)
	}

	// The schema does not expect this container here. Validate it as the empty container of the
	// same Go type, which gives Validate's TYPE_MISMATCH (or nothing, for ANY), then skip it.
	if prop != nil {
		var placeholder interface{} = map[string]interface{}{}
		if delim == '[' {
			placeholder = []interface{}{}
		}
		w.v.validateValueIntoState(placeholder, prop, path, w.state)
		if err := w.stop(); err != nil {
			return err
		}
	}
	switch {
	case delim == '[' && w.isRecords(path):
		return w.walkArray(nil, path)
	case delim == '{' && w.mayHoldRecords(path):
		return w.walkObject(nil, path)
	}
	return w.skipRest()
}

// walkObject walks an object whose '{' has been read. A nil prop validates nothing.
func (w *streamWalker) walkObject(prop *Property, path string) error {
	var props map[string]*Property
	if prop != nil {
		props = prop.Properties
	}
	var seen map[string]struct{}
	if len(props) > 0 {
		seen = make(map[string]struct{}, len(props))
	}
	for w.dec.More() {
		tok, err := w.dec.Token()
		if err != nil {
			return syntaxErr(err)
		}
		key, ok := tok.(string)
		if !ok {
			return fmt.Errorf("invalid input JSON: object key is %T", tok)
		}
		propPath := fmt.Sprintf("%s.%s", path, key)
		propDef, known := props[key]
		if known {
			seen[key] = struct{}{}
		}
		switch {
		case known && propDef != nil:
			err = w.walkValue(propDef, propPath)
		case w.mayHoldRecords(propPath):
			// Not in the schema, so Validate ignores it, but the records array may be in it.
			err = w.walkValue(nil, propPath)
		default:
			// Validate ignores keys the schema does not name (and a property defined as null).
			err = w.skipValue()
		}
		if err != nil {
			return err
		}
	}
	if _, err := w.dec.Token(); err != nil { // '}'
		return syntaxErr(err)
	}

	// required: Validate ranges over the schema map, so its order is random; this order is sorted.
	names := make([]string, 0, len(props))
	for name, def := range props {
		if _, ok := seen[name]; !ok && propRequired(def) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		if w.state.add(contracts.ValidationError{Path: fmt.Sprintf("%s.%s", path, name), Message: "required field missing", Code: "REQUIRED"}) {
			return errStreamStopped
		}
	}
	return nil
}

// walkArray walks an array whose '[' has been read. Each item is decoded whole and validated with
// validateValueIntoState. A nil prop validates nothing (a records array the schema does not describe
// as an ARRAY).
func (w *streamWalker) walkArray(prop *Property, path string) error {
	var rules *ValidationRules
	var items *Property
	if prop != nil {
		rules, items = prop.Validation, prop.Items
	}
	records := w.isRecords(path)

	minN, maxN := -1, -1
	var seen map[[sha256.Size]byte]struct{}
	if rules != nil {
		if rules.MinItems != nil {
			minN = int(*rules.MinItems)
		}
		if rules.MaxItems != nil {
			maxN = int(*rules.MaxItems)
		}
		if rules.UniqueItems != nil && *rules.UniqueItems {
			seen = make(map[[sha256.Size]byte]struct{})
		}
	}

	var count int64
	for w.dec.More() {
		index := count
		count++
		if maxN >= 0 && !w.state.collectAll && count > int64(maxN) {
			// Fail as soon as it is passed; the final length is unknown, so it is not stated.
			w.state.add(contracts.ValidationError{Path: path, Message: fmt.Sprintf("array length exceeds maximum %d", maxN), Code: "MAX_ITEMS"})
			return errStreamStopped
		}
		if items == nil && seen == nil && !records {
			if err := w.skipValue(); err != nil {
				return err
			}
			continue
		}

		var item interface{}
		if err := w.dec.Decode(&item); err != nil {
			return syntaxErr(err)
		}
		val, err := floatNumbers(item)
		if err != nil {
			return err
		}
		itemPath := fmt.Sprintf("%s[%d]", path, index)
		if seen != nil {
			key := itemDigest(val)
			if _, dup := seen[key]; dup {
				// Validate reports the first duplicate only, then stops looking.
				seen = nil
				if w.state.add(contracts.ValidationError{Path: itemPath, Message: "duplicate item found", Code: "DUPLICATE_ITEM"}) {
					return errStreamStopped
				}
			} else {
				seen[key] = struct{}{}
			}
		}
		if items != nil {
			w.v.validateValueIntoState(val, items, itemPath, w.state)
			if err := w.stop(); err != nil {
				return err
			}
		}
		if records {
			if err := w.opts.OnItem(item, index); err != nil {
				return fmt.Errorf("records item %d: %w", index, err)
			}
		}
	}
	if _, err := w.dec.Token(); err != nil { // ']'
		return syntaxErr(err)
	}

	if minN >= 0 && count < int64(minN) {
		if w.state.add(contracts.ValidationError{Path: path, Message: fmt.Sprintf("array length %d is less than minimum %d", count, minN), Code: "MIN_ITEMS"}) {
			return errStreamStopped
		}
	}
	if maxN >= 0 && count > int64(maxN) {
		// Only reached when collecting every error; the message matches Validate's.
		if w.state.add(contracts.ValidationError{Path: path, Message: fmt.Sprintf("array length %d exceeds maximum %d", count, maxN), Code: "MAX_ITEMS"}) {
			return errStreamStopped
		}
	}
	return nil
}

// skipValue reads and discards the next value, holding only its nesting depth.
func (w *streamWalker) skipValue() error {
	tok, err := w.dec.Token()
	if err != nil {
		return syntaxErr(err)
	}
	if d, ok := tok.(json.Delim); ok && (d == '{' || d == '[') {
		return w.skipRest()
	}
	_, err = scalarValue(tok)
	return err
}

// skipRest discards the rest of a container whose opening delimiter has been read.
func (w *streamWalker) skipRest() error {
	for depth := 1; depth > 0; {
		tok, err := w.dec.Token()
		if err != nil {
			return syntaxErr(err)
		}
		if d, ok := tok.(json.Delim); ok {
			if d == '{' || d == '[' {
				depth++
			} else {
				depth--
			}
		}
	}
	return nil
}

// syntaxErr wraps a decoder error; an early end of input is not a clean io.EOF to the caller.
func syntaxErr(err error) error {
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return fmt.Errorf("invalid input JSON: %w", err)
}

// scalarValue returns a scalar token as json.Unmarshal would give it to Validate: a json.Number
// (from a decoder with UseNumber) becomes float64.
func scalarValue(tok json.Token) (interface{}, error) {
	if n, ok := tok.(json.Number); ok {
		f, err := strconv.ParseFloat(string(n), 64)
		if err != nil {
			return nil, fmt.Errorf("invalid input JSON: number %s: %w", n, err)
		}
		return f, nil
	}
	return tok, nil
}

// floatNumbers returns v with every json.Number as float64, copying only when there is one, so the
// item is validated and hashed as Validate would see it while OnItem still gets it as decoded.
func floatNumbers(v interface{}) (interface{}, error) {
	if !hasNumber(v) {
		return v, nil
	}
	switch t := v.(type) {
	case json.Number:
		return scalarValue(t)
	case map[string]interface{}:
		out := make(map[string]interface{}, len(t))
		for k, e := range t {
			c, err := floatNumbers(e)
			if err != nil {
				return nil, err
			}
			out[k] = c
		}
		return out, nil
	case []interface{}:
		out := make([]interface{}, len(t))
		for i, e := range t {
			c, err := floatNumbers(e)
			if err != nil {
				return nil, err
			}
			out[i] = c
		}
		return out, nil
	}
	return v, nil
}

// hasNumber reports whether v holds a json.Number.
func hasNumber(v interface{}) bool {
	switch t := v.(type) {
	case json.Number:
		return true
	case map[string]interface{}:
		for _, e := range t {
			if hasNumber(e) {
				return true
			}
		}
	case []interface{}:
		for _, e := range t {
			if hasNumber(e) {
				return true
			}
		}
	}
	return false
}
