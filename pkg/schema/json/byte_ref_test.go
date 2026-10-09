package json

import (
	"strings"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/schema/contracts"
)

func byteRefSchema(minL, maxL float64) *Schema {
	return &Schema{Type: TypeObject, Properties: map[string]*Property{
		"payload": {Type: TypeByte, Validation: &ValidationRules{MinLength: &minL, MaxLength: &maxL}},
	}}
}

func refValue(size int64) map[string]interface{} {
	return map[string]interface{}{"$file": map[string]interface{}{
		"path": "results/wf/run/node/payload.bin", "size": float64(size),
	}}
}

// A byte value is a file reference, so minLength and maxLength apply to the file's size and the
// value is not a type mismatch (raw payloads: BYTE defaults and schema BYTE fields).
func TestValidateByteFileReference(t *testing.T) {
	v := NewValidator()
	s := byteRefSchema(2, 10)

	if res := v.Validate(map[string]interface{}{"payload": refValue(5)}, s); !res.Valid {
		t.Fatalf("a 5 byte file within 2..10 should be valid: %v", res.Errors)
	}
	cases := map[string]struct {
		size int64
		code string
	}{
		"short": {1, "MIN_LENGTH"},
		"long":  {11, "MAX_LENGTH"},
	}
	for name, c := range cases {
		res := v.Validate(map[string]interface{}{"payload": refValue(c.size)}, s)
		if res.Valid || !hasCode(res.Errors, c.code) {
			t.Fatalf("%s: want %s, got %v", name, c.code, res.Errors)
		}
	}
	// Anything else that is not a string or bytes is still a mismatch, a plain object included.
	res := v.Validate(map[string]interface{}{"payload": map[string]interface{}{"a": 1}}, s)
	if res.Valid || !hasCode(res.Errors, "TYPE_MISMATCH") {
		t.Fatalf("a plain object is a type mismatch, got %v", res.Errors)
	}
}

// The streaming validator agrees with Validate on a file reference.
func TestValidateStreamByteFileReference(t *testing.T) {
	s := byteRefSchema(2, 10)
	v := NewValidator()
	for _, in := range []string{
		`{"payload":{"$file":{"path":"results/wf/run/n/p.bin","size":5}}}`,
		`{"payload":{"$file":{"path":"results/wf/run/n/p.bin","size":1}}}`,
		`{"payload":{"$file":{"path":"results/wf/run/n/p.bin","size":11}}}`,
		`{"payload":{"x":1}}`,
	} {
		if !checkDifferential(t, v, s, []byte(in)) {
			t.Fatalf("input %s was skipped", in)
		}
	}
}

func hasCode(errs []contracts.ValidationError, code string) bool {
	for _, e := range errs {
		if strings.EqualFold(e.Code, code) {
			return true
		}
	}
	return false
}
