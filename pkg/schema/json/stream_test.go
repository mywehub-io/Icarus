package json

import (
	"bufio"
	"bytes"
	stdjson "encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/schema/contracts"
)

func sPtrBool(b bool) *bool        { return &b }
func sPtrFloat(f float64) *float64 { return &f }
func sPtrString(s string) *string  { return &s }

// streamCase is one schema and the inputs to run through both Validate and ValidateStream.
type streamCase struct {
	name   string
	schema *Schema
	inputs []string
}

// streamCorpus ports the validator cases of Icarus/tests/schema_test.go (TestSchemaValidator,
// TestSchemaFormats, TestSchemaEdgeCases, TestSchemaByteType) and adds the cases where streaming
// could plausibly part from Validate: nulls, unknown keys, nested arrays of objects, type
// mismatches on containers, dates, BYTE, UUID, invalid patterns.
func streamCorpus() []streamCase {
	return []streamCase{
		{
			name:   "required",
			schema: &Schema{Type: TypeObject, Properties: map[string]*Property{"name": {Type: TypeString, Required: sPtrBool(true)}}},
			inputs: []string{`{"name":"John Doe"}`, `{}`, `{"name":null}`, `{"name":1}`, `{"other":"x"}`, `null`, `[]`, `"x"`},
		},
		{
			name: "string constraints",
			schema: &Schema{Type: TypeObject, Properties: map[string]*Property{"username": {Type: TypeString, Validation: &ValidationRules{
				MinLength: sPtrFloat(3), MaxLength: sPtrFloat(10), Pattern: sPtrString("^[a-z]+$"),
			}}}},
			inputs: []string{`{"username":"john"}`, `{"username":"ab"}`, `{"username":"verylongusername"}`, `{"username":"John123"}`, `{"username":["a"]}`, `{"username":{"a":1}}`},
		},
		{
			name: "number constraints",
			schema: &Schema{Type: TypeObject, Properties: map[string]*Property{"score": {Type: TypeNumber, Validation: &ValidationRules{
				Minimum: sPtrFloat(0), Maximum: sPtrFloat(100),
			}}}},
			inputs: []string{`{"score":50}`, `{"score":-10}`, `{"score":150}`, `{"score":"50"}`, `{"score":1e2}`, `{"score":100.0000001}`},
		},
		{
			name: "array constraints",
			schema: &Schema{Type: TypeObject, Properties: map[string]*Property{"tags": {Type: TypeArray, Items: &Property{Type: TypeString},
				Validation: &ValidationRules{MinItems: sPtrFloat(1), MaxItems: sPtrFloat(5), UniqueItems: sPtrBool(true)},
			}}},
			inputs: []string{
				`{"tags":["tag1","tag2","tag3"]}`, `{"tags":[]}`, `{"tags":["t1","t2","t3","t4","t5","t6"]}`,
				`{"tags":["tag1","tag1"]}`, `{"tags":["1",1]}`, `{"tags":[1,1,2,2,3,3,4]}`, `{"tags":"x"}`, `{"tags":{}}`,
				`{"tags":[null,null]}`, `{"tags":[{"a":1,"b":2},{"b":2,"a":1}]}`, `{"tags":[[1,2],[1,2]]}`,
			},
		},
		{
			name: "formats",
			schema: &Schema{Type: TypeObject, Properties: map[string]*Property{
				"email":     {Type: TypeString, Validation: &ValidationRules{Format: sPtrString("email")}},
				"website":   {Type: TypeString, Validation: &ValidationRules{Format: sPtrString("uri")}},
				"id":        {Type: TypeString, Validation: &ValidationRules{Format: sPtrString("uuid")}},
				"birthdate": {Type: TypeString, Validation: &ValidationRules{Format: sPtrString("date")}},
				"phone":     {Type: TypeString, Validation: &ValidationRules{Format: sPtrString("phone")}},
			}},
			inputs: []string{
				`{"email":"user@example.com","website":"https://example.com"}`, `{"email":"not-an-email"}`,
				`{"email":"john.doe@company.co.uk"}`, `{"email":"@example.com"}`, `{"email":""}`,
				`{"id":"550e8400-e29b-41d4-a716-446655440000"}`, `{"id":"123e4567e89b12d3a456426614174000"}`,
				`{"birthdate":"2024-01-15"}`, `{"birthdate":"01/15/2024"}`, `{"phone":"1234567890"}`,
			},
		},
		{
			name: "enum",
			schema: &Schema{Type: TypeObject, Properties: map[string]*Property{"status": {Type: TypeString, Validation: &ValidationRules{
				Enum: []string{"active", "inactive", "pending"},
			}}}},
			inputs: []string{`{"status":"active"}`, `{"status":"unknown"}`},
		},
		{
			name: "nested array with object items",
			schema: &Schema{Type: TypeObject, Properties: map[string]*Property{"users": {Type: TypeArray, Validation: &ValidationRules{MinItems: sPtrFloat(1)},
				Items: &Property{Type: TypeObject, Properties: map[string]*Property{
					"name":   {Type: TypeString, Required: sPtrBool(true)},
					"status": {Type: TypeString, Default: "active"},
					"emails": {Type: TypeArray, Items: &Property{Type: TypeString, Validation: &ValidationRules{Format: sPtrString("email")}}},
				}},
			}}},
			inputs: []string{
				`{"users":[{"name":"John"},{"name":"Jane","status":"inactive"}]}`, `{"users":[]}`,
				`{"users":[{"status":"x"},{"name":2},null,"x",{"name":"a","emails":["a@b.co","bad"]}]}`,
				`{"users":[{"name":"a","name":3}]}`,
			},
		},
		{
			name: "byte",
			schema: &Schema{Type: TypeObject, Properties: map[string]*Property{
				"result":    {Type: TypeByte, Required: sPtrBool(true)},
				"thumbnail": {Type: TypeByte, Validation: &ValidationRules{MinLength: sPtrFloat(5), MaxLength: sPtrFloat(20)}},
				"raw":       {Type: TypeByte, Validation: &ValidationRules{}},
			}},
			inputs: []string{
				`{"result":"SGVsbG8gV29ybGQ="}`, `{"result":"x","raw":"not-valid-base64!!!"}`, `{"result":"x","thumbnail":"SGk="}`,
				`{"result":"x","thumbnail":"VGhpcyBpcyBhIHZlcnkgbG9uZyBzdHJpbmcgdGhhdCBleGNlZWRzIHR3ZW50eSBieXRlcw=="}`,
				`{"result":"x","thumbnail":"SGVsbG8gV29ybGQ="}`, `{"result":5}`, `{"result":["x"]}`, `{"raw":"SGVsbG8_V29ybGQ"}`,
			},
		},
		{
			name: "dates and uuid",
			schema: &Schema{Type: TypeObject, Properties: map[string]*Property{
				"dob":  {Type: TypeDate, Validation: &ValidationRules{MinDate: sPtrString("1900-01-01"), MaxDate: sPtrString("2026-10-06"), Pattern: sPtrString(`^\d{4}-\d{2}-\d{2}$`)}},
				"at":   {Type: TypeDateTime, Validation: &ValidationRules{Format: sPtrString("datetime")}},
				"ref":  {Type: TypeUUID, Prefix: sPtrString("urn:uuid:")},
				"any":  {Type: TypeAny},
				"bool": {Type: TypeBoolean},
			}},
			inputs: []string{
				`{"dob":"1980-02-03","at":"2025-01-09T10:30:00Z","ref":"urn:uuid:550e8400-e29b-41d4-a716-446655440000"}`,
				`{"dob":"1800-01-01"}`, `{"dob":"2099-01-01"}`, `{"dob":19800203}`, `{"at":"yesterday"}`, `{"ref":"550e8400-e29b-41d4-a716-446655440000"}`,
				`{"any":{"deep":[1,{"x":[]}]},"bool":true}`, `{"bool":"true"}`, `{"bool":{}}`,
			},
		},
		{
			name:   "invalid pattern",
			schema: &Schema{Type: TypeObject, Properties: map[string]*Property{"p": {Type: TypeString, Validation: &ValidationRules{Pattern: sPtrString("[")}}}},
			inputs: []string{`{"p":"a"}`, `{"p":"b"}`},
		},
		{
			name: "root array",
			schema: &Schema{Type: TypeArray, Items: &Property{Type: TypeObject, Properties: map[string]*Property{
				"id":    {Type: TypeNumber, Required: sPtrBool(true), Validation: &ValidationRules{Minimum: sPtrFloat(0)}},
				"email": {Type: TypeString, Validation: &ValidationRules{Format: sPtrString("email")}},
			}}},
			inputs: []string{`[]`, `[{"id":1,"email":"a@b.co"},{"id":-1},{"email":"bad"},null,[1],"x"]`, `{"id":1}`, `[[[]]]`},
		},
		{
			name:   "object without properties",
			schema: &Schema{Type: TypeObject},
			inputs: []string{`{}`, `{"a":[1,2,{"b":null}]}`, `[1]`},
		},
		{
			name:   "array without items",
			schema: &Schema{Type: TypeObject, Properties: map[string]*Property{"xs": {Type: TypeArray, Validation: &ValidationRules{MaxItems: sPtrFloat(2), MinItems: sPtrFloat(3)}}}},
			inputs: []string{`{"xs":[1,{"a":[]},[2]]}`, `{"xs":[]}`, `{"xs":[1,2]}`},
		},
	}
}

type errKey struct{ path, code, message string }

func errKeys(errs []contracts.ValidationError, withMessage bool) []errKey {
	keys := make([]errKey, 0, len(errs))
	for _, e := range errs {
		k := errKey{path: e.Path, code: e.Code}
		if withMessage {
			k.message = e.Message
		}
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if a.path != b.path {
			return a.path < b.path
		}
		if a.code != b.code {
			return a.code < b.code
		}
		return a.message < b.message
	})
	return keys
}

// hasDuplicateKeys reports whether any object in b repeats a key; such input is outside the
// differential contract (see ValidateStream).
func hasDuplicateKeys(b []byte) bool {
	dec := stdjson.NewDecoder(bytes.NewReader(b))
	type frame struct {
		keys     map[string]bool
		isObject bool
		wantKey  bool
	}
	var stack []*frame
	for {
		tok, err := dec.Token()
		if err != nil {
			return false
		}
		var top *frame
		if len(stack) > 0 {
			top = stack[len(stack)-1]
		}
		if d, ok := tok.(stdjson.Delim); ok {
			switch d {
			case '{', '[':
				if top != nil && top.isObject {
					top.wantKey = true
				}
				stack = append(stack, &frame{keys: map[string]bool{}, isObject: d == '{', wantKey: true})
			default:
				stack = stack[:len(stack)-1]
			}
			continue
		}
		if top != nil && top.isObject {
			if top.wantKey {
				k := tok.(string)
				if top.keys[k] {
					return true
				}
				top.keys[k] = true
				top.wantKey = false
			} else {
				top.wantKey = true
			}
		}
	}
}

// checkDifferential runs Validate and ValidateStream on input and fails on any difference outside
// the documented ones. It returns false when the input is outside the contract and was skipped.
func checkDifferential(t testing.TB, v *Validator, s *Schema, input []byte) bool {
	t.Helper()
	var data interface{}
	if err := stdjson.Unmarshal(input, &data); err != nil {
		return false
	}
	if hasDuplicateKeys(input) {
		return false
	}
	want := v.Validate(data, s)
	wantFirst := v.ValidateWithOptions(data, s, false)
	wantAll := errKeys(want.Errors, true)
	wantCodes := map[errKey]bool{}
	for _, k := range errKeys(want.Errors, false) {
		wantCodes[k] = true
	}

	run := func(label string, collectAll, useNumber, records bool) *contracts.ValidationResult {
		dec := stdjson.NewDecoder(bytes.NewReader(input))
		if useNumber {
			dec.UseNumber()
		}
		opts := StreamOptions{CollectAllErrors: collectAll}
		if records {
			opts.OnItem = func(interface{}, int64) error { return nil }
		}
		got, err := v.ValidateStream(dec, s, opts)
		if err != nil {
			t.Fatalf("%s: ValidateStream(%s) error on valid JSON: %v", label, input, err)
		}
		return got
	}

	for _, mode := range []struct {
		label             string
		useNumber, record bool
	}{{"plain", false, false}, {"UseNumber", true, false}, {"records", false, true}} {
		got := run(mode.label, true, mode.useNumber, mode.record)
		if got.Valid != want.Valid {
			t.Fatalf("%s: input %s: Valid %v, Validate says %v (stream %v, validate %v)", mode.label, input, got.Valid, want.Valid, got.Errors, want.Errors)
		}
		if g := errKeys(got.Errors, true); fmt.Sprint(g) != fmt.Sprint(wantAll) {
			t.Fatalf("%s: input %s: errors differ\nstream:   %v\nvalidate: %v", mode.label, input, g, wantAll)
		}

		first := run(mode.label+" first", false, mode.useNumber, mode.record)
		if first.Valid != wantFirst.Valid || first.Valid != want.Valid {
			t.Fatalf("%s first: input %s: Valid %v, Validate says %v", mode.label, input, first.Valid, wantFirst.Valid)
		}
		if len(first.Errors) > 1 {
			t.Fatalf("%s first: input %s: %d errors, want at most 1", mode.label, input, len(first.Errors))
		}
		if len(first.Errors) == 1 {
			k := errKey{path: first.Errors[0].Path, code: first.Errors[0].Code}
			if !wantCodes[k] {
				t.Fatalf("%s first: input %s: error %v is not one Validate reports (%v)", mode.label, input, k, want.Errors)
			}
		}
	}
	return true
}

func TestValidateStream_DifferentialCorpus(t *testing.T) {
	v := NewValidator()
	v.RegisterFormat("phone", func(value string) bool { return len(value) >= 10 && len(value) <= 15 })
	for _, c := range streamCorpus() {
		for _, in := range c.inputs {
			if !checkDifferential(t, v, c.schema, []byte(in)) && !hasDuplicateKeys([]byte(in)) {
				t.Fatalf("%s: corpus input %s is not valid JSON", c.name, in)
			}
		}
	}
}

// randomProperty generates a schema property biased towards the rules where streaming differs from
// whole-value validation.
func randomProperty(r *rand.Rand, depth int) *Property {
	types := []SchemaType{TypeString, TypeNumber, TypeBoolean, TypeDate, TypeDateTime, TypeByte, TypeUUID, TypeAny}
	if depth > 0 {
		types = append(types, TypeObject, TypeObject, TypeArray, TypeArray)
	}
	p := &Property{Type: types[r.Intn(len(types))]}
	if r.Intn(3) == 0 {
		p.Required = sPtrBool(r.Intn(2) == 0)
	}
	rules := &ValidationRules{}
	if r.Intn(4) == 0 {
		p.Validation = rules
	}
	switch p.Type {
	case TypeString, TypeDate, TypeDateTime, TypeByte:
		if r.Intn(2) == 0 {
			rules.MinLength = sPtrFloat(float64(r.Intn(4)))
		}
		if r.Intn(2) == 0 {
			rules.MaxLength = sPtrFloat(float64(r.Intn(8)))
		}
		if r.Intn(3) == 0 {
			rules.Pattern = sPtrString([]string{"^[a-z]+$", `^\d`, "[", "b"}[r.Intn(4)])
		}
		if r.Intn(4) == 0 {
			rules.Format = sPtrString([]string{"email", "uri", "uuid", "date", "datetime", "nope"}[r.Intn(6)])
		}
		if r.Intn(4) == 0 {
			rules.Enum = []string{"a", "b", "2024-01-01"}
		}
		if p.Type == TypeDate && r.Intn(2) == 0 {
			rules.MinDate, rules.MaxDate = sPtrString("2000-01-01"), sPtrString("2030-12-31")
		}
	case TypeNumber:
		rules.Minimum, rules.Maximum = sPtrFloat(-5), sPtrFloat(float64(r.Intn(50)))
	case TypeUUID:
		if r.Intn(2) == 0 {
			p.Prefix = sPtrString("urn:uuid:")
		}
	case TypeObject:
		if r.Intn(6) != 0 {
			p.Properties = map[string]*Property{}
			for _, name := range []string{"a", "b", "c"}[:1+r.Intn(3)] {
				p.Properties[name] = randomProperty(r, depth-1)
			}
		}
	case TypeArray:
		if r.Intn(5) != 0 {
			p.Items = randomProperty(r, depth-1)
		}
		p.Validation = rules
		if r.Intn(2) == 0 {
			rules.MinItems = sPtrFloat(float64(r.Intn(3)))
		}
		if r.Intn(2) == 0 {
			rules.MaxItems = sPtrFloat(float64(r.Intn(4)))
		}
		if r.Intn(2) == 0 {
			rules.UniqueItems = sPtrBool(true)
		}
	}
	return p
}

// randomValue generates a value that mostly fits p, with a fair share of misfits.
func randomValue(r *rand.Rand, p *Property, depth int) interface{} {
	if p == nil || r.Intn(8) == 0 || depth < 0 {
		return randomAny(r, 2)
	}
	switch p.Type {
	case TypeString, TypeByte, TypeDate, TypeDateTime, TypeUUID:
		return []string{"", "a", "ab", "abc", "2024-01-01", "1999-12-31", "2025-01-09T10:30:00Z", "SGk=", "SGVsbG8gV29ybGQ=",
			"!!", "user@example.com", "https://x", "550e8400-e29b-41d4-a716-446655440000", "urn:uuid:550e8400-e29b-41d4-a716-446655440000", "1"}[r.Intn(15)]
	case TypeNumber:
		return float64(r.Intn(80) - 20)
	case TypeBoolean:
		return r.Intn(2) == 0
	case TypeObject:
		obj := map[string]interface{}{}
		for name, def := range p.Properties {
			if r.Intn(4) != 0 {
				obj[name] = randomValue(r, def, depth-1)
			}
		}
		if r.Intn(4) == 0 {
			obj["extra"] = randomAny(r, 2)
		}
		return obj
	case TypeArray:
		n := r.Intn(6)
		arr := make([]interface{}, 0, n)
		for i := 0; i < n; i++ {
			if i > 0 && r.Intn(5) == 0 {
				arr = append(arr, arr[r.Intn(i)])
				continue
			}
			arr = append(arr, randomValue(r, p.Items, depth-1))
		}
		return arr
	}
	return randomAny(r, 2)
}

func randomAny(r *rand.Rand, depth int) interface{} {
	k := r.Intn(7)
	if depth <= 0 && k >= 5 {
		k = r.Intn(5)
	}
	switch k {
	case 0:
		return nil
	case 1:
		return r.Intn(2) == 0
	case 2:
		return float64(r.Intn(10))
	case 3:
		return "1"
	case 4:
		return "x"
	case 5:
		return []interface{}{randomAny(r, depth-1), randomAny(r, depth-1)}
	default:
		return map[string]interface{}{"a": randomAny(r, depth-1), "z": randomAny(r, depth-1)}
	}
}

func randomSchema(r *rand.Rand) *Schema {
	var root *Property
	if r.Intn(3) == 0 {
		root = &Property{Type: TypeArray, Items: randomProperty(r, 3)}
	} else {
		root = randomProperty(r, 4)
		if root.Type != TypeObject {
			root = &Property{Type: TypeObject, Properties: map[string]*Property{"a": root, "data": randomProperty(r, 3)}}
		}
	}
	return &Schema{Type: root.Type, Properties: root.Properties, Items: root.Items}
}

func TestValidateStream_DifferentialGenerated(t *testing.T) {
	r := rand.New(rand.NewSource(20261006))
	v := NewValidator()
	n := 4000
	if testing.Short() {
		n = 500
	}
	checked, invalid := 0, 0
	for i := 0; i < n; i++ {
		s := randomSchema(r)
		root := &Property{Type: s.Type, Properties: s.Properties, Items: s.Items}
		for j := 0; j < 5; j++ {
			b, err := stdjson.Marshal(randomValue(r, root, 5))
			if err != nil {
				t.Fatal(err)
			}
			if checkDifferential(t, v, s, b) {
				checked++
				var data interface{}
				_ = stdjson.Unmarshal(b, &data)
				if !v.Validate(data, s).Valid {
					invalid++
				}
			}
		}
	}
	// Both verdicts must be well represented, or the comparison proves little.
	if invalid < checked/10 || checked-invalid < checked/10 {
		t.Fatalf("%d cases checked, %d invalid: the generator is too one-sided", checked, invalid)
	}
	t.Logf("%d cases checked, %d invalid", checked, invalid)
}

// fuzzSchemas are the schemas FuzzValidateStream picks from.
func fuzzSchemas() []*Schema {
	var out []*Schema
	for _, c := range streamCorpus() {
		out = append(out, c.schema)
	}
	r := rand.New(rand.NewSource(7))
	for i := 0; i < 16; i++ {
		out = append(out, randomSchema(r))
	}
	return out
}

func FuzzValidateStream(f *testing.F) {
	schemas := fuzzSchemas()
	for i, c := range streamCorpus() {
		for _, in := range c.inputs {
			f.Add(uint8(i), []byte(in))
		}
	}
	f.Add(uint8(20), []byte(`{"a":[1,"1",1],"data":{"a":{"b":[]}}}`))
	v := NewValidator()
	f.Fuzz(func(t *testing.T, idx uint8, input []byte) {
		s := schemas[int(idx)%len(schemas)]
		if !checkDifferential(t, v, s, input) {
			// Not JSON json.Unmarshal accepts: the stream must not panic, whatever it returns.
			_, _ = v.ValidateStream(stdjson.NewDecoder(bytes.NewReader(input)), s, StreamOptions{CollectAllErrors: true})
		}
	})
}

func TestValidateStream_RecordsPathAndErrorPaths(t *testing.T) {
	s := &Schema{Type: TypeObject, Properties: map[string]*Property{
		"data": {Type: TypeArray, Items: &Property{Type: TypeObject, Properties: map[string]*Property{
			"email": {Type: TypeString, Validation: &ValidationRules{Format: sPtrString("email")}},
		}}},
	}}
	var b strings.Builder
	b.WriteString(`{"meta":{"n":1},"data":[`)
	for i := 0; i < 50; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		email := fmt.Sprintf("user%d@example.com", i)
		if i == 41 {
			email = "not-an-email"
		}
		fmt.Fprintf(&b, `{"email":%q}`, email)
	}
	b.WriteString(`]}`)

	var got []int64
	res, err := NewValidator().ValidateStream(stdjson.NewDecoder(strings.NewReader(b.String())), s, StreamOptions{
		CollectAllErrors: true,
		RecordsPath:      "data",
		OnItem: func(item interface{}, index int64) error {
			if _, ok := item.(map[string]interface{}); !ok {
				t.Fatalf("item %d is %T", index, item)
			}
			got = append(got, index)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 50 || got[49] != 49 {
		t.Fatalf("OnItem saw %d items", len(got))
	}
	if res.Valid || len(res.Errors) != 1 || res.Errors[0].Path != "root.data[41].email" || res.Errors[0].Code != "FORMAT_MISMATCH" {
		t.Fatalf("got %+v", res)
	}

	// An error from OnItem stops the walk and is returned.
	stopErr := errors.New("sink full")
	_, err = NewValidator().ValidateStream(stdjson.NewDecoder(strings.NewReader(b.String())), s, StreamOptions{
		RecordsPath: "data",
		OnItem: func(_ interface{}, index int64) error {
			if index == 3 {
				return stopErr
			}
			return nil
		},
	})
	if !errors.Is(err, stopErr) {
		t.Fatalf("err = %v, want %v", err, stopErr)
	}

	// A records array the schema does not name is still delivered, and not validated.
	n := 0
	res, err = NewValidator().ValidateStream(stdjson.NewDecoder(strings.NewReader(`{"other":{"rows":[1,"x",{}]}}`)), s, StreamOptions{
		RecordsPath: "other.rows",
		OnItem:      func(interface{}, int64) error { n++; return nil },
	})
	if err != nil || !res.Valid || n != 3 {
		t.Fatalf("res %+v err %v n %d", res, err, n)
	}

	// A root array with RecordsPath "".
	n = 0
	rootSchema := &Schema{Type: TypeArray, Items: &Property{Type: TypeNumber}}
	res, err = NewValidator().ValidateStream(stdjson.NewDecoder(strings.NewReader(`[1,2,"x"]`)), rootSchema, StreamOptions{
		CollectAllErrors: true,
		OnItem:           func(interface{}, int64) error { n++; return nil },
	})
	if err != nil || res.Valid || n != 3 || res.Errors[0].Path != "root[2]" {
		t.Fatalf("res %+v err %v n %d", res, err, n)
	}
}

func TestValidateStream_StopsAtFirstError(t *testing.T) {
	s := &Schema{Type: TypeObject, Properties: map[string]*Property{
		"xs": {Type: TypeArray, Validation: &ValidationRules{MaxItems: sPtrFloat(2)}},
	}}
	// maxItems fails as soon as it is passed: the malformed tail is never read.
	res, err := NewValidator().ValidateStream(stdjson.NewDecoder(strings.NewReader(`{"xs":[1,2,3,!!!`)), s, StreamOptions{})
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if res.Valid || len(res.Errors) != 1 || res.Errors[0].Code != "MAX_ITEMS" || res.Errors[0].Path != "root.xs" {
		t.Fatalf("got %+v", res)
	}
	// Collecting every error reads on and reports the malformed tail.
	if _, err := NewValidator().ValidateStream(stdjson.NewDecoder(strings.NewReader(`{"xs":[1,2,3,!!!`)), s, StreamOptions{CollectAllErrors: true}); err == nil {
		t.Fatal("want a syntax error")
	}
}

func TestValidateStream_SyntaxErrors(t *testing.T) {
	s := &Schema{Type: TypeObject, Properties: map[string]*Property{"a": {Type: TypeArray, Items: &Property{Type: TypeString}}}}
	for _, in := range []string{``, `{`, `{"a":[`, `{"a":["x",]}`, `{"a" 1}`, `[1e400]`} {
		if _, err := NewValidator().ValidateStream(stdjson.NewDecoder(strings.NewReader(in)), s, StreamOptions{CollectAllErrors: true}); err == nil {
			t.Errorf("input %q: want an error", in)
		}
	}
}

func TestValidator_PatternCompiledOnce(t *testing.T) {
	v := NewValidator()
	s := &Schema{Type: TypeArray, Items: &Property{Type: TypeString, Validation: &ValidationRules{Pattern: sPtrString("^[a-z]+$")}}}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := v.ValidateStream(stdjson.NewDecoder(strings.NewReader(`["abc","ABC","x"]`)), s, StreamOptions{CollectAllErrors: true})
			if err != nil || len(res.Errors) != 1 || res.Errors[0].Path != "root[1]" {
				t.Errorf("res %+v err %v", res, err)
			}
		}()
	}
	wg.Wait()
	n := 0
	v.patterns.Range(func(_, _ interface{}) bool { n++; return true })
	if n != 1 {
		t.Fatalf("%d cached patterns, want 1", n)
	}
	first, _ := v.compilePattern("^[a-z]+$")
	second, _ := v.compilePattern("^[a-z]+$")
	if first != second {
		t.Fatal("pattern compiled twice")
	}
	if _, err := v.compilePattern("["); err == nil {
		t.Fatal("want an error for an invalid pattern")
	}
}

// writeArray streams `{"data":[item(0),…,item(n-1)]}` (or a bare array when wrap is false) into a
// pipe, so the input is never held in memory.
func writeArray(n int, wrap bool, item func(w *bufio.Writer, i int)) io.Reader {
	pr, pw := io.Pipe()
	go func() {
		w := bufio.NewWriterSize(pw, 64<<10)
		if wrap {
			w.WriteString(`{"data":`)
		}
		w.WriteByte('[')
		for i := 0; i < n; i++ {
			if i > 0 {
				w.WriteByte(',')
			}
			item(w, i)
		}
		w.WriteByte(']')
		if wrap {
			w.WriteByte('}')
		}
		pw.CloseWithError(w.Flush())
	}()
	return pr
}

func TestValidateStream_UniqueItemsMillionApart(t *testing.T) {
	const n = 1_000_001
	s := &Schema{Type: TypeObject, Properties: map[string]*Property{
		"data": {Type: TypeArray, Items: &Property{Type: TypeObject}, Validation: &ValidationRules{UniqueItems: sPtrBool(true)}},
	}}
	in := writeArray(n, true, func(w *bufio.Writer, i int) {
		k := i
		if i == n-1 {
			k = 0 // the same as item 0, a million items later
		}
		fmt.Fprintf(w, `{"k":%d,"tag":"t"}`, k)
	})
	res, err := NewValidator().ValidateStream(stdjson.NewDecoder(in), s, StreamOptions{CollectAllErrors: true})
	if err != nil {
		t.Fatal(err)
	}
	if res.Valid || len(res.Errors) != 1 || res.Errors[0].Path != "root.data[1000000]" || res.Errors[0].Code != "DUPLICATE_ITEM" {
		t.Fatalf("got %+v", res)
	}
}

func TestValidateStream_LargeRootArrayBoundedMemory(t *testing.T) {
	if testing.Short() {
		t.Skip("streams about 200 MB")
	}
	const n = 1_000_000 // about 200 bytes an item
	s := &Schema{Type: TypeArray, Items: &Property{Type: TypeObject, Properties: map[string]*Property{
		"id":    {Type: TypeNumber, Required: sPtrBool(true), Validation: &ValidationRules{Minimum: sPtrFloat(0)}},
		"name":  {Type: TypeString, Validation: &ValidationRules{Pattern: sPtrString("^[A-Za-z ]+$"), MaxLength: sPtrFloat(200)}},
		"email": {Type: TypeString, Validation: &ValidationRules{Pattern: sPtrString(`^[a-z0-9]+@example\.com$`)}},
		"tags":  {Type: TypeArray, Items: &Property{Type: TypeString}, Validation: &ValidationRules{UniqueItems: sPtrBool(true)}},
	}}}
	padding := strings.Repeat("Patient Name ", 10)
	var written int64
	in := writeArray(n, false, func(w *bufio.Writer, i int) {
		c, _ := fmt.Fprintf(w, `{"id":%d,"name":%q,"email":"user%d@example.com","tags":["a","b","c"]}`, i, padding, i)
		written += int64(c)
	})

	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	base := ms.HeapInuse
	var peak uint64
	var count int64
	res, err := NewValidator().ValidateStream(stdjson.NewDecoder(in), s, StreamOptions{
		RecordsPath: "",
		OnItem: func(_ interface{}, index int64) error {
			count++
			if index%8192 == 0 {
				runtime.ReadMemStats(&ms)
				if ms.HeapInuse > peak {
					peak = ms.HeapInuse
				}
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Valid || count != n {
		t.Fatalf("valid %v, %d items, errors %v", res.Valid, count, res.Errors)
	}
	if written < 200_000_000 {
		t.Fatalf("only %d bytes streamed", written)
	}
	const ceiling = 64 << 20
	if peak > base && peak-base > ceiling {
		t.Fatalf("heap in use grew by %d MiB streaming %d MiB, ceiling %d MiB", (peak-base)>>20, written>>20, ceiling>>20)
	}
	t.Logf("streamed %d MiB, %d items, peak heap growth %d MiB", written>>20, count, int64(peak-base)>>20)
}

// Duplicate keys are the one documented difference in verdict: json.Unmarshal keeps the last
// occurrence, and so does ValidateStream inside an array item (decoded whole), but in a walked
// object ValidateStream validates every occurrence.
func TestValidateStream_DuplicateKeys(t *testing.T) {
	s := &Schema{Type: TypeObject, Properties: map[string]*Property{
		"a":     {Type: TypeString},
		"items": {Type: TypeArray, Items: &Property{Type: TypeObject, Properties: map[string]*Property{"a": {Type: TypeString}}}},
	}}
	v := NewValidator()
	validate := func(in string) (*contracts.ValidationResult, *contracts.ValidationResult) {
		var data interface{}
		if err := stdjson.Unmarshal([]byte(in), &data); err != nil {
			t.Fatal(err)
		}
		got, err := v.ValidateStream(stdjson.NewDecoder(strings.NewReader(in)), s, StreamOptions{CollectAllErrors: true})
		if err != nil {
			t.Fatal(err)
		}
		return v.Validate(data, s), got
	}

	want, got := validate(`{"items":[{"a":1,"a":"x"}]}`)
	if !want.Valid || !got.Valid {
		t.Fatalf("in an item the last occurrence wins: validate %v, stream %v", want.Errors, got.Errors)
	}
	want, got = validate(`{"a":1,"a":"x"}`)
	if !want.Valid || got.Valid || got.Errors[0].Path != "root.a" || got.Errors[0].Code != "TYPE_MISMATCH" {
		t.Fatalf("in a walked object every occurrence is validated: validate %v, stream %v", want.Errors, got.Errors)
	}
}
