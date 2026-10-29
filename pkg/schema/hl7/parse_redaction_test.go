package hl7

import (
	"strings"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/schema/contracts"
)

// A parse error reaches the validation result and logs; it gives the segment's length, not its
// content. The content is synthetic.
func TestParseSegment_TooShortDoesNotEchoContent(t *testing.T) {
	_, err := parseSegment("Q7", DefaultDelimiters())
	if err == nil {
		t.Fatal("expected an error for a two-character segment")
	}
	if strings.Contains(err.Error(), "Q7") {
		t.Errorf("error %q echoes the segment content", err)
	}
	if !strings.Contains(err.Error(), "length 2") {
		t.Errorf("error %q should give the segment length", err)
	}
}

// A CEL rule's runtime error becomes an HL7_CUSTOM_RULE_RUNTIME_ERROR message. The HL7 helper
// functions name the field location, never the field's value. The message data is synthetic.
func TestCELRuntimeErrors_DoNotEchoFieldValues(t *testing.T) {
	proc := NewHL7SchemaProcessor()
	schemaDef := []byte(`{
		"segments": [
			{"name": "MSH", "usage": "R", "rpt": "1", "fields": [
				{"position": "MSH.1", "dataType": "ST", "usage": "R"},
				{"position": "MSH.2", "dataType": "ST", "usage": "R"},
				{"position": "MSH.9", "dataType": "ST", "usage": "R"}
			]},
			{"name": "PID", "usage": "R", "rpt": "1", "fields": [
				{"position": "PID.3", "dataType": "ST", "usage": "R"},
				{"position": "PID.5", "dataType": "ST", "usage": "O"},
				{"position": "PID.7", "dataType": "ST", "usage": "O"}
			]}
		],
		"rules": [
			{"id": "num", "name": "to number", "assert": "toNumber('PID-5') > 0.0", "message": "m", "severity": "ERROR"},
			{"id": "dtm", "name": "to dtm", "assert": "toDTM('PID-7') < timestamp('2100-01-01T00:00:00Z')", "message": "m", "severity": "ERROR"},
			{"id": "val", "name": "validate as", "assert": "validateAs(msg('PID-5'), 'NM')", "message": "m", "severity": "ERROR"},
			{"id": "pat", "name": "pattern", "assert": "matchesPattern(msg('PID-5'), '^x')", "message": "m", "severity": "ERROR"}
		]
	}`)
	compiled, err := proc.ParseSchema(schemaDef)
	if err != nil {
		t.Fatalf("ParseSchema: %v", err)
	}
	msg := []byte("MSH|^~\\&|||||||ADT^A01\rPID|||999999||SYNTHETICNAME||1987X231")
	result, err := proc.Process(msg, compiled, contracts.ProcessOptions{CollectAllErrors: true})
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	var runtimeErrors int
	for _, e := range result.Errors {
		if e.Code != "HL7_CUSTOM_RULE_RUNTIME_ERROR" {
			continue
		}
		runtimeErrors++
		for _, s := range []string{"SYNTHETICNAME", "1987X231"} {
			if strings.Contains(e.Message, s) || strings.Contains(e.Path, s) {
				t.Errorf("runtime error %q (path %q) echoes the field value %q", e.Message, e.Path, s)
			}
		}
	}
	if runtimeErrors != 4 {
		t.Errorf("expected 4 HL7_CUSTOM_RULE_RUNTIME_ERROR, got %d: %v", runtimeErrors, result.Errors)
	}
}
