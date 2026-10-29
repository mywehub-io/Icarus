package schema

import (
	"strings"
	"testing"
)

// Validation messages reach logs, spans and Sentry, so they name the rule a value broke and never
// the value. The values below are synthetic.

func assertNotEchoed(t *testing.T, result *ValidationResult, wantCode string, secrets ...string) {
	t.Helper()
	if result == nil || result.Valid {
		t.Fatalf("expected an invalid result, got %+v", result)
	}
	var found bool
	for _, e := range result.Errors {
		if e.Code == wantCode {
			found = true
		}
		for _, s := range secrets {
			if strings.Contains(e.Message, s) || strings.Contains(e.Path, s) {
				t.Errorf("%s message %q echoes the submitted value %q", e.Code, e.Message, s)
			}
		}
	}
	if !found {
		t.Errorf("expected %s, got %v", wantCode, result.Errors)
	}
	for _, s := range secrets {
		if strings.Contains(result.ErrorMessage(), s) {
			t.Errorf("ErrorMessage() %q echoes the submitted value %q", result.ErrorMessage(), s)
		}
	}
}

func TestValidateOnly_EnumMismatchDoesNotEchoValue(t *testing.T) {
	def := []byte(`{"type":"OBJECT","properties":{"ward":{"type":"STRING","validation":{"enum":["A1","B2"]}}}}`)
	result, err := NewEngine().ValidateOnly([]byte(`{"ward":"SYNTHETIC-WARD-7731"}`), def)
	if err != nil {
		t.Fatalf("ValidateOnly: %v", err)
	}
	assertNotEchoed(t, result, "ENUM_MISMATCH", "SYNTHETIC-WARD-7731")
	if msg := result.Errors[0].Message; !strings.Contains(msg, "not in allowed values [A1 B2]") {
		t.Errorf("message %q should still name the allowed values", msg)
	}
}

func TestValidateOnly_NumberRangeDoesNotEchoValue(t *testing.T) {
	def := []byte(`{"type":"OBJECT","properties":{
		"low":{"type":"NUMBER","validation":{"minimum":0}},
		"high":{"type":"NUMBER","validation":{"maximum":10.5}}}}`)
	engine := NewEngine()

	result, err := engine.ValidateOnly([]byte(`{"low":-4321.987}`), def)
	if err != nil {
		t.Fatalf("ValidateOnly: %v", err)
	}
	assertNotEchoed(t, result, "MIN_VALUE", "4321")
	if msg := result.Errors[0].Message; msg != "value is less than minimum 0" {
		t.Errorf("MIN_VALUE message = %q", msg)
	}

	result, err = engine.ValidateOnly([]byte(`{"high":98765.4321}`), def)
	if err != nil {
		t.Fatalf("ValidateOnly: %v", err)
	}
	assertNotEchoed(t, result, "MAX_VALUE", "98765")
	if msg := result.Errors[0].Message; msg != "value exceeds maximum 10.5" {
		t.Errorf("MAX_VALUE message = %q", msg)
	}
}

func TestValidateOnly_StringRulesDoNotEchoValue(t *testing.T) {
	def := []byte(`{"type":"OBJECT","properties":{
		"code":{"type":"STRING","validation":{"pattern":"^[0-9]+$","maxLength":4}},
		"mail":{"type":"STRING","validation":{"format":"email"}}}}`)
	result, err := NewEngine().ValidateOnly([]byte(`{"code":"SYNTHETIC-CODE-4410","mail":"SYNTHETIC-MAIL-5520"}`), def)
	if err != nil {
		t.Fatalf("ValidateOnly: %v", err)
	}
	assertNotEchoed(t, result, "PATTERN_MISMATCH", "SYNTHETIC-CODE-4410", "SYNTHETIC-MAIL-5520")
}

func TestValidateCSVOnly_RulesDoNotEchoValue(t *testing.T) {
	def := []byte(`{"columnHeaders":{
		"Ward":{"position":0,"type":"STRING","validation":{"enum":["A1"]}},
		"Score":{"position":1,"type":"NUMBER","validation":{"maximum":10}}}}`)
	engine := NewEngine()
	// Validation stops at the first error, so each rule gets its own row.
	result, err := engine.ValidateCSVOnly([]byte(`[{"Ward":"SYNTHETIC-WARD-7731","Score":1}]`), def)
	if err != nil {
		t.Fatalf("ValidateCSVOnly: %v", err)
	}
	assertNotEchoed(t, result, "ENUM_MISMATCH", "SYNTHETIC-WARD-7731")
	result, err = engine.ValidateCSVOnly([]byte(`[{"Ward":"A1","Score":98765}]`), def)
	if err != nil {
		t.Fatalf("ValidateCSVOnly: %v", err)
	}
	assertNotEchoed(t, result, "MAX_VALUE", "98765")
}

func TestValidateHL7Only_DateRangeDoesNotEchoValue(t *testing.T) {
	def := []byte(`{"segments":[
		{"name":"MSH","usage":"R","rpt":"1","fields":[
			{"position":"MSH-1","dataType":"ST","usage":"R"},
			{"position":"MSH-2","dataType":"ST","usage":"R"},
			{"position":"MSH-9","dataType":"ST","usage":"R"}]},
		{"name":"PID","usage":"R","rpt":"1","fields":[
			{"position":"PID-3","dataType":"ST","usage":"R"},
			{"position":"PID-7","dataType":"DT","usage":"O"}]}]}`)
	// 31 February: the day is out of range for the month, so the error names the component.
	msg := []byte("MSH|^~\\&|||||||ADT^A01\rPID|||999999||||19870231")
	result, err := NewEngine().ValidateHL7Only(msg, def)
	if err != nil {
		t.Fatalf("ValidateHL7Only: %v", err)
	}
	assertNotEchoed(t, result, "HL7_DATATYPE", "1987", "31")
	var found bool
	for _, e := range result.Errors {
		found = found || e.Message == "invalid day for month in date"
	}
	if !found {
		t.Errorf("expected the day range error, got %v", result.Errors)
	}
}
