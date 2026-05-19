package tests

import (
	"encoding/json"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/embedded/processors/constantvalue"
)

func TestConfigValidateEmptyConstants(t *testing.T) {
	cfg := constantvalue.Config{}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for empty constants list")
	}
}

func TestConfigValidateDuplicateNames(t *testing.T) {
	cfg := constantvalue.Config{
		Constants: []constantvalue.ConstantEntry{
			{Name: "dup", Type: constantvalue.DataTypeString, Value: "a"},
			{Name: "dup", Type: constantvalue.DataTypeString, Value: "b"},
		},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for duplicate constant names")
	}
}

func TestConstantEntryValidateErrors(t *testing.T) {
	cases := []constantvalue.ConstantEntry{
		{},
		{Name: "", Type: constantvalue.DataTypeString, Value: "x"},
		{Name: "c", Type: "", Value: "x"},
		{Name: "c", Type: "invalid", Value: "x"},
	}
	for i, entry := range cases {
		if err := entry.Validate(); err == nil {
			t.Fatalf("expected validation error for case %d", i)
		}
	}
}

func TestCastValue(t *testing.T) {
	stringEntry := constantvalue.ConstantEntry{
		Name:  "s",
		Type:  constantvalue.DataTypeString,
		Value: "hello",
	}
	val, err := stringEntry.CastValue()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "hello" {
		t.Fatalf("expected 'hello', got %v", val)
	}

	numberEntry := constantvalue.ConstantEntry{
		Name:  "n",
		Type:  constantvalue.DataTypeNumber,
		Value: "42.5",
	}
	val, err = numberEntry.CastValue()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if f, ok := val.(float64); !ok || f != 42.5 {
		t.Fatalf("expected 42.5, got %v", val)
	}

	boolEntry := constantvalue.ConstantEntry{
		Name:  "b",
		Type:  constantvalue.DataTypeBoolean,
		Value: "true",
	}
	val, err = boolEntry.CastValue()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if b, ok := val.(bool); !ok || !b {
		t.Fatalf("expected true, got %v", val)
	}

	numberEntry.Value = "not a number"
	if _, err := numberEntry.CastValue(); err == nil {
		t.Fatal("expected error for invalid number")
	}

	boolEntry.Value = "maybe"
	if _, err := boolEntry.CastValue(); err == nil {
		t.Fatal("expected error for invalid boolean")
	}
}

func TestConstantEntryUnmarshalJSONNativeTypes(t *testing.T) {
	raw := `{"name":"flag","type":"boolean","value":true}`
	var entry constantvalue.ConstantEntry
	if err := json.Unmarshal([]byte(raw), &entry); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if entry.Value != "true" {
		t.Fatalf("expected value 'true', got %q", entry.Value)
	}
	val, err := entry.CastValue()
	if err != nil {
		t.Fatalf("CastValue failed: %v", err)
	}
	if b, ok := val.(bool); !ok || !b {
		t.Fatalf("expected true, got %v", val)
	}

	raw = `{"name":"count","type":"number","value":99}`
	if err := json.Unmarshal([]byte(raw), &entry); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	val, err = entry.CastValue()
	if err != nil {
		t.Fatalf("CastValue failed: %v", err)
	}
	if f, ok := val.(float64); !ok || f != 99 {
		t.Fatalf("expected 99, got %v", val)
	}
}
