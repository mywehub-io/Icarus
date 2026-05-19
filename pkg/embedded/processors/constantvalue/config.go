package constantvalue

import (
	"encoding/json"
	"fmt"
	"strconv"
)

// DataType specifies how to interpret constant values.
type DataType string

const (
	DataTypeString  DataType = "string"
	DataTypeNumber  DataType = "number"
	DataTypeBoolean DataType = "boolean"
)

// Config defines the configuration for the constant value generator processor.
type Config struct {
	Constants []ConstantEntry `json:"constants"`
}

// ConstantEntry defines a single constant output field.
type ConstantEntry struct {
	Name  string   `json:"name"`
	Type  DataType `json:"type"`
	Value string   `json:"value"`
}

// UnmarshalJSON accepts configs where "value" is a JSON string, boolean, number, or null.
func (c *ConstantEntry) UnmarshalJSON(data []byte) error {
	var aux struct {
		Name  string          `json:"name"`
		Type  DataType        `json:"type"`
		Value json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	c.Name = aux.Name
	c.Type = aux.Type

	if len(aux.Value) == 0 || string(aux.Value) == "null" {
		c.Value = ""
		return nil
	}

	var s string
	if err := json.Unmarshal(aux.Value, &s); err == nil {
		c.Value = s
		return nil
	}
	var b bool
	if err := json.Unmarshal(aux.Value, &b); err == nil {
		if b {
			c.Value = "true"
		} else {
			c.Value = "false"
		}
		return nil
	}
	var f float64
	if err := json.Unmarshal(aux.Value, &f); err == nil {
		c.Value = strconv.FormatFloat(f, 'f', -1, 64)
		return nil
	}
	return fmt.Errorf("constants.value: unsupported JSON type: %s", string(aux.Value))
}

// Validate checks if the configuration is valid.
func (c *Config) Validate() error {
	if len(c.Constants) == 0 {
		return fmt.Errorf("at least one constant must be specified")
	}

	names := make(map[string]bool)
	for i, entry := range c.Constants {
		if err := entry.Validate(); err != nil {
			return fmt.Errorf("constant at index %d: %w", i, err)
		}
		if names[entry.Name] {
			return fmt.Errorf("duplicate constant name '%s'", entry.Name)
		}
		names[entry.Name] = true

		if _, err := entry.CastValue(); err != nil {
			return fmt.Errorf("constant '%s': %w", entry.Name, err)
		}
	}
	return nil
}

// Validate checks if a constant entry is valid.
func (e *ConstantEntry) Validate() error {
	if e.Name == "" {
		return fmt.Errorf("name is required")
	}
	if e.Type == "" {
		return fmt.Errorf("type is required")
	}
	if e.Type != DataTypeString && e.Type != DataTypeNumber && e.Type != DataTypeBoolean {
		return fmt.Errorf("invalid type '%s', must be 'string', 'number', or 'boolean'", e.Type)
	}
	return nil
}

// CastValue converts the string value to the appropriate Go type based on DataType.
func (e *ConstantEntry) CastValue() (interface{}, error) {
	switch e.Type {
	case DataTypeString:
		return e.Value, nil
	case DataTypeNumber:
		result, err := strconv.ParseFloat(e.Value, 64)
		if err != nil {
			return nil, fmt.Errorf("cannot convert value '%s' to number: %w", e.Value, err)
		}
		return result, nil
	case DataTypeBoolean:
		switch e.Value {
		case "true", "True", "TRUE", "1":
			return true, nil
		case "false", "False", "FALSE", "0":
			return false, nil
		default:
			return nil, fmt.Errorf("cannot convert value '%s' to boolean (expected 'true' or 'false')", e.Value)
		}
	default:
		return nil, fmt.Errorf("unsupported type '%s'", e.Type)
	}
}
