package jsrunner

import (
	"encoding/json"
	"fmt"
	"time"
)

// SecurityLevel defines the security restrictions for JavaScript execution
const (
	SecurityLevelStrict     = "strict"
	SecurityLevelStandard   = "standard"
	SecurityLevelPermissive = "permissive"
)

// Config represents the configuration for a JavaScript execution node
type Config struct {
	// Script is the JavaScript code to execute
	Script string `json:"script"`

	// InputSchemaID is the ID of the input schema (enriched to inputSchema by Elysium).
	// JSON key is input_schema_id per seed-node-schemas.sql.
	InputSchemaID string `json:"input_schema_id,omitempty"`

	// InputSchema is the inline JSON schema for input validation (enriched from input_schema_id)
	InputSchema map[string]interface{} `json:"inputSchema,omitempty"`

	// OutputSchemaID is the ID of the output schema (enriched to outputSchema by Elysium).
	// JSON key is output_schema_id per seed-node-schemas.sql.
	OutputSchemaID string `json:"output_schema_id,omitempty"`

	// OutputSchema is the inline JSON schema for output validation (enriched from outputSchemaID)
	OutputSchema map[string]interface{} `json:"outputSchema,omitempty"`

	// ManualInputs allows manually specifying inputs instead of using ProcessInput.Data
	ManualInputs map[string]interface{} `json:"manual_inputs,omitempty"`

	// Timeout is the maximum execution time for the script
	Timeout time.Duration `json:"timeout,omitempty"`

	// SecurityLevel defines security restrictions (strict, standard, permissive)
	SecurityLevel string `json:"security_level,omitempty"`

	// EnabledUtilities is a list of utility modules to enable (console, json, encoding, timers)
	EnabledUtilities []string `json:"enabled_utilities,omitempty"`

	// ApplySchemaDefaults specifies whether to apply schema defaults
	ApplySchemaDefaults bool `json:"apply_defaults,omitempty"`

	// StructureData specifies whether to structure data according to schema
	StructureData bool `json:"structure_data,omitempty"`

	// StrictValidation specifies whether to use strict schema validation.
	// Default: true. Set explicitly to false to allow invalid data to pass through.
	StrictValidation *bool `json:"strict_validation,omitempty"`

	// MaxStackDepth is the maximum call stack depth
	MaxStackDepth int `json:"max_stack_depth,omitempty"`
}

// ApplyDefaults sets default values for configuration fields
func (c *Config) ApplyDefaults() {
	if c.Timeout == 0 {
		c.Timeout = time.Minute
	}
	if c.SecurityLevel == "" {
		c.SecurityLevel = SecurityLevelStandard
	}
	if c.EnabledUtilities == nil {
		c.EnabledUtilities = DefaultUtilitiesByLevel[c.SecurityLevel]
	}
	if c.MaxStackDepth == 0 {
		c.MaxStackDepth = 100
	}
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.Script == "" {
		return fmt.Errorf("script is required")
	}
	if c.Timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}
	if c.SecurityLevel != SecurityLevelStrict &&
		c.SecurityLevel != SecurityLevelStandard &&
		c.SecurityLevel != SecurityLevelPermissive {
		return fmt.Errorf("invalid security level: %s", c.SecurityLevel)
	}
	if c.MaxStackDepth <= 0 {
		return fmt.Errorf("max_stack_depth must be positive")
	}
	return nil
}

// GetStrictValidation returns whether strict schema validation mode should be used.
// Defaults to true when not explicitly configured.
func (c *Config) GetStrictValidation() bool {
	if c.StrictValidation != nil {
		return *c.StrictValidation
	}
	return true
}

// HasInputSchema returns true if an input schema is configured
func (c *Config) HasInputSchema() bool {
	return c.InputSchemaID != "" || len(c.InputSchema) > 0
}

// HasOutputSchema returns true if an output schema is configured
func (c *Config) HasOutputSchema() bool {
	return c.OutputSchemaID != "" || len(c.OutputSchema) > 0
}

// GetInputSchema returns the input schema, preferring enriched schema over ID
func (c *Config) GetInputSchema() map[string]interface{} {
	if len(c.InputSchema) > 0 {
		return c.InputSchema
	}
	return nil
}

// GetOutputSchema returns the output schema, preferring enriched schema over ID
func (c *Config) GetOutputSchema() map[string]interface{} {
	if len(c.OutputSchema) > 0 {
		return c.OutputSchema
	}
	return nil
}

// DefaultUtilitiesByLevel defines default utilities for each security level
var DefaultUtilitiesByLevel = map[string][]string{
	SecurityLevelStrict:     {"console", "json"},
	SecurityLevelStandard:   {"console", "json", "encoding"},
	SecurityLevelPermissive: {"console", "json", "encoding", "timers"},
}

// UnmarshalJSON implements custom JSON unmarshaling for Config
func (c *Config) UnmarshalJSON(data []byte) error {
	type Alias Config
	aux := &struct {
		Timeout string `json:"timeout,omitempty"`
		*Alias
	}{
		Alias: (*Alias)(c),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	// Parse timeout if provided as string
	if aux.Timeout != "" {
		duration, err := time.ParseDuration(aux.Timeout)
		if err != nil {
			return fmt.Errorf("invalid timeout format: %w", err)
		}
		c.Timeout = duration
	}

	return nil
}

// copyJSONMap returns a deep copy of a JSON-shaped map (nested maps and slices are copied; other
// values are immutable scalars), so a script's VM never holds the cached configuration's own maps.
func copyJSONMap(m map[string]interface{}) map[string]interface{} {
	if m == nil {
		return nil
	}
	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		out[k] = copyJSONValue(v)
	}
	return out
}

func copyJSONValue(v interface{}) interface{} {
	switch t := v.(type) {
	case map[string]interface{}:
		return copyJSONMap(t)
	case []interface{}:
		out := make([]interface{}, len(t))
		for i, e := range t {
			out[i] = copyJSONValue(e)
		}
		return out
	default:
		return v
	}
}
