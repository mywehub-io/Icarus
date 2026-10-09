package json

import (
	"encoding/json"
	"fmt"
)

// Parser handles parsing of schema definitions
type Parser struct{}

// NewParser creates a new schema parser
func NewParser() *Parser {
	return &Parser{}
}

// Parse parses a schema from JSON bytes
func (p *Parser) Parse(schemaBytes []byte) (*Schema, error) {
	if len(schemaBytes) == 0 {
		return nil, fmt.Errorf("schema bytes cannot be empty")
	}

	var schema Schema
	if err := json.Unmarshal(schemaBytes, &schema); err != nil {
		return nil, fmt.Errorf("failed to parse schema: %w", err)
	}

	// Validate schema structure
	if err := p.validateSchema(&schema); err != nil {
		return nil, fmt.Errorf("invalid schema: %w", err)
	}

	return &schema, nil
}

// validateSchema ensures the schema structure is valid
func (p *Parser) validateSchema(schema *Schema) error {
	if schema.Type == "" {
		return fmt.Errorf("schema type is required")
	}

	if !IsValidType(schema.Type) {
		return fmt.Errorf("invalid schema type: %s", schema.Type)
	}

	// Validate OBJECT type has properties
	if schema.Type == TypeObject && schema.Properties != nil {
		for propName, prop := range schema.Properties {
			if err := p.validateProperty(prop, propName); err != nil {
				return err
			}
		}
	}

	// Validate ARRAY type has items definition
	if schema.Type == TypeArray && schema.Items != nil {
		if err := p.validateProperty(schema.Items, "items"); err != nil {
			return fmt.Errorf("invalid array items: %w", err)
		}
	}

	return nil
}

// validateProperty validates a property definition
func (p *Parser) validateProperty(prop *Property, name string) error {
	if prop.Type == "" {
		return fmt.Errorf("property '%s' must have a type", name)
	}

	if !IsValidType(prop.Type) {
		return fmt.Errorf("property '%s' has invalid type: %s", name, prop.Type)
	}

	// Validate nested OBJECT properties
	if prop.Type == TypeObject && prop.Properties != nil {
		for nestedName, nestedProp := range prop.Properties {
			if err := p.validateProperty(nestedProp, name+"."+nestedName); err != nil {
				return err
			}
		}
	}

	// Validate ARRAY items
	if prop.Type == TypeArray && prop.Items != nil {
		if err := p.validateProperty(prop.Items, name+"[]"); err != nil {
			return err
		}
	}

	// Validate validation rules if present
	if prop.Validation != nil {
		if err := p.validateValidationRules(prop.Validation, prop.Type, name); err != nil {
			return err
		}
	}

	return nil
}

// ValidateValidationRulesForType ensures validation rules are appropriate for the field type.
// Exported for use by the csv package when validating column definitions
func ValidateValidationRulesForType(rules *ValidationRules, fieldType SchemaType, name string) error {
	if rules == nil {
		return nil
	}
	patternSet := rules.Pattern != nil && *rules.Pattern != ""
	formatSet := rules.Format != nil && *rules.Format != ""
	uniqueItemsSet := rules.UniqueItems != nil && *rules.UniqueItems

	// UUID type does not support validation rules
	if fieldType == TypeUUID {
		if rules.MinLength != nil || rules.MaxLength != nil || patternSet || formatSet ||
			len(rules.Enum) > 0 || rules.Minimum != nil || rules.Maximum != nil ||
			rules.MinItems != nil || rules.MaxItems != nil || uniqueItemsSet ||
			rules.MinDate != nil || rules.MaxDate != nil {
			return fmt.Errorf("property '%s': validation rules not supported for type UUID", name)
		}
	}
	if fieldType != TypeString && fieldType != TypeByte && fieldType != TypeDate && fieldType != TypeDateTime {
		if rules.MinLength != nil || rules.MaxLength != nil {
			return fmt.Errorf("property '%s': minLength/maxLength validation rules used on non-string/non-byte type %s", name, fieldType)
		}
	}
	if fieldType != TypeString && fieldType != TypeDate && fieldType != TypeDateTime {
		if patternSet || formatSet || len(rules.Enum) > 0 {
			return fmt.Errorf("property '%s': string validation rules (pattern/format/enum) used on non-string type %s", name, fieldType)
		}
	}
	if fieldType != TypeNumber {
		if rules.Minimum != nil || rules.Maximum != nil {
			return fmt.Errorf("property '%s': number validation rules used on non-number type %s", name, fieldType)
		}
	}
	if fieldType != TypeArray {
		if rules.MinItems != nil || rules.MaxItems != nil || uniqueItemsSet {
			return fmt.Errorf("property '%s': array validation rules used on non-array type %s", name, fieldType)
		}
	}
	// MinDate/MaxDate only allowed for DATE type
	if fieldType != TypeDate && (rules.MinDate != nil || rules.MaxDate != nil) {
		return fmt.Errorf("property '%s': minDate/maxDate validation rules only supported for type DATE", name)
	}
	if fieldType == TypeDate && (rules.MinDate != nil || rules.MaxDate != nil) {
		// Validate format YYYY-MM-DD
		for _, s := range []*string{rules.MinDate, rules.MaxDate} {
			if s != nil && *s != "" {
				if !dateRe.MatchString(*s) {
					return fmt.Errorf("property '%s': minDate/maxDate must be ISO 8601 date (YYYY-MM-DD)", name)
				}
			}
		}
		if rules.MinDate != nil && rules.MaxDate != nil && *rules.MinDate != "" && *rules.MaxDate != "" && *rules.MinDate > *rules.MaxDate {
			return fmt.Errorf("property '%s': minDate must be <= maxDate", name)
		}
	}
	return nil
}

// validateValidationRules ensures validation rules are appropriate for the field type
func (p *Parser) validateValidationRules(rules *ValidationRules, fieldType SchemaType, name string) error {
	return ValidateValidationRulesForType(rules, fieldType, name)
}
