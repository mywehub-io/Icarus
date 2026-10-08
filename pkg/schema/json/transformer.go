package json

import (
	"fmt"

	"github.com/google/uuid"
)

// Transformer handles data transformation operations
type Transformer struct{}

// NewTransformer creates a new data transformer
func NewTransformer() *Transformer {
	return &Transformer{}
}

// ApplyDefaults applies default values from schema to data
// Only applies to fields that are missing (doesn't override existing values)
func (t *Transformer) ApplyDefaults(data interface{}, schema *Schema) (interface{}, error) {
	switch schema.Type {
	case TypeArray:
		if data == nil {
			data = []interface{}{}
		} else if _, ok := data.([]interface{}); !ok {
			return nil, fmt.Errorf("schema type is ARRAY but data is %T, expected []interface{}", data)
		}
	case TypeObject:
		if data == nil {
			data = make(map[string]interface{})
		} else if _, ok := data.(map[string]interface{}); !ok {
			return nil, fmt.Errorf("schema type is OBJECT but data is %T, expected map[string]interface{}", data)
		}
	}

	prop := &Property{
		Type:       schema.Type,
		Properties: schema.Properties,
		Items:      schema.Items,
	}
	return t.applyDefaultsToValue(data, prop)
}

// applyDefaultsToValue recursively applies defaults to a value
func (t *Transformer) applyDefaultsToValue(data interface{}, prop *Property) (interface{}, error) {
	switch prop.Type {
	case TypeObject:
		obj, ok := data.(map[string]interface{})
		if !ok {
			obj = make(map[string]interface{})
		}

		if prop.Properties == nil {
			return obj, nil
		}

		for propName, propDef := range prop.Properties {
			value, exists := obj[propName]

			if !exists && propDef.Default != nil {
				// A copy: the transform below may fill nested defaults into it, and the schema
				// (cached and shared, see schema.Engine) must never change.
				obj[propName] = copyDefault(propDef.Default)
			} else if !exists && propDef.Type == TypeUUID {
				generated := uuid.New().String()
				prefix := ""
				if propDef.Prefix != nil {
					prefix = *propDef.Prefix
				}
				postfix := ""
				if propDef.Postfix != nil {
					postfix = *propDef.Postfix
				}
				obj[propName] = prefix + generated + postfix
			} else if exists {
				if propDef.Type == TypeObject && propDef.Properties != nil {
					obj[propName], _ = t.applyDefaultsToValue(value, propDef)
				} else if propDef.Type == TypeArray && propDef.Items != nil {
					obj[propName], _ = t.applyDefaultsToValue(value, propDef)
				}
			} else if !exists && propDef.Type == TypeObject && propDef.Properties != nil {
				nestedObj, _ := t.applyDefaultsToValue(nil, propDef)
				if nestedMap, ok := nestedObj.(map[string]interface{}); ok && len(nestedMap) > 0 {
					obj[propName] = nestedObj
				}
			}
		}
		return obj, nil

	case TypeArray:
		arr, ok := data.([]interface{})
		if !ok {
			return data, nil
		}

		if prop.Items != nil {
			for i, item := range arr {
				processedItem, _ := t.applyDefaultsToValue(item, prop.Items)
				arr[i] = processedItem
			}
		}
		return arr, nil
	}

	return data, nil
}

// StructureData ensures data conforms to schema structure
// Creates missing structures from schema and removes fields not defined in schema
func (t *Transformer) StructureData(data interface{}, schema *Schema) (interface{}, error) {
	switch schema.Type {
	case TypeArray:
		if data == nil {
			data = []interface{}{}
		} else if _, ok := data.([]interface{}); !ok {
			return nil, fmt.Errorf("schema type is ARRAY but data is %T, expected []interface{}", data)
		}
	case TypeObject:
		if data == nil {
			data = make(map[string]interface{})
		} else if _, ok := data.(map[string]interface{}); !ok {
			return nil, fmt.Errorf("schema type is OBJECT but data is %T, expected map[string]interface{}", data)
		}
	}

	prop := &Property{
		Type:       schema.Type,
		Properties: schema.Properties,
		Items:      schema.Items,
	}
	return t.structureValue(data, prop, true)
}

// structureValue recursively structures a value according to property definition
func (t *Transformer) structureValue(data interface{}, prop *Property, createMissing bool) (interface{}, error) {
	switch prop.Type {
	case TypeObject:
		obj, ok := data.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("expected object, got %T", data)
		}

		if prop.Properties == nil {
			return obj, nil
		}

		structured := make(map[string]interface{})

		for propName, propDef := range prop.Properties {
			if value, exists := obj[propName]; exists {
				if propDef.Type == TypeObject && propDef.Properties != nil {
					structuredValue, err := t.structureValue(value, propDef, createMissing)
					if err != nil {
						continue
					}
					structured[propName] = structuredValue
				} else if propDef.Type == TypeArray && propDef.Items != nil {
					structuredValue, err := t.structureValue(value, propDef, createMissing)
					if err != nil {
						continue
					}
					structured[propName] = structuredValue
				} else {
					structured[propName] = value
				}
			} else if createMissing {
				if propDef.Type == TypeObject && propDef.Properties != nil {
					emptyObj := make(map[string]interface{})
					structuredValue, err := t.structureValue(emptyObj, propDef, createMissing)
					if err == nil && len(structuredValue.(map[string]interface{})) > 0 {
						structured[propName] = structuredValue
					}
				} else if propDef.Type == TypeArray && propDef.Items != nil {
					structured[propName] = []interface{}{}
				}
			}
		}

		return structured, nil

	case TypeArray:
		arr, ok := data.([]interface{})
		if !ok {
			return nil, fmt.Errorf("expected array, got %T", data)
		}

		if prop.Items == nil {
			return arr, nil
		}

		structured := make([]interface{}, len(arr))

		for i, item := range arr {
			structuredItem, err := t.structureValue(item, prop.Items, createMissing)
			if err != nil {
				structured[i] = item
			} else {
				structured[i] = structuredItem
			}
		}

		return structured, nil
	}

	return data, nil
}

// TruncateFields truncates nested fields beyond specified level
func (t *Transformer) TruncateFields(data interface{}, level int) interface{} {
	if level <= 0 {
		return data
	}
	return t.truncateRecursive(data, level, 0)
}

// truncateRecursive recursively truncates nested structures
func (t *Transformer) truncateRecursive(data interface{}, maxLevel, currentLevel int) interface{} {
	if currentLevel >= maxLevel {
		switch data.(type) {
		case map[string]interface{}:
			return map[string]interface{}{"__truncated": true}
		case []interface{}:
			return []interface{}{"__truncated"}
		default:
			return data
		}
	}

	switch v := data.(type) {
	case map[string]interface{}:
		result := make(map[string]interface{})
		for key, value := range v {
			result[key] = t.truncateRecursive(value, maxLevel, currentLevel+1)
		}
		return result

	case []interface{}:
		result := make([]interface{}, len(v))
		for i, item := range v {
			result[i] = t.truncateRecursive(item, maxLevel, currentLevel+1)
		}
		return result
	}

	return data
}

// copyDefault deep-copies a default value made of JSON maps and slices.
func copyDefault(v interface{}) interface{} {
	switch t := v.(type) {
	case map[string]interface{}:
		out := make(map[string]interface{}, len(t))
		for k, x := range t {
			out[k] = copyDefault(x)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(t))
		for i, x := range t {
			out[i] = copyDefault(x)
		}
		return out
	default:
		return v
	}
}
