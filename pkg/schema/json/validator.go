package json

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"sync"

	"github.com/wehubfusion/Icarus/pkg/fileref"
	"github.com/wehubfusion/Icarus/pkg/schema/contracts"
)

// Validator validates data against schemas
type Validator struct {
	formatValidators map[string]FormatValidator
	// patterns caches each compiled `pattern` rule by its source string, so a pattern is compiled
	// once and not once per value. Safe for concurrent use; the zero value is ready.
	patterns sync.Map // string -> compiledPattern
}

// compiledPattern is a cached regexp.Compile result; err is kept so an invalid pattern reports the
// same INVALID_PATTERN error on every value without being recompiled.
type compiledPattern struct {
	re  *regexp.Regexp
	err error
}

// compilePattern returns the compiled pattern, compiling it on first use.
func (v *Validator) compilePattern(pattern string) (*regexp.Regexp, error) {
	if c, ok := v.patterns.Load(pattern); ok {
		cp := c.(compiledPattern)
		return cp.re, cp.err
	}
	re, err := regexp.Compile(pattern)
	c, _ := v.patterns.LoadOrStore(pattern, compiledPattern{re: re, err: err})
	cp := c.(compiledPattern)
	return cp.re, cp.err
}

// itemDigest is the uniqueItems key of an array item: SHA-256 of its canonical JSON (json.Marshal
// sorts map keys). Validate and ValidateStream both use it, so they agree on what a duplicate is.
func itemDigest(item interface{}) [sha256.Size]byte {
	b, err := json.Marshal(item)
	if err != nil {
		// Not reachable for decoded JSON; fall back to the Go representation.
		b = []byte(fmt.Sprintf("%T:%v", item, item))
	}
	return sha256.Sum256(b)
}

// NewValidator creates a new schema validator
func NewValidator() *Validator {
	v := &Validator{
		formatValidators: make(map[string]FormatValidator),
	}

	// Register default format validators
	v.RegisterFormat("email", validateEmail)
	v.RegisterFormat("uri", validateURI)
	v.RegisterFormat("uuid", validateUUID)
	v.RegisterFormat("date", validateDate)
	v.RegisterFormat("datetime", validateDateTime)

	return v
}

// RegisterFormat registers a custom format validator
func (v *Validator) RegisterFormat(format string, validator FormatValidator) {
	v.formatValidators[format] = validator
}

// validationState holds errors and whether to collect all or stop after first
type validationState struct {
	errors     []contracts.ValidationError
	collectAll bool
}

func (s *validationState) add(e contracts.ValidationError) bool {
	s.errors = append(s.errors, e)
	return !s.collectAll && len(s.errors) >= 1
}

func (s *validationState) shouldStop() bool {
	return !s.collectAll && len(s.errors) >= 1
}

// ValidateWithRootPath validates data against a schema using the given root path prefix for errors.
// When collectAllErrors is false, validation stops after the first error.
func (v *Validator) ValidateWithRootPath(data interface{}, s *Schema, rootPath string, collectAllErrors bool) *contracts.ValidationResult {
	state := &validationState{collectAll: collectAllErrors}
	prop := &Property{
		Type:       s.Type,
		Properties: s.Properties,
		Items:      s.Items,
	}
	v.validateValueIntoState(data, prop, rootPath, state)
	return &contracts.ValidationResult{
		Valid:  len(state.errors) == 0,
		Errors: state.errors,
	}
}

// ValidateWithOptions validates data against a schema. When collectAllErrors is false (default),
// validation stops after the first error. When true, all errors are collected.
func (v *Validator) ValidateWithOptions(data interface{}, s *Schema, collectAllErrors bool) *contracts.ValidationResult {
	return v.ValidateWithRootPath(data, s, "root", collectAllErrors)
}

// Validate validates data against a schema (collects all errors; backward compatible).
func (v *Validator) Validate(data interface{}, s *Schema) *contracts.ValidationResult {
	return v.ValidateWithOptions(data, s, true)
}

// propRequired returns true if the property is required (Morpheus-aligned: Required is *bool).
func propRequired(prop *Property) bool {
	return prop != nil && prop.Required != nil && *prop.Required
}

// validateValueIntoState validates a value and appends errors to state; stops when state is full (stop-on-first-error).
func (v *Validator) validateValueIntoState(value interface{}, prop *Property, path string, state *validationState) {
	if propRequired(prop) && value == nil {
		if state.add(contracts.ValidationError{Path: path, Message: "field is required", Code: "REQUIRED"}) {
			return
		}
		return
	}
	if value == nil {
		return
	}
	switch prop.Type {
	case TypeString:
		if str, ok := value.(string); ok {
			for _, e := range v.validateString(str, prop.Validation, path) {
				if state.add(e) {
					return
				}
			}
		} else {
			if state.add(contracts.ValidationError{Path: path, Message: fmt.Sprintf("expected string, got %T", value), Code: "TYPE_MISMATCH"}) {
				return
			}
		}
	case TypeNumber:
		var num float64
		switch val := value.(type) {
		case float64:
			num = val
		case int:
			num = float64(val)
		case int64:
			num = float64(val)
		case int32:
			num = float64(val)
		default:
			if state.add(contracts.ValidationError{Path: path, Message: fmt.Sprintf("expected number, got %T", value), Code: "TYPE_MISMATCH"}) {
				return
			}
			return
		}
		for _, e := range v.validateNumber(num, prop.Validation, path) {
			if state.add(e) {
				return
			}
		}
	case TypeBoolean:
		if _, ok := value.(bool); !ok {
			if state.add(contracts.ValidationError{Path: path, Message: fmt.Sprintf("expected boolean, got %T", value), Code: "TYPE_MISMATCH"}) {
				return
			}
		}
	case TypeArray:
		if arr, ok := value.([]interface{}); ok {
			v.validateArrayIntoState(arr, prop, path, state)
		} else {
			if state.add(contracts.ValidationError{Path: path, Message: fmt.Sprintf("expected array, got %T", value), Code: "TYPE_MISMATCH"}) {
				return
			}
		}
	case TypeObject:
		if obj, ok := value.(map[string]interface{}); ok {
			v.validateObjectIntoState(obj, prop, path, state)
		} else {
			if state.add(contracts.ValidationError{Path: path, Message: fmt.Sprintf("expected object, got %T", value), Code: "TYPE_MISMATCH"}) {
				return
			}
		}
	case TypeDate, TypeDateTime:
		if str, ok := value.(string); ok {
			for _, e := range v.validateString(str, prop.Validation, path) {
				if state.add(e) {
					return
				}
			}
			if prop.Type == TypeDate {
				for _, e := range v.validateDateRange(str, prop.Validation, path) {
					if state.add(e) {
						return
					}
				}
			}
		} else {
			if state.add(contracts.ValidationError{Path: path, Message: fmt.Sprintf("expected string for date/datetime, got %T", value), Code: "TYPE_MISMATCH"}) {
				return
			}
		}
	case TypeByte:
		switch val := value.(type) {
		case string:
			for _, e := range v.validateByte(val, prop.Validation, path) {
				if state.add(e) {
					return
				}
			}
		case []byte:
			for _, e := range v.validateByte(string(val), prop.Validation, path) {
				if state.add(e) {
					return
				}
			}
		default:
			if ref, ok := fileref.Parse(value); ok {
				// A byte value is a file reference: its length is the file's size, not a decoded string.
				for _, e := range v.validateByteLength(ref.Size, prop.Validation, path) {
					if state.add(e) {
						return
					}
				}
			} else if state.add(contracts.ValidationError{Path: path, Message: fmt.Sprintf("expected string or bytes, got %T", value), Code: "TYPE_MISMATCH"}) {
				return
			}
		}
	case TypeUUID:
		if str, ok := value.(string); ok {
			for _, e := range v.validateUUIDValue(str, prop, path) {
				if state.add(e) {
					return
				}
			}
		} else {
			if state.add(contracts.ValidationError{Path: path, Message: fmt.Sprintf("expected string for UUID, got %T", value), Code: "TYPE_MISMATCH"}) {
				return
			}
		}
	case TypeAny:
		// no validation
	}
}

// validateArrayIntoState validates array and items, appending to state; stops when state is full.
func (v *Validator) validateArrayIntoState(arr []interface{}, prop *Property, path string, state *validationState) {
	if prop.Validation != nil {
		if prop.Validation.MinItems != nil {
			minN := int(*prop.Validation.MinItems)
			if len(arr) < minN {
				if state.add(contracts.ValidationError{
					Path: path, Message: fmt.Sprintf("array length %d is less than minimum %d", len(arr), minN), Code: "MIN_ITEMS",
				}) {
					return
				}
			}
		}
		if prop.Validation.MaxItems != nil {
			maxN := int(*prop.Validation.MaxItems)
			if len(arr) > maxN {
				if state.add(contracts.ValidationError{
					Path: path, Message: fmt.Sprintf("array length %d exceeds maximum %d", len(arr), maxN), Code: "MAX_ITEMS",
				}) {
					return
				}
			}
		}
		if prop.Validation.UniqueItems != nil && *prop.Validation.UniqueItems {
			seen := make(map[[sha256.Size]byte]bool)
			for i, item := range arr {
				key := itemDigest(item)
				if seen[key] {
					if state.add(contracts.ValidationError{Path: fmt.Sprintf("%s[%d]", path, i), Message: "duplicate item found", Code: "DUPLICATE_ITEM"}) {
						return
					}
					break
				}
				seen[key] = true
			}
		}
	}
	if prop.Items != nil {
		for i, item := range arr {
			if state.shouldStop() {
				return
			}
			v.validateValueIntoState(item, prop.Items, fmt.Sprintf("%s[%d]", path, i), state)
		}
	}
}

// validateObjectIntoState validates object properties, appending to state; stops when state is full.
func (v *Validator) validateObjectIntoState(obj map[string]interface{}, prop *Property, path string, state *validationState) {
	if prop.Properties == nil {
		return
	}
	for propName, propDef := range prop.Properties {
		if state.shouldStop() {
			return
		}
		value, exists := obj[propName]
		propPath := fmt.Sprintf("%s.%s", path, propName)
		if !exists && propRequired(propDef) {
			if state.add(contracts.ValidationError{Path: propPath, Message: "required field missing", Code: "REQUIRED"}) {
				return
			}
			continue
		}
		if exists {
			v.validateValueIntoState(value, propDef, propPath, state)
		}
	}
}

// validateValue validates a value against a property definition (used when collectAllErrors is true).
func (v *Validator) validateValue(value interface{}, prop *Property, path string) []contracts.ValidationError {
	var errors []contracts.ValidationError

	// Check required
	if propRequired(prop) && value == nil {
		errors = append(errors, contracts.ValidationError{
			Path:    path,
			Message: "field is required",
			Code:    "REQUIRED",
		})
		return errors
	}

	// If value is nil and not required, it's valid
	if value == nil {
		return errors
	}

	// Type-specific validation
	switch prop.Type {
	case TypeString:
		if str, ok := value.(string); ok {
			errors = append(errors, v.validateString(str, prop.Validation, path)...)
		} else {
			errors = append(errors, contracts.ValidationError{
				Path:    path,
				Message: fmt.Sprintf("expected string, got %T", value),
				Code:    "TYPE_MISMATCH",
			})
		}

	case TypeNumber:
		var num float64
		switch val := value.(type) {
		case float64:
			num = val
		case int:
			num = float64(val)
		case int64:
			num = float64(val)
		case int32:
			num = float64(val)
		default:
			errors = append(errors, contracts.ValidationError{
				Path:    path,
				Message: fmt.Sprintf("expected number, got %T", value),
				Code:    "TYPE_MISMATCH",
			})
			return errors
		}
		errors = append(errors, v.validateNumber(num, prop.Validation, path)...)

	case TypeBoolean:
		if _, ok := value.(bool); !ok {
			errors = append(errors, contracts.ValidationError{
				Path:    path,
				Message: fmt.Sprintf("expected boolean, got %T", value),
				Code:    "TYPE_MISMATCH",
			})
		}

	case TypeArray:
		if arr, ok := value.([]interface{}); ok {
			errors = append(errors, v.validateArray(arr, prop, path)...)
		} else {
			errors = append(errors, contracts.ValidationError{
				Path:    path,
				Message: fmt.Sprintf("expected array, got %T", value),
				Code:    "TYPE_MISMATCH",
			})
		}

	case TypeObject:
		if obj, ok := value.(map[string]interface{}); ok {
			errors = append(errors, v.validateObject(obj, prop, path)...)
		} else {
			errors = append(errors, contracts.ValidationError{
				Path:    path,
				Message: fmt.Sprintf("expected object, got %T", value),
				Code:    "TYPE_MISMATCH",
			})
		}

	case TypeDate, TypeDateTime:
		if str, ok := value.(string); ok {
			errors = append(errors, v.validateString(str, prop.Validation, path)...)
		} else {
			errors = append(errors, contracts.ValidationError{
				Path:    path,
				Message: fmt.Sprintf("expected string for date/datetime, got %T", value),
				Code:    "TYPE_MISMATCH",
			})
		}

	case TypeByte:
		switch val := value.(type) {
		case string:
			errors = append(errors, v.validateByte(val, prop.Validation, path)...)
		case []byte:
			errors = append(errors, v.validateByte(string(val), prop.Validation, path)...)
		default:
			if ref, ok := fileref.Parse(value); ok {
				// A byte value is a file reference: its length is the file's size, not a decoded string.
				errors = append(errors, v.validateByteLength(ref.Size, prop.Validation, path)...)
			} else {
				errors = append(errors, contracts.ValidationError{
					Path:    path,
					Message: fmt.Sprintf("expected string or bytes, got %T", value),
					Code:    "TYPE_MISMATCH",
				})
			}
		}

	case TypeUUID:
		if str, ok := value.(string); ok {
			errors = append(errors, v.validateUUIDValue(str, prop, path)...)
		} else {
			errors = append(errors, contracts.ValidationError{
				Path:    path,
				Message: fmt.Sprintf("expected string for UUID, got %T", value),
				Code:    "TYPE_MISMATCH",
			})
		}

	case TypeAny:
		// Any type - no validation needed
	}

	return errors
}

// validateUUIDValue validates a string value as UUID with optional prefix/postfix from the property.
func (v *Validator) validateUUIDValue(value string, prop *Property, path string) []contracts.ValidationError {
	prefix := ""
	if prop.Prefix != nil {
		prefix = *prop.Prefix
	}
	postfix := ""
	if prop.Postfix != nil {
		postfix = *prop.Postfix
	}
	if !ValidateUUIDWithPrefixPostfix(value, prefix, postfix) {
		return []contracts.ValidationError{{
			Path:    path,
			Message: "value does not match UUID format (with optional prefix/postfix)",
			Code:    "INVALID_UUID",
		}}
	}
	return nil
}

// validateDateRange validates DATE value against MinDate/MaxDate (Morpheus-aligned; YYYY-MM-DD string comparison).
func (v *Validator) validateDateRange(value string, rules *ValidationRules, path string) []contracts.ValidationError {
	var errors []contracts.ValidationError
	if rules == nil {
		return errors
	}
	if rules.MinDate != nil && value < *rules.MinDate {
		errors = append(errors, contracts.ValidationError{
			Path:    path,
			Message: fmt.Sprintf("date is before minimum %s", *rules.MinDate), // the value is not echoed: it can be a date of birth
			Code:    "MIN_DATE",
		})
	}
	if rules.MaxDate != nil && value > *rules.MaxDate {
		errors = append(errors, contracts.ValidationError{
			Path:    path,
			Message: fmt.Sprintf("date is after maximum %s", *rules.MaxDate),
			Code:    "MAX_DATE",
		})
	}
	return errors
}

// validateString validates string-specific rules (Morpheus-aligned: *float64 lengths, *string pattern/format).
func (v *Validator) validateString(value string, rules *ValidationRules, path string) []contracts.ValidationError {
	var errors []contracts.ValidationError

	if rules == nil {
		return errors
	}

	if rules.MinLength != nil {
		minL := int(*rules.MinLength)
		if len(value) < minL {
			errors = append(errors, contracts.ValidationError{
				Path:    path,
				Message: fmt.Sprintf("length %d is less than minimum %d", len(value), minL),
				Code:    "MIN_LENGTH",
			})
		}
	}

	if rules.MaxLength != nil {
		maxL := int(*rules.MaxLength)
		if len(value) > maxL {
			errors = append(errors, contracts.ValidationError{
				Path:    path,
				Message: fmt.Sprintf("length %d exceeds maximum %d", len(value), maxL),
				Code:    "MAX_LENGTH",
			})
		}
	}

	if rules.Pattern != nil && *rules.Pattern != "" {
		re, err := v.compilePattern(*rules.Pattern)
		if err != nil {
			errors = append(errors, contracts.ValidationError{
				Path:    path,
				Message: fmt.Sprintf("invalid regex pattern: %v", err),
				Code:    "INVALID_PATTERN",
			})
		} else if !re.MatchString(value) {
			errors = append(errors, contracts.ValidationError{
				Path:    path,
				Message: fmt.Sprintf("value does not match pattern '%s'", *rules.Pattern),
				Code:    "PATTERN_MISMATCH",
			})
		}
	}

	if rules.Format != nil && *rules.Format != "" {
		format := *rules.Format
		if validator, exists := v.formatValidators[format]; exists {
			if !validator(value) {
				errors = append(errors, contracts.ValidationError{
					Path:    path,
					Message: fmt.Sprintf("value does not match format '%s'", format),
					Code:    "FORMAT_MISMATCH",
				})
			}
		} else {
			errors = append(errors, contracts.ValidationError{
				Path:    path,
				Message: fmt.Sprintf("unknown format validator: %s", format),
				Code:    "UNKNOWN_FORMAT",
			})
		}
	}

	if len(rules.Enum) > 0 {
		found := false
		for _, allowed := range rules.Enum {
			if value == allowed {
				found = true
				break
			}
		}
		if !found {
			errors = append(errors, contracts.ValidationError{
				Path:    path,
				Message: fmt.Sprintf("value '%s' not in allowed values %v", value, rules.Enum),
				Code:    "ENUM_MISMATCH",
			})
		}
	}

	return errors
}

// validateNumber validates number-specific rules
func (v *Validator) validateNumber(value float64, rules *ValidationRules, path string) []contracts.ValidationError {
	var errors []contracts.ValidationError

	if rules == nil {
		return errors
	}

	if rules.Minimum != nil && value < *rules.Minimum {
		errors = append(errors, contracts.ValidationError{
			Path:    path,
			Message: fmt.Sprintf("value %f is less than minimum %f", value, *rules.Minimum),
			Code:    "MIN_VALUE",
		})
	}

	if rules.Maximum != nil && value > *rules.Maximum {
		errors = append(errors, contracts.ValidationError{
			Path:    path,
			Message: fmt.Sprintf("value %f exceeds maximum %f", value, *rules.Maximum),
			Code:    "MAX_VALUE",
		})
	}

	return errors
}

// validateByte validates byte-specific rules (expects base64-encoded string)
func (v *Validator) validateByte(value string, rules *ValidationRules, path string) []contracts.ValidationError {
	var errors []contracts.ValidationError

	if rules == nil {
		return errors
	}

	decoded, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		decoded, err = base64.URLEncoding.DecodeString(value)
		if err != nil {
			errors = append(errors, contracts.ValidationError{
				Path:    path,
				Message: fmt.Sprintf("invalid base64 encoding: %v", err),
				Code:    "INVALID_BASE64",
			})
			return errors
		}
	}

	return v.validateByteLength(int64(len(decoded)), rules, path)
}

// validateByteLength applies minLength and maxLength to a byte value's length in bytes: the
// decoded length of a base64 string, or the size of a file reference.
func (v *Validator) validateByteLength(length int64, rules *ValidationRules, path string) []contracts.ValidationError {
	var errors []contracts.ValidationError
	if rules == nil {
		return errors
	}
	byteLength := int(length)

	if rules.MinLength != nil {
		minL := int(*rules.MinLength)
		if byteLength < minL {
			errors = append(errors, contracts.ValidationError{
				Path:    path,
				Message: fmt.Sprintf("byte length %d is less than minimum %d", byteLength, minL),
				Code:    "MIN_LENGTH",
			})
		}
	}

	if rules.MaxLength != nil {
		maxL := int(*rules.MaxLength)
		if byteLength > maxL {
			errors = append(errors, contracts.ValidationError{
				Path:    path,
				Message: fmt.Sprintf("byte length %d exceeds maximum %d", byteLength, maxL),
				Code:    "MAX_LENGTH",
			})
		}
	}

	return errors
}

// validateArray validates array-specific rules (Morpheus-aligned: *float64 MinItems/MaxItems, *bool UniqueItems).
func (v *Validator) validateArray(arr []interface{}, prop *Property, path string) []contracts.ValidationError {
	var errors []contracts.ValidationError

	if prop.Validation != nil {
		if prop.Validation.MinItems != nil {
			minN := int(*prop.Validation.MinItems)
			if len(arr) < minN {
				errors = append(errors, contracts.ValidationError{
					Path:    path,
					Message: fmt.Sprintf("array length %d is less than minimum %d", len(arr), minN),
					Code:    "MIN_ITEMS",
				})
			}
		}

		if prop.Validation.MaxItems != nil {
			maxN := int(*prop.Validation.MaxItems)
			if len(arr) > maxN {
				errors = append(errors, contracts.ValidationError{
					Path:    path,
					Message: fmt.Sprintf("array length %d exceeds maximum %d", len(arr), maxN),
					Code:    "MAX_ITEMS",
				})
			}
		}

		if prop.Validation.UniqueItems != nil && *prop.Validation.UniqueItems {
			seen := make(map[[sha256.Size]byte]bool)
			for i, item := range arr {
				key := itemDigest(item)
				if seen[key] {
					errors = append(errors, contracts.ValidationError{
						Path:    fmt.Sprintf("%s[%d]", path, i),
						Message: "duplicate item found",
						Code:    "DUPLICATE_ITEM",
					})
					break
				}
				seen[key] = true
			}
		}
	}

	if prop.Items != nil {
		for i, item := range arr {
			itemPath := fmt.Sprintf("%s[%d]", path, i)
			errors = append(errors, v.validateValue(item, prop.Items, itemPath)...)
		}
	}

	return errors
}

// validateObject validates object properties
func (v *Validator) validateObject(obj map[string]interface{}, prop *Property, path string) []contracts.ValidationError {
	var errors []contracts.ValidationError

	if prop.Properties == nil {
		return errors
	}

	for propName, propDef := range prop.Properties {
		value, exists := obj[propName]
		propPath := fmt.Sprintf("%s.%s", path, propName)

		if !exists && propRequired(propDef) {
			errors = append(errors, contracts.ValidationError{
				Path:    propPath,
				Message: "required field missing",
				Code:    "REQUIRED",
			})
			continue
		}

		if exists {
			errors = append(errors, v.validateValue(value, propDef, propPath)...)
		}
	}

	return errors
}
