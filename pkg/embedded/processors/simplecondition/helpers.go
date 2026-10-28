package simplecondition

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// getValueFromPath extracts a value from a map using dot notation path.
func getValueFromPath(data map[string]interface{}, path string) (interface{}, bool) {
	if path == "" {
		return nil, false
	}
	parts := strings.Split(path, ".")
	current := interface{}(data)
	for i, part := range parts {
		if m, ok := current.(map[string]interface{}); ok {
			val, exists := m[part]
			if !exists {
				return nil, false
			}
			current = val
			continue
		}
		if i < len(parts)-1 {
			return nil, false
		}
	}
	return current, true
}

// toFloat64 converts a value to float64.
func toFloat64(value interface{}) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			// Neither the string nor the strconv error (which repeats it) is echoed: it is node input.
			return 0, fmt.Errorf("cannot convert string of length %d to number", len(v))
		}
		return f, nil
	default:
		return 0, fmt.Errorf("cannot convert type %T to number", value)
	}
}

// toString converts a value to string.
func toString(value interface{}) string {
	if value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return v
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case bool:
		return strconv.FormatBool(v)
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v)
		}
		return string(b)
	}
}

// toSlice converts a value to a slice.
func toSlice(value interface{}) ([]interface{}, error) {
	switch v := value.(type) {
	case []interface{}:
		return v, nil
	case []string:
		result := make([]interface{}, len(v))
		for i, s := range v {
			result[i] = s
		}
		return result, nil
	case []int:
		result := make([]interface{}, len(v))
		for i, n := range v {
			result[i] = n
		}
		return result, nil
	case []float64:
		result := make([]interface{}, len(v))
		for i, f := range v {
			result[i] = f
		}
		return result, nil
	default:
		return nil, fmt.Errorf("cannot convert type %T to slice", value)
	}
}

// isEmpty returns true if the value is considered empty: nil, empty string, zero number, false, or empty slice.
func isEmpty(value interface{}) bool {
	if value == nil {
		return true
	}
	switch v := value.(type) {
	case string:
		return v == ""
	case float64:
		return v == 0
	case int:
		return v == 0
	case int64:
		return v == 0
	case bool:
		return !v
	case []interface{}:
		return len(v) == 0
	case []string:
		return len(v) == 0
	case []int:
		return len(v) == 0
	case []float64:
		return len(v) == 0
	default:
		return false
	}
}

// valuesEqual checks if two values are equal, with optional case-insensitive comparison.
func valuesEqual(actualValue, expectedValue interface{}, caseInsensitive bool) bool {
	if actualValue == nil && expectedValue == nil {
		return true
	}
	if actualValue == nil || expectedValue == nil {
		return false
	}
	actualStr := toString(actualValue)
	expectedStr := toString(expectedValue)
	if caseInsensitive {
		return strings.EqualFold(actualStr, expectedStr)
	}
	return actualStr == expectedStr
}

// countMetConditions counts how many conditions were met.
func countMetConditions(results map[string]bool) int {
	count := 0
	for _, met := range results {
		if met {
			count++
		}
	}
	return count
}

// countUnmetConditions counts how many conditions were not met.
func countUnmetConditions(results map[string]bool) int {
	count := 0
	for _, met := range results {
		if !met {
			count++
		}
	}
	return count
}
