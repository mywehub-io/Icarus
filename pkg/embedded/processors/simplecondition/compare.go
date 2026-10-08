package simplecondition

import (
	"fmt"
	"strings"

	"github.com/wehubfusion/Icarus/pkg/embedded/processors/internal/compiled"
)

// compareValues dispatches to the appropriate comparison function based on operator.
func compareValues(nodeID string, itemIndex int, actualValue, expectedValue interface{}, operator ComparisonOperator, caseInsensitive bool) (bool, error) {
	switch operator {
	case OpEquals:
		return compareEquals(actualValue, expectedValue, caseInsensitive)
	case OpNotEquals:
		result, err := compareEquals(actualValue, expectedValue, caseInsensitive)
		return !result, err
	case OpGreaterThan:
		return compareGreaterThan(nodeID, itemIndex, actualValue, expectedValue)
	case OpLessThan:
		return compareLessThan(nodeID, itemIndex, actualValue, expectedValue)
	case OpGreaterThanOrEqual:
		return compareGreaterThanOrEqual(nodeID, itemIndex, actualValue, expectedValue)
	case OpLessThanOrEqual:
		return compareLessThanOrEqual(nodeID, itemIndex, actualValue, expectedValue)
	case OpContains:
		return compareContains(actualValue, expectedValue, caseInsensitive)
	case OpNotContains:
		result, err := compareContains(actualValue, expectedValue, caseInsensitive)
		return !result, err
	case OpStartsWith:
		return compareStartsWith(actualValue, expectedValue, caseInsensitive)
	case OpEndsWith:
		return compareEndsWith(actualValue, expectedValue, caseInsensitive)
	case OpRegex:
		return compareRegex(nodeID, itemIndex, actualValue, expectedValue)
	default:
		return false, NewComparisonError(nodeID, itemIndex, string(operator), "unsupported operator")
	}
}

func compareEquals(actualValue, expectedValue interface{}, caseInsensitive bool) (bool, error) {
	return valuesEqual(actualValue, expectedValue, caseInsensitive), nil
}

func compareGreaterThan(nodeID string, itemIndex int, actualValue, expectedValue interface{}) (bool, error) {
	actual, err := toFloat64(actualValue)
	if err != nil {
		return false, NewComparisonError(nodeID, itemIndex, string(OpGreaterThan), fmt.Sprintf("actual value conversion failed: %v", err))
	}
	expected, err := toFloat64(expectedValue)
	if err != nil {
		return false, NewComparisonError(nodeID, itemIndex, string(OpGreaterThan), fmt.Sprintf("expected value conversion failed: %v", err))
	}
	return actual > expected, nil
}

func compareLessThan(nodeID string, itemIndex int, actualValue, expectedValue interface{}) (bool, error) {
	actual, err := toFloat64(actualValue)
	if err != nil {
		return false, NewComparisonError(nodeID, itemIndex, string(OpLessThan), fmt.Sprintf("actual value conversion failed: %v", err))
	}
	expected, err := toFloat64(expectedValue)
	if err != nil {
		return false, NewComparisonError(nodeID, itemIndex, string(OpLessThan), fmt.Sprintf("expected value conversion failed: %v", err))
	}
	return actual < expected, nil
}

func compareGreaterThanOrEqual(nodeID string, itemIndex int, actualValue, expectedValue interface{}) (bool, error) {
	actual, err := toFloat64(actualValue)
	if err != nil {
		return false, NewComparisonError(nodeID, itemIndex, string(OpGreaterThanOrEqual), fmt.Sprintf("actual value conversion failed: %v", err))
	}
	expected, err := toFloat64(expectedValue)
	if err != nil {
		return false, NewComparisonError(nodeID, itemIndex, string(OpGreaterThanOrEqual), fmt.Sprintf("expected value conversion failed: %v", err))
	}
	return actual >= expected, nil
}

func compareLessThanOrEqual(nodeID string, itemIndex int, actualValue, expectedValue interface{}) (bool, error) {
	actual, err := toFloat64(actualValue)
	if err != nil {
		return false, NewComparisonError(nodeID, itemIndex, string(OpLessThanOrEqual), fmt.Sprintf("actual value conversion failed: %v", err))
	}
	expected, err := toFloat64(expectedValue)
	if err != nil {
		return false, NewComparisonError(nodeID, itemIndex, string(OpLessThanOrEqual), fmt.Sprintf("expected value conversion failed: %v", err))
	}
	return actual <= expected, nil
}

func compareContains(actualValue, expectedValue interface{}, caseInsensitive bool) (bool, error) {
	actual := toString(actualValue)
	expected := toString(expectedValue)
	if caseInsensitive {
		actual = strings.ToLower(actual)
		expected = strings.ToLower(expected)
	}
	return strings.Contains(actual, expected), nil
}

func compareStartsWith(actualValue, expectedValue interface{}, caseInsensitive bool) (bool, error) {
	actual := toString(actualValue)
	expected := toString(expectedValue)
	if caseInsensitive {
		actual = strings.ToLower(actual)
		expected = strings.ToLower(expected)
	}
	return strings.HasPrefix(actual, expected), nil
}

func compareEndsWith(actualValue, expectedValue interface{}, caseInsensitive bool) (bool, error) {
	actual := toString(actualValue)
	expected := toString(expectedValue)
	if caseInsensitive {
		actual = strings.ToLower(actual)
		expected = strings.ToLower(expected)
	}
	return strings.HasSuffix(actual, expected), nil
}

func compareRegex(nodeID string, itemIndex int, actualValue, expectedValue interface{}) (bool, error) {
	actual := toString(actualValue)
	pattern := toString(expectedValue)
	re, err := compiled.Regexp(pattern)
	if err != nil {
		return false, NewComparisonError(nodeID, itemIndex, string(OpRegex), fmt.Sprintf("invalid regex pattern '%s': %v", pattern, err))
	}
	return re.MatchString(actual), nil
}
