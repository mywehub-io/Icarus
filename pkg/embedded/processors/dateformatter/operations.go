package dateformatter

import (
	"fmt"
	"strings"
	"time"

	"github.com/wehubfusion/Icarus/pkg/embedded/processors/internal/compiled"
)

// executeFormat performs date format conversion with optional timezone transformation.
func executeFormat(nodeID string, itemIndex int, input map[string]interface{}, cfg Config) (map[string]interface{}, error) {
	// Gracefully handle missing input map.
	if input == nil {
		return map[string]interface{}{"result": nil}, nil
	}

	val, exists := input["date"]
	if !exists || val == nil {
		return map[string]interface{}{"result": nil}, nil
	}

	dateStr, ok := val.(string)
	if !ok {
		return nil, NewInputError(nodeID, itemIndex, "date", fmt.Sprintf("expected string, got %T", val))
	}

	if strings.TrimSpace(dateStr) == "" {
		return map[string]interface{}{"result": nil}, nil
	}

	// Normalize input date string (handle partial DateTime and compact DateOnly).
	normalized := normalizeInputDate(dateStr, TimeFormat(cfg.InFormat))

	inputLayout := cfg.GetInputLayout()
	outputLayout := cfg.GetOutputLayout()

	parsedTime, err := parseDateTime(nodeID, itemIndex, normalized, inputLayout, cfg.InTimezone)
	if err != nil {
		return nil, err
	}

	// Convert to output timezone if specified.
	if cfg.OutTimezone != "" {
		location, tzErr := compiled.Location(cfg.OutTimezone)
		if tzErr != nil {
			return nil, NewTimezoneError(nodeID, itemIndex, cfg.OutTimezone, "invalid output timezone", tzErr)
		}
		parsedTime = parsedTime.In(location)
	}

	// Format in UTC when output format is UTC.
	if TimeFormat(cfg.OutFormat) == FormatUTC {
		parsedTime = parsedTime.UTC()
	}

	// Format the date using output layout.
	formatted := parsedTime.Format(outputLayout)

	return map[string]interface{}{"result": formatted}, nil
}

// parseDateTime parses a date string with timezone support.
func parseDateTime(nodeID string, itemIndex int, dateStr, layout, timezone string) (time.Time, error) {
	var parsed time.Time
	var err error

	if timezone != "" {
		location, locErr := compiled.Location(timezone)
		if locErr != nil {
			return time.Time{}, NewTimezoneError(nodeID, itemIndex, timezone, "invalid input timezone", locErr)
		}
		parsed, err = time.ParseInLocation(layout, dateStr, location)
	} else {
		parsed, err = time.Parse(layout, dateStr)
	}

	if err != nil {
		return time.Time{}, NewParseError(nodeID, itemIndex, dateStr, layout, "invalid date format", err)
	}

	return parsed, nil
}

// normalizeInputDate handles partial DateTime inputs and format-specific normalization.
func normalizeInputDate(dateStr string, format TimeFormat) string {
	// Handle partial DateTime inputs (add missing time components).
	if format == FormatDateTime {
		if len(dateStr) == 10 && strings.Count(dateStr, "-") == 2 {
			return dateStr + " 00:00:00"
		}
		if len(dateStr) == 16 && strings.Count(dateStr, ":") == 1 {
			return dateStr + ":00"
		}
	}

	// Handle DateOnly compact format (YYYYMMDD -> YYYY-MM-DD).
	if format == FormatDateOnly {
		if len(dateStr) == 8 && !strings.Contains(dateStr, "-") && !strings.Contains(dateStr, "/") {
			return dateStr[:4] + "-" + dateStr[4:6] + "-" + dateStr[6:8]
		}
	}

	return dateStr
}
