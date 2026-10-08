package json

import (
	"regexp"
	"strings"
)

// FormatValidator is a function that validates a string format
type FormatValidator func(value string) bool

// The format patterns are compiled once: a validator runs them for every value of every item.
var (
	emailRe    = regexp.MustCompile(`^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`)
	uuidRe     = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	dateRe     = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
	dateTimeRe = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(Z|[+-]\d{2}:\d{2})$`)
)

// validateEmail validates email format (RFC 5322 basic validation)
func validateEmail(email string) bool {
	if email == "" {
		return false
	}
	return emailRe.MatchString(email)
}

// validateURI validates URI format (basic HTTP/HTTPS/FTP check)
func validateURI(uri string) bool {
	if uri == "" {
		return false
	}
	return strings.HasPrefix(uri, "http://") ||
		strings.HasPrefix(uri, "https://") ||
		strings.HasPrefix(uri, "ftp://") ||
		strings.HasPrefix(uri, "ws://") ||
		strings.HasPrefix(uri, "wss://")
}

// validateUUID validates UUID format (accepts v1-v5)
func validateUUID(uuid string) bool {
	if uuid == "" {
		return false
	}
	// Accept any UUID version (v1-v5), not just v4
	return uuidRe.MatchString(strings.ToLower(uuid))
}

// ValidateUUIDWithPrefixPostfix validates a value that may have an optional prefix and postfix around a UUID segment.
// Example: value "urn:uuid:550e8400-e29b-41d4-a716-446655440000" with prefix "urn:uuid:" and postfix "".
func ValidateUUIDWithPrefixPostfix(value, prefix, postfix string) bool {
	if value == "" {
		return false
	}
	if prefix != "" && !strings.HasPrefix(value, prefix) {
		return false
	}
	if postfix != "" && !strings.HasSuffix(value, postfix) {
		return false
	}
	segment := value
	if prefix != "" {
		segment = segment[len(prefix):]
	}
	if postfix != "" {
		if len(segment) < len(postfix) {
			return false
		}
		segment = segment[:len(segment)-len(postfix)]
	}
	if segment == "" {
		return false
	}
	return validateUUID(segment)
}

// validateDate validates ISO 8601 date format (YYYY-MM-DD)
func validateDate(date string) bool {
	if date == "" {
		return false
	}
	return dateRe.MatchString(date)
}

// validateDateTime validates ISO 8601 datetime format
func validateDateTime(datetime string) bool {
	if datetime == "" {
		return false
	}
	// Matches: 2025-01-09T10:30:00Z or 2025-01-09T10:30:00+00:00
	return dateTimeRe.MatchString(datetime)
}

// GetFormatValidator returns a format validator by name
func GetFormatValidator(format string) (FormatValidator, bool) {
	validators := map[string]FormatValidator{
		"email":    validateEmail,
		"uri":      validateURI,
		"uuid":     validateUUID,
		"date":     validateDate,
		"datetime": validateDateTime,
	}

	validator, exists := validators[format]
	return validator, exists
}
