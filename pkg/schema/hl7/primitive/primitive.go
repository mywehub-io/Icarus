// Package primitive validates HL7 primitive datatypes without depending on pkg/schema/hl7,
// so callers like pkg/cel/hl7 can avoid import cycles.
package primitive

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/wehubfusion/Icarus/pkg/schema/hl7/datatypes"
)

var (
	reDT  = regexp.MustCompile(`^\d{4}(\d{2}(\d{2})?)?$`)
	reTM  = regexp.MustCompile(`^\d{2}(\d{2}(\d{2}(\.\d{1,4})?)?)?([+-]\d{4})?$`)
	reDTM = regexp.MustCompile(`^\d{4}(\d{2}(\d{2}(\d{2}(\d{2}(\d{2}(\.\d{1,4})?)?)?)?)?)?([+-]\d{4})?$`)
	reNM  = regexp.MustCompile(`^[-+]?\d*\.?\d+$`)
	reSI  = regexp.MustCompile(`^\d+$`)
)

// FieldError describes a primitive datatype validation failure (same shape as hl7.ValidationError).
type FieldError struct {
	Path, Message, Code string
}

func (e *FieldError) Error() string { return e.Message }

// ValidatePrimitive validates HL7 scalar primitive data types.
// Only truly scalar types are validated here:
//   ST, TX, FT — free text (no format constraint)
//   NM         — numeric
//   SI         — sequence ID (non-negative integer)
//   DT         — date (YYYY[MM[DD]])
//   TM         — time (HH[MM[SS[.S+]]][±ZZZZ])
//   DTM / TS   — date-time
//   ID, IS     — coded value (table-driven; format not checked here)
//
// Composite types (SN, MO, NR) are NOT validated here — their components are
// split by the HL7 parser and each component is validated by its own scalar type.
// Unknown or composite-used-as-leaf types are accepted without error.
func ValidatePrimitive(dataType, value, path string, _ int) *FieldError {
	dt := strings.ToUpper(strings.TrimSpace(dataType))
	if value == "" || value == `""` {
		return nil
	}
	switch dt {
	case "ST", "TX", "FT", "": // string types
		return nil
	case "NM":
		if !reNM.MatchString(value) {
			return &FieldError{Path: path, Message: "value must be numeric", Code: "HL7_DATATYPE"}
		}
	case "SI":
		if !reSI.MatchString(value) {
			return &FieldError{Path: path, Message: "value must be integer", Code: "HL7_DATATYPE"}
		}
	case "DT":
		if !reDT.MatchString(value) {
			return &FieldError{Path: path, Message: "value must be date (YYYY[MM[DD]])", Code: "HL7_DATATYPE"}
		}
		if err := validateDTCalendarRange(value, path); err != nil {
			return err
		}
	case "TM":
		if !reTM.MatchString(value) {
			return &FieldError{Path: path, Message: "value must be time (HH[MM[SS]])", Code: "HL7_DATATYPE"}
		}
		if err := validateTMRange(value, path); err != nil {
			return err
		}
		if err := validateTZOffset(value, path); err != nil {
			return err
		}
	case "DTM", "TS":
		if !reDTM.MatchString(value) {
			return &FieldError{Path: path, Message: "value must be timestamp (DTM/TS format)", Code: "HL7_DATATYPE"}
		}
		if err := validateDTMCalendarRange(value, path); err != nil {
			return err
		}
		if err := validateTZOffset(value, path); err != nil {
			return err
		}
	case "ID", "IS", "GTS":
		// Coded/table-driven or free-form timing: no format constraint at this level.
		return nil
	default:
		// Composite types (SN, MO, NR, …) and unrecognized types: accept without error.
		// Composite types have their components split by the HL7 parser before reaching
		// this function, so each component is validated by its own scalar type.
	}
	return nil
}

// ValidatePrimitiveType reports whether value conforms to the primitive HL7 datatype.
// Empty values and VARIES are treated as valid. Composite types skip leaf primitive checks.
func ValidatePrimitiveType(typeID string, value string, reg *datatypes.Registry, version string) bool {
	tid := strings.ToUpper(strings.TrimSpace(typeID))
	if value == "" || value == `""` {
		return true
	}
	if tid == "" || tid == "VARIES" {
		return true
	}
	if reg != nil {
		if def, ok := reg.Lookup(version, tid); ok && def != nil && def.IsComposite {
			return true
		}
	}
	return ValidatePrimitive(tid, value, "", 0) == nil
}

func isLeapYear(y int) bool {
	return y%4 == 0 && (y%100 != 0 || y%400 == 0)
}

// maxDaysInMonth returns the number of days in the given month (1-based) for the given year.
func maxDaysInMonth(year, month int) int {
	switch month {
	case 1, 3, 5, 7, 8, 10, 12:
		return 31
	case 4, 6, 9, 11:
		return 30
	case 2:
		if isLeapYear(year) {
			return 29
		}
		return 28
	default:
		return 31
	}
}

// The range checks below name the component, not its value: a date or timestamp can be a date of birth.
func validateDTCalendarRange(value, path string) *FieldError {
	yyyy, _ := strconv.Atoi(value[0:4])
	if len(value) >= 6 {
		mm, _ := strconv.Atoi(value[4:6])
		if mm < 1 || mm > 12 {
			return &FieldError{Path: path, Message: "invalid month in date", Code: "HL7_DATATYPE"}
		}
		if len(value) == 8 {
			dd, _ := strconv.Atoi(value[6:8])
			if maxDD := maxDaysInMonth(yyyy, mm); dd < 1 || dd > maxDD {
				return &FieldError{Path: path, Message: "invalid day for month in date", Code: "HL7_DATATYPE"}
			}
		}
	}
	return nil
}

func validateTMRange(value, path string) *FieldError {
	digits := stripTZOffset(value)
	if len(digits) >= 2 {
		hh, _ := strconv.Atoi(digits[0:2])
		if hh > 23 {
			return &FieldError{Path: path, Message: "invalid hour in time", Code: "HL7_DATATYPE"}
		}
	}
	if len(digits) >= 4 {
		mm, _ := strconv.Atoi(digits[2:4])
		if mm > 59 {
			return &FieldError{Path: path, Message: "invalid minute in time", Code: "HL7_DATATYPE"}
		}
	}
	if len(digits) >= 6 {
		ssPart := digits[4:6]
		ss, _ := strconv.Atoi(ssPart)
		if ss > 59 {
			return &FieldError{Path: path, Message: "invalid second in time", Code: "HL7_DATATYPE"}
		}
	}
	return nil
}

func validateDTMCalendarRange(value, path string) *FieldError {
	digits := stripTZOffset(value)
	yyyy, _ := strconv.Atoi(digits[0:4])
	if len(digits) >= 6 {
		mm, _ := strconv.Atoi(digits[4:6])
		if mm < 1 || mm > 12 {
			return &FieldError{Path: path, Message: "invalid month in timestamp", Code: "HL7_DATATYPE"}
		}
		if len(digits) >= 8 {
			dd, _ := strconv.Atoi(digits[6:8])
			if maxDD := maxDaysInMonth(yyyy, mm); dd < 1 || dd > maxDD {
				return &FieldError{Path: path, Message: "invalid day for month in timestamp", Code: "HL7_DATATYPE"}
			}
		}
	}
	if len(digits) >= 10 {
		hh, _ := strconv.Atoi(digits[8:10])
		if hh > 23 {
			return &FieldError{Path: path, Message: "invalid hour in timestamp", Code: "HL7_DATATYPE"}
		}
	}
	if len(digits) >= 12 {
		min, _ := strconv.Atoi(digits[10:12])
		if min > 59 {
			return &FieldError{Path: path, Message: "invalid minute in timestamp", Code: "HL7_DATATYPE"}
		}
	}
	if len(digits) >= 14 {
		ss, _ := strconv.Atoi(digits[12:14])
		if ss > 59 {
			return &FieldError{Path: path, Message: "invalid second in timestamp", Code: "HL7_DATATYPE"}
		}
	}
	return nil
}

func stripTZOffset(value string) string {
	s := value
	for _, sign := range []byte{'+', '-'} {
		if idx := strings.LastIndexByte(s, sign); idx > 0 {
			s = s[:idx]
			break
		}
	}
	if idx := strings.IndexByte(s, '.'); idx >= 0 {
		s = s[:idx]
	}
	return s
}

func validateTZOffset(value, path string) *FieldError {
	var tzIdx int = -1
	for i := len(value) - 5; i >= 1; i-- {
		if value[i] == '+' || value[i] == '-' {
			tzIdx = i
			break
		}
	}
	if tzIdx < 0 {
		return nil
	}
	sign := value[tzIdx]
	tz := value[tzIdx+1:]
	if len(tz) != 4 {
		return nil
	}
	tzH, err1 := strconv.Atoi(tz[0:2])
	tzM, err2 := strconv.Atoi(tz[2:4])
	if err1 != nil || err2 != nil {
		return nil
	}
	// Valid UTC offset range: -12:00 (UTC-12) to +14:00 (UTC+14, Line Islands).
	maxH := 14
	if sign == '-' {
		maxH = 12
	}
	if tzH > maxH || (tzH == maxH && tzM > 0) {
		return &FieldError{
			Path:    path,
			Message: fmt.Sprintf("invalid timezone offset %c%s: exceeds allowed range", sign, tz),
			Code:    "HL7_DATATYPE",
		}
	}
	if tzM > 59 {
		return &FieldError{
			Path:    path,
			Message: fmt.Sprintf("invalid timezone offset %c%s: minutes must be 00-59", sign, tz),
			Code:    "HL7_DATATYPE",
		}
	}
	return nil
}
