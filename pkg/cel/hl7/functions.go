package celhl7

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dlclark/regexp2"
	celgo "github.com/google/cel-go/cel"
	celtypes "github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/wehubfusion/Icarus/pkg/schema/hl7/datatypes"
	hl7msg "github.com/wehubfusion/Icarus/pkg/schema/hl7/message"
	"github.com/wehubfusion/Icarus/pkg/schema/hl7/primitive"
)

// compiledPatternCache stores compiled regexp2 patterns keyed by pattern string.
// Rules typically use constant patterns, so compiling once per unique pattern
// avoids repeated compilation on every rule evaluation.
var compiledPatternCache sync.Map // string → *regexp2.Regexp

func stringVal(v ref.Val) string { return fmt.Sprint(v.Value()) }

func msgGet(ctx *bindCtx, loc string) string {
	if ctx.msg == nil {
		return ""
	}
	if ctx.scopeSeg != "" {
		seg, _, _, _, _ := hl7msg.LocationParts(loc)
		if seg != "" && strings.EqualFold(seg, ctx.scopeSeg) {
			return ctx.msg.GetAtSegmentInstance(ctx.scopeSeg, ctx.instanceIdx0+1, loc)
		}
	}
	return ctx.msg.Get(loc)
}

// hl7EnvOptions returns EnvOptions that bind all HL7 helper implementations to ctx.
// Each Function() call overrides the LateFunctionBinding() placeholder declared in engine.go.
func hl7EnvOptions(ctx *bindCtx) []celgo.EnvOption {
	return []celgo.EnvOption{
		celgo.Function("msg",
			celgo.Overload("msg_string", []*celgo.Type{celgo.StringType}, celgo.StringType,
				celgo.UnaryBinding(func(v ref.Val) ref.Val {
					return celtypes.String(msgGet(ctx, stringVal(v)))
				}))),
		celgo.Function("valued",
			celgo.Overload("valued_string", []*celgo.Type{celgo.StringType}, celgo.BoolType,
				celgo.UnaryBinding(func(v ref.Val) ref.Val {
					// Trim first so that a value like `  ""  ` (space-padded HL7 blank)
					// is treated identically to `""`.
					s := strings.TrimSpace(msgGet(ctx, stringVal(v)))
					return celtypes.Bool(s != "" && s != `""`)
				}))),
		celgo.Function("validateAs",
			celgo.Overload("validateAs_string_string", []*celgo.Type{celgo.StringType, celgo.StringType}, celgo.BoolType,
				celgo.BinaryBinding(func(lhs, rhs ref.Val) ref.Val {
					// lhs = HL7 location to validate ('OBX-5', 'OBX-5.1', ...)
					// rhs = type code as a plain string — either a literal ('NM', 'DT')
					//       or the result of msg('OBX-2') evaluated by CEL first
					ok, err := validateAsImpl(ctx, stringVal(lhs), stringVal(rhs))
					if err != nil {
						return celtypes.WrapErr(err)
					}
					return celtypes.Bool(ok)
				}))),
		celgo.Function("matchesPattern",
			celgo.Overload("matchesPattern_string_string", []*celgo.Type{celgo.StringType, celgo.StringType}, celgo.BoolType,
				celgo.BinaryBinding(func(lhs, rhs ref.Val) ref.Val {
					ok, err := matchesPatternImpl(ctx, stringVal(lhs), stringVal(rhs))
					if err != nil {
						return celtypes.WrapErr(err)
					}
					return celtypes.Bool(ok)
				}))),
		celgo.Function("toDTM",
			celgo.Overload("toDTM_string", []*celgo.Type{celgo.StringType}, celgo.TimestampType,
				celgo.UnaryBinding(func(v ref.Val) ref.Val {
					loc := stringVal(v)
					s := msgGet(ctx, loc)
					if strings.TrimSpace(s) == "" {
						return celtypes.WrapErr(fmt.Errorf("toDTM(%q): field is empty — guard with valued()", locLabel(loc)))
					}
					t, err := parseHL7DTM(s)
					if err != nil {
						return celtypes.WrapErr(err)
					}
					return celtypes.Timestamp{Time: t}
				}))),
		celgo.Function("toNumber",
			celgo.Overload("toNumber_string", []*celgo.Type{celgo.StringType}, celgo.DoubleType,
				celgo.UnaryBinding(func(v ref.Val) ref.Val {
					loc := stringVal(v)
					s := strings.TrimSpace(msgGet(ctx, loc))
					if s == "" {
						return celtypes.WrapErr(fmt.Errorf("toNumber(%q): field is empty — guard with valued()", locLabel(loc)))
					}
					f, err := strconv.ParseFloat(s, 64)
					if err != nil {
						return celtypes.WrapErr(fmt.Errorf("toNumber(%q): field value of length %d is not a valid number", locLabel(loc), len(s)))
					}
					return celtypes.Double(f)
				}))),
	}
}

// validateAsKnownPrimitives are scalar HL7 datatype codes that primitive.ValidatePrimitive
// can evaluate. VARIES skips leaf validation by design.
var validateAsKnownPrimitives = map[string]struct{}{
	"ST": {}, "TX": {}, "FT": {}, "NM": {}, "SI": {}, "DT": {}, "TM": {},
	"DTM": {}, "TS": {}, "ID": {}, "IS": {}, "GTS": {}, "VARIES": {},
}

func isValidateAsTypeCodeAllowed(tid string, reg *datatypes.Registry, version string) bool {
	tid = strings.ToUpper(strings.TrimSpace(tid))
	if tid == "" {
		return false
	}
	if _, ok := validateAsKnownPrimitives[tid]; ok {
		return true
	}
	if reg != nil {
		if _, ok := reg.Lookup(version, tid); ok {
			return true
		}
	}
	return false
}

// validateAsImpl checks whether the value at loc conforms to typeCode.
//
// loc      — HL7 location resolved via msgGet ('OBX-5', 'OBX-5.1', …).
// typeCode — HL7 datatype code (literal or msg('OBX-2')).
//
// Passing msg('OBX-5') as the first argument is invalid (field value, not a location) and
// surfaces as HL7_CUSTOM_RULE_RUNTIME_ERROR. Unknown datatype codes yield false (not an error)
// so rule authors get HL7_CUSTOM_RULE_VIOLATION instead of a runtime error.
func validateAsImpl(ctx *bindCtx, loc, typeCode string) (bool, error) {
	if ctx.msg == nil {
		return false, nil
	}
	loc = strings.TrimSpace(loc)
	locNorm := strings.ToUpper(loc)
	if !reHL7Location.MatchString(locNorm) {
		return false, fmt.Errorf(
			"validateAs: first argument must be an HL7 field location (e.g. 'OBX-5'), not a field value; got a string of length %d",
			len(loc), // not quoted: the usual mistake passes msg(...), a field value
		)
	}
	tid := strings.ToUpper(strings.TrimSpace(typeCode))
	if tid == "" {
		return false, fmt.Errorf("validateAs: second argument (datatype code) must be non-empty")
	}
	if !isValidateAsTypeCodeAllowed(tid, ctx.reg, ctx.version) {
		return false, nil
	}
	val := msgGet(ctx, locNorm)
	return primitive.ValidatePrimitiveType(tid, val, ctx.reg, ctx.version), nil
}

func matchesPatternImpl(ctx *bindCtx, loc, pattern string) (bool, error) {
	val := strings.TrimSpace(msgGet(ctx, loc))
	if val == "" {
		return false, fmt.Errorf("matchesPattern(%q): field is empty — guard with valued()", locLabel(loc))
	}
	re, err := compiledPattern(pattern)
	if err != nil {
		return false, fmt.Errorf("matchesPattern(%q): invalid pattern %q: %v", locLabel(loc), pattern, err)
	}
	ok, err := re.MatchString(val)
	if err != nil {
		// regexp2 returns an error on match timeout (50 ms ReDoS guard) or
		// internal runtime failure. Surface it as HL7_CUSTOM_RULE_RUNTIME_ERROR rather
		// than silently returning false (which would produce a spurious
		// violation) or true (which would silently pass a potentially-matching field).
		return false, fmt.Errorf("matchesPattern(%q): match failed: %w", locLabel(loc), err)
	}
	return ok, nil
}

// locLabel is loc for an error message when it is an HL7 field location. Anything else is reported
// by length only: the usual mistake passes msg(...), which is the field's value.
func locLabel(loc string) string {
	if reHL7Location.MatchString(strings.ToUpper(strings.TrimSpace(loc))) {
		return loc
	}
	return fmt.Sprintf("<not a field location, length %d>", len(loc))
}

// compiledPattern returns a cached *regexp2.Regexp for pattern, compiling it on
// first use. The timeout is set once at compile time so every match respects it
// without per-call overhead.
func compiledPattern(pattern string) (*regexp2.Regexp, error) {
	if v, ok := compiledPatternCache.Load(pattern); ok {
		return v.(*regexp2.Regexp), nil
	}
	re, err := regexp2.Compile(pattern, regexp2.None)
	if err != nil {
		return nil, err
	}
	re.MatchTimeout = 50 * time.Millisecond
	// Store only on success; a bad pattern stays uncached so callers always get the error.
	compiledPatternCache.Store(pattern, re)
	return re, nil
}

// parseHL7DTM parses an HL7 DTM/TS string (partial precision allowed) into UTC.
// Its errors do not quote the value: it is message data, often a date of birth, and the error
// reaches the validation result as an HL7_CUSTOM_RULE_RUNTIME_ERROR message.
//
// HL7 DTM format: YYYY[MM[DD[HH[MM[SS[.S[S[S[S]]]]]]]]][+/-ZZZZ]
//
// Partial values are extended to the start of the period:
//   - "2026"     → 2026-01-01 00:00:00 UTC
//   - "202601"   → 2026-01-01 00:00:00 UTC
//   - "20260115" → 2026-01-15 00:00:00 UTC
//
// A timezone offset, when present, is parsed and applied so the result is UTC.
// An empty string returns the zero time without error.
// Calendar components are validated after parsing; out-of-range values (e.g.
// month 14, Feb 31) are rejected even though time.Date would silently normalise them.
func parseHL7DTM(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, nil
	}

	// Extract and apply timezone offset suffix (+/-HHMM), if present.
	offsetSecs := 0
	body := s
	for _, sign := range []byte{'+', '-'} {
		if idx := strings.LastIndexByte(body, sign); idx > 0 {
			tail := body[idx+1:]
			if len(tail) == 4 {
				if mins, err := strconv.Atoi(tail); err == nil {
					hh := mins / 100
					mm := mins % 100
					total := hh*3600 + mm*60
					if sign == '-' {
						offsetSecs = -total
					} else {
						offsetSecs = total
					}
					body = body[:idx]
					break
				}
			}
		}
	}

	// Strip fractional seconds (.SSSS).
	if dot := strings.IndexByte(body, '.'); dot >= 0 {
		body = body[:dot]
	}

	// The remaining body must be pure ASCII digits. Any non-digit character
	// (e.g. "2026abcd01") means the input is not a valid HL7 DTM value.
	for _, c := range body {
		if c < '0' || c > '9' {
			return time.Time{}, fmt.Errorf("invalid HL7 DTM value (non-digit character in date-time body)")
		}
	}

	digits := []byte(body)
	if len(digits) > 14 {
		digits = digits[:14]
	}
	realLen := len(digits) // track how many digits were actually present

	// Zero-pad time components to 14 digits. Month and day default to "01"
	// (start of period) not "00", since time.Date(y,0,0) wraps backward.
	for len(digits) < 14 {
		digits = append(digits, '0')
	}

	// strconv.Atoi on a slice of guaranteed digit bytes cannot fail.
	y, _ := strconv.Atoi(string(digits[0:4]))
	if y == 0 {
		return time.Time{}, fmt.Errorf("invalid HL7 DTM value (year 0000)")
	}

	// Use 1 as the default for month and day when omitted (partial precision).
	mo := 1
	if realLen >= 6 {
		mo, _ = strconv.Atoi(string(digits[4:6]))
	}
	d := 1
	if realLen >= 8 {
		d, _ = strconv.Atoi(string(digits[6:8]))
	}
	h, _ := strconv.Atoi(string(digits[8:10]))
	mi, _ := strconv.Atoi(string(digits[10:12]))
	sec, _ := strconv.Atoi(string(digits[12:14]))

	// Build in the declared timezone, then convert to UTC.
	zone := time.FixedZone("", offsetSecs)
	t := time.Date(y, time.Month(mo), d, h, mi, sec, 0, zone)

	// time.Date normalises out-of-range values silently (Feb 31 → Mar 2/3,
	// month 13 → Jan of next year). Detect this by comparing back.
	if t.Year() != y || int(t.Month()) != mo || t.Day() != d ||
		t.Hour() != h || t.Minute() != mi || t.Second() != sec {
		return time.Time{}, fmt.Errorf("invalid HL7 DTM value (calendar components out of range)")
	}
	return t.UTC(), nil
}
