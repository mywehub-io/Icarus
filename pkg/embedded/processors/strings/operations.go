package strings

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	stdstrings "strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"github.com/wehubfusion/Icarus/pkg/embedded/processors/internal/compiled"
)

func executeAction(nodeID string, itemIndex int, action string, params map[string]interface{}, input map[string]interface{}) (interface{}, error) {
	switch action {
	case "concatenate":
		separator := getString(params, "separator", "")
		parts := getStringSlice(params, "parts", nil)
		if parts == nil {
			parts = extractStringValues(input)
		}
		return concatenate(separator, parts...), nil

	case "split":
		str := getInputString(params, input, "")
		delimiter := getString(params, "delimiter", "")
		return split(str, delimiter), nil

	case "join":
		items := getStringSlice(params, "items", getStringSlice(input, "items", nil))
		if items == nil {
			items = extractStringValues(input) // seed-node-schemas.sql: manual_inputs
		}
		separator := getString(params, "separator", "")
		return join(items, separator), nil

	case "trim":
		str := getInputString(params, input, "")
		cutset := getString(params, "cutset", "")
		return trim(str, cutset), nil

	case "replace":
		str := getInputString(params, input, "")
		old := getString(params, "old", "")
		newVal := getString(params, "new", "")
		count := getInt(params, "count", -1)
		useRegex := getBool(params, "use_regex", false)
		replaced, err := replace(str, old, newVal, count, useRegex)
		if err != nil {
			return nil, NewActionError(nodeID, itemIndex, action, fmt.Sprintf("replace failed: %v", err), err)
		}
		return replaced, nil

	case "substring":
		str := getInputString(params, input, "")
		start := getInt(params, "start", 0)
		end := getInt(params, "end", 0)
		return substring(str, start, end), nil

	case "to_upper":
		str := getInputString(params, input, "")
		return toUpper(str), nil

	case "to_lower":
		str := getInputString(params, input, "")
		return toLower(str), nil

	case "title_case":
		str := getInputString(params, input, "")
		return titleCase(str), nil

	case "capitalize":
		str := getInputString(params, input, "")
		return capitalize(str), nil

	case "contains":
		str := getInputString(params, input, "")
		sub := getString(params, "substring", "")
		useRegex := getBool(params, "use_regex", false)
		ok, err := contains(str, sub, useRegex)
		if err != nil {
			return nil, NewActionError(nodeID, itemIndex, action, fmt.Sprintf("contains failed: %v", err), err)
		}
		return ok, nil

	case "length":
		str := getInputString(params, input, "")
		return length(str), nil

	case "regex_extract":
		str := getInputString(params, input, "")
		pattern := getString(params, "pattern", "")
		matches, err := regexExtract(str, pattern)
		if err != nil {
			return nil, NewActionError(nodeID, itemIndex, action, fmt.Sprintf("regex_extract failed: %v", err), err)
		}
		return matches, nil

	case "format":
		template := getString(params, "template", getString(input, "template", ""))
		data := getStringMap(params, "data", getStringMap(input, "data", inputToStringMap(input)))
		return format(template, data), nil

	case "base64_encode":
		str := getInputString(params, input, "")
		return base64Encode(str), nil

	case "base64_decode":
		str := getInputString(params, input, "")
		decoded, err := base64Decode(str)
		if err != nil {
			return nil, NewActionError(nodeID, itemIndex, action, fmt.Sprintf("base64_decode failed: %v", err), err)
		}
		return decoded, nil

	case "uri_encode":
		str := getInputString(params, input, "")
		return uriEncode(str), nil

	case "uri_decode":
		str := getInputString(params, input, "")
		decoded, err := uriDecode(str)
		if err != nil {
			return nil, NewActionError(nodeID, itemIndex, action, fmt.Sprintf("uri_decode failed: %v", err), err)
		}
		return decoded, nil

	case "normalize":
		str := getInputString(params, input, "")
		return normalize(str), nil

	default:
		return nil, NewActionError(nodeID, itemIndex, action, "unsupported action", nil)
	}
}

// --- String helpers (ported) ---

func concatenate(separator string, parts ...string) string {
	if len(parts) == 0 {
		return ""
	}
	if separator == "" {
		return stdstrings.Join(parts, "")
	}
	return stdstrings.Join(parts, separator)
}

func split(s, delimiter string) []string {
	if delimiter == "" {
		return []string{s}
	}
	return stdstrings.Split(s, delimiter)
}

func join(items []string, separator string) string { return stdstrings.Join(items, separator) }

func trim(s, cutset string) string {
	if cutset == "" {
		return stdstrings.TrimSpace(s)
	}
	return stdstrings.Trim(s, cutset)
}

func replace(s, old, new string, count int, useRegex bool) (string, error) {
	if !useRegex {
		return stdstrings.Replace(s, old, new, count), nil
	}
	re, err := compileRegex(old)
	if err != nil {
		return "", err
	}
	if count < 0 {
		return re.ReplaceAllString(s, new), nil
	}
	return replaceRegexCount(s, re, new, count), nil
}

func substring(s string, start, end int) string {
	runes := []rune(s)
	n := len(runes)
	if start < 0 {
		start = n + start
	}
	if end <= 0 {
		end = n + end
	}
	if start < 0 {
		start = 0
	}
	if end > n {
		end = n
	}
	if start > end {
		start, end = end, start
	}
	return string(runes[start:end])
}

func toUpper(s string) string { return stdstrings.ToUpper(s) }
func toLower(s string) string { return stdstrings.ToLower(s) }

func titleCase(s string) string { return cases.Title(language.Und).String(s) }

func capitalize(s string) string {
	if s == "" {
		return s
	}
	r, size := utf8.DecodeRuneInString(s)
	return stdstrings.ToUpper(string(r)) + s[size:]
}

func contains(s, sub string, useRegex bool) (bool, error) {
	if !useRegex {
		return stdstrings.Contains(s, sub), nil
	}
	re, err := compileRegex(sub)
	if err != nil {
		return false, err
	}
	return re.MatchString(s), nil
}

func length(s string) int { return utf8.RuneCountInString(s) }

func regexExtract(s, pattern string) ([][]string, error) {
	re, err := compileRegex(pattern)
	if err != nil {
		return nil, err
	}
	return re.FindAllStringSubmatch(s, -1), nil
}

// format fills "${key}" and "{key}" placeholders in one pass. It used to replace key by key in
// random map order, so a value containing another key's placeholder could be replaced again or not,
// run to run; a single pass replaces only what the template itself contains.
func format(template string, data map[string]string) string {
	if len(data) == 0 {
		return template
	}
	pairs := make([]string, 0, len(data)*4)
	for k, v := range data {
		pairs = append(pairs, "${"+k+"}", v, "{"+k+"}", v)
	}
	return stdstrings.NewReplacer(pairs...).Replace(template)
}

func base64Encode(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

func base64Decode(s string) (string, error) {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func uriEncode(s string) string { return url.QueryEscape(s) }

func uriDecode(s string) (string, error) { return url.QueryUnescape(s) }

func normalize(s string) string { return removeDiacritics(s) }

// regex helpers
type regex struct{ r *regexp.Regexp }

func compileRegex(pattern string) (*regex, error) {
	r, err := compiled.Regexp(pattern)
	if err != nil {
		return nil, err
	}
	return &regex{r: r}, nil
}

func replaceRegexCount(s string, re *regex, replacement string, count int) string {
	if count < 0 {
		return re.ReplaceAllString(s, replacement)
	}
	var out bytes.Buffer
	idxs := re.FindAllStringIndex(s, -1)
	if len(idxs) == 0 || count == 0 {
		return s
	}
	last := 0
	for i, pair := range idxs {
		if i >= count {
			break
		}
		start, end := pair[0], pair[1]
		out.WriteString(s[last:start])
		out.WriteString(replacement)
		last = end
	}
	out.WriteString(s[last:])
	return out.String()
}

func (re *regex) ReplaceAllString(s, repl string) string     { return re.r.ReplaceAllString(s, repl) }
func (re *regex) FindAllStringIndex(s string, n int) [][]int { return re.r.FindAllStringIndex(s, n) }
func (re *regex) MatchString(s string) bool                  { return re.r.MatchString(s) }
func (re *regex) FindAllStringSubmatch(s string, n int) [][]string {
	return re.r.FindAllStringSubmatch(s, n)
}

// removeDiacritics strips common diacritics and combining marks.
func removeDiacritics(s string) string {
	var b bytes.Buffer
	for _, r := range s {
		if unicode.Is(unicode.Mn, r) {
			continue
		}
		switch r {
		case 'À', 'Á', 'Â', 'Ã', 'Ä', 'Å':
			b.WriteByte('A')
		case 'à', 'á', 'â', 'ã', 'ä', 'å':
			b.WriteByte('a')
		case 'Æ':
			b.WriteString("AE")
		case 'æ':
			b.WriteString("ae")
		case 'Ç':
			b.WriteByte('C')
		case 'ç':
			b.WriteByte('c')
		case 'È', 'É', 'Ê', 'Ë':
			b.WriteByte('E')
		case 'è', 'é', 'ê', 'ë':
			b.WriteByte('e')
		case 'Ì', 'Í', 'Î', 'Ï':
			b.WriteByte('I')
		case 'ì', 'í', 'î', 'ï':
			b.WriteByte('i')
		case 'Ñ':
			b.WriteByte('N')
		case 'ñ':
			b.WriteByte('n')
		case 'Ò', 'Ó', 'Ô', 'Õ', 'Ö', 'Ø':
			b.WriteByte('O')
		case 'ò', 'ó', 'ô', 'õ', 'ö', 'ø':
			b.WriteByte('o')
		case 'Ù', 'Ú', 'Û', 'Ü':
			b.WriteByte('U')
		case 'ù', 'ú', 'û', 'ü':
			b.WriteByte('u')
		case 'Ý':
			b.WriteByte('Y')
		case 'ý', 'ÿ':
			b.WriteByte('y')
		case 'ẞ':
			b.WriteString("SS")
		case 'ß':
			b.WriteString("ss")
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// --- map helpers ---

// getInputString returns the main string for an action: params["string"] if set,
// else input["value"] (seed-node-schemas.sql input field key), else input["string"].
func getInputString(params, input map[string]interface{}, defaultVal string) string {
	if s := getString(params, "string", ""); s != "" {
		return s
	}
	return getString(input, "value", getString(input, "string", defaultVal))
}

func getString(m map[string]interface{}, key string, defaultValue string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return defaultValue
}

func getInt(m map[string]interface{}, key string, defaultValue int) int {
	if v, ok := m[key]; ok {
		switch val := v.(type) {
		case int:
			return val
		case float64:
			return int(val)
		case int64:
			return int(val)
		}
	}
	return defaultValue
}

func getBool(m map[string]interface{}, key string, defaultValue bool) bool {
	if v, ok := m[key]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return defaultValue
}

func getStringSlice(m map[string]interface{}, key string, defaultValue []string) []string {
	if v, ok := m[key]; ok {
		if items, ok := v.([]interface{}); ok {
			result := make([]string, 0, len(items))
			for _, item := range items {
				if s, ok := item.(string); ok {
					result = append(result, s)
				}
			}
			return result
		}
		if items, ok := v.([]string); ok {
			return items
		}
	}
	return defaultValue
}

func getStringMap(m map[string]interface{}, key string, defaultValue map[string]string) map[string]string {
	if v, ok := m[key]; ok {
		if dataMap, ok := v.(map[string]interface{}); ok {
			result := make(map[string]string)
			for k, val := range dataMap {
				if s, ok := val.(string); ok {
					result[k] = s
				}
			}
			return result
		}
		if dataMap, ok := v.(map[string]string); ok {
			return dataMap
		}
	}
	return defaultValue
}

// extractStringValues returns the string values of m ordered by key.
//
// Ordering is not cosmetic here. This feeds concatenate and join whenever no explicit
// "parts"/"items" list is configured, so ranging over the map directly meant a String
// Concatenate node produced a differently ordered result on each execution of the same
// workflow — {"first":"hello","second":"world"} yielded "hello world" or "world hello"
// at random, which failed TestProcessConcatenate roughly one run in ten. Sorting by key
// makes the output a function of the input alone, and field order is the only ordering
// the caller has expressed.
func extractStringValues(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k, v := range m {
		if _, ok := v.(string); ok {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	result := make([]string, 0, len(keys))
	for _, k := range keys {
		result = append(result, m[k].(string))
	}
	return result
}

// inputToStringMap converts input to map[string]string for format placeholders.
// Used when manual_inputs flow in as flat keys (seed-node-schemas.sql String Format).
func inputToStringMap(m map[string]interface{}) map[string]string {
	result := make(map[string]string)
	for k, v := range m {
		if s, ok := v.(string); ok {
			result[k] = s
		}
	}
	return result
}
