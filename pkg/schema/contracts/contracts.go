package contracts

import (
	"fmt"
	"strings"
)

// SchemaFormat identifies the schema definition format (JSON, CSV, HL7).
type SchemaFormat string

const (
	FormatJSON SchemaFormat = "JSON"
	FormatCSV  SchemaFormat = "CSV"
	FormatHL7  SchemaFormat = "HL7"
)

type Severity string

const (
	SeverityError   Severity = "ERROR"
	SeverityWarning Severity = "WARNING"
	SeverityInfo    Severity = "INFO"
	SeverityDrop    Severity = "DROP"
)

// ValidationIssue represents a single validation finding (error, warning, or info).
type ValidationIssue struct {
	Path    string `json:"path"`
	Message string `json:"message"`
	Code    string `json:"code"`
	Severity Severity `json:"severity"`
}

// ValidationError is preserved for backward compatibility (all previous findings were treated as errors).
type ValidationError = ValidationIssue

// ValidationResult holds the result of validation
type ValidationResult struct {
	Valid    bool              `json:"valid"`
	Errors   []ValidationError `json:"errors,omitempty"`
	Warnings []ValidationIssue `json:"warnings,omitempty"`
	Infos    []ValidationIssue `json:"infos,omitempty"`
}

// ErrorMessage returns a single well-formatted error string for the validation result.
func (r *ValidationResult) ErrorMessage() string {
	if r == nil || r.Valid || len(r.Errors) == 0 {
		return ""
	}
	n := len(r.Errors)
	if n == 1 {
		return fmt.Sprintf("schema validation failed: %s: %s", r.Errors[0].Path, r.Errors[0].Message)
	}
	const maxShow = 10
	parts := make([]string, 0, maxShow+1)
	for i := 0; i < n && i < maxShow; i++ {
		parts = append(parts, fmt.Sprintf("%s: %s", r.Errors[i].Path, r.Errors[i].Message))
	}
	msg := fmt.Sprintf("schema validation failed with %d errors: %s", n, strings.Join(parts, "; "))
	if n > maxShow {
		msg += fmt.Sprintf(" ... and %d more", n-maxShow)
	}
	return msg
}

// ProcessOptions controls schema processing behavior.
//
//   - CollectAllErrors: when false (default) processing stops at the first error;
//     when true every issue is gathered before returning.
//
//   - CodeSeverityOverrides: maps validation error codes to explicit severities
//     (ERROR / WARNING / INFO / DROP). Respected by all processors — codes are
//     processor-specific (see json.KnownErrorCodes, hl7.KnownErrorCodes).
//     Codes absent from the map default to ERROR. DROP silences the issue entirely.
//     When nil, every issue defaults to ERROR.
//
//   - ApplyDefaults: run the defaults-fill pass before validation.
//     Applies to JSON and CSV processors; no-op for processors without a
//     transformation pipeline (e.g. HL7).
//
//   - StructureData: run the structuring pass before validation.
//     Applies to JSON and CSV processors; no-op for processors without a
//     transformation pipeline (e.g. HL7).
type ProcessOptions struct {
	CollectAllErrors      bool
	CodeSeverityOverrides map[string]Severity
	ApplyDefaults         bool
	StructureData         bool
	// ByteDefault, when set, is called for a BYTE property that takes its default value (a base64
	// string saved in the schema) and returns the value to use instead: the processor decodes the
	// default and writes it as a file, so the property carries a file reference like every other
	// byte value (raw payloads Q4). Nil keeps the base64 string. JSON schemas only.
	ByteDefault func(property, base64Value string) (interface{}, error)
}

// ProcessResult contains the result of schema processing.
//
// Valid is false when Errors contains at least one issue (ERROR severity).
// Warnings and Infos never affect Valid — they are informational only.
// Every ValidationIssue in all three slices has its Severity field populated
// by ApplyAndBucket before being placed in the result.
type ProcessResult struct {
	Valid    bool              `json:"valid"`
	Data     []byte            `json:"data"`
	Errors   []ValidationError `json:"errors,omitempty"`
	Warnings []ValidationIssue `json:"warnings,omitempty"`
	Infos    []ValidationIssue `json:"infos,omitempty"`
}

// ApplyAndBucket applies CodeSeverityOverrides to a slice of raw ValidationIssues and
// distributes them into error, warning, and info buckets.
//
// Each issue is handled as follows:
//   - If the issue already carries a Severity (non-empty), that value is respected.
//   - Otherwise, the override map is consulted by Code; absent codes default to SeverityError.
//   - Issues resolved to SeverityDrop are silently discarded.
//
// This function is the shared implementation used by all schema processors so that
// CodeSeverityOverrides works uniformly regardless of the schema format.
func ApplyAndBucket(raw []ValidationIssue, overrides map[string]Severity) (errs, warns, infos []ValidationIssue) {
	for _, issue := range raw {
		sev := issue.Severity
		if sev == "" {
			if overrides != nil {
				if custom, ok := overrides[issue.Code]; ok {
					sev = custom
				} else {
					sev = SeverityError
				}
			} else {
				sev = SeverityError
			}
		}
		if sev == SeverityDrop {
			continue
		}
		issue.Severity = sev
		switch sev {
		case SeverityInfo:
			infos = append(infos, issue)
		case SeverityWarning:
			warns = append(warns, issue)
		default:
			errs = append(errs, issue)
		}
	}
	return
}

// SchemaProcessor is the extension point for all schema formats.
type SchemaProcessor interface {
	Type() string
	ParseSchema(definition []byte) (CompiledSchema, error)
	Process(inputData []byte, schema CompiledSchema, opts ProcessOptions) (*ProcessResult, error)
}

// CompiledSchema is the opaque result of ParseSchema.
type CompiledSchema interface {
	SchemaType() string
}
