# Schema Engine

The schema engine provides format-aware parsing, validation, and (where applicable) transformation of data against schema definitions. It supports **JSON**, **CSV** (row objects), and **HL7 v2.x** messages through a single `Process` API.

## Quick start

```go
engine := schema.NewEngine()

// JSON
result, err := engine.ProcessWithSchema(jsonBytes, jsonSchemaDef, schema.ProcessOptions{
    ApplyDefaults: true, StructureData: true, CollectAllErrors: true,
})

// CSV (input = JSON array of row objects)
result, err := engine.ProcessCSVWithSchema(csvRowsJSON, csvSchemaDef, options)

// HL7 (validation only; result.Data = original bytes)
result, err := engine.ProcessHL7WithSchema(hl7MessageBytes, hl7SchemaDef, schema.ProcessOptions{
    CollectAllErrors: true,
})
```

All processors route their issues through the same severity pipeline. Results are bucketed into three slices:

- `ProcessResult.Errors` — ERROR-severity issues (make `Valid = false`)
- `ProcessResult.Warnings` — WARNING-severity issues
- `ProcessResult.Infos` — INFO-severity issues

Every code defaults to `SeverityError`. Use `ProcessOptions.CodeSeverityOverrides` to override the severity for any code across any format, or use `SeverityDrop` to suppress it entirely. Codes are format-specific: `json.KnownErrorCodes` for JSON/CSV, `hl7.KnownErrorCodes` for HL7.

## Schema formats

| Format | Package | README | Description |
|--------|---------|--------|-------------|
| **JSON** | [json](./json/) | [json/README.md](./json/README.md) | Object/array schemas; validation + defaults + structuring |
| **CSV**  | [csv](./csv/)  | [csv/README.md](./csv/README.md)  | Typed column headers; input = JSON array of row objects |
| **HL7**  | [hl7](./hl7/)  | [hl7/README.md](./hl7/README.md)  | HL7 v2.x message validation + optional CEL custom rules; no transformation |

## Engine API

| Method | Description |
|--------|-------------|
| `Process(input, schemaDef, format, opts)` | Unified entry point; `format` is `FormatJSON`, `FormatCSV`, or `FormatHL7` |
| `ProcessWithSchema(input, schemaDef, opts)` | JSON shortcut |
| `ProcessCSVWithSchema(input, schemaDef, opts)` | CSV shortcut |
| `ProcessHL7WithSchema(input, schemaDef, opts)` | HL7 shortcut |
| `ValidateOnly`, `ValidateCSVOnly`, `ValidateHL7Only` | Validation without defaults/structuring |
| `TransformOnly(input, schemaDef)` | JSON only: apply defaults and structuring without re-validation |
| `ParseJSONSchema(schemaDef)` | Parse a JSON schema for inspection without processing data |

## ProcessOptions

| Option | JSON | CSV | HL7 | Description |
|--------|------|-----|-----|-------------|
| `ApplyDefaults` | ✓ | ✓ | — | Fill in default values from the schema |
| `StructureData` | ✓ | ✓ | — | Reshape data to match schema structure; remove undeclared keys |
| `CollectAllErrors` | ✓ | ✓ | ✓ | Collect every issue (true) or stop at the first error-severity issue (false) |
| `CodeSeverityOverrides` | ✓ | ✓ | ✓ | Override per-code severity for any format; use `SeverityDrop` to suppress a code entirely |

## ProcessResult

| Field | Description |
|-------|-------------|
| `Valid` | `true` when there are no ERROR-severity issues (warnings alone, including HL7 CEL eval warnings, do not set `Valid` to false) |
| `Data` | Processed output — JSON/CSV: transformed bytes; HL7: original input bytes unchanged |
| `Errors` | ERROR-severity validation issues (path, message, code) |
| `Warnings` | WARNING-severity issues (populated when `CodeSeverityOverrides` downgrades a code, or for HL7 CEL rules) |
| `Infos` | INFO-severity issues (populated when `CodeSeverityOverrides` sets INFO for a code) |

## Package layout

| Path | Purpose |
|------|---------|
| `contracts/` | Shared interfaces (`SchemaProcessor`, `CompiledSchema`, `ProcessOptions`, `ProcessResult`, severity constants) — avoids import cycles |
| `json/` | JSON schema parser, validator, transformer, processor |
| `csv/` | CSV schema parser, validator, transformer, processor (reuses JSON validation rules for columns) |
| `hl7/` | HL7 message parser, schema parser, matcher, field/component validator, CEL integration, processor |
| `engine.go` | `Engine` struct and `Process` dispatcher |
| `types.go` | Backward-compatible type aliases re-exported from `contracts`, `json`, and `csv` |
| `transformer.go` | Root transformer wrapper (JSON + CSV helpers) |
| `registry.go` | Processor registry keyed by format name |
| `errors.go` | `SchemaError` and `NewSchemaError` |

For details on each format, see the README in the corresponding sub-package.

## Error handling

### Severity overrides

You control what constitutes a failure by overriding the severity of any error code:

```go
opts := schema.ProcessOptions{
    CollectAllErrors: true,
    CodeSeverityOverrides: map[string]schema.Severity{
        "REQUIRED_FIELD_MISSING": schema.SeverityWarning, // demote: missing field no longer blocks
        "UNKNOWN_FIELD":          schema.SeverityDrop,    // suppress entirely
        hl7.CodeMissingSegment:   schema.SeverityInfo,    // HL7 missing segment = informational
    },
}
result, _ := engine.Process(input, schemaDef, schema.FormatHL7, opts)
// result.Valid is true even if REQUIRED_FIELD_MISSING occurred (only a warning)
```

Valid severity values:

| Constant | Effect |
| --- | --- |
| `SeverityError` | Issue is added to `ProcessResult.Errors`; `Valid = false` |
| `SeverityWarning` | Issue is added to `ProcessResult.Warnings`; `Valid` unaffected |
| `SeverityInfo` | Issue is added to `ProcessResult.Infos`; `Valid` unaffected |
| `SeverityDrop` | Issue is silently discarded; nothing added to any slice |

### Distinguishing hard errors from validation issues

`Process` returns a Go `error` only for infrastructure failures (schema parse error, malformed input that cannot be tokenised). A valid-but-failing schema produces a non-nil `ProcessResult` with `Valid = false` and populated `Errors`; the Go `error` is nil. Always check both:

```go
result, err := engine.ProcessHL7WithSchema(msg, schemaDef, opts)
if err != nil {
    // infra failure: schema could not be compiled, or message tokenisation failed
    return fmt.Errorf("schema engine fatal: %w", err)
}
if !result.Valid {
    // data failed validation; result.Errors contains the issues
    for _, e := range result.Errors {
        log.Warn("validation error", "path", e.Path, "code", e.Code, "msg", e.Message)
    }
}
```

## HL7 validation

### Supported HL7 versions

The HL7 processor supports v2.1 through v2.8 (the version must appear in MSH-12 for automatic detection). Schema definitions reference a specific version; the processor validates the message against that version's segment library.

### Strict vs lenient mode

| Aspect | Strict (`StrictMode: true`) | Lenient (default) |
| --- | --- | --- |
| Unknown segments | Error | Warning |
| Missing optional segments | Warning | Info |
| Unknown field in known segment | Error | Warning |
| Truncated required component | Error | Warning |
| Extra data after last defined component | Error | Drop |

Enable strict mode in `ProcessOptions`:

```go
opts := schema.ProcessOptions{
    CollectAllErrors: true,
    HL7Options: &schema.HL7ProcessOptions{StrictMode: true},
}
```

### Custom CEL validation rules

Attach arbitrary CEL expressions to any field or segment definition in the schema:

```yaml
# in the HL7 schema definition YAML
segments:
  PID:
    fields:
      - path: PID.3         # Patient ID
        cel: "value.matches('^[A-Z0-9]{6,20}$')"
        cel_error_code: PID3_FORMAT
        cel_severity: error  # optional; defaults to error
```

CEL variables available inside expressions:

| Variable | Type | Description |
| --- | --- | --- |
| `value` | `string` | The raw field string (empty string if absent) |
| `present` | `bool` | Whether the field was present in the message |
| `msg` | `map[string]any` | The full parsed message as a map |

CEL evaluation errors (compile or runtime) are reported as `SeverityWarning` by default (not `SeverityError`) so a bad CEL rule does not silently block valid messages.

## Performance

### Goroutine safety

`Engine` is safe for concurrent use from multiple goroutines. Schema compilation (parsing the schema definition bytes) is cached internally: calling `Process` with the same schema bytes a second time reuses the compiled representation without re-parsing. The cache is a sync.Map keyed by the schema bytes hash.

### Compilation cost

JSON/CSV schema compilation is O(n) in the number of fields and is fast (< 1 ms for schemas up to ~1 000 fields). HL7 schema compilation is more expensive because it builds the segment library and compiles CEL expressions; cache the `Engine` instance across requests rather than creating a new one per message.

### Known hot paths

- `hl7.Process` with many CEL rules: CEL expression evaluation dominates. Pre-compile heavy schemas using `engine.ParseHL7Schema` and pass the compiled schema to avoid repeated compilation on hot paths.
- JSON transformation with `StructureData: true` on deeply nested schemas: the recursion depth is proportional to schema nesting. Schemas > 10 levels deep may cause stack growth; benchmark before deploying.
- For throughput benchmarks, see `hl7/hl7_test.go` and `json/json_test.go` (`BenchmarkProcess*` functions).
