# Upgrade guide

## Upgrading to v0.8.0

### `ValidationMode` / `StrictValidation` removed

v0.8.0 removes `ProcessOptions.ValidationMode` and `ProcessOptions.StrictValidation`. These
fields no longer exist; code that sets them will fail to compile.

**Migration:** Use `CodeSeverityOverrides` instead.

Before:

```go
result, err := engine.ProcessWithSchema(input, schema, schema.ProcessOptions{
    StrictValidation: true,
})
```

After:

```go
result, err := engine.ProcessWithSchema(input, schema, schema.ProcessOptions{
    // Upgrade all warnings to errors for strict behaviour:
    CodeSeverityOverrides: map[string]schema.Severity{
        json.CodeUnknownField:       schema.SeverityError,
        json.CodeTypeMismatch:       schema.SeverityError,
    },
    CollectAllErrors: true,
})
```

For HL7, promote codes that were previously WARNING to ERROR:

```go
CodeSeverityOverrides: map[string]schema.Severity{
    hl7.CodeMissingOptionalSegment: schema.SeverityError,
}
```

Use `schema.SeverityDrop` to completely suppress a code that was previously causing failures
in your pipeline.

## Upgrading to v0.5.0

### Argus dependency added

v0.5.0 adds `github.com/wehubfusion/Argus` as a dependency for embedded node lifecycle
observation. If your `go.sum` pins a different version, run:

```bash
go get github.com/wehubfusion/Argus@v0.3.6
go mod tidy
```

### `ProcessFailureObserver` signature change

In v0.5.0, `ProcessFailureObserver` receives `*message.Message` as its second argument
(previously only `error`). Update all registered observers:

Before:

```go
runner.WithProcessFailureObserver(func(ctx context.Context, err error) error { ... })
```

After:

```go
runner.WithProcessFailureObserver(func(ctx context.Context, msg *message.Message, err error) error { ... })
```

## Upgrading from pre-v0.4.0

### `SourceSectionId` on `FieldMapping`

v0.4.0 adds `SourceSectionId` and `IsEventTrigger` to `FieldMapping`. These fields are
additive (zero value is backward-compatible). No action required unless you serialise
`FieldMapping` structs directly.

### Nested array iteration

`SubflowProcessor`, `ArrayPathSegment`, and `IterationStack` replace the flat iteration
model. If you have custom iteration logic in an embedded node processor, review the new
`pkg/embedded/runtime/subflow.go` for the updated traversal API.
