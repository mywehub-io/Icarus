# Error handling patterns

## Runner error flow

When `Processor.Process` returns an error:

1. The runner calls `ReportError` to publish a failure result to Zeus via the result subject.
2. `ReportError` acknowledges the original JetStream message (prevents redelivery by the runner).
3. If `ReportError` fails, the runner retries once after 2 seconds with a fresh context.
4. If the retry also fails, the error is logged as CRITICAL. The workflow may hang.
5. `ProcessFailureObserver` is called (if registered) for side effects such as Argus emission.

```
Process(msg) → error
    → ReportError (10-min timeout, 1 retry on failure)
    → ProcessFailureObserver (30-s timeout, best-effort, not retried)
```

## Using `pkg/errors.AppError`

Create typed errors with machine-readable codes:

```go
return nil, errors.NewNotFoundError("schema not found", "SCHEMA_NOT_FOUND", originalErr)
```

Extract at call sites:

```go
var appErr *errors.AppError
if errs.As(err, &appErr) {
    switch appErr.Type {
    case errors.NotFound:
        // treat as permanent failure — do not retry
    case errors.Internal:
        // Sentry already captured; log and propagate
    }
}
```

## Permanent vs transient errors

The embedded runtime distinguishes these via `NodeFailureError.Permanent`:

- `Permanent = true`: the runner does not NAK the message (no redelivery).
- `Permanent = false`: the message is NAK'd for JetStream redelivery up to `MaxDeliver`.

For plugin processors, return a `NodeFailureError` from `EmbeddedNode.Process` when the
failure is deterministic (e.g. schema mismatch, missing required field). Return a plain
error for transient failures (e.g. downstream timeout, network hiccup).

## Messages without workflow context

When `msg.Workflow` is nil (no `WorkflowID` / `RunID`), the runner cannot call
`ReportError`. Instead it NAKs the message for redelivery and logs a WARN:

```
Runner: processing failed without workflow/run_id on message — NAK for redelivery
```

Ensure that all messages dispatched to Icarus consumers carry `WorkflowID` and `RunID`.
Messages without these fields will cycle through the consumer indefinitely until
`MaxDeliver` is reached.

## Resolver errors

`resolver.ResolveInput` returns an error when:

- Both `inline` and `blobRef` are empty — the caller passed an empty payload.
- `blobRef.URL` is set but the blob client is nil — storage was not injected.
- The blob download fails — storage is unavailable or the blob was deleted.

Pattern for handling resolver errors gracefully:

```go
inputBytes, err := resolver.ResolveInput(ctx, inline, blobRef)
if err != nil {
    return message.Message{}, errors.NewInternalError(
        "resolver", "failed to resolve input", "INPUT_RESOLVE_FAILED", err)
}
```

## Blob storage errors in `AzureBlobClient`

`UploadResult` and `DownloadResult` return wrapped errors. The container is created lazily;
`ContainerAlreadyExists` from Azure is silently swallowed. Any other creation error is
returned to the caller.

## gRPC boundary conversion

At gRPC handler boundaries, convert `*AppError` to a gRPC status error:

```go
func (s *Server) GetWorkflow(ctx context.Context, req *pb.GetWorkflowRequest) (*pb.Workflow, error) {
    w, err := s.svc.Get(ctx, req.Id)
    if err != nil {
        var appErr *icerrors.AppError
        if errs.As(err, &appErr) {
            return nil, appErr.ToGRPCError()
        }
        return nil, status.Error(codes.Internal, "unexpected error")
    }
    return w, nil
}
```

## Schema validation errors

`engine.Process` never returns a Go error for validation failures — it returns a
`ProcessResult` with `Valid: false` and populated `Errors` slice. Go errors are returned
only for unrecoverable failures (nil schema, malformed JSON input).

```go
result, err := engine.Process(input, schemaDef, schema.FormatJSON, opts)
if err != nil {
    return nil, errors.NewInternalError("schema", "schema processing failed", "SCHEMA_ENGINE_ERROR", err)
}
if !result.Valid {
    // result.Errors contains the validation issues
    return result.Errors, nil
}
```
