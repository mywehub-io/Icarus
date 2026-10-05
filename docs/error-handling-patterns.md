# Error handling patterns

## Runner error flow

When `Processor.Process` returns an error, the runner first decides whether to retry it.

**Transient error with attempts left** (`message.IsTransientError`, on a delivery before the
consumer's `MaxDeliver`, and not a unit that outran `processTimeout`). An error is transient
only when something in its chain says so; anything else is permanent:

| In the error chain | Transient |
| --- | --- |
| An error implementing `message.TransientClassifier` | What its `Transient()` returns |
| An `*errors.AppError` | When its type is `Internal` |
| A network timeout, a refused, reset or unreachable connection, a DNS failure | Yes |
| A gRPC status `Unavailable`, `ResourceExhausted` or `DeadlineExceeded` | Yes |
| An Azure `*azcore.ResponseError` with status 408, 429 or 5xx | Yes |
| `context.DeadlineExceeded` from a bound the unit set itself | Yes |
| Anything else, such as a plain `errors.New` or `fmt.Errorf` | No |

Wrap causes with `%w`: a dependency failure formatted with `%v` loses its type and is permanent.


1. The unit's `EXECUTION_HEARTBEATS` entry is marked `retrying`.
2. The message is nak'd with a delay: 5 s after attempt 1, 15 s after 2, 30 s after 3, 60 s after.
3. Nothing is published and `ProcessFailureObserver` is not called. Zeus records the first
   failed result it receives as final, so reporting here would end the node before the retry
   could succeed.

The redelivery takes the claim over from the `retrying` entry and runs the unit again.

**Permanent error, or the last attempt:**

1. The runner calls `ReportError` to publish the failure result once, with its attempt number.
2. The message is acknowledged (permanent) or terminated (transient on the last attempt), so it
   is not redelivered.
3. If `ReportError` fails, the runner retries once after 2 seconds with a fresh context.
4. If the retry also fails, the error is logged as CRITICAL. The workflow may hang.
5. `ProcessFailureObserver` is called (if registered) for side effects such as Argus emission.

```
Process(msg) → error
    ├─ transient, attempts left → mark retrying → NakWithDelay(backoff)
    └─ permanent or last attempt
         → ReportError (10-min timeout, 1 retry on failure)
         → ProcessFailureObserver (30-s timeout, best-effort, not retried)
```

`MaxDeliver` is read from the consumer when the runner resolves it. A consumer with no limit
(0 or -1) is treated as 5, so a transient failure is always reported eventually.

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
- `Permanent = false`: the runner retries the unit with backoff up to `MaxDeliver`, and reports
  the failure only on the last attempt (see Runner error flow).

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
