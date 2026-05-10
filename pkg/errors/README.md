# pkg/errors

Typed application errors with gRPC status mapping and Sentry capture for internal faults.

## `AppError`

```go
type AppError struct {
    Type    ErrorType
    Message string
    Code    string // machine-readable code, e.g. "CONNECTION_FAILED"
    Err     error  // underlying cause; not serialized to JSON
}
```

`AppError` implements `error` and `Unwrap()`. Use `errors.As(err, &appErr)` to extract it.

## Error types

| Constant | gRPC code | Typical use |
|---|---|---|
| `Internal` | `codes.Internal` | Unexpected server failure; auto-captures to Sentry |
| `NotFound` | `codes.NotFound` | Entity missing |
| `BadRequest` | `codes.InvalidArgument` | Caller error in input |
| `Unauthorized` | `codes.Unauthenticated` | Missing or invalid credentials |
| `Conflict` | `codes.AlreadyExists` | Duplicate resource |
| `ValidationFailed` | `codes.InvalidArgument` | Schema or field validation failure |
| `PermissionDenied` | `codes.PermissionDenied` | Authenticated but not authorized |

## Constructors

```go
func NewInternalError(metaData, message, code string, cause error) *AppError
func NewNotFoundError(message, code string, cause error) *AppError
func NewBadRequestError(message, code string, cause error) *AppError
func NewValidationError(message, code string, cause error) *AppError
func NewConflictError(message, code string, cause error) *AppError
func NewUnauthorizedError(message, code string, cause error) *AppError
func NewPermissionDeniedError(message, code string, cause error) *AppError
```

`NewInternalError` calls `sentry.CaptureMessage` automatically. The `metaData` parameter is
prepended to the Sentry message for context; pass `""` to omit it.

## gRPC conversion

```go
err := appErr.ToGRPCError()
// Returns a status.Error with the mapped gRPC code and the AppError.Message as the message.
```

Use this at the gRPC handler boundary to convert internal errors to gRPC status errors.

## Example

```go
entity, err := repo.Find(id)
if err != nil {
    return errors.NewNotFoundError("workflow not found", "WORKFLOW_NOT_FOUND", err)
}
```
