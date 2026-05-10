package errors

import (
	"fmt"

	"github.com/getsentry/sentry-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrorType classifies the nature of an AppError so callers and gRPC handlers
// can decide on the appropriate response code and retry strategy without
// parsing message strings.
type ErrorType int

const (
	// Internal indicates an unexpected server-side error. Maps to gRPC
	// codes.Internal (HTTP 500). NewInternalError also captures the event in
	// Sentry. Callers should NOT retry on Internal without backoff.
	Internal ErrorType = iota

	// NotFound indicates a requested resource does not exist or is not
	// visible to the caller. Maps to gRPC codes.NotFound (HTTP 404).
	// Retrying will not help unless the resource is created concurrently.
	NotFound

	// BadRequest indicates that the caller supplied invalid or malformed input.
	// Maps to gRPC codes.InvalidArgument (HTTP 400). Do not retry; fix the request.
	BadRequest

	// Unauthorized indicates that the caller is not authenticated. Maps to
	// gRPC codes.Unauthenticated (HTTP 401). The caller should refresh the
	// token and retry once.
	Unauthorized

	// Conflict indicates a state conflict such as a duplicate unique key.
	// Maps to gRPC codes.AlreadyExists (HTTP 409). Retry only after resolving
	// the conflict.
	Conflict

	// ValidationFailed indicates that input passed schema checks but violated
	// business rules (e.g. referential integrity, state invariants). Maps to
	// gRPC codes.InvalidArgument (HTTP 400). Do not retry; fix the input.
	ValidationFailed

	// PermissionDenied indicates that the caller is authenticated but lacks
	// the required Cerberus permission. Maps to gRPC codes.PermissionDenied
	// (HTTP 403). Do not retry; grant the permission first.
	PermissionDenied
)

// AppError is the canonical error type across all Olympus Go services.
// It wraps an optional underlying cause and carries a machine-readable code
// alongside a human-readable message.
//
// Use ToGRPCError to convert AppError to a gRPC status error before returning
// from a gRPC handler. Never return an AppError directly from a handler;
// gRPC will treat it as codes.Unknown.
type AppError struct {
	// Type classifies the error for gRPC mapping and retry decisions.
	Type ErrorType `json:"type"`
	// Message is the human-readable error summary, safe to return to callers.
	// It MUST NOT contain secrets, tokens, or raw SQL.
	Message string `json:"message"`
	// Code is an optional machine-readable error identifier (e.g. "WORKFLOW_NOT_FOUND").
	// It is included in JSON responses for programmatic error handling.
	Code string `json:"code,omitempty"`
	// Err is the underlying cause for debugging. It is never serialised to JSON
	// or returned to callers; it is only visible in server-side logs via Error().
	Err error `json:"-"`
}

// Error implements the error interface. It returns Message followed by the
// stringified Err when Err is non-nil, or Message alone. This form appears in
// structured log fields; it MUST NOT be returned to external callers.
func (e *AppError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}

// Unwrap returns the underlying cause so errors.Is / errors.As work through
// AppError wrappers.
func (e *AppError) Unwrap() error {
	return e.Err
}

// NewNotFoundError constructs an AppError with Type == NotFound.
// Use when a requested entity does not exist in the data store.
func NewNotFoundError(message string, code string, cause error) *AppError {
	return &AppError{
		Type:    NotFound,
		Message: message,
		Code:    code,
		Err:     cause,
	}
}

// NewBadRequestError constructs an AppError with Type == BadRequest.
// Use when input is syntactically invalid or missing required fields.
func NewBadRequestError(message string, code string, cause error) *AppError {
	return &AppError{
		Type:    BadRequest,
		Message: message,
		Code:    code,
		Err:     cause,
	}
}

// NewValidationError constructs an AppError with Type == ValidationFailed.
// Use when input passes proto/JSON schema validation but violates a business rule.
func NewValidationError(message string, code string, cause error) *AppError {
	return &AppError{
		Type:    ValidationFailed,
		Message: message,
		Code:    code,
		Err:     cause,
	}
}

// NewConflictError constructs an AppError with Type == Conflict.
// Use when a create or update operation would violate a uniqueness constraint.
func NewConflictError(message string, code string, cause error) *AppError {
	return &AppError{
		Type:    Conflict,
		Message: message,
		Code:    code,
		Err:     cause,
	}
}

// NewUnauthorizedError constructs an AppError with Type == Unauthorized.
// Use when the caller has no valid JWT or the token is expired.
func NewUnauthorizedError(message string, code string, cause error) *AppError {
	return &AppError{
		Type:    Unauthorized,
		Message: message,
		Code:    code,
		Err:     cause,
	}
}

// NewPermissionDeniedError constructs an AppError with Type == PermissionDenied.
// Use when a Cerberus CheckAccess call returns denied.
func NewPermissionDeniedError(message string, code string, cause error) *AppError {
	return &AppError{
		Type:    PermissionDenied,
		Message: message,
		Code:    code,
		Err:     cause,
	}
}

// NewInternalError constructs an AppError with Type == Internal and captures
// the event in Sentry. metaData is an optional structured string prepended to
// the Sentry message (e.g. "service=zeus run_id=abc"); pass "" to omit it.
// NewInternalError MUST NOT be called in hot paths because it synchronously
// invokes the Sentry SDK.
func NewInternalError(metaData string, message string, code string, cause error) *AppError {
	var errorMessage string
	if metaData != "" {
		errorMessage = fmt.Sprintf("MetaData: %s\nMessage: %s\nCode: %s\nError: %v", metaData, message, code, cause)
	} else {
		errorMessage = fmt.Sprintf("Message: %s\nCode: %s\nError: %v", message, code, cause)
	}
	sentry.CaptureMessage(errorMessage)
	return &AppError{
		Type:    Internal,
		Message: message,
		Code:    code,
		Err:     cause,
	}
}

// ToGRPCError converts the AppError to a gRPC status error suitable for
// returning from a gRPC handler. The mapping is:
//
//	NotFound        → codes.NotFound
//	BadRequest      → codes.InvalidArgument
//	ValidationFailed → codes.InvalidArgument
//	Unauthorized    → codes.Unauthenticated
//	PermissionDenied → codes.PermissionDenied
//	Conflict        → codes.AlreadyExists
//	Internal (+ unknown) → codes.Internal
//
// The gRPC status message is set to AppError.Message. The underlying Err and
// Code fields are not propagated to the gRPC status to avoid leaking internal
// detail to callers.
func (e *AppError) ToGRPCError() error {
	var grpcCode codes.Code
	switch e.Type {
	case NotFound:
		grpcCode = codes.NotFound
	case BadRequest:
		grpcCode = codes.InvalidArgument
	case Unauthorized:
		grpcCode = codes.Unauthenticated
	case PermissionDenied:
		grpcCode = codes.PermissionDenied
	case Conflict:
		grpcCode = codes.AlreadyExists
	case ValidationFailed:
		grpcCode = codes.InvalidArgument
	case Internal:
		fallthrough
	default:
		grpcCode = codes.Internal
	}

	return status.Error(grpcCode, e.Message)
}
