package message

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"syscall"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	sdkerrors "github.com/wehubfusion/Icarus/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type classified bool

func (c classified) Error() string   { return "classified" }
func (c classified) Transient() bool { return bool(c) }

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

func TestIsTransientError(t *testing.T) {
	wrap := func(err error) error { return fmt.Errorf("enrich connection: %w", err) }
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"plain error, such as an Error node", errors.New("order rejected"), false},
		{"resolver with no input", errors.New("resolver: no inline data or blob reference provided"), false},
		{"wrapped plain error", wrap(errors.New("invalid regex")), false},
		{"permanent AppError", sdkerrors.NewValidationError("bad", "BAD", nil), false},
		{"permanent AppError, wrapped", wrap(sdkerrors.NewValidationError("bad", "BAD", nil)), false},
		{"Internal AppError", sdkerrors.NewInternalError("", "publish failed", "PUBLISH_FAILED", nil), true},
		{"Internal AppError, wrapped", wrap(sdkerrors.NewInternalError("", "publish failed", "X", nil)), true},
		{"classifier says transient, despite a permanent cause", classified(true), true},
		{"classifier says permanent, despite a transient cause", fmt.Errorf("%w: %w", classified(false), syscall.ECONNREFUSED), false},
		{"connection refused", wrap(&net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}), true},
		{"connection reset", wrap(syscall.ECONNRESET), true},
		{"dns failure", wrap(&net.DNSError{Err: "no such host", Name: "nyx-agent", IsNotFound: true}), true},
		{"http client timeout", wrap(&url.Error{Op: "Get", URL: "http://x", Err: timeoutErr{}}), true},
		{"grpc unavailable, wrapped", wrap(status.Error(codes.Unavailable, "connection refused")), true},
		{"grpc resource exhausted", status.Error(codes.ResourceExhausted, "quota"), true},
		{"grpc invalid argument", wrap(status.Error(codes.InvalidArgument, "bad schema id")), false},
		{"grpc not found", status.Error(codes.NotFound, "no such connection"), false},
		{"azure 503", wrap(&azcore.ResponseError{StatusCode: http.StatusServiceUnavailable}), true},
		{"azure 429", &azcore.ResponseError{StatusCode: http.StatusTooManyRequests}, true},
		{"azure 404", wrap(&azcore.ResponseError{StatusCode: http.StatusNotFound}), false},
		{"azure 403", &azcore.ResponseError{StatusCode: http.StatusForbidden}, false},
		{"a deadline the unit set itself", wrap(context.DeadlineExceeded), true},
		{"cancelled", context.Canceled, false},
	} {
		if got := IsTransientError(tc.err); got != tc.want {
			t.Errorf("%s: IsTransientError(%v) = %v, want %v", tc.name, tc.err, got, tc.want)
		}
	}
}
