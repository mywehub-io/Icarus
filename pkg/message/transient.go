package message

import (
	"context"
	"errors"
	"net"
	"net/http"
	"syscall"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	sdkerrors "github.com/wehubfusion/Icarus/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TransientClassifier is implemented by an error that says for itself whether retrying it can
// succeed. It is found anywhere in the error chain, so a caller can classify an error without
// changing its type or its text.
type TransientClassifier interface {
	Transient() bool
}

// IsTransientError reports whether err is worth retrying. ReportError and the runner's retry
// decision both use it. The first rule that applies decides, looking through the whole chain:
//
//  1. an error implementing TransientClassifier;
//  2. an *AppError: transient when its Type is Internal;
//  3. a failure to reach a dependency: a network timeout, a refused, reset or unreachable
//     connection, a DNS failure, a gRPC Unavailable, ResourceExhausted or DeadlineExceeded,
//     or an Azure 408, 429 or 5xx response;
//  4. anything else is permanent.
//
// Permanent is the default because a retry is not free: the runner waits 5s, 15s, 30s and 60s
// between attempts, so retrying a failure that will recur (a bad input, an Error node, an
// invalid expression) only delays it by nearly two minutes. Before retries existed every
// failure was reported at once, so an unclassified error behaves as it always did.
func IsTransientError(err error) bool {
	if err == nil {
		return false
	}
	var tc TransientClassifier
	if errors.As(err, &tc) {
		return tc.Transient()
	}
	var appErr *sdkerrors.AppError
	if errors.As(err, &appErr) {
		return appErr.Type == sdkerrors.Internal
	}
	return isDependencyFailure(err)
}

// isDependencyFailure reports whether err is a failure to reach, or a refusal from, something
// the unit depends on, rather than a fault in the unit's own input or configuration.
func isDependencyFailure(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}
	for _, errno := range []error{syscall.ECONNREFUSED, syscall.ECONNRESET, syscall.ECONNABORTED,
		syscall.EPIPE, syscall.EHOSTUNREACH, syscall.ENETUNREACH} {
		if errors.Is(err, errno) {
			return true
		}
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op == "dial" {
		return true
	}
	var grpcErr interface{ GRPCStatus() *status.Status }
	if errors.As(err, &grpcErr) {
		switch grpcErr.GRPCStatus().Code() {
		case codes.Unavailable, codes.ResourceExhausted, codes.DeadlineExceeded:
			return true
		}
	}
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		switch respErr.StatusCode {
		case http.StatusRequestTimeout, http.StatusTooManyRequests, http.StatusInternalServerError,
			http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			return true
		}
	}
	// A deadline that is not the network's, such as a call the unit bounded itself, is treated
	// like a timeout reaching a dependency. The runner's own process timeout never gets here:
	// the runner reports a unit that outran it as final without asking.
	return errors.Is(err, context.DeadlineExceeded)
}
