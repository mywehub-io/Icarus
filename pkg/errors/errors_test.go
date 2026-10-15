package errors

import (
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestToGRPCErrorCarriesTheCode(t *testing.T) {
	err := NewConflictError("tag v1 already exists", "TAG_EXISTS", nil).ToGRPCError()
	st, _ := status.FromError(err)
	if st.Code() != codes.AlreadyExists {
		t.Fatalf("code = %v, want AlreadyExists", st.Code())
	}
	if st.Message() != "tag v1 already exists" {
		t.Fatalf("message = %q", st.Message())
	}
	if got := CodeFromGRPC(err); got != "TAG_EXISTS" {
		t.Fatalf("CodeFromGRPC = %q, want TAG_EXISTS", got)
	}
}

func TestToGRPCErrorWithoutACodeHasNoDetail(t *testing.T) {
	err := NewBadRequestError("bad", "", nil).ToGRPCError()
	st, _ := status.FromError(err)
	if len(st.Details()) != 0 {
		t.Fatalf("details = %v, want none", st.Details())
	}
	if got := CodeFromGRPC(err); got != "" {
		t.Fatalf("CodeFromGRPC = %q, want empty", got)
	}
}

func TestCodeFromGRPCOnAPlainError(t *testing.T) {
	if got := CodeFromGRPC(status.Error(codes.Internal, "x")); got != "" {
		t.Fatalf("CodeFromGRPC = %q", got)
	}
}
