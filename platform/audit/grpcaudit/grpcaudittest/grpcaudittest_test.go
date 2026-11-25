package grpcaudittest_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/wehubfusion/Icarus/platform/audit/grpcaudit"
	"github.com/wehubfusion/Icarus/platform/audit/grpcaudit/grpcaudittest"
)

// recorder captures Errorf calls in place of a real test.
type recorder struct {
	testing.TB
	errs []string
}

func (r *recorder) Helper() {}

func (r *recorder) Errorf(format string, args ...any) {
	r.errs = append(r.errs, fmt.Sprintf(format, args...))
}

func TestAssertAllClassified(t *testing.T) {
	missing := &grpc.ServiceDesc{
		ServiceName: "nope.v1.Gone",
		Methods: []grpc.MethodDesc{
			{MethodName: "Covered"},
			{MethodName: "Uncovered"},
		},
	}
	cfg := grpcaudit.Config{Overrides: map[string]grpcaudit.Class{
		"/nope.v1.Gone/Covered": grpcaudit.ClassRead,
	}}

	r := &recorder{}
	grpcaudittest.AssertAllClassified(r, cfg, &healthpb.Health_ServiceDesc, missing, nil)
	assert.Len(t, r.errs, 1)
	assert.Contains(t, r.errs[0], "/nope.v1.Gone/Uncovered")

	// A registered descriptor with no binding is classified (as a write), so it passes.
	r = &recorder{}
	grpcaudittest.AssertAllClassified(r, grpcaudit.Config{}, &healthpb.Health_ServiceDesc)
	assert.Empty(t, r.errs)
}
