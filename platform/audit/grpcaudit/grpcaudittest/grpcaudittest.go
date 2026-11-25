// Package grpcaudittest holds test helpers for services that use grpcaudit.
package grpcaudittest

import (
	"testing"

	"google.golang.org/grpc"

	"github.com/wehubfusion/Icarus/platform/audit/grpcaudit"
)

// AssertAllClassified fails t for every unary method in sds that grpcaudit cannot classify: one
// whose descriptor is not in protoregistry.GlobalFiles and that has no entry in cfg.Overrides.
// Call it from a test in each service, with every ServiceDesc the service registers, so a new
// RPC cannot ship unclassified.
func AssertAllClassified(t testing.TB, cfg grpcaudit.Config, sds ...*grpc.ServiceDesc) {
	t.Helper()
	for _, sd := range sds {
		if sd == nil {
			continue
		}
		for _, m := range sd.Methods {
			full := "/" + sd.ServiceName + "/" + m.MethodName
			if grpcaudit.Classify(cfg, full) == grpcaudit.ClassAuto {
				t.Errorf("grpcaudit: %s is unclassified: its descriptor is not registered and it has no override", full)
			}
		}
	}
}
