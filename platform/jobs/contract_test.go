package jobs

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/wehubfusion/Icarus/platform/jobs/iriswire"
)

// The values must match github.com/wehubfusion/Icarus/pkg/jobs and Iris exactly; this module
// keeps its own copy so it never requires the root module.
func TestContractConstantsAreGolden(t *testing.T) {
	golden := map[string]string{
		ContractVersion:         "v1",
		UpdateTypeStatusChanged: "job.status_changed",
		UpdateTypeProgress:      "job.progress",
		UpdateTypeDataPatched:   "job.data_patched",
		StatusQueued:            "queued",
		StatusRunning:           "running",
		StatusSucceeded:         "succeeded",
		StatusFailed:            "failed",
		StatusCanceled:          "canceled",
	}
	for got, want := range golden {
		assert.Equal(t, want, got)
	}
	assert.Equal(t, "JOB-1:2", MessageID("JOB-1", 2))
	assert.Equal(t, "JOB-1:2147483648", MessageID("JOB-1", TerminalSeq))
	assert.Equal(t, uint64(2), SeqRunning)
	assert.Equal(t, uint64(3), SeqProgressStart)
	assert.Equal(t, uint64(2147483648), TerminalSeq)
	assert.Equal(t, "/iris.JobService/CreateJobWithLease", MethodCreateJobWithLease)
	assert.Equal(t, "/iris.ResourceLockService/RenewResourceLock", MethodRenewResourceLock)
}

type wireField struct {
	name     string
	number   protoreflect.FieldNumber
	kind     protoreflect.Kind
	repeated bool
	// message names a message field's type: a bare name for an Iris message, a full name for
	// a well-known type.
	message string
}

// irisFields is copied by hand from protos/services/iris/iris.proto and events.proto. A change
// there must be made here and in proto/iriswire/v1/iriswire.proto together.
var irisFields = map[string][]wireField{
	"JobResource": {
		{"type", 1, protoreflect.StringKind, false, ""},
		{"id", 2, protoreflect.StringKind, false, ""},
	},
	"JobProgress": {
		{"percent", 1, protoreflect.Int32Kind, false, ""},
		{"message", 2, protoreflect.StringKind, false, ""},
	},
	"JobError": {
		{"code", 1, protoreflect.StringKind, false, ""},
		{"message", 2, protoreflect.StringKind, false, ""},
	},
	"Job": {
		{"job_id", 1, protoreflect.StringKind, false, ""},
		{"client_id", 2, protoreflect.StringKind, false, ""},
		{"kind", 3, protoreflect.StringKind, false, ""},
		{"producer", 4, protoreflect.StringKind, false, ""},
		{"status", 5, protoreflect.StringKind, false, ""},
		{"error", 7, protoreflect.MessageKind, false, "JobError"},
		{"metadata", 8, protoreflect.MessageKind, false, "google.protobuf.Struct"},
		{"resource", 9, protoreflect.MessageKind, false, "JobResource"},
		{"idempotency_key", 10, protoreflect.StringKind, false, ""},
		{"last_seq", 11, protoreflect.Uint64Kind, false, ""},
		{"user_id", 12, protoreflect.StringKind, false, ""},
		{"user_display_name", 22, protoreflect.StringKind, false, ""},
		{"created_at", 13, protoreflect.MessageKind, false, "google.protobuf.Timestamp"},
		{"updated_at", 14, protoreflect.MessageKind, false, "google.protobuf.Timestamp"},
		{"started_at", 15, protoreflect.MessageKind, false, "google.protobuf.Timestamp"},
		{"finished_at", 16, protoreflect.MessageKind, false, "google.protobuf.Timestamp"},
		{"duration_ms", 17, protoreflect.Int64Kind, false, ""},
		{"schema", 18, protoreflect.MessageKind, false, "JobSchema"},
		{"parent_job_id", 19, protoreflect.StringKind, false, ""},
		{"children", 21, protoreflect.MessageKind, true, "Job"},
	},
	"JobSchemaField": {
		{"key", 1, protoreflect.StringKind, false, ""},
		{"label", 2, protoreflect.StringKind, false, ""},
		{"type", 3, protoreflect.StringKind, false, ""},
		{"section", 4, protoreflect.StringKind, false, ""},
		{"order", 5, protoreflect.Int32Kind, false, ""},
		{"copyable", 8, protoreflect.BoolKind, false, ""},
		{"truncate", 9, protoreflect.BoolKind, false, ""},
	},
	"JobSchemaSection": {
		{"key", 1, protoreflect.StringKind, false, ""},
		{"label", 2, protoreflect.StringKind, false, ""},
	},
	"JobSchema": {
		{"kind", 1, protoreflect.StringKind, false, ""},
		{"producer", 2, protoreflect.StringKind, false, ""},
		{"display_name", 3, protoreflect.StringKind, false, ""},
		{"fields", 4, protoreflect.MessageKind, true, "JobSchemaField"},
		{"sections", 5, protoreflect.MessageKind, true, "JobSchemaSection"},
	},
	"ResourceLock": {
		{"client_id", 1, protoreflect.StringKind, false, ""},
		{"resource", 2, protoreflect.MessageKind, false, "JobResource"},
		{"job_id", 3, protoreflect.StringKind, false, ""},
		{"kind", 4, protoreflect.StringKind, false, ""},
		{"producer", 5, protoreflect.StringKind, false, ""},
		{"user_id", 6, protoreflect.StringKind, false, ""},
		{"fence", 7, protoreflect.Int64Kind, false, ""},
		{"ttl_seconds", 8, protoreflect.Int32Kind, false, ""},
		{"acquired_at", 9, protoreflect.MessageKind, false, "google.protobuf.Timestamp"},
		{"renewed_at", 10, protoreflect.MessageKind, false, "google.protobuf.Timestamp"},
		{"expires_at", 11, protoreflect.MessageKind, false, "google.protobuf.Timestamp"},
	},
	"CreateJobWithLeaseRequest": {
		{"client_id", 1, protoreflect.StringKind, false, ""},
		{"kind", 2, protoreflect.StringKind, false, ""},
		{"producer", 3, protoreflect.StringKind, false, ""},
		{"request_id", 4, protoreflect.StringKind, false, ""},
		{"resource", 5, protoreflect.MessageKind, false, "JobResource"},
		{"user_id", 6, protoreflect.StringKind, false, ""},
		{"data", 7, protoreflect.MessageKind, false, "google.protobuf.Struct"},
		{"ttl_seconds", 8, protoreflect.Int32Kind, false, ""},
		{"parent_job_id", 9, protoreflect.StringKind, false, ""},
	},
	"CreateJobWithLeaseResponse": {
		{"acquired", 1, protoreflect.BoolKind, false, ""},
		{"job", 2, protoreflect.MessageKind, false, "Job"},
		{"locks", 3, protoreflect.MessageKind, true, "ResourceLock"},
		{"reentrant", 4, protoreflect.BoolKind, false, ""},
		{"holder", 5, protoreflect.MessageKind, false, "ResourceLock"},
	},
	"RenewResourceLockRequest": {
		{"client_id", 1, protoreflect.StringKind, false, ""},
		{"resource", 2, protoreflect.MessageKind, false, "JobResource"},
		{"job_id", 3, protoreflect.StringKind, false, ""},
		{"ttl_seconds", 4, protoreflect.Int32Kind, false, ""},
	},
	"RenewResourceLockResponse": {
		{"renewed", 1, protoreflect.BoolKind, false, ""},
		{"locks", 2, protoreflect.MessageKind, true, "ResourceLock"},
	},
	"JobUpdateMessage": {
		{"id", 1, protoreflect.StringKind, false, ""},
		{"v", 2, protoreflect.StringKind, false, ""},
		{"job_id", 3, protoreflect.StringKind, false, ""},
		{"seq", 4, protoreflect.Uint64Kind, false, ""},
		{"type", 5, protoreflect.StringKind, false, ""},
		{"timestamp", 6, protoreflect.StringKind, false, ""},
		{"client_id", 7, protoreflect.StringKind, false, ""},
		{"producer", 8, protoreflect.StringKind, false, ""},
		{"status", 9, protoreflect.StringKind, false, ""},
		{"progress", 10, protoreflect.MessageKind, false, "JobProgress"},
		{"error", 11, protoreflect.MessageKind, false, "JobError"},
		{"data", 12, protoreflect.MessageKind, false, "google.protobuf.Struct"},
		{"kind", 13, protoreflect.StringKind, false, ""},
		{"resource", 14, protoreflect.MessageKind, false, "JobResource"},
		{"idempotency_key", 15, protoreflect.StringKind, false, ""},
		{"user_id", 16, protoreflect.StringKind, false, ""},
		{"parent_job_id", 17, protoreflect.StringKind, false, ""},
	},
}

func TestIriswireMatchesIrisProto(t *testing.T) {
	fd := iriswire.File_iriswire_v1_iriswire_proto
	assert.Equal(t, protoreflect.FullName("wehub.platform.iriswire.v1"), fd.Package())
	assert.Zero(t, fd.Services().Len(), "the wire file declares no services")
	assert.Equal(t, len(irisFields), fd.Messages().Len(), "every wire message is in the table")

	for msgName, fields := range irisFields {
		md := fd.Messages().ByName(protoreflect.Name(msgName))
		if !assert.NotNil(t, md, msgName) {
			continue
		}
		assert.Equal(t, len(fields), md.Fields().Len(), "%s field count", msgName)
		for _, want := range fields {
			f := md.Fields().ByName(protoreflect.Name(want.name))
			if !assert.NotNil(t, f, "%s.%s", msgName, want.name) {
				continue
			}
			assert.Equal(t, want.number, f.Number(), "%s.%s number", msgName, want.name)
			assert.Equal(t, want.kind, f.Kind(), "%s.%s kind", msgName, want.name)
			assert.Equal(t, want.repeated, f.Cardinality() == protoreflect.Repeated, "%s.%s repeated", msgName, want.name)
			if want.message != "" {
				got := string(f.Message().FullName())
				if f.Message().ParentFile() == fd {
					got = string(f.Message().Name())
				}
				assert.Equal(t, want.message, got, "%s.%s type", msgName, want.name)
			}
		}
	}

	reserved := map[string][]protoreflect.FieldNumber{"Job": {6}, "JobSchemaField": {6, 7}}
	for msgName, nums := range reserved {
		md := fd.Messages().ByName(protoreflect.Name(msgName))
		for _, n := range nums {
			assert.True(t, md.ReservedRanges().Has(n), "%s reserves %d", msgName, n)
		}
	}
}
