package jobs

import (
	"fmt"
	"strconv"

	"github.com/google/uuid"
)

const (
	ContractVersion = "v1"

	UpdateTypeStatusChanged = "job.status_changed"
	UpdateTypeProgress      = "job.progress"
	UpdateTypeDataPatched   = "job.data_patched"

	StatusQueued    = "queued"
	StatusRunning   = "running"
	StatusSucceeded = "succeeded"
	StatusFailed    = "failed"
	StatusCanceled  = "canceled"
)

// NewJobID returns a producer-minted job identifier (JOB-<uuid>).
func NewJobID() string {
	return fmt.Sprintf("JOB-%s", uuid.NewString())
}

// MessageID is the JetStream deduplication key ({job_id}:{seq}).
func MessageID(jobID string, seq uint64) string {
	return jobID + ":" + strconv.FormatUint(seq, 10)
}
