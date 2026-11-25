// Package jobs creates Iris jobs and publishes their updates.
//
// A Coordinator creates a job and takes its resource lease in one Iris call
// (CreateJobWithLease), then publishes the job's later updates (running, progress, terminal) as
// JobUpdateMessage JSON on the JetStream jobs subject. It fails open by default: when Iris cannot
// be reached the work goes ahead without a job, unless the caller asks to fail closed. The
// library never creates or updates the JOBS stream; Iris owns it.
//
// The Iris wire messages live in package iriswire, a copy of the Iris messages under a separate
// proto package, so a service can use this package beside its own generated Iris code.
package jobs

import "strconv"

// The jobs update contract, copied from the root Icarus module's pkg/jobs so this module never
// requires it. Tests pin every value.
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

// Update sequence numbers. Iris created the job at seq 1, so the first update is running at
// SeqRunning, progress counts up from SeqProgressStart, and every terminal update uses
// TerminalSeq so it always sorts last.
const (
	SeqRunning       uint64 = 2
	SeqProgressStart uint64 = 3
	TerminalSeq      uint64 = 1 << 31
)

// MessageID is the JetStream deduplication key of an update: {job_id}:{seq}.
func MessageID(jobID string, seq uint64) string {
	return jobID + ":" + strconv.FormatUint(seq, 10)
}
