package audit_test

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/wehubfusion/Icarus/platform/audit"
)

func TestCollectorKeepsDecidingDecision(t *testing.T) {
	ctx, col := audit.WithCollector(context.Background())
	require.Same(t, col, audit.CollectorFrom(ctx))

	audit.RecordDecision(ctx, "workflow", "read", "w1", true)
	audit.RecordDecision(ctx, "workflow", "write", "w2", true)
	s := col.Snapshot()
	assert.True(t, s.Granted)
	assert.Equal(t, "w2", s.Decision.GetResourceId(), "with no denial, the last grant decides")

	audit.RecordDecision(ctx, "project", "write", "p1", false)
	audit.RecordDecision(ctx, "project", "delete", "p2", false)
	audit.RecordDecision(ctx, "workflow", "read", "w3", true)
	s = col.Snapshot()
	assert.True(t, s.Granted)
	assert.False(t, s.Decision.GetGranted())
	assert.Equal(t, "p1", s.Decision.GetResourceId(), "the first denial decides")
	assert.Equal(t, "write", s.Decision.GetAction())
}

func TestCollectorSetters(t *testing.T) {
	ctx, col := audit.WithCollector(context.Background())
	audit.SetJobID(ctx, "JOB-1")
	audit.SetResource(ctx, "runner", "r1")
	audit.SetReason(ctx, "LEASE_EXPIRED")
	for i := 0; i < audit.MaxAttrs+2; i++ {
		audit.SetAttr(ctx, "k"+strconv.Itoa(i), "v")
	}
	audit.SetAttr(ctx, "k0", "replaced")

	s := col.Snapshot()
	assert.Equal(t, "JOB-1", s.JobID)
	assert.True(t, s.ResourceSet)
	assert.Equal(t, "runner", s.ResourceType)
	assert.Equal(t, "r1", s.ResourceID)
	assert.Equal(t, "LEASE_EXPIRED", s.Reason)
	assert.Len(t, s.Attrs, audit.MaxAttrs)
	assert.Equal(t, "replaced", s.Attrs["k0"], "an existing key can still be updated")
	assert.True(t, s.Truncated)
}

func TestCollectorHelpersWithoutCollector(t *testing.T) {
	ctx := context.Background()
	assert.Nil(t, audit.CollectorFrom(ctx))
	audit.RecordDecision(ctx, "a", "b", "c", false)
	audit.SetJobID(ctx, "j")
	audit.SetResource(ctx, "t", "i")
	audit.SetAttr(ctx, "k", "v")
	audit.SetReason(ctx, "r")
	var col *audit.Collector
	assert.Equal(t, audit.Snapshot{}, col.Snapshot())
}

func TestCollectorConcurrentUse(t *testing.T) {
	ctx, col := audit.WithCollector(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			audit.RecordDecision(ctx, "workflow", "read", strconv.Itoa(i), i%2 == 0)
			audit.SetAttr(ctx, "k", strconv.Itoa(i))
			_ = col.Snapshot()
		}(i)
	}
	wg.Wait()
	s := col.Snapshot()
	assert.True(t, s.Granted)
	assert.False(t, s.Decision.GetGranted())
}
