package chaos_test

import (
	"context"
	"testing"
	"time"

	v1 "github.com/R-zin/Aetherflow/api/proto/v1"
	"github.com/R-zin/Aetherflow/pkg/cluster"
	"github.com/R-zin/Aetherflow/pkg/storage"
	"github.com/R-zin/Aetherflow/pkg/storage/memory"
)

func TestChaos_ShardFailoverAndFencingTokenEnforcement(t *testing.T) {
	ctx := context.Background()
	store := memory.NewMemoryStore()
	defer store.Close()

	leases := cluster.NewLeaseManager()
	shardID := uint32(15)
	workflowID := "wf-failover-test"

	// 1. Node 1 acquires lease for shard 15 (TTL = 40ms)
	l1, err := leases.AcquireOrRenew(shardID, "node-1", 40*time.Millisecond)
	if err != nil {
		t.Fatalf("node 1 failed initial lease: %v", err)
	}
	if l1.Epoch != 1 {
		t.Fatalf("initial epoch should be 1, got %d", l1.Epoch)
	}

	// 2. Node 1 writes event 1 to storage with epoch 1
	e1 := []*v1.HistoryEvent{
		{
			EventId:     1,
			TimestampNs: time.Now().UnixNano(),
			EventType:   v1.EventType_EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			Epoch:       1,
		},
	}
	if err := store.AppendEvents(ctx, shardID, workflowID, 1, e1); err != nil {
		t.Fatalf("failed appending event with epoch 1: %v", err)
	}

	// 3. Node 1 experiences network partition or hangs. Its lease expires!
	time.Sleep(50 * time.Millisecond)

	// 4. Node 2 detects failure and takes over shard 15
	l2, err := leases.AcquireOrRenew(shardID, "node-2", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("node 2 failed to acquire expired lease: %v", err)
	}
	if l2.Epoch != 2 {
		t.Fatalf("failover epoch should have incremented to 2, got %d", l2.Epoch)
	}

	// 5. Node 2 writes event 2 with epoch 2
	e2 := []*v1.HistoryEvent{
		{
			EventId:     2,
			TimestampNs: time.Now().UnixNano(),
			EventType:   v1.EventType_EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
			Epoch:       2,
		},
	}
	if err := store.AppendEvents(ctx, shardID, workflowID, 2, e2); err != nil {
		t.Fatalf("failed appending event with epoch 2: %v", err)
	}

	// 6. Split-brain scenario: Zombie Node 1 wakes up and attempts a delayed write with stale epoch 1!
	zombieEvent := []*v1.HistoryEvent{
		{
			EventId:     3,
			TimestampNs: time.Now().UnixNano(),
			EventType:   v1.EventType_EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
			Epoch:       1, // Stale!
		},
	}
	err = store.AppendEvents(ctx, shardID, workflowID, 1, zombieEvent)
	if err != storage.ErrStaleLeaseEpoch {
		t.Fatalf("expected ErrStaleLeaseEpoch on zombie node write, got %v", err)
	}
}
