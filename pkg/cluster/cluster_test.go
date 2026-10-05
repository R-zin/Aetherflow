package cluster_test

import (
	"testing"
	"time"

	"github.com/R-zin/Aetherflow/pkg/cluster"
)

func TestHashRing_ConsistentHashingAndDistribution(t *testing.T) {
	ring := cluster.NewHashRing(1024, 128)

	ring.AddNode("node-1")
	ring.AddNode("node-2")
	ring.AddNode("node-3")

	dist := ring.ShardDistribution()
	if len(dist) != 3 {
		t.Fatalf("expected 3 nodes in distribution, got %d", len(dist))
	}

	totalShards := 0
	for node, shards := range dist {
		if len(shards) == 0 {
			t.Fatalf("node %s received 0 shards", node)
		}
		totalShards += len(shards)
	}

	if totalShards != 1024 {
		t.Fatalf("expected exactly 1024 shards distributed, got %d", totalShards)
	}

	// Verify consistent lookup
	owner1, _ := ring.GetShardOwner(42)
	owner2, _ := ring.GetShardOwner(42)
	if owner1 != owner2 {
		t.Fatalf("inconsistent owner for shard 42: %s vs %s", owner1, owner2)
	}

	// Remove node-3: its shards should migrate to node-1 and node-2
	ring.RemoveNode("node-3")
	newDist := ring.ShardDistribution()
	if len(newDist) != 2 {
		t.Fatalf("expected 2 nodes after removal, got %d", len(newDist))
	}
	remainingTotal := len(newDist["node-1"]) + len(newDist["node-2"])
	if remainingTotal != 1024 {
		t.Fatalf("expected 1024 shards distributed across remaining nodes, got %d", remainingTotal)
	}
}

func TestLeaseManager_FencingTokensAndFailover(t *testing.T) {
	lm := cluster.NewLeaseManager()

	// 1. Initial acquisition: Epoch = 1
	l1, err := lm.AcquireOrRenew(10, "node-1", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("failed initial lease acquisition: %v", err)
	}
	if l1.Epoch != 1 || l1.OwnerNode != "node-1" {
		t.Fatalf("unexpected lease: %+v", l1)
	}

	// 2. Renewal by same owner: Epoch stays 1
	l2, err := lm.AcquireOrRenew(10, "node-1", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("failed renewal: %v", err)
	}
	if l2.Epoch != 1 {
		t.Fatalf("epoch should remain 1 on renewal, got %d", l2.Epoch)
	}

	// 3. Competing acquisition while active fails
	_, err = lm.AcquireOrRenew(10, "node-2", 50*time.Millisecond)
	if err == nil {
		t.Fatalf("expected error acquiring unexpired lease from node-1")
	}

	// 4. Wait for lease to expire
	time.Sleep(60 * time.Millisecond)

	// 5. Failover takeover by node-2: Epoch increments to 2
	l3, err := lm.AcquireOrRenew(10, "node-2", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("failed failover lease acquisition: %v", err)
	}
	if l3.Epoch != 2 || l3.OwnerNode != "node-2" {
		t.Fatalf("expected epoch 2 and owner node-2, got %+v", l3)
	}
}
