package cluster

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrShardLeaseHeld = errors.New("shard lease is currently held by an active node")
	ErrNotLeaseOwner  = errors.New("node is not the current lease owner")
)

type ShardLease struct {
	ShardID   uint32
	OwnerNode string
	Epoch     int64
	ExpiresAt time.Time
}

type LeaseManager struct {
	mu     sync.RWMutex
	leases map[uint32]*ShardLease
}

func NewLeaseManager() *LeaseManager {
	return &LeaseManager{
		leases: make(map[uint32]*ShardLease),
	}
}

func (lm *LeaseManager) AcquireOrRenew(shardID uint32, nodeID string, ttl time.Duration) (*ShardLease, error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	now := time.Now()
	lease, exists := lm.leases[shardID]
	if !exists {
		newLease := &ShardLease{
			ShardID:   shardID,
			OwnerNode: nodeID,
			Epoch:     1,
			ExpiresAt: now.Add(ttl),
		}
		lm.leases[shardID] = newLease
		return newLease, nil
	}

	// Case 1: Same owner renews lease (heartbeat)
	if lease.OwnerNode == nodeID {
		lease.ExpiresAt = now.Add(ttl)
		return lease, nil
	}

	// Case 2: Different node attempts acquisition while lease is still valid
	if now.Before(lease.ExpiresAt) {
		return nil, fmt.Errorf("%w: shard %d owned by %s until %v", ErrShardLeaseHeld, shardID, lease.OwnerNode, lease.ExpiresAt)
	}

	// Case 3: Lease has expired! Failover takeover with incremented Epoch (fencing token)
	lease.OwnerNode = nodeID
	lease.Epoch++
	lease.ExpiresAt = now.Add(ttl)
	return lease, nil
}

func (lm *LeaseManager) GetLease(shardID uint32) (*ShardLease, bool) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	lease, ok := lm.leases[shardID]
	if !ok {
		return nil, false
	}
	cp := *lease
	return &cp, true
}

func (lm *LeaseManager) Release(shardID uint32, nodeID string) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	lease, ok := lm.leases[shardID]
	if !ok {
		return nil
	}
	if lease.OwnerNode != nodeID {
		return ErrNotLeaseOwner
	}
	delete(lm.leases, shardID)
	return nil
}

func (lm *LeaseManager) GetAllLeases() []*ShardLease {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	res := make([]*ShardLease, 0, len(lm.leases))
	for _, l := range lm.leases {
		cp := *l
		res = append(res, &cp)
	}
	return res
}
