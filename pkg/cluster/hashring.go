package cluster

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

const (
	DefaultNumShards    = 1024
	DefaultVirtualNodes = 256
)

func FNV1aHash(data []byte) uint32 {
	h := fnv.New32a()
	_, _ = h.Write(data)
	return h.Sum32()
}

type HashRing struct {
	mu           sync.RWMutex
	numShards    uint32
	vnodesCount  int
	ringNodes    []uint32         // Sorted ring hashes
	ringMap      map[uint32]string // ringHash -> physical nodeID
	activeNodes  map[string]bool
}

func NewHashRing(numShards uint32, vnodesCount int) *HashRing {
	if numShards == 0 {
		numShards = DefaultNumShards
	}
	if vnodesCount <= 0 {
		vnodesCount = DefaultVirtualNodes
	}
	return &HashRing{
		numShards:   numShards,
		vnodesCount: vnodesCount,
		ringNodes:   make([]uint32, 0),
		ringMap:     make(map[uint32]string),
		activeNodes: make(map[string]bool),
	}
}

func (hr *HashRing) ShardForWorkflow(workflowID string) uint32 {
	return FNV1aHash([]byte(workflowID)) % hr.numShards
}

func (hr *HashRing) AddNode(nodeID string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	if hr.activeNodes[nodeID] {
		return
	}
	hr.activeNodes[nodeID] = true

	for i := 0; i < hr.vnodesCount; i++ {
		vnodeKey := nodeID + "#" + strconv.Itoa(i)
		hash := FNV1aHash([]byte(vnodeKey))
		hr.ringNodes = append(hr.ringNodes, hash)
		hr.ringMap[hash] = nodeID
	}

	sort.Slice(hr.ringNodes, func(i, j int) bool {
		return hr.ringNodes[i] < hr.ringNodes[j]
	})
}

func (hr *HashRing) RemoveNode(nodeID string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()

	if !hr.activeNodes[nodeID] {
		return
	}
	delete(hr.activeNodes, nodeID)

	newRing := make([]uint32, 0, len(hr.ringNodes)-hr.vnodesCount)
	for _, hash := range hr.ringNodes {
		if hr.ringMap[hash] == nodeID {
			delete(hr.ringMap, hash)
		} else {
			newRing = append(newRing, hash)
		}
	}
	hr.ringNodes = newRing
}

func (hr *HashRing) GetShardOwner(shardID uint32) (string, error) {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	if len(hr.ringNodes) == 0 {
		return "", fmt.Errorf("hash ring has no active nodes")
	}

	shardKey := fmt.Sprintf("shard-%d", shardID)
	hash := FNV1aHash([]byte(shardKey))

	// Binary search for first node hash >= shard hash
	idx := sort.Search(len(hr.ringNodes), func(i int) bool {
		return hr.ringNodes[i] >= hash
	})

	// Wrap around circular ring
	if idx == len(hr.ringNodes) {
		idx = 0
	}

	return hr.ringMap[hr.ringNodes[idx]], nil
}

func (hr *HashRing) GetWorkflowOwner(workflowID string) (uint32, string, error) {
	shardID := hr.ShardForWorkflow(workflowID)
	owner, err := hr.GetShardOwner(shardID)
	return shardID, owner, err
}

func (hr *HashRing) GetActiveNodes() []string {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	nodes := make([]string, 0, len(hr.activeNodes))
	for n := range hr.activeNodes {
		nodes = append(nodes, n)
	}
	sort.Strings(nodes)
	return nodes
}

func (hr *HashRing) ShardDistribution() map[string][]uint32 {
	hr.mu.RLock()
	defer hr.mu.RUnlock()

	dist := make(map[string][]uint32)
	for _, n := range hr.GetActiveNodes() {
		dist[n] = make([]uint32, 0)
	}

	if len(hr.ringNodes) == 0 {
		return dist
	}

	for s := uint32(0); s < hr.numShards; s++ {
		shardKey := fmt.Sprintf("shard-%d", s)
		hash := FNV1aHash([]byte(shardKey))
		idx := sort.Search(len(hr.ringNodes), func(i int) bool {
			return hr.ringNodes[i] >= hash
		})
		if idx == len(hr.ringNodes) {
			idx = 0
		}
		owner := hr.ringMap[hr.ringNodes[idx]]
		dist[owner] = append(dist[owner], s)
	}

	return dist
}
