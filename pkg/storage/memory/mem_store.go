package memory

import (
	"context"
	"sync"

	v1 "github.com/R-zin/Aetherflow/api/proto/v1"
	"github.com/R-zin/Aetherflow/pkg/storage"
	"google.golang.org/protobuf/proto"
)

type workflowData struct {
	shardID   uint32
	lastEpoch int64
	events    []*v1.HistoryEvent
	snapshot  []byte
}

type MemoryStore struct {
	mu        sync.RWMutex
	workflows map[string]*workflowData
	closed    bool
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		workflows: make(map[string]*workflowData),
	}
}

func (s *MemoryStore) AppendEvents(ctx context.Context, shardID uint32, workflowID string, expectedEpoch int64, events []*v1.HistoryEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return storage.ErrWorkflowNotFound
	}

	data, exists := s.workflows[workflowID]
	if !exists {
		data = &workflowData{
			shardID:   shardID,
			lastEpoch: expectedEpoch,
			events:    make([]*v1.HistoryEvent, 0),
		}
		s.workflows[workflowID] = data
	}

	// Validate epoch fencing: stale writes rejected
	if expectedEpoch < data.lastEpoch {
		return storage.ErrStaleLeaseEpoch
	}
	data.lastEpoch = expectedEpoch

	for _, e := range events {
		cloned := proto.Clone(e).(*v1.HistoryEvent)
		data.events = append(data.events, cloned)
	}

	return nil
}

func (s *MemoryStore) GetHistory(ctx context.Context, workflowID string, fromEventID int64) ([]*v1.HistoryEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, exists := s.workflows[workflowID]
	if !exists {
		return nil, storage.ErrWorkflowNotFound
	}

	result := make([]*v1.HistoryEvent, 0)
	for _, e := range data.events {
		if e.EventId >= fromEventID {
			result = append(result, proto.Clone(e).(*v1.HistoryEvent))
		}
	}
	return result, nil
}

func (s *MemoryStore) SaveSnapshot(ctx context.Context, shardID uint32, workflowID string, snapshot []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, exists := s.workflows[workflowID]
	if !exists {
		data = &workflowData{
			shardID: shardID,
			events:  make([]*v1.HistoryEvent, 0),
		}
		s.workflows[workflowID] = data
	}

	data.snapshot = append([]byte(nil), snapshot...)
	return nil
}

func (s *MemoryStore) GetSnapshot(ctx context.Context, workflowID string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, exists := s.workflows[workflowID]
	if !exists || data.snapshot == nil {
		return nil, storage.ErrWorkflowNotFound
	}

	res := make([]byte, len(data.snapshot))
	copy(res, data.snapshot)
	return res, nil
}

func (s *MemoryStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}
