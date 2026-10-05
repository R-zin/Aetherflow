package pebble

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble"
	v1 "github.com/R-zin/Aetherflow/api/proto/v1"
	"github.com/R-zin/Aetherflow/pkg/storage"
	"google.golang.org/protobuf/proto"
)

type PebbleStore struct {
	mu sync.RWMutex
	db *pebble.DB
}

func OpenPebbleStore(dir string) (*PebbleStore, error) {
	db, err := pebble.Open(dir, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble database at %s: %w", dir, err)
	}
	return &PebbleStore{db: db}, nil
}

func (s *PebbleStore) metaKey(workflowID string) []byte {
	return []byte(fmt.Sprintf("meta/%s", workflowID))
}

func (s *PebbleStore) eventKey(workflowID string, eventID int64) []byte {
	return []byte(fmt.Sprintf("event/%s/%012d", workflowID, eventID))
}

func (s *PebbleStore) snapshotKey(workflowID string) []byte {
	return []byte(fmt.Sprintf("snap/%s", workflowID))
}

type workflowMeta struct {
	ShardID   uint32
	LastEpoch int64
}

func encodeMeta(m *workflowMeta) []byte {
	buf := make([]byte, 12)
	binary.BigEndian.PutUint32(buf[0:4], m.ShardID)
	binary.BigEndian.PutUint64(buf[4:12], uint64(m.LastEpoch))
	return buf
}

func decodeMeta(buf []byte) (*workflowMeta, error) {
	if len(buf) < 12 {
		return nil, fmt.Errorf("invalid meta payload size: %d", len(buf))
	}
	shardID := binary.BigEndian.Uint32(buf[0:4])
	lastEpoch := int64(binary.BigEndian.Uint64(buf[4:12]))
	return &workflowMeta{ShardID: shardID, LastEpoch: lastEpoch}, nil
}

func (s *PebbleStore) AppendEvents(ctx context.Context, shardID uint32, workflowID string, expectedEpoch int64, events []*v1.HistoryEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	metaK := s.metaKey(workflowID)
	metaVal, closer, err := s.db.Get(metaK)
	var meta *workflowMeta
	if err == pebble.ErrNotFound {
		meta = &workflowMeta{
			ShardID:   shardID,
			LastEpoch: expectedEpoch,
		}
	} else if err != nil {
		return fmt.Errorf("failed to read workflow meta: %w", err)
	} else {
		meta, err = decodeMeta(metaVal)
		_ = closer.Close()
		if err != nil {
			return err
		}
	}

	// Epoch fencing check
	if expectedEpoch < meta.LastEpoch {
		return storage.ErrStaleLeaseEpoch
	}
	meta.LastEpoch = expectedEpoch
	meta.ShardID = shardID

	batch := s.db.NewBatch()
	defer batch.Close()

	for _, event := range events {
		eventBytes, err := proto.Marshal(event)
		if err != nil {
			return fmt.Errorf("failed to marshal history event %d: %w", event.EventId, err)
		}
		if err := batch.Set(s.eventKey(workflowID, event.EventId), eventBytes, nil); err != nil {
			return err
		}
	}

	if err := batch.Set(metaK, encodeMeta(meta), nil); err != nil {
		return err
	}

	return batch.Commit(pebble.Sync)
}

func (s *PebbleStore) GetHistory(ctx context.Context, workflowID string, fromEventID int64) ([]*v1.HistoryEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check if workflow exists
	metaK := s.metaKey(workflowID)
	_, closer, err := s.db.Get(metaK)
	if err == pebble.ErrNotFound {
		return nil, storage.ErrWorkflowNotFound
	} else if err != nil {
		return nil, err
	}
	_ = closer.Close()

	prefix := []byte(fmt.Sprintf("event/%s/", workflowID))
	upper := []byte(fmt.Sprintf("event/%s0", workflowID)) // lexicographical upper bound
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	startKey := s.eventKey(workflowID, fromEventID)
	var events []*v1.HistoryEvent

	for valid := iter.SeekGE(startKey); valid; valid = iter.Next() {
		event := &v1.HistoryEvent{}
		if err := proto.Unmarshal(iter.Value(), event); err != nil {
			return nil, fmt.Errorf("failed to unmarshal event: %w", err)
		}
		events = append(events, event)
	}

	if err := iter.Error(); err != nil {
		return nil, err
	}

	return events, nil
}

func (s *PebbleStore) SaveSnapshot(ctx context.Context, shardID uint32, workflowID string, snapshot []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.db.Set(s.snapshotKey(workflowID), snapshot, pebble.Sync)
}

func (s *PebbleStore) GetSnapshot(ctx context.Context, workflowID string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	val, closer, err := s.db.Get(s.snapshotKey(workflowID))
	if err == pebble.ErrNotFound {
		return nil, storage.ErrWorkflowNotFound
	} else if err != nil {
		return nil, err
	}
	defer closer.Close()

	res := make([]byte, len(val))
	copy(res, val)
	return res, nil
}

func (s *PebbleStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Close()
}
