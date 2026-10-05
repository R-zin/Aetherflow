package storage

import (
	"context"
	"errors"

	v1 "github.com/R-zin/Aetherflow/api/proto/v1"
)

var (
	ErrStaleLeaseEpoch  = errors.New("storage write rejected: stale lease epoch")
	ErrWorkflowNotFound = errors.New("workflow history not found")
)

type EventStore interface {
	AppendEvents(ctx context.Context, shardID uint32, workflowID string, expectedEpoch int64, events []*v1.HistoryEvent) error
	GetHistory(ctx context.Context, workflowID string, fromEventID int64) ([]*v1.HistoryEvent, error)
	SaveSnapshot(ctx context.Context, shardID uint32, workflowID string, snapshot []byte) error
	GetSnapshot(ctx context.Context, workflowID string) ([]byte, error)
	Close() error
}
