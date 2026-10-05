package storage_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	v1 "github.com/R-zin/Aetherflow/api/proto/v1"
	"github.com/R-zin/Aetherflow/pkg/storage"
	"github.com/R-zin/Aetherflow/pkg/storage/memory"
	"github.com/R-zin/Aetherflow/pkg/storage/pebble"
)

func runStoreTestSuite(t *testing.T, store storage.EventStore) {
	ctx := context.Background()

	// 1. Check non-existent workflow
	_, err := store.GetHistory(ctx, "non-existent", 1)
	if err != storage.ErrWorkflowNotFound {
		t.Fatalf("expected ErrWorkflowNotFound, got %v", err)
	}

	// 2. Append events with epoch 10
	events := []*v1.HistoryEvent{
		{
			EventId:     1,
			TimestampNs: time.Now().UnixNano(),
			EventType:   v1.EventType_EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			Epoch:       10,
		},
		{
			EventId:     2,
			TimestampNs: time.Now().UnixNano(),
			EventType:   v1.EventType_EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
			Epoch:       10,
		},
	}

	err = store.AppendEvents(ctx, 1, "wf-test-1", 10, events)
	if err != nil {
		t.Fatalf("failed to append events: %v", err)
	}

	// 3. Retrieve history from event 1
	hist, err := store.GetHistory(ctx, "wf-test-1", 1)
	if err != nil {
		t.Fatalf("failed to get history: %v", err)
	}
	if len(hist) != 2 {
		t.Fatalf("expected 2 events, got %d", len(hist))
	}
	if hist[0].EventId != 1 || hist[1].EventId != 2 {
		t.Fatalf("history events out of order: %+v", hist)
	}

	// 4. Retrieve history from event 2
	histPart, err := store.GetHistory(ctx, "wf-test-1", 2)
	if err != nil {
		t.Fatalf("failed to get partial history: %v", err)
	}
	if len(histPart) != 1 || histPart[0].EventId != 2 {
		t.Fatalf("expected only event 2, got %+v", histPart)
	}

	// 5. Epoch fencing: appending with lower epoch (9 < 10) must fail
	staleEvent := []*v1.HistoryEvent{
		{
			EventId:   3,
			EventType: v1.EventType_EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		},
	}
	err = store.AppendEvents(ctx, 1, "wf-test-1", 9, staleEvent)
	if err != storage.ErrStaleLeaseEpoch {
		t.Fatalf("expected ErrStaleLeaseEpoch on stale epoch write, got %v", err)
	}

	// 6. Append with epoch >= 10 succeeds
	err = store.AppendEvents(ctx, 1, "wf-test-1", 11, staleEvent)
	if err != nil {
		t.Fatalf("expected success with epoch 11, got %v", err)
	}

	// 7. Snapshot test
	snapData := []byte("checkpoint-state-blob")
	err = store.SaveSnapshot(ctx, 1, "wf-test-1", snapData)
	if err != nil {
		t.Fatalf("failed to save snapshot: %v", err)
	}
	loadedSnap, err := store.GetSnapshot(ctx, "wf-test-1")
	if err != nil {
		t.Fatalf("failed to get snapshot: %v", err)
	}
	if string(loadedSnap) != string(snapData) {
		t.Fatalf("snapshot mismatch: got %s, want %s", loadedSnap, snapData)
	}
}

func TestMemoryStore(t *testing.T) {
	store := memory.NewMemoryStore()
	defer store.Close()
	runStoreTestSuite(t, store)
}

func TestPebbleStore(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "pebble_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	store, err := pebble.OpenPebbleStore(filepath.Join(tempDir, "data"))
	if err != nil {
		t.Fatalf("failed to open pebble store: %v", err)
	}
	defer store.Close()

	runStoreTestSuite(t, store)
}
