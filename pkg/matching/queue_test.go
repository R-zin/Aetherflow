package matching_test

import (
	"context"
	"testing"
	"time"

	"github.com/R-zin/Aetherflow/pkg/matching"
)

func TestMatchingEngine_EnqueueAndPoll(t *testing.T) {
	engine := matching.NewMatchingEngine()

	// 1. Poll on empty queue with short timeout (should return has_task = false)
	resp, err := engine.PollTask(context.Background(), "test-queue", "worker-1", 50*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error polling: %v", err)
	}
	if resp.HasTask {
		t.Fatalf("expected has_task = false on empty queue")
	}

	// 2. Dispatch task asynchronously while worker is long-polling
	go func() {
		time.Sleep(20 * time.Millisecond)
		engine.DispatchTask(&matching.ActivityTask{
			WorkflowID:   "wf-101",
			RunID:        "run-101",
			ActivityID:   "act-101",
			ActivityType: "EmailActivity",
			TaskQueue:    "test-queue",
			InputPayload: []byte(`{"to": "user@example.com"}`),
			Attempt:      1,
		})
	}()

	resp, err = engine.PollTask(context.Background(), "test-queue", "worker-1", 200*time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected error polling: %v", err)
	}
	if !resp.HasTask {
		t.Fatalf("expected to receive task during long poll")
	}
	if resp.WorkflowId != "wf-101" || resp.ActivityId != "act-101" {
		t.Fatalf("unexpected task received: %+v", resp)
	}
	if engine.InflightCount() != 1 {
		t.Fatalf("expected 1 inflight task, got %d", engine.InflightCount())
	}

	// 3. Heartbeat and complete
	ok := engine.RecordHeartbeat("wf-101", "act-101")
	if !ok {
		t.Fatalf("expected heartbeat recording to succeed")
	}

	engine.CompleteTask("wf-101", "act-101")
	if engine.InflightCount() != 0 {
		t.Fatalf("expected 0 inflight tasks after completion, got %d", engine.InflightCount())
	}
}
