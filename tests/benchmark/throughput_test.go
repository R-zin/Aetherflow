package benchmark_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	v1 "github.com/R-zin/Aetherflow/api/proto/v1"
	"github.com/R-zin/Aetherflow/pkg/history"
	"github.com/R-zin/Aetherflow/pkg/sdk"
	"github.com/R-zin/Aetherflow/pkg/server"
	"github.com/R-zin/Aetherflow/pkg/storage/memory"
)

func BenchmarkStateMachineReplay(b *testing.B) {
	events := make([]*v1.HistoryEvent, 100)
	events[0] = &v1.HistoryEvent{
		EventId:     1,
		TimestampNs: time.Now().UnixNano(),
		EventType:   v1.EventType_EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &v1.HistoryEvent_WorkflowExecutionStartedAttributes{
			WorkflowExecutionStartedAttributes: &v1.WorkflowExecutionStartedAttributes{
				WorkflowId:   "bench-wf",
				WorkflowType: "BenchWorkflow",
			},
		},
	}

	for i := 1; i < 99; i += 2 {
		actID := fmt.Sprintf("act-%d", i)
		events[i] = &v1.HistoryEvent{
			EventId:     int64(i + 1),
			TimestampNs: time.Now().UnixNano(),
			EventType:   v1.EventType_EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
			Attributes: &v1.HistoryEvent_ActivityTaskScheduledAttributes{
				ActivityTaskScheduledAttributes: &v1.ActivityTaskScheduledAttributes{
					ActivityId: actID,
				},
			},
		}
		events[i+1] = &v1.HistoryEvent{
			EventId:     int64(i + 2),
			TimestampNs: time.Now().UnixNano(),
			EventType:   v1.EventType_EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
			Attributes: &v1.HistoryEvent_ActivityTaskCompletedAttributes{
				ActivityTaskCompletedAttributes: &v1.ActivityTaskCompletedAttributes{
					ActivityId:    actID,
					ResultPayload: []byte(`{"status":"ok"}`),
				},
			},
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sm := history.NewWorkflowStateMachine("bench-wf", "run-1")
		if err := sm.ApplyEvents(events[:10]); err != nil {
			b.Fatalf("failed applying events: %v", err)
		}
	}
}

func BenchmarkEndToEndWorkflowThroughput(b *testing.B) {
	registry := sdk.NewRegistry()
	registry.RegisterWorkflow("QuickWorkflow", func(ctx sdk.WorkflowContext, input []byte) ([]byte, error) {
		var out map[string]string
		err := ctx.ExecuteActivity("QuickAct", nil, &out, sdk.WithTaskQueue("bench-q"))
		if err != nil {
			return nil, err
		}
		return []byte(`{"done":true}`), nil
	})

	store := memory.NewMemoryStore()
	defer store.Close()

	srv := server.NewWorkflowServiceServer(
		server.CoordinatorConfig{
			NodeID:          "bench-node",
			NumShards:       64,
			TimerResolution: 10 * time.Millisecond,
		},
		store,
		sdk.NewWorkflowRunner(registry),
	)
	defer srv.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wfID := fmt.Sprintf("wf-bench-%d", i)

		// 1. Start workflow
		_, err := srv.StartWorkflowExecution(ctx, &v1.StartWorkflowRequest{
			WorkflowId:   wfID,
			WorkflowType: "QuickWorkflow",
			TaskQueue:    "bench-q",
		})
		if err != nil {
			b.Fatalf("failed start workflow: %v", err)
		}

		// 2. Worker poll
		poll, err := srv.PollActivityTaskQueue(ctx, &v1.PollActivityTaskQueueRequest{
			TaskQueue:      "bench-q",
			WorkerIdentity: "bench-worker",
			TimeoutSeconds: 1,
		})
		if err != nil || !poll.HasTask {
			b.Fatalf("failed to poll: %v", err)
		}

		// 3. Worker respond
		_, err = srv.RespondActivityTaskCompleted(ctx, &v1.RespondActivityCompletedRequest{
			WorkflowId:     poll.WorkflowId,
			RunId:          poll.RunId,
			ActivityId:     poll.ActivityId,
			WorkerIdentity: "bench-worker",
			ResultPayload:  []byte(`{"ok":true}`),
		})
		if err != nil {
			b.Fatalf("failed to complete: %v", err)
		}
	}
}
