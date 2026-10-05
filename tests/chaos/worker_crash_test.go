package chaos_test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/R-zin/Aetherflow/api/proto/v1"
	"github.com/R-zin/Aetherflow/pkg/sdk"
	"github.com/R-zin/Aetherflow/pkg/server"
	"github.com/R-zin/Aetherflow/pkg/storage/memory"
)

func TestChaos_WorkerCrashAndResumption(t *testing.T) {
	registry := sdk.NewRegistry()

	var step1ExecutionCount int32
	var step2ExecutionCount int32

	registry.RegisterWorkflow("DurableSequenceWorkflow", func(ctx sdk.WorkflowContext, input []byte) ([]byte, error) {
		var out1 map[string]string
		err := ctx.ExecuteActivity("Step1", nil, &out1, sdk.WithActivityID("step-1"), sdk.WithTaskQueue("chaos-queue"))
		if err != nil {
			return nil, err
		}

		var out2 map[string]string
		err = ctx.ExecuteActivity("Step2", out1, &out2, sdk.WithActivityID("step-2"), sdk.WithTaskQueue("chaos-queue"))
		if err != nil {
			return nil, err
		}

		return json.Marshal(map[string]string{
			"status": "ALL_COMPLETED",
			"val1":   out1["v"],
			"val2":   out2["v"],
		})
	})

	store := memory.NewMemoryStore()
	defer store.Close()

	srv := server.NewWorkflowServiceServer(
		server.CoordinatorConfig{
			NodeID:          "coord-chaos-1",
			NumShards:       8,
			TimerResolution: 10 * time.Millisecond,
		},
		store,
		sdk.NewWorkflowRunner(registry),
	)
	defer srv.Close()

	ctx := context.Background()

	// 1. Start workflow
	startResp, err := srv.StartWorkflowExecution(ctx, &v1.StartWorkflowRequest{
		WorkflowId:   "wf-crash-resumption",
		WorkflowType: "DurableSequenceWorkflow",
		TaskQueue:    "chaos-queue",
	})
	if err != nil {
		t.Fatalf("failed to start workflow: %v", err)
	}

	// 2. Worker 1 polls and executes Step 1
	worker1Poll, err := srv.PollActivityTaskQueue(ctx, &v1.PollActivityTaskQueueRequest{
		TaskQueue:      "chaos-queue",
		WorkerIdentity: "worker-1-destined-to-die",
		TimeoutSeconds: 2,
	})
	if err != nil || !worker1Poll.HasTask || worker1Poll.ActivityId != "step-1" {
		t.Fatalf("worker 1 failed to acquire step-1: %+v, err: %v", worker1Poll, err)
	}
	atomic.AddInt32(&step1ExecutionCount, 1)

	_, err = srv.RespondActivityTaskCompleted(ctx, &v1.RespondActivityCompletedRequest{
		WorkflowId:     worker1Poll.WorkflowId,
		RunId:          worker1Poll.RunId,
		ActivityId:     worker1Poll.ActivityId,
		WorkerIdentity: "worker-1-destined-to-die",
		ResultPayload:  []byte(`{"v": "step-1-done"}`),
	})
	if err != nil {
		t.Fatalf("failed to complete step-1: %v", err)
	}

	// 3. Worker 1 crashes! (It completely stops executing or responding)
	// (worker 1 is abandoned)

	// 4. Worker 2 boots up in replacement
	worker2Poll, err := srv.PollActivityTaskQueue(ctx, &v1.PollActivityTaskQueueRequest{
		TaskQueue:      "chaos-queue",
		WorkerIdentity: "worker-2-replacement",
		TimeoutSeconds: 2,
	})
	if err != nil || !worker2Poll.HasTask {
		t.Fatalf("worker 2 failed to poll task: %v", err)
	}
	if worker2Poll.ActivityId != "step-2" {
		t.Fatalf("worker 2 should receive step-2, but received %s", worker2Poll.ActivityId)
	}
	atomic.AddInt32(&step2ExecutionCount, 1)

	_, err = srv.RespondActivityTaskCompleted(ctx, &v1.RespondActivityCompletedRequest{
		WorkflowId:     worker2Poll.WorkflowId,
		RunId:          worker2Poll.RunId,
		ActivityId:     worker2Poll.ActivityId,
		WorkerIdentity: "worker-2-replacement",
		ResultPayload:  []byte(`{"v": "step-2-done"}`),
	})
	if err != nil {
		t.Fatalf("worker 2 failed to complete step-2: %v", err)
	}

	// 5. Verify final status and execution counts
	histResp, err := srv.GetWorkflowExecutionHistory(ctx, &v1.GetHistoryRequest{
		WorkflowId: startResp.WorkflowId,
	})
	if err != nil {
		t.Fatalf("failed to get history: %v", err)
	}
	if histResp.Status != v1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED {
		t.Fatalf("workflow should be COMPLETED, got %v", histResp.Status)
	}

	if atomic.LoadInt32(&step1ExecutionCount) != 1 {
		t.Fatalf("step 1 should only be executed once, but ran %d times", step1ExecutionCount)
	}
	if atomic.LoadInt32(&step2ExecutionCount) != 1 {
		t.Fatalf("step 2 should only be executed once, but ran %d times", step2ExecutionCount)
	}
}
