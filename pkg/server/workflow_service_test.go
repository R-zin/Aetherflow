package server_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	v1 "github.com/R-zin/Aetherflow/api/proto/v1"
	"github.com/R-zin/Aetherflow/pkg/sdk"
	"github.com/R-zin/Aetherflow/pkg/server"
	"github.com/R-zin/Aetherflow/pkg/storage/memory"
)

type GreetInput struct {
	Name string `json:"name"`
}

type GreetOutput struct {
	Greeting string `json:"greeting"`
}

func TestWorkflowService_EndToEndExecution(t *testing.T) {
	registry := sdk.NewRegistry()

	// Workflow: receives Name, calls SayHello activity, returns greeting
	registry.RegisterWorkflow("GreetingWorkflow", func(ctx sdk.WorkflowContext, input []byte) ([]byte, error) {
		var in GreetInput
		_ = json.Unmarshal(input, &in)

		var out GreetOutput
		err := ctx.ExecuteActivity("SayHelloActivity", in, &out, sdk.WithTaskQueue("greeting-queue"))
		if err != nil {
			return nil, err
		}

		return json.Marshal(map[string]string{
			"message": out.Greeting,
		})
	})

	store := memory.NewMemoryStore()
	defer store.Close()

	runner := sdk.NewWorkflowRunner(registry)
	srv := server.NewWorkflowServiceServer(
		server.CoordinatorConfig{
			NodeID:          "coord-1",
			NumShards:       16,
			TimerResolution: 10 * time.Millisecond,
		},
		store,
		runner,
	)
	defer srv.Close()

	ctx := context.Background()

	// 1. Start Workflow
	startResp, err := srv.StartWorkflowExecution(ctx, &v1.StartWorkflowRequest{
		WorkflowId:   "wf-greet-101",
		WorkflowType: "GreetingWorkflow",
		TaskQueue:    "greeting-queue",
		InputPayload: sdk.JSONPayload(GreetInput{Name: "Alice"}),
	})
	if err != nil {
		t.Fatalf("failed to start workflow: %v", err)
	}
	if startResp.WorkflowId != "wf-greet-101" {
		t.Fatalf("unexpected workflow ID: %s", startResp.WorkflowId)
	}

	// 2. Poll for activity task
	pollResp, err := srv.PollActivityTaskQueue(ctx, &v1.PollActivityTaskQueueRequest{
		TaskQueue:      "greeting-queue",
		WorkerIdentity: "worker-greet-1",
		TimeoutSeconds: 2,
	})
	if err != nil {
		t.Fatalf("failed to poll activity task: %v", err)
	}
	if !pollResp.HasTask {
		t.Fatalf("expected to receive SayHelloActivity task")
	}
	if pollResp.ActivityType != "SayHelloActivity" {
		t.Fatalf("unexpected activity type: %s", pollResp.ActivityType)
	}

	// 3. Worker executes SayHello and responds
	var inputPayload GreetInput
	_ = json.Unmarshal(pollResp.InputPayload, &inputPayload)
	resultPayload := sdk.JSONPayload(GreetOutput{Greeting: "Hello, " + inputPayload.Name + "!"})

	compResp, err := srv.RespondActivityTaskCompleted(ctx, &v1.RespondActivityCompletedRequest{
		WorkflowId:    pollResp.WorkflowId,
		RunId:         pollResp.RunId,
		ActivityId:    pollResp.ActivityId,
		WorkerIdentity: "worker-greet-1",
		ResultPayload: resultPayload,
	})
	if err != nil {
		t.Fatalf("failed to respond activity completed: %v", err)
	}
	if !compResp.Acknowledged {
		t.Fatalf("expected acknowledgement")
	}

	// 4. Fetch history and verify final completion
	histResp, err := srv.GetWorkflowExecutionHistory(ctx, &v1.GetHistoryRequest{
		WorkflowId: "wf-greet-101",
	})
	if err != nil {
		t.Fatalf("failed to get history: %v", err)
	}
	if histResp.Status != v1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED {
		t.Fatalf("expected workflow status COMPLETED, got %v", histResp.Status)
	}

	// Check sequence of events:
	// 1: Started
	// 2: ActivityScheduled
	// 3: ActivityStarted
	// 4: ActivityCompleted
	// 5: WorkflowExecutionCompleted
	events := histResp.History.Events
	if len(events) != 5 {
		t.Fatalf("expected exactly 5 events in history, got %d", len(events))
	}
	if events[0].EventType != v1.EventType_EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
		t.Fatalf("event 1 should be STARTED, got %v", events[0].EventType)
	}
	if events[4].EventType != v1.EventType_EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED {
		t.Fatalf("event 5 should be COMPLETED, got %v", events[4].EventType)
	}

	finalPayload := events[4].GetWorkflowExecutionCompletedAttributes().ResultPayload
	var finalMap map[string]string
	_ = json.Unmarshal(finalPayload, &finalMap)
	if finalMap["message"] != "Hello, Alice!" {
		t.Fatalf("expected greeting 'Hello, Alice!', got %s", finalMap["message"])
	}
}
