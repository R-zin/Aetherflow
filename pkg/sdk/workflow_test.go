package sdk_test

import (
	"encoding/json"
	"testing"
	"time"

	v1 "github.com/R-zin/Aetherflow/api/proto/v1"
	"github.com/R-zin/Aetherflow/pkg/sdk"
)

type StepInput struct {
	StepName string `json:"step_name"`
}

type StepOutput struct {
	Value string `json:"value"`
}

func TestWorkflowReplay_MultiStepDurableExecution(t *testing.T) {
	registry := sdk.NewRegistry()

	// Multi-step workflow: Step 1 -> Step 2 -> Finished
	registry.RegisterWorkflow("TwoStepWorkflow", func(ctx sdk.WorkflowContext, input []byte) ([]byte, error) {
		var out1 StepOutput
		err := ctx.ExecuteActivity("Step1Activity", StepInput{StepName: "First"}, &out1, sdk.WithActivityID("act-1"))
		if err != nil {
			return nil, err
		}

		var out2 StepOutput
		err = ctx.ExecuteActivity("Step2Activity", StepInput{StepName: out1.Value + "-Second"}, &out2, sdk.WithActivityID("act-2"))
		if err != nil {
			return nil, err
		}

		return json.Marshal(map[string]string{
			"final": out2.Value,
		})
	})

	runner := sdk.NewWorkflowRunner(registry)

	// Step 1: Run with initial empty history (Workflow Started only)
	now := time.Now().UnixNano()
	h0 := []*v1.HistoryEvent{
		{
			EventId:     1,
			TimestampNs: now,
			EventType:   v1.EventType_EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		},
	}

	res1, err := runner.ExecuteOrReplay("wf-1", "run-1", "TwoStepWorkflow", nil, h0)
	if err != nil {
		t.Fatalf("unexpected error in step 1: %v", err)
	}
	if res1.IsComplete {
		t.Fatalf("workflow should yield for act-1, but completed")
	}
	if len(res1.Commands) != 1 || res1.Commands[0].Type != sdk.CommandScheduleActivity {
		t.Fatalf("expected 1 schedule activity command, got %+v", res1.Commands)
	}
	schedAttrs1 := res1.Commands[0].Payload.(*v1.ActivityTaskScheduledAttributes)
	if schedAttrs1.ActivityId != "act-1" || schedAttrs1.ActivityType != "Step1Activity" {
		t.Fatalf("expected act-1 schedule command, got %+v", schedAttrs1)
	}

	// Step 2: Now simulate worker executing act-1 and recording into history
	step1ResultBytes, _ := json.Marshal(StepOutput{Value: "Step1Success"})
	h1 := []*v1.HistoryEvent{
		h0[0],
		{
			EventId:     2,
			TimestampNs: now + 1000,
			EventType:   v1.EventType_EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
			Attributes: &v1.HistoryEvent_ActivityTaskScheduledAttributes{
				ActivityTaskScheduledAttributes: schedAttrs1,
			},
		},
		{
			EventId:     3,
			TimestampNs: now + 2000,
			EventType:   v1.EventType_EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
			Attributes: &v1.HistoryEvent_ActivityTaskCompletedAttributes{
				ActivityTaskCompletedAttributes: &v1.ActivityTaskCompletedAttributes{
					ActivityId:    "act-1",
					ResultPayload: step1ResultBytes,
				},
			},
		},
	}

	// Replay: Should skip act-1 (using cached output "Step1Success") and yield act-2!
	res2, err := runner.ExecuteOrReplay("wf-1", "run-1", "TwoStepWorkflow", nil, h1)
	if err != nil {
		t.Fatalf("unexpected error in step 2: %v", err)
	}
	if res2.IsComplete {
		t.Fatalf("workflow should yield for act-2, but completed")
	}
	if len(res2.Commands) != 1 || res2.Commands[0].Type != sdk.CommandScheduleActivity {
		t.Fatalf("expected 1 schedule activity command for act-2, got %+v", res2.Commands)
	}
	schedAttrs2 := res2.Commands[0].Payload.(*v1.ActivityTaskScheduledAttributes)
	if schedAttrs2.ActivityId != "act-2" || schedAttrs2.ActivityType != "Step2Activity" {
		t.Fatalf("expected act-2 schedule command, got %+v", schedAttrs2)
	}
	var input2 StepInput
	_ = json.Unmarshal(schedAttrs2.InputPayload, &input2)
	if input2.StepName != "Step1Success-Second" {
		t.Fatalf("act-2 input should contain Step1Success result, got %s", input2.StepName)
	}

	// Step 3: Now simulate act-2 completed in history
	step2ResultBytes, _ := json.Marshal(StepOutput{Value: "AllDone"})
	h2 := append(h1,
		&v1.HistoryEvent{
			EventId:     4,
			TimestampNs: now + 3000,
			EventType:   v1.EventType_EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
			Attributes: &v1.HistoryEvent_ActivityTaskScheduledAttributes{
				ActivityTaskScheduledAttributes: schedAttrs2,
			},
		},
		&v1.HistoryEvent{
			EventId:     5,
			TimestampNs: now + 4000,
			EventType:   v1.EventType_EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
			Attributes: &v1.HistoryEvent_ActivityTaskCompletedAttributes{
				ActivityTaskCompletedAttributes: &v1.ActivityTaskCompletedAttributes{
					ActivityId:    "act-2",
					ResultPayload: step2ResultBytes,
				},
			},
		},
	)

	// Replay: Both act-1 and act-2 are in history, workflow must complete!
	res3, err := runner.ExecuteOrReplay("wf-1", "run-1", "TwoStepWorkflow", nil, h2)
	if err != nil {
		t.Fatalf("unexpected error in step 3: %v", err)
	}
	if !res3.IsComplete {
		t.Fatalf("workflow should be complete")
	}
	var finalMap map[string]string
	_ = json.Unmarshal(res3.Output, &finalMap)
	if finalMap["final"] != "AllDone" {
		t.Fatalf("expected final value 'AllDone', got %v", finalMap["final"])
	}
}

func TestWorkflowReplay_NonDeterministicDriftDetection(t *testing.T) {
	registry := sdk.NewRegistry()

	// Initial workflow was registered as AlteredWorkflow but history expected OldActivity
	registry.RegisterWorkflow("AlteredWorkflow", func(ctx sdk.WorkflowContext, input []byte) ([]byte, error) {
		var out StepOutput
		// Developer changed activity from OldActivity to RogueActivity!
		return nil, ctx.ExecuteActivity("RogueActivity", nil, &out, sdk.WithActivityID("act-1"))
	})

	runner := sdk.NewWorkflowRunner(registry)

	historyEvents := []*v1.HistoryEvent{
		{
			EventId:   1,
			EventType: v1.EventType_EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		},
		{
			EventId:   2,
			EventType: v1.EventType_EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
			Attributes: &v1.HistoryEvent_ActivityTaskScheduledAttributes{
				ActivityTaskScheduledAttributes: &v1.ActivityTaskScheduledAttributes{
					ActivityId:   "act-1",
					ActivityType: "OldActivity", // History had OldActivity
				},
			},
		},
	}

	_, err := runner.ExecuteOrReplay("wf-drift", "run-drift", "AlteredWorkflow", nil, historyEvents)
	if err == nil {
		t.Fatalf("expected ErrNonDeterministicWorkflow due to activity drift, got nil")
	}
}
