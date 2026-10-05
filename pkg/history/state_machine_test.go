package history_test

import (
	"testing"
	"time"

	v1 "github.com/R-zin/Aetherflow/api/proto/v1"
	"github.com/R-zin/Aetherflow/pkg/history"
)

func TestWorkflowStateMachine_HappyPath(t *testing.T) {
	sm := history.NewWorkflowStateMachine("wf-100", "run-001")

	now := time.Now().UnixNano()

	// 1. Workflow Started
	err := sm.ApplyEvent(&v1.HistoryEvent{
		EventId:     1,
		TimestampNs: now,
		EventType:   v1.EventType_EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Epoch:       1,
		Attributes: &v1.HistoryEvent_WorkflowExecutionStartedAttributes{
			WorkflowExecutionStartedAttributes: &v1.WorkflowExecutionStartedAttributes{
				WorkflowId:   "wf-100",
				RunId:        "run-001",
				WorkflowType: "OrderProcessingWorkflow",
				TaskQueue:    "main-queue",
				InputPayload: []byte(`{"order_id": "ORD-123"}`),
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error applying started event: %v", err)
	}

	if sm.Status != v1.WorkflowStatus_WORKFLOW_STATUS_RUNNING {
		t.Fatalf("expected status RUNNING, got %v", sm.Status)
	}

	// 2. Activity Scheduled
	err = sm.ApplyEvent(&v1.HistoryEvent{
		EventId:     2,
		TimestampNs: now + int64(time.Millisecond),
		EventType:   v1.EventType_EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		Epoch:       1,
		Attributes: &v1.HistoryEvent_ActivityTaskScheduledAttributes{
			ActivityTaskScheduledAttributes: &v1.ActivityTaskScheduledAttributes{
				ActivityId:   "act-1",
				ActivityType: "ValidateOrderActivity",
				TaskQueue:    "main-queue",
				InputPayload: []byte(`{"order_id": "ORD-123"}`),
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error scheduling activity: %v", err)
	}
	if len(sm.InflightActivities) != 1 {
		t.Fatalf("expected 1 inflight activity, got %d", len(sm.InflightActivities))
	}

	// 3. Activity Started
	err = sm.ApplyEvent(&v1.HistoryEvent{
		EventId:     3,
		TimestampNs: now + int64(2*time.Millisecond),
		EventType:   v1.EventType_EVENT_TYPE_ACTIVITY_TASK_STARTED,
		Epoch:       1,
		Attributes: &v1.HistoryEvent_ActivityTaskStartedAttributes{
			ActivityTaskStartedAttributes: &v1.ActivityTaskStartedAttributes{
				ActivityId:     "act-1",
				Attempt:        1,
				WorkerIdentity: "worker-node-1",
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error starting activity: %v", err)
	}

	// 4. Activity Completed
	err = sm.ApplyEvent(&v1.HistoryEvent{
		EventId:     4,
		TimestampNs: now + int64(3*time.Millisecond),
		EventType:   v1.EventType_EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		Epoch:       1,
		Attributes: &v1.HistoryEvent_ActivityTaskCompletedAttributes{
			ActivityTaskCompletedAttributes: &v1.ActivityTaskCompletedAttributes{
				ActivityId:    "act-1",
				ResultPayload: []byte(`{"valid": true}`),
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error completing activity: %v", err)
	}
	if len(sm.InflightActivities) != 0 {
		t.Fatalf("expected 0 inflight activities, got %d", len(sm.InflightActivities))
	}
	res, ok := sm.GetActivityCompletion("act-1")
	if !ok || !res.Completed || string(res.Result) != `{"valid": true}` {
		t.Fatalf("expected completed activity result, got %+v", res)
	}

	// 5. Workflow Completed
	err = sm.ApplyEvent(&v1.HistoryEvent{
		EventId:     5,
		TimestampNs: now + int64(4*time.Millisecond),
		EventType:   v1.EventType_EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		Epoch:       1,
		Attributes: &v1.HistoryEvent_WorkflowExecutionCompletedAttributes{
			WorkflowExecutionCompletedAttributes: &v1.WorkflowExecutionCompletedAttributes{
				ResultPayload: []byte(`{"status": "SUCCESS"}`),
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error completing workflow: %v", err)
	}
	if !sm.IsCompleted() {
		t.Fatalf("expected workflow to be completed")
	}
}

func TestWorkflowStateMachine_OutdatedSequenceError(t *testing.T) {
	sm := history.NewWorkflowStateMachine("wf-200", "run-002")

	err := sm.ApplyEvent(&v1.HistoryEvent{
		EventId:   5, // Should be 1
		EventType: v1.EventType_EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
	})
	if err == nil {
		t.Fatalf("expected sequence error for skipping event IDs")
	}
}

func TestWorkflowStateMachine_ReplayInterruptedWorkflow(t *testing.T) {
	events := []*v1.HistoryEvent{
		{
			EventId:     1,
			TimestampNs: 1000,
			EventType:   v1.EventType_EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			Epoch:       1,
			Attributes: &v1.HistoryEvent_WorkflowExecutionStartedAttributes{
				WorkflowExecutionStartedAttributes: &v1.WorkflowExecutionStartedAttributes{
					WorkflowId:   "wf-300",
					RunId:        "run-003",
					WorkflowType: "MultiStepWorkflow",
				},
			},
		},
		{
			EventId:     2,
			TimestampNs: 2000,
			EventType:   v1.EventType_EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
			Epoch:       1,
			Attributes: &v1.HistoryEvent_ActivityTaskScheduledAttributes{
				ActivityTaskScheduledAttributes: &v1.ActivityTaskScheduledAttributes{
					ActivityId:   "step-1",
					ActivityType: "Step1Activity",
				},
			},
		},
		{
			EventId:     3,
			TimestampNs: 3000,
			EventType:   v1.EventType_EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
			Epoch:       1,
			Attributes: &v1.HistoryEvent_ActivityTaskCompletedAttributes{
				ActivityTaskCompletedAttributes: &v1.ActivityTaskCompletedAttributes{
					ActivityId:    "step-1",
					ResultPayload: []byte("step-1-ok"),
				},
			},
		},
	}

	sm := history.NewWorkflowStateMachine("wf-300", "run-003")
	if err := sm.ApplyEvents(events); err != nil {
		t.Fatalf("failed applying event history: %v", err)
	}

	// Verify Step 1 is recorded as completed
	res, ok := sm.GetActivityCompletion("step-1")
	if !ok || !res.Completed {
		t.Fatalf("step 1 should be cached as completed")
	}

	// Now proceed to Step 2 smoothly
	err := sm.ApplyEvent(&v1.HistoryEvent{
		EventId:     4,
		TimestampNs: 4000,
		EventType:   v1.EventType_EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		Epoch:       1,
		Attributes: &v1.HistoryEvent_ActivityTaskScheduledAttributes{
			ActivityTaskScheduledAttributes: &v1.ActivityTaskScheduledAttributes{
				ActivityId:   "step-2",
				ActivityType: "Step2Activity",
			},
		},
	})
	if err != nil {
		t.Fatalf("step 2 scheduling failed: %v", err)
	}
	if len(sm.InflightActivities) != 1 {
		t.Fatalf("expected step 2 to be in flight")
	}
}

func TestWorkflowStateMachine_TimerLifecycle(t *testing.T) {
	sm := history.NewWorkflowStateMachine("wf-400", "run-004")

	_ = sm.ApplyEvent(&v1.HistoryEvent{
		EventId:   1,
		EventType: v1.EventType_EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &v1.HistoryEvent_WorkflowExecutionStartedAttributes{
			WorkflowExecutionStartedAttributes: &v1.WorkflowExecutionStartedAttributes{
				WorkflowId: "wf-400",
				RunId:      "run-004",
			},
		},
	})

	err := sm.ApplyEvent(&v1.HistoryEvent{
		EventId:   2,
		EventType: v1.EventType_EVENT_TYPE_TIMER_STARTED,
		Attributes: &v1.HistoryEvent_TimerStartedAttributes{
			TimerStartedAttributes: &v1.TimerStartedAttributes{
				TimerId:         "timer-1",
				DurationMs:      5000,
				FireTimestampMs: time.Now().Add(5 * time.Second).UnixMilli(),
			},
		},
	})
	if err != nil {
		t.Fatalf("failed starting timer: %v", err)
	}
	if len(sm.ActiveTimers) != 1 {
		t.Fatalf("expected 1 active timer")
	}

	err = sm.ApplyEvent(&v1.HistoryEvent{
		EventId:   3,
		EventType: v1.EventType_EVENT_TYPE_TIMER_FIRED,
		Attributes: &v1.HistoryEvent_TimerFiredAttributes{
			TimerFiredAttributes: &v1.TimerFiredAttributes{
				TimerId: "timer-1",
			},
		},
	})
	if err != nil {
		t.Fatalf("failed firing timer: %v", err)
	}
	if len(sm.ActiveTimers) != 0 {
		t.Fatalf("expected timer to be removed after firing")
	}
}
