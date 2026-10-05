package history

import (
	"errors"
	"fmt"
	"sync"
	"time"

	v1 "github.com/R-zin/Aetherflow/api/proto/v1"
)

var (
	ErrWorkflowAlreadyStarted   = errors.New("workflow execution already started")
	ErrWorkflowNotRunning       = errors.New("workflow execution is not in running state")
	ErrActivityAlreadyScheduled = errors.New("activity is already scheduled or completed")
	ErrActivityNotFound         = errors.New("activity task not found in inflight registry")
	ErrTimerAlreadyStarted      = errors.New("timer is already started")
	ErrTimerNotFound            = errors.New("timer not found in active registry")
	ErrInvalidEventSequence     = errors.New("event ID sequence is out of order")
	ErrNonDeterministicWorkflow = errors.New("non-deterministic workflow execution detected")
)

type ActivityState struct {
	ActivityID         string
	ActivityType       string
	TaskQueue          string
	InputPayload       []byte
	Attempt            int32
	WorkerIdentity     string
	ScheduledTime      time.Time
	StartedTime        time.Time
	ScheduleToCloseSec int64
	StartToCloseSec    int64
	HeartbeatSec       int64
	RetryPolicy        *v1.RetryPolicy
}

type ActivityResult struct {
	ActivityID string
	Result     []byte
	Completed  bool
	Failed     bool
	Error      string
}

type TimerState struct {
	TimerID    string
	Duration   time.Duration
	FireTime   time.Time
	StartedAt  time.Time
}

type CompensationState struct {
	CompensationID   string
	ActivityID       string
	CompensationName string
	InputPayload     []byte
	Status           string // "RECORDED", "STARTED", "COMPLETED", "FAILED"
	ResultPayload    []byte
	ErrorMessage     string
}

type WorkflowStateMachine struct {
	mu sync.RWMutex

	WorkflowID          string
	RunID               string
	WorkflowType        string
	TaskQueue           string
	Status              v1.WorkflowStatus
	NextEventID         int64
	CurrentEpoch        int64
	ExecutionTimeoutSec int64
	RetryPolicy         *v1.RetryPolicy
	InputPayload        []byte
	ResultPayload       []byte
	ErrorMessage        string
	StartTime           time.Time
	EndTime             time.Time

	InflightActivities  map[string]*ActivityState
	CompletedActivities map[string]*ActivityResult
	ActiveTimers        map[string]*TimerState
	Compensations       []*CompensationState

	EventHistory        []*v1.HistoryEvent
}

func NewWorkflowStateMachine(workflowID, runID string) *WorkflowStateMachine {
	return &WorkflowStateMachine{
		WorkflowID:          workflowID,
		RunID:               runID,
		Status:              v1.WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED,
		NextEventID:         1,
		InflightActivities:  make(map[string]*ActivityState),
		CompletedActivities: make(map[string]*ActivityResult),
		ActiveTimers:        make(map[string]*TimerState),
		Compensations:       make([]*CompensationState, 0),
		EventHistory:        make([]*v1.HistoryEvent, 0),
	}
}

func (sm *WorkflowStateMachine) ApplyEvents(events []*v1.HistoryEvent) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for _, event := range events {
		if err := sm.applyEventLocked(event); err != nil {
			return fmt.Errorf("failed to apply event ID %d (%s): %w", event.EventId, event.EventType, err)
		}
	}
	return nil
}

func (sm *WorkflowStateMachine) ApplyEvent(event *v1.HistoryEvent) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.applyEventLocked(event)
}

func (sm *WorkflowStateMachine) applyEventLocked(event *v1.HistoryEvent) error {
	if event.EventId != sm.NextEventID {
		return fmt.Errorf("%w: expected event %d, got %d", ErrInvalidEventSequence, sm.NextEventID, event.EventId)
	}

	sm.NextEventID++
	sm.EventHistory = append(sm.EventHistory, event)
	if event.Epoch > sm.CurrentEpoch {
		sm.CurrentEpoch = event.Epoch
	}

	eventTime := time.Unix(0, event.TimestampNs)

	switch event.EventType {
	case v1.EventType_EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
		if sm.Status != v1.WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED {
			return ErrWorkflowAlreadyStarted
		}
		attrs := event.GetWorkflowExecutionStartedAttributes()
		if attrs == nil {
			return errors.New("missing workflow execution started attributes")
		}
		sm.Status = v1.WorkflowStatus_WORKFLOW_STATUS_RUNNING
		sm.WorkflowType = attrs.WorkflowType
		sm.TaskQueue = attrs.TaskQueue
		sm.InputPayload = attrs.InputPayload
		sm.ExecutionTimeoutSec = attrs.ExecutionTimeoutSeconds
		sm.RetryPolicy = attrs.RetryPolicy
		sm.StartTime = eventTime

	case v1.EventType_EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		if sm.Status != v1.WorkflowStatus_WORKFLOW_STATUS_RUNNING {
			return ErrWorkflowNotRunning
		}
		attrs := event.GetWorkflowExecutionCompletedAttributes()
		if attrs != nil {
			sm.ResultPayload = attrs.ResultPayload
		}
		sm.Status = v1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED
		sm.EndTime = eventTime

	case v1.EventType_EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		if sm.Status != v1.WorkflowStatus_WORKFLOW_STATUS_RUNNING {
			return ErrWorkflowNotRunning
		}
		attrs := event.GetWorkflowExecutionFailedAttributes()
		if attrs != nil {
			sm.ErrorMessage = attrs.ErrorMessage
		}
		sm.Status = v1.WorkflowStatus_WORKFLOW_STATUS_FAILED
		sm.EndTime = eventTime

	case v1.EventType_EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		sm.Status = v1.WorkflowStatus_WORKFLOW_STATUS_TIMED_OUT
		sm.EndTime = eventTime

	case v1.EventType_EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
		if sm.Status != v1.WorkflowStatus_WORKFLOW_STATUS_RUNNING {
			return ErrWorkflowNotRunning
		}
		attrs := event.GetActivityTaskScheduledAttributes()
		if attrs == nil {
			return errors.New("missing activity task scheduled attributes")
		}
		if _, exists := sm.InflightActivities[attrs.ActivityId]; exists {
			return fmt.Errorf("%w: activity %s already inflight", ErrActivityAlreadyScheduled, attrs.ActivityId)
		}
		if _, exists := sm.CompletedActivities[attrs.ActivityId]; exists {
			return fmt.Errorf("%w: activity %s already completed", ErrActivityAlreadyScheduled, attrs.ActivityId)
		}
		sm.InflightActivities[attrs.ActivityId] = &ActivityState{
			ActivityID:         attrs.ActivityId,
			ActivityType:       attrs.ActivityType,
			TaskQueue:          attrs.TaskQueue,
			InputPayload:       attrs.InputPayload,
			ScheduleToCloseSec: attrs.ScheduleToCloseTimeoutSeconds,
			StartToCloseSec:    attrs.StartToCloseTimeoutSeconds,
			HeartbeatSec:       attrs.HeartbeatTimeoutSeconds,
			RetryPolicy:        attrs.RetryPolicy,
			Attempt:            attrs.Attempt,
			ScheduledTime:      eventTime,
		}

	case v1.EventType_EVENT_TYPE_ACTIVITY_TASK_STARTED:
		attrs := event.GetActivityTaskStartedAttributes()
		if attrs == nil {
			return errors.New("missing activity task started attributes")
		}
		act, exists := sm.InflightActivities[attrs.ActivityId]
		if !exists {
			return fmt.Errorf("%w: activity %s", ErrActivityNotFound, attrs.ActivityId)
		}
		act.Attempt = attrs.Attempt
		act.WorkerIdentity = attrs.WorkerIdentity
		act.StartedTime = eventTime

	case v1.EventType_EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
		attrs := event.GetActivityTaskCompletedAttributes()
		if attrs == nil {
			return errors.New("missing activity task completed attributes")
		}
		delete(sm.InflightActivities, attrs.ActivityId)
		sm.CompletedActivities[attrs.ActivityId] = &ActivityResult{
			ActivityID: attrs.ActivityId,
			Result:     attrs.ResultPayload,
			Completed:  true,
		}

	case v1.EventType_EVENT_TYPE_ACTIVITY_TASK_FAILED:
		attrs := event.GetActivityTaskFailedAttributes()
		if attrs == nil {
			return errors.New("missing activity task failed attributes")
		}
		delete(sm.InflightActivities, attrs.ActivityId)
		sm.CompletedActivities[attrs.ActivityId] = &ActivityResult{
			ActivityID: attrs.ActivityId,
			Failed:     true,
			Error:      attrs.ErrorMessage,
		}

	case v1.EventType_EVENT_TYPE_TIMER_STARTED:
		if sm.Status != v1.WorkflowStatus_WORKFLOW_STATUS_RUNNING {
			return ErrWorkflowNotRunning
		}
		attrs := event.GetTimerStartedAttributes()
		if attrs == nil {
			return errors.New("missing timer started attributes")
		}
		if _, exists := sm.ActiveTimers[attrs.TimerId]; exists {
			return fmt.Errorf("%w: timer %s", ErrTimerAlreadyStarted, attrs.TimerId)
		}
		sm.ActiveTimers[attrs.TimerId] = &TimerState{
			TimerID:   attrs.TimerId,
			Duration:  time.Duration(attrs.DurationMs) * time.Millisecond,
			FireTime:  time.UnixMilli(attrs.FireTimestampMs),
			StartedAt: eventTime,
		}

	case v1.EventType_EVENT_TYPE_TIMER_FIRED:
		attrs := event.GetTimerFiredAttributes()
		if attrs == nil {
			return errors.New("missing timer fired attributes")
		}
		delete(sm.ActiveTimers, attrs.TimerId)

	case v1.EventType_EVENT_TYPE_TIMER_CANCELLED:
		attrs := event.GetTimerCancelledAttributes()
		if attrs == nil {
			return errors.New("missing timer cancelled attributes")
		}
		delete(sm.ActiveTimers, attrs.TimerId)

	case v1.EventType_EVENT_TYPE_COMPENSATION_RECORDED:
		attrs := event.GetCompensationRecordedAttributes()
		if attrs == nil {
			return errors.New("missing compensation recorded attributes")
		}
		sm.Compensations = append(sm.Compensations, &CompensationState{
			CompensationID:   attrs.CompensationId,
			ActivityID:       attrs.ActivityId,
			CompensationName: attrs.CompensationName,
			InputPayload:     attrs.InputPayload,
			Status:           "RECORDED",
		})

	case v1.EventType_EVENT_TYPE_COMPENSATION_STARTED:
		attrs := event.GetCompensationStartedAttributes()
		if attrs != nil {
			for _, comp := range sm.Compensations {
				if comp.CompensationID == attrs.CompensationId {
					comp.Status = "STARTED"
					break
				}
			}
		}

	case v1.EventType_EVENT_TYPE_COMPENSATION_COMPLETED:
		attrs := event.GetCompensationCompletedAttributes()
		if attrs != nil {
			for _, comp := range sm.Compensations {
				if comp.CompensationID == attrs.CompensationId {
					if attrs.ErrorMessage != "" {
						comp.Status = "FAILED"
						comp.ErrorMessage = attrs.ErrorMessage
					} else {
						comp.Status = "COMPLETED"
						comp.ResultPayload = attrs.ResultPayload
					}
					break
				}
			}
		}

	default:
		// Other informational events (e.g. WorkflowTaskScheduled, WorkflowTaskStarted) are recorded in history
	}

	return nil
}

func (sm *WorkflowStateMachine) GetActivityCompletion(activityID string) (*ActivityResult, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	res, ok := sm.CompletedActivities[activityID]
	return res, ok
}

func (sm *WorkflowStateMachine) IsCompleted() bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.Status == v1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED ||
		sm.Status == v1.WorkflowStatus_WORKFLOW_STATUS_FAILED ||
		sm.Status == v1.WorkflowStatus_WORKFLOW_STATUS_TIMED_OUT ||
		sm.Status == v1.WorkflowStatus_WORKFLOW_STATUS_TERMINATED
}

func (sm *WorkflowStateMachine) HistorySnapshot() []*v1.HistoryEvent {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	copied := make([]*v1.HistoryEvent, len(sm.EventHistory))
	copy(copied, sm.EventHistory)
	return copied
}
