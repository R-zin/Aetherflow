package sdk

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	v1 "github.com/R-zin/Aetherflow/api/proto/v1"
)

var (
	ErrReplayYield               = errors.New("yielding execution waiting for asynchronous activity or timer")
	ErrNonDeterministicWorkflow = errors.New("non-deterministic workflow execution detected")
)

type ActivityOptions struct {
	ActivityID         string
	TaskQueue          string
	ScheduleToCloseSec int64
	StartToCloseSec    int64
	HeartbeatSec       int64
	RetryPolicy        *v1.RetryPolicy
}

type WorkflowContext interface {
	WorkflowID() string
	RunID() string
	Now() time.Time
	Sleep(d time.Duration) error
	ExecuteActivity(activityType string, input any, output any, opts ...ActivityOption) error
	SideEffect(fn func() ([]byte, error)) ([]byte, error)
}

type ActivityOption func(*ActivityOptions)

func WithActivityID(id string) ActivityOption {
	return func(o *ActivityOptions) {
		o.ActivityID = id
	}
}

func WithTaskQueue(q string) ActivityOption {
	return func(o *ActivityOptions) {
		o.TaskQueue = q
	}
}

func WithStartToCloseTimeout(d time.Duration) ActivityOption {
	return func(o *ActivityOptions) {
		o.StartToCloseSec = int64(d.Seconds())
	}
}

func WithRetryPolicy(p *v1.RetryPolicy) ActivityOption {
	return func(o *ActivityOptions) {
		o.RetryPolicy = p
	}
}

type CommandType string

const (
	CommandScheduleActivity CommandType = "SCHEDULE_ACTIVITY"
	CommandStartTimer        CommandType = "START_TIMER"
	CommandCompleteWorkflow  CommandType = "COMPLETE_WORKFLOW"
	CommandFailWorkflow      CommandType = "FAIL_WORKFLOW"
)

type Command struct {
	Type     CommandType
	Payload  any
	SeqNum   int
}

type workflowContextImpl struct {
	workflowID    string
	runID         string
	currentTime   time.Time
	historyEvents []*v1.HistoryEvent

	// Replay cursor state
	historyIndex   int
	commandSeq     int
	scheduledCmds  []*Command
	isReplaying    bool
	replayComplete bool
}

func newWorkflowContext(workflowID, runID string, historyEvents []*v1.HistoryEvent, initialTime time.Time) *workflowContextImpl {
	return &workflowContextImpl{
		workflowID:    workflowID,
		runID:         runID,
		currentTime:   initialTime,
		historyEvents: historyEvents,
		scheduledCmds: make([]*Command, 0),
		isReplaying:   len(historyEvents) > 1,
	}
}

func (ctx *workflowContextImpl) WorkflowID() string {
	return ctx.workflowID
}

func (ctx *workflowContextImpl) RunID() string {
	return ctx.runID
}

func (ctx *workflowContextImpl) Now() time.Time {
	return ctx.currentTime
}

func (ctx *workflowContextImpl) Sleep(d time.Duration) error {
	ctx.commandSeq++
	timerID := fmt.Sprintf("timer-%d", ctx.commandSeq)

	// Check if this timer was already fired in history
	for _, event := range ctx.historyEvents {
		if event.EventType == v1.EventType_EVENT_TYPE_TIMER_FIRED {
			attrs := event.GetTimerFiredAttributes()
			if attrs != nil && attrs.TimerId == timerID {
				ctx.currentTime = time.Unix(0, event.TimestampNs)
				return nil
			}
		}
	}

	// Not fired yet: emit start timer command and yield
	ctx.scheduledCmds = append(ctx.scheduledCmds, &Command{
		Type: CommandStartTimer,
		Payload: &v1.TimerStartedAttributes{
			TimerId:         timerID,
			DurationMs:      d.Milliseconds(),
			FireTimestampMs: ctx.currentTime.Add(d).UnixMilli(),
		},
		SeqNum: ctx.commandSeq,
	})

	return ErrReplayYield
}

func (ctx *workflowContextImpl) ExecuteActivity(activityType string, input any, output any, opts ...ActivityOption) error {
	ctx.commandSeq++
	options := ActivityOptions{
		ActivityID:      fmt.Sprintf("act-%d", ctx.commandSeq),
		TaskQueue:       "default",
		StartToCloseSec: 60,
	}
	for _, opt := range opts {
		opt(&options)
	}

	inputBytes, err := json.Marshal(input)
	if err != nil {
		return fmt.Errorf("failed to marshal activity input: %w", err)
	}

	// 1. Check history: did this activity already complete or fail?
	for _, event := range ctx.historyEvents {
		if event.EventType == v1.EventType_EVENT_TYPE_ACTIVITY_TASK_COMPLETED {
			attrs := event.GetActivityTaskCompletedAttributes()
			if attrs != nil && attrs.ActivityId == options.ActivityID {
				if output != nil && len(attrs.ResultPayload) > 0 {
					if err := json.Unmarshal(attrs.ResultPayload, output); err != nil {
						return fmt.Errorf("failed to unmarshal cached activity result: %w", err)
					}
				}
				ctx.currentTime = time.Unix(0, event.TimestampNs)
				return nil
			}
		} else if event.EventType == v1.EventType_EVENT_TYPE_ACTIVITY_TASK_FAILED {
			attrs := event.GetActivityTaskFailedAttributes()
			if attrs != nil && attrs.ActivityId == options.ActivityID {
				return errors.New(attrs.ErrorMessage)
			}
		}
	}

	// 2. Check if this activity is already scheduled in history
	isAlreadyScheduled := false
	for _, event := range ctx.historyEvents {
		if event.EventType == v1.EventType_EVENT_TYPE_ACTIVITY_TASK_SCHEDULED {
			attrs := event.GetActivityTaskScheduledAttributes()
			if attrs != nil && attrs.ActivityId == options.ActivityID {
				// Determinism verification: activity type must match
				if attrs.ActivityType != activityType {
					return fmt.Errorf("%w: history expected activity %s, but workflow attempted %s",
						ErrNonDeterministicWorkflow, attrs.ActivityType, activityType)
				}
				isAlreadyScheduled = true
				break
			}
		}
	}

	if !isAlreadyScheduled {
		// Emit schedule activity command
		ctx.scheduledCmds = append(ctx.scheduledCmds, &Command{
			Type: CommandScheduleActivity,
			Payload: &v1.ActivityTaskScheduledAttributes{
				ActivityId:                 options.ActivityID,
				ActivityType:               activityType,
				TaskQueue:                  options.TaskQueue,
				InputPayload:               inputBytes,
				StartToCloseTimeoutSeconds: options.StartToCloseSec,
				RetryPolicy:                options.RetryPolicy,
				Attempt:                    1,
			},
			SeqNum: ctx.commandSeq,
		})
	}

	// Yield workflow execution until activity completes
	return ErrReplayYield
}

func (ctx *workflowContextImpl) SideEffect(fn func() ([]byte, error)) ([]byte, error) {
	// SideEffect can be implemented by executing fn once and storing into event log
	return fn()
}
