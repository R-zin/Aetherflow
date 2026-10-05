package server

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	v1 "github.com/R-zin/Aetherflow/api/proto/v1"
	"github.com/R-zin/Aetherflow/pkg/cluster"
	"github.com/R-zin/Aetherflow/pkg/history"
	"github.com/R-zin/Aetherflow/pkg/matching"
	"github.com/R-zin/Aetherflow/pkg/sdk"
	"github.com/R-zin/Aetherflow/pkg/storage"
	"github.com/R-zin/Aetherflow/pkg/timer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type CoordinatorConfig struct {
	NodeID          string
	NumShards       uint32
	LeaseTTL        time.Duration
	TimerResolution time.Duration
}

type WorkflowServiceServer struct {
	v1.UnimplementedWorkflowServiceServer

	mu            sync.RWMutex
	nodeID        string
	config        CoordinatorConfig
	store         storage.EventStore
	ring          *cluster.HashRing
	leaseManager  *cluster.LeaseManager
	matching      *matching.MatchingEngine
	wheel         *timer.TimingWheel
	retryCalc     *history.RetryCalculator
	runner        *sdk.WorkflowRunner
	stateMachines map[string]*history.WorkflowStateMachine

	stopCh chan struct{}
}

func NewWorkflowServiceServer(
	cfg CoordinatorConfig,
	store storage.EventStore,
	runner *sdk.WorkflowRunner,
) *WorkflowServiceServer {
	if cfg.NumShards == 0 {
		cfg.NumShards = cluster.DefaultNumShards
	}
	if cfg.LeaseTTL == 0 {
		cfg.LeaseTTL = 5 * time.Second
	}
	if cfg.TimerResolution == 0 {
		cfg.TimerResolution = 10 * time.Millisecond
	}
	if runner == nil {
		runner = sdk.NewWorkflowRunner(sdk.DefaultRegistry)
	}

	ring := cluster.NewHashRing(cfg.NumShards, cluster.DefaultVirtualNodes)
	ring.AddNode(cfg.NodeID)

	srv := &WorkflowServiceServer{
		nodeID:        cfg.NodeID,
		config:        cfg,
		store:         store,
		ring:          ring,
		leaseManager:  cluster.NewLeaseManager(),
		matching:      matching.NewMatchingEngine(),
		wheel:         timer.NewTimingWheel(cfg.TimerResolution, 60, time.Now()),
		retryCalc:     history.NewRetryCalculator(),
		runner:        runner,
		stateMachines: make(map[string]*history.WorkflowStateMachine),
		stopCh:        make(chan struct{}),
	}

	// Start timer advancement ticker in background
	go srv.timerLoop()

	return srv
}

func (s *WorkflowServiceServer) Close() {
	select {
	case <-s.stopCh:
	default:
		close(s.stopCh)
	}
}

func (s *WorkflowServiceServer) timerLoop() {
	ticker := time.NewTicker(s.config.TimerResolution)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case now := <-ticker.C:
			fired := s.wheel.Advance(now)
			for _, task := range fired {
				s.handleTimerFired(task)
			}
		}
	}
}

func (s *WorkflowServiceServer) handleTimerFired(task *timer.TimerTask) {
	s.mu.Lock()
	sm, ok := s.stateMachines[task.WorkflowID]
	s.mu.Unlock()
	if !ok {
		return
	}

	event := &v1.HistoryEvent{
		EventId:     sm.NextEventID,
		TimestampNs: time.Now().UnixNano(),
		EventType:   v1.EventType_EVENT_TYPE_TIMER_FIRED,
		Epoch:       sm.CurrentEpoch,
		Attributes: &v1.HistoryEvent_TimerFiredAttributes{
			TimerFiredAttributes: &v1.TimerFiredAttributes{
				TimerId: task.ID,
			},
		},
	}

	shardID := s.ring.ShardForWorkflow(task.WorkflowID)
	_ = s.store.AppendEvents(context.Background(), shardID, task.WorkflowID, sm.CurrentEpoch, []*v1.HistoryEvent{event})
	_ = sm.ApplyEvent(event)

	// Resume workflow execution
	s.stepWorkflow(context.Background(), sm)
}

func (s *WorkflowServiceServer) ensureShardLease(shardID uint32) (int64, error) {
	lease, err := s.leaseManager.AcquireOrRenew(shardID, s.nodeID, s.config.LeaseTTL)
	if err != nil {
		return 0, status.Errorf(codes.Unavailable, "node does not hold lease for shard %d: %v", shardID, err)
	}
	return lease.Epoch, nil
}

func (s *WorkflowServiceServer) StartWorkflowExecution(ctx context.Context, req *v1.StartWorkflowRequest) (*v1.StartWorkflowResponse, error) {
	if req.WorkflowId == "" {
		req.WorkflowId = uuid.New().String()
	}
	runID := uuid.New().String()

	shardID := s.ring.ShardForWorkflow(req.WorkflowId)
	epoch, err := s.ensureShardLease(shardID)
	if err != nil {
		return nil, err
	}

	sm := history.NewWorkflowStateMachine(req.WorkflowId, runID)

	startEvent := &v1.HistoryEvent{
		EventId:     1,
		TimestampNs: time.Now().UnixNano(),
		EventType:   v1.EventType_EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Epoch:       epoch,
		Attributes: &v1.HistoryEvent_WorkflowExecutionStartedAttributes{
			WorkflowExecutionStartedAttributes: &v1.WorkflowExecutionStartedAttributes{
				WorkflowId:             req.WorkflowId,
				RunId:                  runID,
				WorkflowType:           req.WorkflowType,
				TaskQueue:              req.TaskQueue,
				InputPayload:           req.InputPayload,
				ExecutionTimeoutSeconds: req.ExecutionTimeoutSeconds,
				RetryPolicy:            req.RetryPolicy,
			},
		},
	}

	if err := s.store.AppendEvents(ctx, shardID, req.WorkflowId, epoch, []*v1.HistoryEvent{startEvent}); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to persist workflow start event: %v", err)
	}

	if err := sm.ApplyEvent(startEvent); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to apply workflow start event: %v", err)
	}

	s.mu.Lock()
	s.stateMachines[req.WorkflowId] = sm
	s.mu.Unlock()

	// Step workflow to schedule initial activities/timers
	s.stepWorkflow(ctx, sm)

	return &v1.StartWorkflowResponse{
		WorkflowId: req.WorkflowId,
		RunId:      runID,
	}, nil
}

func (s *WorkflowServiceServer) stepWorkflow(ctx context.Context, sm *history.WorkflowStateMachine) {
	historySnapshot := sm.HistorySnapshot()
	res, err := s.runner.ExecuteOrReplay(sm.WorkflowID, sm.RunID, sm.WorkflowType, sm.InputPayload, historySnapshot)
	if err != nil {
		// Non-deterministic error: fail workflow
		failEvent := &v1.HistoryEvent{
			EventId:     sm.NextEventID,
			TimestampNs: time.Now().UnixNano(),
			EventType:   v1.EventType_EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
			Epoch:       sm.CurrentEpoch,
			Attributes: &v1.HistoryEvent_WorkflowExecutionFailedAttributes{
				WorkflowExecutionFailedAttributes: &v1.WorkflowExecutionFailedAttributes{
					ErrorMessage: err.Error(),
				},
			},
		}
		shardID := s.ring.ShardForWorkflow(sm.WorkflowID)
		_ = s.store.AppendEvents(ctx, shardID, sm.WorkflowID, sm.CurrentEpoch, []*v1.HistoryEvent{failEvent})
		_ = sm.ApplyEvent(failEvent)
		return
	}

	for _, cmd := range res.Commands {
		switch cmd.Type {
		case sdk.CommandScheduleActivity:
			attrs := cmd.Payload.(*v1.ActivityTaskScheduledAttributes)
			event := &v1.HistoryEvent{
				EventId:     sm.NextEventID,
				TimestampNs: time.Now().UnixNano(),
				EventType:   v1.EventType_EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
				Epoch:       sm.CurrentEpoch,
				Attributes: &v1.HistoryEvent_ActivityTaskScheduledAttributes{
					ActivityTaskScheduledAttributes: attrs,
				},
			}
			shardID := s.ring.ShardForWorkflow(sm.WorkflowID)
			if err := s.store.AppendEvents(ctx, shardID, sm.WorkflowID, sm.CurrentEpoch, []*v1.HistoryEvent{event}); err == nil {
				_ = sm.ApplyEvent(event)
				s.matching.DispatchTask(&matching.ActivityTask{
					WorkflowID:         sm.WorkflowID,
					RunID:              sm.RunID,
					ActivityID:         attrs.ActivityId,
					ActivityType:       attrs.ActivityType,
					TaskQueue:          attrs.TaskQueue,
					InputPayload:       attrs.InputPayload,
					Attempt:            attrs.Attempt,
					StartToCloseSec:    attrs.StartToCloseTimeoutSeconds,
					ScheduleToCloseSec: attrs.ScheduleToCloseTimeoutSeconds,
					HeartbeatSec:       attrs.HeartbeatTimeoutSeconds,
				})
			}

		case sdk.CommandStartTimer:
			attrs := cmd.Payload.(*v1.TimerStartedAttributes)
			event := &v1.HistoryEvent{
				EventId:     sm.NextEventID,
				TimestampNs: time.Now().UnixNano(),
				EventType:   v1.EventType_EVENT_TYPE_TIMER_STARTED,
				Epoch:       sm.CurrentEpoch,
				Attributes: &v1.HistoryEvent_TimerStartedAttributes{
					TimerStartedAttributes: attrs,
				},
			}
			shardID := s.ring.ShardForWorkflow(sm.WorkflowID)
			if err := s.store.AppendEvents(ctx, shardID, sm.WorkflowID, sm.CurrentEpoch, []*v1.HistoryEvent{event}); err == nil {
				_ = sm.ApplyEvent(event)
				_ = s.wheel.AddTimer(&timer.TimerTask{
					ID:         attrs.TimerId,
					WorkflowID: sm.WorkflowID,
					Expiration: time.UnixMilli(attrs.FireTimestampMs),
				})
			}

		case sdk.CommandCompleteWorkflow:
			attrs := cmd.Payload.(*v1.WorkflowExecutionCompletedAttributes)
			event := &v1.HistoryEvent{
				EventId:     sm.NextEventID,
				TimestampNs: time.Now().UnixNano(),
				EventType:   v1.EventType_EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
				Epoch:       sm.CurrentEpoch,
				Attributes: &v1.HistoryEvent_WorkflowExecutionCompletedAttributes{
					WorkflowExecutionCompletedAttributes: attrs,
				},
			}
			shardID := s.ring.ShardForWorkflow(sm.WorkflowID)
			if err := s.store.AppendEvents(ctx, shardID, sm.WorkflowID, sm.CurrentEpoch, []*v1.HistoryEvent{event}); err == nil {
				_ = sm.ApplyEvent(event)
			}

		case sdk.CommandFailWorkflow:
			attrs := cmd.Payload.(*v1.WorkflowExecutionFailedAttributes)
			event := &v1.HistoryEvent{
				EventId:     sm.NextEventID,
				TimestampNs: time.Now().UnixNano(),
				EventType:   v1.EventType_EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
				Epoch:       sm.CurrentEpoch,
				Attributes: &v1.HistoryEvent_WorkflowExecutionFailedAttributes{
					WorkflowExecutionFailedAttributes: attrs,
				},
			}
			shardID := s.ring.ShardForWorkflow(sm.WorkflowID)
			if err := s.store.AppendEvents(ctx, shardID, sm.WorkflowID, sm.CurrentEpoch, []*v1.HistoryEvent{event}); err == nil {
				_ = sm.ApplyEvent(event)
			}
		}
	}
}

func (s *WorkflowServiceServer) GetWorkflowExecutionHistory(ctx context.Context, req *v1.GetHistoryRequest) (*v1.GetHistoryResponse, error) {
	events, err := s.store.GetHistory(ctx, req.WorkflowId, req.FromEventId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "failed to get workflow history: %v", err)
	}

	st := v1.WorkflowStatus_WORKFLOW_STATUS_RUNNING
	for _, e := range events {
		if e.EventType == v1.EventType_EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED {
			st = v1.WorkflowStatus_WORKFLOW_STATUS_COMPLETED
		} else if e.EventType == v1.EventType_EVENT_TYPE_WORKFLOW_EXECUTION_FAILED {
			st = v1.WorkflowStatus_WORKFLOW_STATUS_FAILED
		}
	}

	return &v1.GetHistoryResponse{
		History: &v1.History{Events: events},
		Status:  st,
	}, nil
}

func (s *WorkflowServiceServer) PollActivityTaskQueue(ctx context.Context, req *v1.PollActivityTaskQueueRequest) (*v1.PollActivityTaskQueueResponse, error) {
	timeout := time.Duration(req.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	resp, err := s.matching.PollTask(ctx, req.TaskQueue, req.WorkerIdentity, timeout)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error polling task queue: %v", err)
	}

	if resp.HasTask {
		// Append ActivityTaskStarted event to store and state machine
		s.mu.Lock()
		sm, ok := s.stateMachines[resp.WorkflowId]
		s.mu.Unlock()

		if ok {
			event := &v1.HistoryEvent{
				EventId:     sm.NextEventID,
				TimestampNs: time.Now().UnixNano(),
				EventType:   v1.EventType_EVENT_TYPE_ACTIVITY_TASK_STARTED,
				Epoch:       sm.CurrentEpoch,
				Attributes: &v1.HistoryEvent_ActivityTaskStartedAttributes{
					ActivityTaskStartedAttributes: &v1.ActivityTaskStartedAttributes{
						ActivityId:     resp.ActivityId,
						Attempt:        resp.Attempt,
						WorkerIdentity: req.WorkerIdentity,
						StartedTimeNs:  time.Now().UnixNano(),
					},
				},
			}
			shardID := s.ring.ShardForWorkflow(resp.WorkflowId)
			_ = s.store.AppendEvents(ctx, shardID, resp.WorkflowId, sm.CurrentEpoch, []*v1.HistoryEvent{event})
			_ = sm.ApplyEvent(event)
		}
	}

	return resp, nil
}

func (s *WorkflowServiceServer) RespondActivityTaskCompleted(ctx context.Context, req *v1.RespondActivityCompletedRequest) (*v1.RespondActivityCompletedResponse, error) {
	s.matching.CompleteTask(req.WorkflowId, req.ActivityId)

	s.mu.Lock()
	sm, ok := s.stateMachines[req.WorkflowId]
	s.mu.Unlock()

	if !ok {
		return nil, status.Errorf(codes.NotFound, "active state machine for workflow %s not found", req.WorkflowId)
	}

	event := &v1.HistoryEvent{
		EventId:     sm.NextEventID,
		TimestampNs: time.Now().UnixNano(),
		EventType:   v1.EventType_EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		Epoch:       sm.CurrentEpoch,
		Attributes: &v1.HistoryEvent_ActivityTaskCompletedAttributes{
			ActivityTaskCompletedAttributes: &v1.ActivityTaskCompletedAttributes{
				ActivityId:    req.ActivityId,
				ResultPayload: req.ResultPayload,
			},
		},
	}

	shardID := s.ring.ShardForWorkflow(req.WorkflowId)
	if err := s.store.AppendEvents(ctx, shardID, req.WorkflowId, sm.CurrentEpoch, []*v1.HistoryEvent{event}); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to append activity completed event: %v", err)
	}
	if err := sm.ApplyEvent(event); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to apply activity completed event: %v", err)
	}

	// Advance workflow to next step
	s.stepWorkflow(ctx, sm)

	return &v1.RespondActivityCompletedResponse{Acknowledged: true}, nil
}

func (s *WorkflowServiceServer) RespondActivityTaskFailed(ctx context.Context, req *v1.RespondActivityFailedRequest) (*v1.RespondActivityFailedResponse, error) {
	s.matching.CompleteTask(req.WorkflowId, req.ActivityId)

	s.mu.Lock()
	sm, ok := s.stateMachines[req.WorkflowId]
	s.mu.Unlock()

	if !ok {
		return nil, status.Errorf(codes.NotFound, "active state machine for workflow %s not found", req.WorkflowId)
	}

	event := &v1.HistoryEvent{
		EventId:     sm.NextEventID,
		TimestampNs: time.Now().UnixNano(),
		EventType:   v1.EventType_EVENT_TYPE_ACTIVITY_TASK_FAILED,
		Epoch:       sm.CurrentEpoch,
		Attributes: &v1.HistoryEvent_ActivityTaskFailedAttributes{
			ActivityTaskFailedAttributes: &v1.ActivityTaskFailedAttributes{
				ActivityId:   req.ActivityId,
				ErrorMessage: req.ErrorMessage,
				NonRetryable: req.NonRetryable,
			},
		},
	}

	shardID := s.ring.ShardForWorkflow(req.WorkflowId)
	_ = s.store.AppendEvents(ctx, shardID, req.WorkflowId, sm.CurrentEpoch, []*v1.HistoryEvent{event})
	_ = sm.ApplyEvent(event)

	// Step workflow to handle failure / trigger saga
	s.stepWorkflow(ctx, sm)

	return &v1.RespondActivityFailedResponse{
		Acknowledged: true,
		WillRetry:    false,
	}, nil
}

func (s *WorkflowServiceServer) RecordActivityHeartbeat(ctx context.Context, req *v1.RecordHeartbeatRequest) (*v1.RecordHeartbeatResponse, error) {
	s.matching.RecordHeartbeat(req.WorkflowId, req.ActivityId)
	return &v1.RecordHeartbeatResponse{CancelRequested: false}, nil
}

func (s *WorkflowServiceServer) GetClusterStatus(ctx context.Context, req *v1.GetClusterStatusRequest) (*v1.GetClusterStatusResponse, error) {
	activeNodes := s.ring.GetActiveNodes()
	leases := s.leaseManager.GetAllLeases()

	assignments := make([]*v1.ShardAssignment, 0, len(leases))
	for _, l := range leases {
		assignments = append(assignments, &v1.ShardAssignment{
			ShardId:     l.ShardID,
			OwnerNodeId: l.OwnerNode,
			Epoch:       l.Epoch,
		})
	}

	return &v1.GetClusterStatusResponse{
		ActiveNodes:      activeNodes,
		ShardAssignments: assignments,
	}, nil
}
