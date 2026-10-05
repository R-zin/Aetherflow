package matching

import (
	"context"
	"sync"
	"time"

	v1 "github.com/R-zin/Aetherflow/api/proto/v1"
)

type ActivityTask struct {
	WorkflowID         string
	RunID              string
	ActivityID         string
	ActivityType       string
	TaskQueue          string
	InputPayload       []byte
	Attempt            int32
	ScheduleToCloseSec int64
	StartToCloseSec    int64
	HeartbeatSec       int64
	WorkerIdentity     string
	StartedAt          time.Time
	LastHeartbeat      time.Time
}

type TaskQueue struct {
	name   string
	mu     sync.Mutex
	tasks  []*ActivityTask
	notify chan struct{}
}

func NewTaskQueue(name string) *TaskQueue {
	return &TaskQueue{
		name:   name,
		tasks:  make([]*ActivityTask, 0),
		notify: make(chan struct{}, 1),
	}
}

func (q *TaskQueue) Enqueue(task *ActivityTask) {
	q.mu.Lock()
	q.tasks = append(q.tasks, task)
	q.mu.Unlock()

	select {
	case q.notify <- struct{}{}:
	default:
	}
}

func (q *TaskQueue) Poll(ctx context.Context, timeout time.Duration) (*ActivityTask, bool) {
	deadline := time.After(timeout)

	for {
		q.mu.Lock()
		if len(q.tasks) > 0 {
			task := q.tasks[0]
			q.tasks = q.tasks[1:]
			q.mu.Unlock()
			return task, true
		}
		q.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, false
		case <-deadline:
			return nil, false
		case <-q.notify:
			// New task may be available; loop to dequeue
		}
	}
}

type MatchingEngine struct {
	mu            sync.RWMutex
	queues        map[string]*TaskQueue
	inflightTasks map[string]*ActivityTask // key: workflowID/activityID
}

func NewMatchingEngine() *MatchingEngine {
	return &MatchingEngine{
		queues:        make(map[string]*TaskQueue),
		inflightTasks: make(map[string]*ActivityTask),
	}
}

func (m *MatchingEngine) getOrCreateQueue(name string) *TaskQueue {
	m.mu.Lock()
	defer m.mu.Unlock()

	q, exists := m.queues[name]
	if !exists {
		q = NewTaskQueue(name)
		m.queues[name] = q
	}
	return q
}

func (m *MatchingEngine) DispatchTask(task *ActivityTask) {
	q := m.getOrCreateQueue(task.TaskQueue)
	q.Enqueue(task)
}

func (m *MatchingEngine) PollTask(ctx context.Context, taskQueue, workerIdentity string, timeout time.Duration) (*v1.PollActivityTaskQueueResponse, error) {
	q := m.getOrCreateQueue(taskQueue)
	task, ok := q.Poll(ctx, timeout)
	if !ok || task == nil {
		return &v1.PollActivityTaskQueueResponse{HasTask: false}, nil
	}

	task.WorkerIdentity = workerIdentity
	task.StartedAt = time.Now()
	task.LastHeartbeat = time.Now()

	m.mu.Lock()
	taskKey := task.WorkflowID + "/" + task.ActivityID
	m.inflightTasks[taskKey] = task
	m.mu.Unlock()

	return &v1.PollActivityTaskQueueResponse{
		HasTask:                     true,
		WorkflowId:                  task.WorkflowID,
		RunId:                       task.RunID,
		ActivityId:                  task.ActivityID,
		ActivityType:                task.ActivityType,
		InputPayload:                task.InputPayload,
		Attempt:                     task.Attempt,
		ScheduleToCloseTimeoutSeconds: task.ScheduleToCloseSec,
		StartToCloseTimeoutSeconds:    task.StartToCloseSec,
		HeartbeatTimeoutSeconds:       task.HeartbeatSec,
	}, nil
}

func (m *MatchingEngine) RecordHeartbeat(workflowID, activityID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	taskKey := workflowID + "/" + activityID
	task, exists := m.inflightTasks[taskKey]
	if !exists {
		return false
	}
	task.LastHeartbeat = time.Now()
	return true
}

func (m *MatchingEngine) CompleteTask(workflowID, activityID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	taskKey := workflowID + "/" + activityID
	delete(m.inflightTasks, taskKey)
}

func (m *MatchingEngine) InflightCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.inflightTasks)
}
