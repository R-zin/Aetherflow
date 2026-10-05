package timer

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrTimerAlreadyExists = errors.New("timer with this ID already exists")
	ErrTimerExpired       = errors.New("timer expiration is in the past")
)

type TimerTask struct {
	ID         string
	WorkflowID string
	Expiration time.Time
	Payload    []byte
	Cancelled  bool
}

type bucket struct {
	tasks []*TimerTask
}

type wheelLevel struct {
	tickDuration time.Duration
	wheelSize    int
	interval     time.Duration
	currentTime  time.Time
	buckets      []*bucket
	overflow     *wheelLevel
}

func newWheelLevel(tickDuration time.Duration, wheelSize int, startTime time.Time) *wheelLevel {
	roundedStart := startTime.Truncate(tickDuration)
	buckets := make([]*bucket, wheelSize)
	for i := range buckets {
		buckets[i] = &bucket{tasks: make([]*TimerTask, 0)}
	}

	return &wheelLevel{
		tickDuration: tickDuration,
		wheelSize:    wheelSize,
		interval:     tickDuration * time.Duration(wheelSize),
		currentTime:  roundedStart,
		buckets:      buckets,
	}
}

func (w *wheelLevel) add(task *TimerTask) {
	if task.Cancelled {
		return
	}

	if task.Expiration.Before(w.currentTime.Add(w.tickDuration)) {
		// Place into current bucket so it fires on next advance
		idx := int((task.Expiration.UnixNano() / int64(w.tickDuration)) % int64(w.wheelSize))
		if idx < 0 {
			idx = 0
		}
		w.buckets[idx].tasks = append(w.buckets[idx].tasks, task)
		return
	}

	if task.Expiration.Before(w.currentTime.Add(w.interval)) {
		virtualID := task.Expiration.Sub(w.currentTime) / w.tickDuration
		idx := int((w.currentTime.Add(virtualID * w.tickDuration).UnixNano() / int64(w.tickDuration)) % int64(w.wheelSize))
		if idx < 0 {
			idx += w.wheelSize
		}
		w.buckets[idx].tasks = append(w.buckets[idx].tasks, task)
		return
	}

	// Fits into higher tier
	if w.overflow == nil {
		w.overflow = newWheelLevel(w.interval, w.wheelSize, w.currentTime)
	}
	w.overflow.add(task)
}

func (w *wheelLevel) advance(now time.Time, reinsertFn func(*TimerTask), fired *[]*TimerTask) {
	for !now.Before(w.currentTime.Add(w.tickDuration)) {
		w.currentTime = w.currentTime.Add(w.tickDuration)
		idx := int((w.currentTime.UnixNano() / int64(w.tickDuration)) % int64(w.wheelSize))
		if idx < 0 {
			idx += w.wheelSize
		}

		b := w.buckets[idx]
		if len(b.tasks) > 0 {
			tasks := b.tasks
			b.tasks = make([]*TimerTask, 0)

			for _, t := range tasks {
				if t.Cancelled {
					continue
				}
				if !t.Expiration.After(now) {
					*fired = append(*fired, t)
				} else {
					reinsertFn(t)
				}
			}
		}
	}

	if w.overflow != nil {
		w.overflow.advance(now, reinsertFn, fired)
	}
}

type TimingWheel struct {
	mu        sync.Mutex
	root      *wheelLevel
	startTime time.Time
	tasksByID map[string]*TimerTask
	overflow  []*TimerTask // Overflow queue for durations > wheel range
}

// NewTimingWheel creates a hierarchical timing wheel with base resolution (e.g. 10ms or 1s)
func NewTimingWheel(tickDuration time.Duration, wheelSize int, startTime time.Time) *TimingWheel {
	return &TimingWheel{
		root:      newWheelLevel(tickDuration, wheelSize, startTime),
		startTime: startTime,
		tasksByID: make(map[string]*TimerTask),
		overflow:  make([]*TimerTask, 0),
	}
}

func (tw *TimingWheel) AddTimer(task *TimerTask) error {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	if _, exists := tw.tasksByID[task.ID]; exists {
		return ErrTimerAlreadyExists
	}

	tw.tasksByID[task.ID] = task
	tw.root.add(task)
	return nil
}

func (tw *TimingWheel) CancelTimer(id string) bool {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	task, exists := tw.tasksByID[id]
	if !exists {
		return false
	}

	task.Cancelled = true
	delete(tw.tasksByID, id)
	return true
}

func (tw *TimingWheel) Advance(now time.Time) []*TimerTask {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	var fired []*TimerTask

	reinsert := func(t *TimerTask) {
		tw.root.add(t)
	}

	tw.root.advance(now, reinsert, &fired)

	// Clean up fired tasks from index
	for _, t := range fired {
		delete(tw.tasksByID, t.ID)
	}

	return fired
}

func (tw *TimingWheel) ActiveTimersCount() int {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	return len(tw.tasksByID)
}
