package timer_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/R-zin/Aetherflow/pkg/timer"
)

func TestTimingWheel_BasicAndCascading(t *testing.T) {
	start := time.Now()
	// Tick = 10ms, wheelSize = 10 (Tier 0 span = 100ms, Tier 1 span = 1s, Tier 2 span = 10s)
	tw := timer.NewTimingWheel(10*time.Millisecond, 10, start)

	t1 := &timer.TimerTask{
		ID:         "t1",
		WorkflowID: "wf-1",
		Expiration: start.Add(35 * time.Millisecond),
	}
	t2 := &timer.TimerTask{
		ID:         "t2",
		WorkflowID: "wf-1",
		Expiration: start.Add(250 * time.Millisecond), // Cascades from tier 1
	}
	t3 := &timer.TimerTask{
		ID:         "t3",
		WorkflowID: "wf-1",
		Expiration: start.Add(1200 * time.Millisecond), // Cascades from tier 2
	}

	if err := tw.AddTimer(t1); err != nil {
		t.Fatalf("failed to add t1: %v", err)
	}
	if err := tw.AddTimer(t2); err != nil {
		t.Fatalf("failed to add t2: %v", err)
	}
	if err := tw.AddTimer(t3); err != nil {
		t.Fatalf("failed to add t3: %v", err)
	}

	if tw.ActiveTimersCount() != 3 {
		t.Fatalf("expected 3 active timers, got %d", tw.ActiveTimersCount())
	}

	// Advance 50ms: t1 should fire
	fired := tw.Advance(start.Add(50 * time.Millisecond))
	if len(fired) != 1 || fired[0].ID != "t1" {
		t.Fatalf("expected t1 to fire, got %+v", fired)
	}

	// Advance 300ms: t2 should fire
	fired = tw.Advance(start.Add(300 * time.Millisecond))
	if len(fired) != 1 || fired[0].ID != "t2" {
		t.Fatalf("expected t2 to fire, got %+v", fired)
	}

	// Advance 1300ms: t3 should fire
	fired = tw.Advance(start.Add(1300 * time.Millisecond))
	if len(fired) != 1 || fired[0].ID != "t3" {
		t.Fatalf("expected t3 to fire, got %+v", fired)
	}

	if tw.ActiveTimersCount() != 0 {
		t.Fatalf("expected 0 active timers, got %d", tw.ActiveTimersCount())
	}
}

func TestTimingWheel_Cancellation(t *testing.T) {
	start := time.Now()
	tw := timer.NewTimingWheel(10*time.Millisecond, 10, start)

	t1 := &timer.TimerTask{
		ID:         "cancel-me",
		WorkflowID: "wf-2",
		Expiration: start.Add(50 * time.Millisecond),
	}
	_ = tw.AddTimer(t1)

	cancelled := tw.CancelTimer("cancel-me")
	if !cancelled {
		t.Fatalf("expected CancelTimer to return true")
	}

	// Advancing should not fire the cancelled timer
	fired := tw.Advance(start.Add(100 * time.Millisecond))
	if len(fired) != 0 {
		t.Fatalf("cancelled timer fired: %+v", fired)
	}
}

func BenchmarkTimingWheel_100kTimers(b *testing.B) {
	start := time.Now()
	tw := timer.NewTimingWheel(1*time.Millisecond, 100, start)

	totalTimers := 100000
	b.ResetTimer()

	// Insert 100,000 timers
	for i := 0; i < totalTimers; i++ {
		_ = tw.AddTimer(&timer.TimerTask{
			ID:         fmt.Sprintf("bench-%d", i),
			Expiration: start.Add(time.Duration(i%500) * time.Millisecond),
		})
	}

	// Advance time across the range
	firedCount := 0
	for step := 1; step <= 500; step += 10 {
		fired := tw.Advance(start.Add(time.Duration(step) * time.Millisecond))
		firedCount += len(fired)
	}

	if firedCount == 0 {
		b.Fatalf("expected timers to fire")
	}
}
