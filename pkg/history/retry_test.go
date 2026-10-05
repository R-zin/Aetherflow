package history_test

import (
	"testing"
	"time"

	v1 "github.com/R-zin/Aetherflow/api/proto/v1"
	"github.com/R-zin/Aetherflow/pkg/history"
)

func TestRetryCalculator(t *testing.T) {
	rc := history.NewRetryCalculator()

	policy := &v1.RetryPolicy{
		InitialIntervalSeconds: 1,
		BackoffCoefficient:     2.0,
		MaxIntervalSeconds:     10,
		MaxAttempts:            4,
		NonRetryableErrorTypes: []string{"FatalError", "ValidationFailed"},
	}

	// 1. First attempt backoff
	d1, ok := rc.CalculateBackoff(policy, 1, "network timeout")
	if !ok {
		t.Fatalf("expected retryable for attempt 1")
	}
	if d1 < 500*time.Millisecond || d1 > 1*time.Second {
		t.Fatalf("attempt 1 backoff out of expected range: %v", d1)
	}

	// 2. Second attempt backoff (should be around 1s to 2s)
	d2, ok := rc.CalculateBackoff(policy, 2, "network timeout")
	if !ok {
		t.Fatalf("expected retryable for attempt 2")
	}
	if d2 < 1*time.Second || d2 > 2*time.Second {
		t.Fatalf("attempt 2 backoff out of expected range: %v", d2)
	}

	// 3. Max attempts reached: attempt 4 should not retry
	_, ok = rc.CalculateBackoff(policy, 4, "network timeout")
	if ok {
		t.Fatalf("expected retryable = false when attempt reached max_attempts")
	}

	// 4. Non-retryable error matching
	_, ok = rc.CalculateBackoff(policy, 1, "ValidationFailed: bad argument")
	if ok {
		t.Fatalf("expected non-retryable error to fail immediately")
	}
}
