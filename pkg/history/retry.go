package history

import (
	"math"
	"math/rand"
	"strings"
	"time"

	v1 "github.com/R-zin/Aetherflow/api/proto/v1"
)

type RetryCalculator struct {
	rnd *rand.Rand
}

func NewRetryCalculator() *RetryCalculator {
	return &RetryCalculator{
		rnd: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (rc *RetryCalculator) IsNonRetryable(policy *v1.RetryPolicy, errString string) bool {
	if policy == nil || len(policy.NonRetryableErrorTypes) == 0 {
		return false
	}
	for _, nonRetry := range policy.NonRetryableErrorTypes {
		if strings.Contains(strings.ToLower(errString), strings.ToLower(nonRetry)) {
			return true
		}
	}
	return false
}

func (rc *RetryCalculator) CalculateBackoff(policy *v1.RetryPolicy, attempt int32, errString string) (time.Duration, bool) {
	if policy == nil {
		return 0, false
	}

	if rc.IsNonRetryable(policy, errString) {
		return 0, false
	}

	if policy.MaxAttempts > 0 && attempt >= policy.MaxAttempts {
		return 0, false
	}

	initSec := float64(policy.InitialIntervalSeconds)
	if initSec <= 0 {
		initSec = 1.0
	}

	coeff := policy.BackoffCoefficient
	if coeff <= 1.0 {
		coeff = 2.0
	}

	maxSec := float64(policy.MaxIntervalSeconds)
	if maxSec <= 0 {
		maxSec = 100.0
	}

	calculated := initSec * math.Pow(coeff, float64(attempt-1))
	if calculated > maxSec {
		calculated = maxSec
	}

	// Full jitter between 0.5 * calculated and calculated to avoid thundering herds
	jitterFactor := 0.5 + (rc.rnd.Float64() * 0.5)
	backoffSec := calculated * jitterFactor

	return time.Duration(backoffSec * float64(time.Second)), true
}
