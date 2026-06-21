package cohort

import (
	"math"
	"math/rand"
	"time"
)

// BackoffPolicy computes exponential-with-jitter durations for bounded retries.
// It is provider-agnostic: the reconciler uses it for RetryableConsistency
// retries; substrate constructs a separate instance (longer cap) for the
// throttle path that feeds Limiter.Backoff.
//
// This file imports nothing provider- or scheduler-specific.
type BackoffPolicy struct {
	Base   time.Duration // duration for attempt 0. Default: 100ms.
	Cap    time.Duration // maximum pre-jitter duration. Default: 30s.
	Jitter float64       // fraction of computed value added as uniform noise. Default: 0.25.
}

// DefaultBackoffPolicy returns a policy for RetryableConsistency retries in
// the reconciler: 100ms base, 30s cap, 25% jitter.
func DefaultBackoffPolicy() BackoffPolicy {
	return BackoffPolicy{
		Base:   100 * time.Millisecond,
		Cap:    30 * time.Second,
		Jitter: 0.25,
	}
}

// Duration returns the backoff duration for attempt (0-indexed).
// Computes base × 2^attempt, caps at Cap, then adds uniform jitter in
// [0, Jitter × capped). Result is bounded by Cap × (1 + Jitter).
//
// Overflow guard: the comparison is done in float64 before converting to
// time.Duration. math.Pow(2, large) produces +Inf in float64, and
// time.Duration(+Inf) wraps to a nonsense value. By comparing the float64
// product to float64(Cap) first, we cap before any conversion occurs.
func (p BackoffPolicy) Duration(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	capF := float64(p.Cap)
	raw := float64(p.Base) * math.Pow(2, float64(attempt))
	// Guard: +Inf, NaN, or any value exceeding Cap → use Cap before converting.
	if raw >= capF || math.IsInf(raw, 0) || math.IsNaN(raw) {
		d := p.Cap
		jitter := time.Duration(float64(d) * p.Jitter * rand.Float64())
		return d + jitter
	}
	d := time.Duration(raw)
	jitter := time.Duration(float64(d) * p.Jitter * rand.Float64())
	return d + jitter
}
