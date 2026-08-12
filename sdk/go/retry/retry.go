// Package retry provides exponential backoff with jitter for retrying failed operations.
//
// Usage:
//
//	policy := retry.DefaultPolicy()
//	result, err := policy.Do(ctx, func(ctx context.Context) (string, error) {
//	    return fetchRemoteData(ctx)
//	})
package retry

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"
)

// Policy configures retry behavior with exponential backoff.
type Policy struct {
	// MaxAttempts is the maximum number of attempts (must be >= 1).
	MaxAttempts int

	// InitialInterval is the initial backoff interval.
	InitialInterval time.Duration

	// BackoffCoefficient is the multiplier for each subsequent attempt.
	BackoffCoefficient float64

	// MaxInterval caps the backoff duration.
	MaxInterval time.Duration

	// Jitter adds randomness to the backoff to prevent thundering herd.
	Jitter bool

	// RetryableError is an optional predicate to determine if an error is retryable.
	// If nil, all errors are considered retryable.
	RetryableError func(error) bool
}

// DefaultPolicy returns a default retry policy (3 attempts, 100ms initial, 2x backoff).
func DefaultPolicy() Policy {
	return Policy{
		MaxAttempts:        3,
		InitialInterval:    100 * time.Millisecond,
		BackoffCoefficient: 2.0,
		MaxInterval:        60 * time.Second,
		Jitter:             true,
	}
}

// Validate checks that the policy configuration is valid.
func (p Policy) Validate() error {
	if p.MaxAttempts < 1 {
		return fmt.Errorf("retry: MaxAttempts must be >= 1, got %d", p.MaxAttempts)
	}
	if p.InitialInterval <= 0 {
		return fmt.Errorf("retry: InitialInterval must be > 0, got %v", p.InitialInterval)
	}
	if p.BackoffCoefficient < 1.0 {
		return fmt.Errorf("retry: BackoffCoefficient must be >= 1.0, got %f", p.BackoffCoefficient)
	}
	if p.MaxInterval < p.InitialInterval {
		return fmt.Errorf("retry: MaxInterval (%v) must be >= InitialInterval (%v)", p.MaxInterval, p.InitialInterval)
	}
	return nil
}

// CalculateBackoff computes the backoff duration for a given attempt (0-based).
func (p Policy) CalculateBackoff(attempt int) time.Duration {
	interval := float64(p.InitialInterval) * math.Pow(p.BackoffCoefficient, float64(attempt))

	if interval > float64(p.MaxInterval) {
		interval = float64(p.MaxInterval)
	}

	if p.Jitter {
		interval = rand.Float64() * interval //nolint:gosec // jitter doesn't need crypto rand
	}

	return time.Duration(interval)
}

// IsRetryable checks if an error should be retried.
func (p Policy) IsRetryable(err error) bool {
	if p.RetryableError == nil {
		return true // retry all by default
	}
	return p.RetryableError(err)
}

// Do executes a function with retry logic.
//
// The function is called up to MaxAttempts times. Between attempts, the goroutine
// sleeps for a calculated backoff duration. If the context is canceled, the last
// error is returned immediately.
//
// Returns the result of the function call or the last error if all retries fail.
func Do[T any](ctx context.Context, policy Policy, fn func(ctx context.Context) (T, error)) (T, error) {
	if err := policy.Validate(); err != nil {
		var zero T
		return zero, err
	}

	var lastErr error
	for attempt := 0; attempt < policy.MaxAttempts; attempt++ {
		result, err := fn(ctx)
		if err == nil {
			return result, nil
		}

		lastErr = err

		if !policy.IsRetryable(err) {
			return result, err
		}

		if attempt < policy.MaxAttempts-1 {
			backoff := policy.CalculateBackoff(attempt)
			select {
			case <-ctx.Done():
				return result, lastErr
			case <-time.After(backoff):
				// continue to next attempt
			}
		}
	}

	var zero T
	return zero, lastErr
}

// DoVoid executes a void function with retry logic.
func DoVoid(ctx context.Context, policy Policy, fn func(ctx context.Context) error) error {
	_, err := Do(ctx, policy, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
	return err
}
