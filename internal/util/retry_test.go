package util

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/require"
)

func TestRetryPolicyBoundsAttemptsByRetries(t *testing.T) {
	attempts := 0
	err := backoff.Retry(func() error {
		attempts++
		return errors.New("transient")
	}, RetryPolicy{Retries: 2, MaxInterval: time.Millisecond}.Backoff(t.Context()))

	require.Error(t, err)
	require.Equal(t, 3, attempts)
}

type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }

func TestRetryPolicyCapsEveryWaitWithoutElapsedTimeLimit(t *testing.T) {
	maxInterval := 100 * time.Millisecond
	b := RetryPolicy{MaxInterval: maxInterval}.exponential()
	clock := &fakeClock{now: time.Now()}
	b.Clock = clock
	b.Reset()

	for range 50 {
		clock.now = clock.now.Add(time.Hour)
		next := b.NextBackOff()
		require.NotEqual(t, backoff.Stop, next)
		require.LessOrEqual(t, next, maxInterval)
	}
}

func TestRetryPolicyStopsWaitingOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	attempts := 0
	start := time.Now()
	err := backoff.Retry(func() error {
		attempts++
		cancel()
		return errors.New("transient")
	}, RetryPolicy{Retries: 5, MaxInterval: time.Hour}.Backoff(ctx))

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, attempts)
	require.Less(t, time.Since(start), time.Second)
}
