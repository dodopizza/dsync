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

func TestRetryPolicyHasNoElapsedTimeLimit(t *testing.T) {
	b := RetryPolicy{Retries: 1, MaxInterval: time.Millisecond}.Backoff(t.Context())
	b.Reset()
	time.Sleep(20 * time.Millisecond)

	require.NotEqual(t, backoff.Stop, b.NextBackOff())
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
