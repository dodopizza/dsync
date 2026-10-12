package util

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
)

type RetryPolicy struct {
	Retries     uint64
	MaxInterval time.Duration
}

func (p RetryPolicy) Backoff(ctx context.Context) backoff.BackOffContext {
	return backoff.WithContext(backoff.WithMaxRetries(p.exponential(), p.Retries), ctx)
}

func (p RetryPolicy) exponential() cappedBackOff {
	return cappedBackOff{
		ExponentialBackOff: backoff.NewExponentialBackOff(
			backoff.WithInitialInterval(min(backoff.DefaultInitialInterval, p.MaxInterval)),
			backoff.WithMaxInterval(p.MaxInterval),
			backoff.WithMaxElapsedTime(0)),
		max: p.MaxInterval,
	}
}

type cappedBackOff struct {
	*backoff.ExponentialBackOff
	max time.Duration
}

func (b cappedBackOff) NextBackOff() time.Duration {
	return min(b.ExponentialBackOff.NextBackOff(), b.max)
}
