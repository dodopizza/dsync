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
	exp := backoff.NewExponentialBackOff(backoff.WithMaxInterval(p.MaxInterval), backoff.WithMaxElapsedTime(0))
	return backoff.WithContext(backoff.WithMaxRetries(exp, p.Retries), ctx)
}
