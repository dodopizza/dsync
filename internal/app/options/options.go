/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package options

import (
	"fmt"
	"time"

	"github.com/adiom-data/dsync/internal/util"
	"github.com/urfave/cli/v2"
)

type Options struct {
	Verbosity string

	StateStoreConnString string

	Logfile string

	NamespaceFrom []string

	Verify           bool
	VerifyQuickCount bool
	VerifyMem        bool
	VerifyMaxTasks   int
	Cleanup          bool
	Progress         bool
	Reverse          bool

	Pprof bool

	PprofPort uint
	WebPort   uint
	WebHost   string

	LoadLevel                      string
	InitialSyncNumParallelCopiers  int
	NumParallelWriters             int
	NumParallelIntegrityCheckTasks int
	CdcResumeTokenUpdateInterval   time.Duration
	CdcResumeTokenCheckInterval    time.Duration
	WriterMaxBatchSize             int
	MultinamespaceBatcher          bool
	Mode                           string

	WriteRateLimit int
	ReadRetry      util.RetryPolicy
	WriteRetry     util.RetryPolicy
	MetaRetry      util.RetryPolicy
}

// works with a copy of the struct to avoid modifying the original
func RedactSensitiveInfo(o Options) Options {
	o.StateStoreConnString = "REDACTED"
	return o
}

func NewFromCLIContext(c *cli.Context) (Options, error) {
	o := Options{}

	o.Verbosity = c.String("verbosity")
	o.StateStoreConnString = c.String("metadata")
	o.Logfile = c.String("logfile")
	o.NamespaceFrom = c.StringSlice("namespace")
	o.Verify = c.Bool("verify")
	o.VerifyQuickCount = c.Bool("verify-quick-count")
	o.VerifyMem = c.Bool("verify-mem")
	o.VerifyMaxTasks = c.Int("verify-max-tasks")
	o.Cleanup = c.Bool("cleanup")
	o.Progress = c.Bool("progress")
	o.Pprof = c.Bool("pprof")
	o.LoadLevel = c.String("load-level")
	o.PprofPort = c.Uint("pprof-port")
	o.WebPort = c.Uint("web-port")
	o.WebHost = c.String("web-host")

	o.InitialSyncNumParallelCopiers = c.Int("parallel-copiers")
	o.NumParallelWriters = c.Int("parallel-writers")
	o.NumParallelIntegrityCheckTasks = c.Int("parallel-integrity-check-workers")
	o.CdcResumeTokenUpdateInterval = c.Duration("cdc-resume-token-interval")
	o.CdcResumeTokenCheckInterval = c.Duration("cdc-resume-token-check-interval")
	o.WriterMaxBatchSize = c.Int("writer-batch-size")
	o.MultinamespaceBatcher = c.Bool("namespace-batcher")
	o.Mode = c.String("mode")
	o.Reverse = c.Bool("reverse")

	o.WriteRateLimit = c.Int("write-rate-limit")
	o.ReadRetry = util.RetryPolicy{Retries: c.Uint64("read-retries"), MaxInterval: c.Duration("read-retry-max-interval")}
	o.WriteRetry = util.RetryPolicy{Retries: c.Uint64("write-retries"), MaxInterval: c.Duration("write-retry-max-interval")}
	o.MetaRetry = util.RetryPolicy{Retries: c.Uint64("meta-retries"), MaxInterval: c.Duration("meta-retry-max-interval")}
	for name, policy := range map[string]util.RetryPolicy{"read": o.ReadRetry, "write": o.WriteRetry, "meta": o.MetaRetry} {
		if policy.MaxInterval <= 0 {
			return o, fmt.Errorf("--%s-retry-max-interval must be positive", name)
		}
	}

	return o, nil
}
