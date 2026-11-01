package dsync

import (
	"io"
	"os"
	"testing"

	"github.com/adiom-data/dsync/internal/app/options"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUsageErrorsHideFlagValues(t *testing.T) {
	const uri = "mongodb://user:PW@127.0.0.1:1/"
	const rootUsage = "dsync [options] source"
	for name, tc := range map[string]struct {
		args  []string
		hint  string
		usage string
	}{
		"root flag":       {[]string{"dsync", "--parallel-writers", uri, "mongodb://127.0.0.1:2/"}, "--parallel-writers", rootUsage},
		"root validator":  {[]string{"dsync", "--load-level", uri, "mongodb://127.0.0.1:1/", "mongodb://127.0.0.1:2/"}, "load level", ""},
		"logfile":         {[]string{"dsync", "--logfile", uri, "mongodb://127.0.0.1:1/", "mongodb://127.0.0.1:2/"}, "--logfile", ""},
		"web host":        {[]string{"dsync", "--web-host", uri, "mongodb://127.0.0.1:1/", "mongodb://127.0.0.1:2/"}, "--web-host", ""},
		"connector flag":  {[]string{"dsync", "mongodb://127.0.0.1:1/", "--ping-timeout", uri}, "--ping-timeout", "mongodb://connection-string [options]"},
		"verify flag":     {[]string{"dsync", "verify", "--cooldown", uri}, "--cooldown", "verify [command options]"},
		"serve bool flag": {[]string{"dsync", "serve", "--no-gzip=" + uri}, "--no-gzip", "serve [command options]"},
		"missing value":   {[]string{"dsync", "verify", "--cooldown"}, "--cooldown", "verify [command options]"},
		"spoofed name":    {[]string{"dsync", "verify", "--cooldown", `x" for flag -limit: PW`}, "--cooldown", "verify [command options]"},
		"unknown flag":    {[]string{"dsync", "--" + uri}, "invalid command-line flags", rootUsage},
	} {
		output, err := captureOutput(t, func() error { return NewApp().Run(tc.args) })
		require.Error(t, err, name)
		assert.NotContains(t, err.Error(), "PW", name)
		assert.NotContains(t, output, "PW", name)
		assert.Contains(t, err.Error(), tc.hint, name)
		if tc.usage != "" {
			assert.Contains(t, output, tc.usage, name)
		}
	}
}

func TestDebugLogHidesMistypedFlagValues(t *testing.T) {
	args := []string{"dsync", "--verbosity", "DEBUG", "--namespace", "mongodb://user:PW@127.0.0.1:1/", "unknown://"}
	output, err := captureOutput(t, func() error { return NewApp().Run(args) })
	require.ErrorIs(t, err, options.ErrMissingConnector)
	assert.Contains(t, output, "Starting dsync")
	assert.NotContains(t, output, "PW")
}

func captureOutput(t *testing.T, run func() error) (string, error) {
	t.Helper()
	r, w, err := os.Pipe()
	require.NoError(t, err)
	defer r.Close()
	stdout, stderr := os.Stdout, os.Stderr
	os.Stdout, os.Stderr = w, w
	read := make(chan []byte)
	go func() {
		out, _ := io.ReadAll(r)
		read <- out
	}()
	runErr := func() error {
		defer func() { os.Stdout, os.Stderr = stdout, stderr; w.Close() }()
		return run()
	}()
	return string(<-read), runErr
}
