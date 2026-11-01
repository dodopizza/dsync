package dsync

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUsageErrorsHideFlagValues(t *testing.T) {
	const uri = "mongodb://user:PW@127.0.0.1:1/"
	for name, tc := range map[string]struct {
		args []string
		hint string
	}{
		"root flag":       {[]string{"dsync", "--parallel-writers", uri, "mongodb://127.0.0.1:2/"}, "--parallel-writers"},
		"root validator":  {[]string{"dsync", "--load-level", uri, "mongodb://127.0.0.1:1/", "mongodb://127.0.0.1:2/"}, "load level"},
		"connector flag":  {[]string{"dsync", "mongodb://127.0.0.1:1/", "--ping-timeout", uri}, "--ping-timeout"},
		"verify flag":     {[]string{"dsync", "verify", "--cooldown", uri}, "--cooldown"},
		"serve bool flag": {[]string{"dsync", "serve", "--no-gzip=" + uri}, "--no-gzip"},
		"missing value":   {[]string{"dsync", "verify", "--cooldown"}, "--cooldown"},
		"spoofed name":    {[]string{"dsync", "verify", "--cooldown", `x" for flag -limit: PW`}, "--cooldown"},
		"unknown flag":    {[]string{"dsync", "--" + uri}, "invalid command-line flags"},
	} {
		output, err := captureOutput(t, func() error { return NewApp().Run(tc.args) })
		require.Error(t, err, name)
		assert.NotContains(t, err.Error(), "PW", name)
		assert.NotContains(t, output, "PW", name)
		assert.Contains(t, err.Error(), tc.hint, name)
	}
}

func TestDebugLogHidesMistypedFlagValues(t *testing.T) {
	args := []string{"dsync", "--verbosity", "DEBUG", "--namespace", "mongodb://user:PW@127.0.0.1:1/", "--logfile", "", "unknown://"}
	output, _ := captureOutput(t, func() error { return NewApp().Run(args) })
	assert.NotContains(t, output, "PW")
}

func captureOutput(t *testing.T, run func() error) (string, error) {
	t.Helper()
	r, w, err := os.Pipe()
	require.NoError(t, err)
	stdout, stderr := os.Stdout, os.Stderr
	os.Stdout, os.Stderr = w, w
	runErr := run()
	os.Stdout, os.Stderr = stdout, stderr
	require.NoError(t, w.Close())
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	return string(out), runErr
}
