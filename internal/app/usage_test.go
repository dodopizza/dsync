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
	for name, args := range map[string][]string{
		"root flag":      {"dsync", "--parallel-writers", uri, "mongodb://127.0.0.1:2/"},
		"root validator": {"dsync", "--load-level", uri, "mongodb://127.0.0.1:1/", "mongodb://127.0.0.1:2/"},
		"connector flag": {"dsync", "mongodb://127.0.0.1:1/", "--ping-timeout", uri},
		"verify flag":    {"dsync", "verify", "--cooldown", uri},
		"serve flag":     {"dsync", "serve", "--no-gzip=" + uri},
	} {
		output, err := captureOutput(t, func() error { return NewApp().Run(args) })
		require.Error(t, err, name)
		assert.NotContains(t, err.Error(), "PW", name)
		assert.NotContains(t, output, "PW", name)
	}
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
