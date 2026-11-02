package dsync

import (
	"io"
	"os"
	"path/filepath"
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
		"root validator":  {[]string{"dsync", "--load-level", uri, "mongodb://127.0.0.1:1/", "mongodb://127.0.0.1:2/"}, "--load-level", ""},
		"logfile":         {[]string{"dsync", "--logfile", uri, "mongodb://127.0.0.1:1/", "mongodb://127.0.0.1:2/"}, "--logfile", ""},
		"web host":        {[]string{"dsync", "--web-host", uri, "mongodb://127.0.0.1:1/", "mongodb://127.0.0.1:2/"}, "--web-host", ""},
		"config":          {[]string{"dsync", "--config", uri, "mongodb://127.0.0.1:1/", "mongodb://127.0.0.1:2/"}, "--config", ""},
		"namespace":       {[]string{"dsync", "--namespace", uri, "mongodb://127.0.0.1:1/", "mongodb://127.0.0.1:2/"}, "--namespace", ""},
		"verify string":   {[]string{"dsync", "verify", "--namespaces", uri, "mongodb://127.0.0.1:1/", "mongodb://127.0.0.1:2/"}, "--namespaces", ""},
		"serve address":   {[]string{"dsync", "serve", "--address", uri, "mongodb://127.0.0.1:1/"}, "--address", ""},
		"serve cert":      {[]string{"dsync", "serve", "--address", "localhost:1", "--cert-file", uri, "--key-file", "k", "mongodb://127.0.0.1:1/"}, "--cert-file", ""},
		"connector flag":  {[]string{"dsync", "mongodb://127.0.0.1:1/", "--ping-timeout", uri}, "--ping-timeout", "mongodb://connection-string [options]"},
		"verify flag":     {[]string{"dsync", "verify", "--cooldown", uri}, "--cooldown", "verify [command options]"},
		"serve bool flag": {[]string{"dsync", "serve", "--no-gzip=" + uri}, "--no-gzip", "serve [command options]"},
		"missing value":   {[]string{"dsync", "verify", "--cooldown"}, "--cooldown", "verify [command options]"},
		"spoofed name":    {[]string{"dsync", "verify", "--cooldown", `x" for flag -limit: PW`}, "--cooldown", "verify [command options]"},
		"unknown flag":    {[]string{"dsync", "--" + uri}, "invalid command-line flags", rootUsage},
		"yaml value":      {[]string{"dsync", "--config", writeConfig(t, "web-host: "+uri), "mongodb://127.0.0.1:1/", "mongodb://127.0.0.1:2/"}, "--web-host", ""},
		"broken yaml":     {[]string{"dsync", "--config", writeConfig(t, "web-host: [PW"), "mongodb://127.0.0.1:1/", "mongodb://127.0.0.1:2/"}, "--config", ""},
		"userinfo host":   {[]string{"dsync", "--web-host", "user:PW@127.0.0.1", "mongodb://127.0.0.1:1/", "mongodb://127.0.0.1:2/"}, "--web-host", ""},
		"userinfo addr":   {[]string{"dsync", "serve", "--address", "user:PW@127.0.0.1:1", "mongodb://127.0.0.1:1/"}, "--address", ""},
		"quoted uri":      {[]string{"dsync", "--web-host", `"` + uri + `"`, "mongodb://127.0.0.1:1/", "mongodb://127.0.0.1:2/"}, "--web-host", ""},
		"mapped uri":      {[]string{"dsync", "verify", "--namespaces", "src:" + uri, "mongodb://127.0.0.1:1/", "mongodb://127.0.0.1:2/"}, "--namespaces", ""},
		"https config":    {[]string{"dsync", "--config", "https://user:PW@127.0.0.1:1/c.yaml", "mongodb://127.0.0.1:1/", "mongodb://127.0.0.1:2/"}, "cannot load --config", ""},
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
	args := []string{"dsync", "--verbosity", "DEBUG", "--namespace", "PW.coll", "unknown://"}
	output, err := captureOutput(t, func() error { return NewApp().Run(args) })
	require.ErrorIs(t, err, options.ErrMissingConnector)
	assert.Contains(t, output, "Starting dsync")
	assert.NotContains(t, output, "PW")
}

func TestFlagValuesMayLookLikeURLs(t *testing.T) {
	for _, args := range [][]string{
		{"dsync", "verify", "--projection", `{"https://x": 1}`, "unknown://", "unknown://"},
		{"dsync", "verify", "--namespaces", "db.src:db.user@tenant", "unknown://", "unknown://"},
		{"dsync", "--namespace", "db.src:db.user@tenant", "unknown://"},
	} {
		_, err := captureOutput(t, func() error { return NewApp().Run(args) })
		require.ErrorIs(t, err, options.ErrMissingConnector, args)
	}
}

func writeConfig(t *testing.T, content string) string {
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func captureOutput(t *testing.T, run func() error) (string, error) {
	t.Helper()
	r, w, err := os.Pipe()
	require.NoError(t, err)
	defer r.Close()
	stdout, stderr := os.Stdout, os.Stderr
	os.Stdout, os.Stderr = w, w
	type result struct {
		out []byte
		err error
	}
	read := make(chan result, 1)
	go func() {
		out, err := io.ReadAll(r)
		read <- result{out, err}
	}()
	runErr := func() error {
		defer func() { os.Stdout, os.Stderr = stdout, stderr; w.Close() }()
		return run()
	}()
	captured := <-read
	require.NoError(t, captured.err)
	return string(captured.out), runErr
}
