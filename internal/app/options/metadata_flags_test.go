package options

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
)

const sentinelDSN = "postgres://sentinel_user:sentinel_pw@sentinel-host/sentinel_db"

func clearMetadataEnv(t *testing.T) {
	t.Helper()
	t.Setenv("DSYNC_METADATA_POSTGRES", "")
	require.NoError(t, os.Unsetenv("DSYNC_METADATA_POSTGRES"))
}

func optionsFor(t *testing.T, args ...string) (Options, error) {
	t.Helper()
	flags, before := GetFlagsAndBeforeFunc()
	var o Options
	app := &cli.App{
		Flags:     flags,
		Before:    before,
		Writer:    &bytes.Buffer{},
		ErrWriter: &bytes.Buffer{},
		Action: func(c *cli.Context) (err error) {
			o, err = NewFromCLIContext(c)
			return err
		},
	}
	err := app.Run(append([]string{"dsync"}, args...))
	return o, err
}

func TestMetadataAndPostgresEnvAreMutuallyExclusive(t *testing.T) {
	t.Setenv("DSYNC_METADATA_POSTGRES", sentinelDSN)
	_, err := optionsFor(t, "--metadata", "mongodb://mongo-sentinel")
	require.EqualError(t, err, "--metadata and DSYNC_METADATA_POSTGRES are mutually exclusive")
	assert.NotContains(t, err.Error(), "mongo-sentinel")
	assert.NotContains(t, err.Error(), "sentinel_pw")
}

func TestMetadataPostgresFromEnv(t *testing.T) {
	t.Setenv("DSYNC_METADATA_POSTGRES", sentinelDSN)
	o, err := optionsFor(t)
	require.NoError(t, err)
	assert.Equal(t, sentinelDSN, o.StateStorePostgresConnString)
	assert.Empty(t, o.StateStoreConnString)
}

func TestMetadataAloneStillWorks(t *testing.T) {
	clearMetadataEnv(t)
	o, err := optionsFor(t, "--metadata", "mongodb://localhost")
	require.NoError(t, err)
	assert.Equal(t, "mongodb://localhost", o.StateStoreConnString)
	assert.Empty(t, o.StateStorePostgresConnString)
}

func TestMetadataPostgresFlagDoesNotExist(t *testing.T) {
	clearMetadataEnv(t)
	_, err := optionsFor(t, "--metadata-postgres", sentinelDSN)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "flag provided but not defined")
}

func TestMetadataPostgresEnvHiddenFromHelp(t *testing.T) {
	t.Setenv("DSYNC_METADATA_POSTGRES", sentinelDSN)
	flags, _ := GetFlagsAndBeforeFunc()
	var buf bytes.Buffer
	app := &cli.App{Flags: flags, Writer: &buf, Action: func(*cli.Context) error { return nil }}
	require.NoError(t, app.Run([]string{"dsync", "--help"}))
	assert.Contains(t, buf.String(), "DSYNC_METADATA_POSTGRES")
	assert.NotContains(t, buf.String(), "sentinel")
}
