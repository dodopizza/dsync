package options

import (
	"testing"

	"github.com/adiom-data/dsync/connectors/mongo"
	"github.com/adiom-data/dsync/connectors/postgres"
	connectorsv1 "github.com/adiom-data/dsync/gen/adiom/commands/connectors/v1"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
)

func mongoSettingsFor(t *testing.T, args ...string) mongo.ConnectorSettings {
	t.Helper()
	var settings mongo.ConnectorSettings
	app := connectorsv1.NewMongoFlagsApp(func(c *cli.Context) error {
		flags, err := connectorsv1.ParseMongoFlags(c)
		if err != nil {
			return err
		}
		settings = mongoSettingsFromFlags(flags, "mongodb://localhost")
		return nil
	})
	require.NoError(t, app.Run(append([]string{"mongodb"}, args...)))
	return settings
}

func TestDeleteUniqueConflictsIsOffUnlessRequested(t *testing.T) {
	require.False(t, mongoSettingsFor(t).DeleteUniqueConflicts)
	require.True(t, mongoSettingsFor(t, "--delete-unique-conflicts").DeleteUniqueConflicts)
}

func TestReplaceIDConflictsIsOffUnlessRequested(t *testing.T) {
	require.False(t, mongoSettingsFor(t).ReplaceIDConflicts)
	require.True(t, mongoSettingsFor(t, "--replace-id-conflicts").ReplaceIDConflicts)
}

func createRegistered(t *testing.T, name string, args ...string) error {
	t.Helper()
	for _, rc := range GetRegisteredConnectors() {
		if rc.Name == name {
			_, _, err := rc.Create(args, AdditionalSettings{})
			return err
		}
	}
	t.Fatalf("connector %s not registered", name)
	return nil
}

func TestPostgresRejectsConnectionStringFlagValues(t *testing.T) {
	const marker = "s3cr3t-marker"
	const url = "postgres://u:" + marker + "@127.0.0.1:1/d?sslmode=disable"

	err := createRegistered(t, "Postgres", url, "--replication-slot", url)
	require.Error(t, err)
	require.Contains(t, err.Error(), "looks like a connection string")
	require.NotContains(t, err.Error(), marker)
	require.NotContains(t, err.Error(), url)

	err = createRegistered(t, "Postgres", url, "--replication-slot", "slot")
	if err != nil {
		require.NotContains(t, err.Error(), "looks like a connection string")
	}
}

func TestOtherConnectorsAcceptURLFlagValues(t *testing.T) {
	for name, args := range map[string][]string{
		"weaviate": {"weaviate", "--url", "http://127.0.0.1:8080"},
		"S3":       {"s3://bucket", "--region", "r", "--endpoint", "http://127.0.0.1:4566"},
	} {
		t.Run(name, func(t *testing.T) {
			err := createRegistered(t, name, args...)
			if err != nil {
				require.NotContains(t, err.Error(), "looks like a connection string")
			}
		})
	}
}

func postgresSettingsFor(t *testing.T, args ...string) postgres.PostgresSettings {
	t.Helper()
	var settings postgres.PostgresSettings
	app := connectorsv1.NewPostgresFlagsApp(func(c *cli.Context) error {
		flags, err := connectorsv1.ParsePostgresFlags(c)
		if err != nil {
			return err
		}
		settings = postgresSettingsFromFlags(flags, "postgres://localhost")
		return nil
	})
	require.NoError(t, app.Run(append([]string{"postgres"}, args...)))
	return settings
}

func TestPostgresDocumentModeFlagsReachTheSettings(t *testing.T) {
	require.False(t, postgresSettingsFor(t).DocumentMode)
	settings := postgresSettingsFor(t, "--document-mode", "--updated-at-column", "changed_at", "--updated-at-field", "modified")
	require.True(t, settings.DocumentMode)
	require.Equal(t, "changed_at", settings.UpdatedAtColumn)
	require.Equal(t, "modified", settings.UpdatedAtField)
}

func TestPostgresUpdatedAtFlagsNeedDocumentModeAndColumn(t *testing.T) {
	url := "postgres://u:p@127.0.0.1:1/d?sslmode=disable"
	for name, tc := range map[string]struct {
		args []string
		want string
	}{
		"column without document mode": {[]string{"--updated-at-column", "c"}, "--document-mode"},
		"field without document mode":  {[]string{"--updated-at-field", "f"}, "--document-mode"},
		"field without column":         {[]string{"--document-mode", "--updated-at-field", "f"}, "--updated-at-column"},
	} {
		err := createRegistered(t, "Postgres", append([]string{url}, tc.args...)...)
		require.Error(t, err, name)
		require.Contains(t, err.Error(), tc.want, name)
		require.NotContains(t, err.Error(), url, name)
	}
}

func TestPostgresDocumentModeStringFlagsRejectConnectionStrings(t *testing.T) {
	const marker = "s3cr3t-marker"
	url := "postgres://u:" + marker + "@127.0.0.1:1/d?sslmode=disable"
	for _, flag := range []string{"--updated-at-column", "--updated-at-field"} {
		err := createRegistered(t, "Postgres", url, "--document-mode", flag, url)
		require.Error(t, err, flag)
		require.Contains(t, err.Error(), "looks like a connection string", flag)
		require.NotContains(t, err.Error(), marker, flag)
	}
}

func TestPostgresDocumentModeStringFlagsRejectKeywordConnectionStrings(t *testing.T) {
	const marker = "s3cr3t-marker"
	for _, value := range []string{
		"host=127.0.0.1 user=u password=" + marker,
		"host=127.0.0.1 PassWord = " + marker,
	} {
		for _, flag := range []string{"--updated-at-column", "--updated-at-field"} {
			err := createRegistered(t, "Postgres", "postgres://u@127.0.0.1:1/d", "--document-mode", flag, value)
			require.Error(t, err, flag)
			require.Contains(t, err.Error(), "looks like a connection string", flag)
			require.NotContains(t, err.Error(), marker, flag)
		}
	}
}
