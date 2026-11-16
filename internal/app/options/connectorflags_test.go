package options

import (
	"testing"

	"github.com/adiom-data/dsync/connectors/mongo"
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
