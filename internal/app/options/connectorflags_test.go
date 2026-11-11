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
