package options

import (
	"io"
	"testing"

	"github.com/adiom-data/dsync/connectors/cosmos"
	"github.com/adiom-data/dsync/connectors/postgres"
	connectorsv1 "github.com/adiom-data/dsync/gen/adiom/commands/connectors/v1"
	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
)

const (
	witnessEnv    = "DSYNC_COSMOS_DELETES_WITNESS_POSTGRES"
	witnessSecret = "s3cr3t-marker"
	witnessDSN    = "postgres://u:" + witnessSecret + "@127.0.0.1:1/d?sslmode=disable"
)

func cosmosSettings(t *testing.T, as AdditionalSettings, args ...string) (cosmos.ConnectorSettings, error) {
	t.Helper()
	var settings cosmos.ConnectorSettings
	var settingsErr error
	app := connectorsv1.NewCosmosFlagsApp(func(c *cli.Context) error {
		flags, err := connectorsv1.ParseCosmosFlags(c)
		if err != nil {
			return err
		}
		settings, settingsErr = cosmosSettingsFromFlags(flags, "mongodb://localhost", as)
		return nil
	})
	require.NoError(t, app.Run(append([]string{"cosmos"}, args...)))
	if closer, ok := settings.Witness.(io.Closer); ok {
		t.Cleanup(func() { require.NoError(t, closer.Close()) })
	}
	return settings, settingsErr
}

func TestCosmosWitnessEnvEnablesEmulatedDeletesWithAPostgresWitness(t *testing.T) {
	t.Setenv(witnessEnv, witnessDSN)

	settings, err := cosmosSettings(t, AdditionalSettings{Namespaces: []string{"db.col:public.items"}})

	require.NoError(t, err)
	require.True(t, settings.EmulateDeletes)
	require.NotNil(t, settings.Witness)
	require.Empty(t, settings.WitnessMongoConnString)
}

func TestCosmosWithoutWitnessEnvKeepsTheMongoWitnessSelection(t *testing.T) {
	t.Setenv(witnessEnv, "")

	settings, err := cosmosSettings(t, AdditionalSettings{}, "--cosmos-deletes-cdc", "mongodb://witness")

	require.NoError(t, err)
	require.True(t, settings.EmulateDeletes)
	require.Nil(t, settings.Witness)
	require.Equal(t, "mongodb://witness", settings.WitnessMongoConnString)
}

func TestCosmosWitnessEnvWithTheMongoFlagIsAUsageErrorNamingBothOptions(t *testing.T) {
	t.Setenv(witnessEnv, witnessDSN)

	_, err := cosmosSettings(t, AdditionalSettings{}, "--cosmos-deletes-cdc", "mongodb://witness-host")

	require.ErrorIs(t, err, ErrInvalidUsage)
	require.ErrorContains(t, err, witnessEnv)
	require.ErrorContains(t, err, "--cosmos-deletes-cdc")
	require.NotContains(t, err.Error(), witnessSecret)
	require.NotContains(t, err.Error(), "witness-host")
}

func TestCosmosWitnessEnvWithReverseIsAUsageError(t *testing.T) {
	t.Setenv(witnessEnv, witnessDSN)

	_, err := cosmosSettings(t, AdditionalSettings{Reverse: true})

	require.ErrorIs(t, err, ErrInvalidUsage)
	require.ErrorContains(t, err, witnessEnv)
	require.ErrorContains(t, err, "--reverse")
	require.NotContains(t, err.Error(), witnessSecret)
}

func TestCosmosWitnessEnvWithAMalformedDSNFailsWithoutEchoingIt(t *testing.T) {
	t.Setenv(witnessEnv, "postgres://u:"+witnessSecret+"@host:notaport/db")

	_, err := cosmosSettings(t, AdditionalSettings{})

	require.Error(t, err)
	require.NotContains(t, err.Error(), witnessSecret)
}

func TestCosmosWitnessDeleteIntervalStillAppliesToThePostgresWitness(t *testing.T) {
	t.Setenv(witnessEnv, witnessDSN)

	settings, err := cosmosSettings(t, AdditionalSettings{}, "--cosmos-delete-interval", "5m")

	require.NoError(t, err)
	require.Equal(t, 5*60*1e9, float64(settings.DeletesCheckInterval))
}

func TestCosmosWitnessMapsTheSourceNamespaceToTheDestinationTable(t *testing.T) {
	t.Setenv(witnessEnv, witnessDSN)

	settings, err := cosmosSettings(t, AdditionalSettings{Namespaces: []string{"db.col:schema.t"}})

	require.NoError(t, err)
	lister, ok := settings.Witness.(*postgres.KeyLister)
	require.True(t, ok)
	require.Equal(t, "schema.t", lister.Table(iface.Namespace{Db: "db", Col: "col"}))
	require.Equal(t, "other.thing", lister.Table(iface.Namespace{Db: "other", Col: "thing"}))
}

func TestCosmosWitnessEnvIsIgnoredWhenCosmosIsTheDestination(t *testing.T) {
	t.Setenv(witnessEnv, witnessDSN)

	settings, err := cosmosSettings(t, AdditionalSettings{Destination: true})

	require.NoError(t, err)
	require.False(t, settings.EmulateDeletes)
	require.Nil(t, settings.Witness)
}

func TestCosmosWitnessWithATransformIsAUsageErrorNamingTheEnvVar(t *testing.T) {
	err := checkWitnessTransform(true, []string{"transform-host:1234"})

	require.ErrorIs(t, err, ErrInvalidUsage)
	require.ErrorContains(t, err, witnessEnv)
	require.ErrorContains(t, err, "transform")
	require.NotContains(t, err.Error(), "transform-host")
}

func TestCosmosWitnessCheckAllowsNoTransformOrNoWitness(t *testing.T) {
	require.NoError(t, checkWitnessTransform(true, nil))
	require.NoError(t, checkWitnessTransform(false, []string{"transform-host:1234"}))
}

func TestCosmosWitnessFactoryReportsThatTheWitnessIsInUse(t *testing.T) {
	t.Setenv(witnessEnv, witnessDSN)
	var used bool

	_, err := cosmosSettings(t, AdditionalSettings{witnessUsed: &used})

	require.NoError(t, err)
	require.True(t, used)
}

func TestCosmosWitnessIsNotReportedInUseWhenTheEnvVarIsIgnored(t *testing.T) {
	t.Setenv(witnessEnv, witnessDSN)
	var used bool

	_, err := cosmosSettings(t, AdditionalSettings{Destination: true, witnessUsed: &used})

	require.NoError(t, err)
	require.False(t, used)
}
