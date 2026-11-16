package cosmos

import (
	"context"
	"testing"

	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type injectedWitness struct{}

func (injectedWitness) ScanKeys(context.Context, iface.Namespace, []string, int, func([][]bson.RawValue) error) error {
	return nil
}

func TestSelectWitnessPrefersTheInjectedWitnessOverTheMongoPath(t *testing.T) {
	settings := ConnectorSettings{EmulateDeletes: true, Witness: injectedWitness{}, WitnessMongoConnString: "not a uri"}

	require.Equal(t, injectedWitness{}, selectWitness(settings))
}

func TestSelectWitnessBuildsTheMongoWitnessFromTheConnectionString(t *testing.T) {
	witness := selectWitness(ConnectorSettings{EmulateDeletes: true, WitnessMongoConnString: "mongodb://localhost:27017"})

	require.IsType(t, mongoWitness{}, witness)
}

func TestSelectWitnessIsNilWhenDeletesAreNotEmulated(t *testing.T) {
	require.Nil(t, selectWitness(ConnectorSettings{}))
}
