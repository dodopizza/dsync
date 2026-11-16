package runnerLocal

import (
	"testing"

	statestoreMongo "github.com/adiom-data/dsync/statestores/mongo"
	statestorePostgres "github.com/adiom-data/dsync/statestores/postgres"
	statestoreTemp "github.com/adiom-data/dsync/statestores/temp"
	"github.com/stretchr/testify/assert"
)

func TestNewStatestoreSelection(t *testing.T) {
	assert.IsType(t, &statestoreTemp.StateStore{}, newStatestore(RunnerLocalSettings{}))
	assert.IsType(t, &statestoreMongo.StateStore{}, newStatestore(RunnerLocalSettings{StateStoreConnString: "x"}))
	assert.IsType(t, &statestorePostgres.StateStore{}, newStatestore(RunnerLocalSettings{StateStorePostgresConnString: "x"}))
}
