package simple

import (
	"encoding/json"
	"sync"
	"testing"

	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/adiom-data/dsync/statestores/temp"
	"github.com/stretchr/testify/require"
)

type marshalingStore struct {
	*temp.StateStore
}

func (s marshalingStore) PersistObject(_ string, _ any, obj any) error {
	_, err := json.Marshal(obj)
	return err
}

func TestResumeTokenUpdateDoesNotRaceWithTaskDone(t *testing.T) {
	c := NewSimpleCoordinator(1)
	c.Setup(t.Context(), nil, marshalingStore{temp.NewTempStateStore()})
	c.flows["f"] = &FlowDetails{
		FlowID:    "f",
		Options:   iface.FlowOptions{SrcId: "src", DstId: "dst"},
		Resumable: true,
		ReadPlan:  iface.ConnectorReadPlan{Tasks: []iface.ReadPlanTask{{Id: 1}}},
	}

	var wg sync.WaitGroup
	wg.Go(func() {
		for range 200 {
			require.NoError(t, c.NotifyTaskDone("f", "dst", 1, nil))
		}
	})
	wg.Go(func() {
		for range 200 {
			require.NoError(t, c.UpdateCDCResumeToken("f", "dst", []byte("token")))
		}
	})
	wg.Wait()
}
