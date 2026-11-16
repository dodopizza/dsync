package simple

import (
	"context"
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

func (s marshalingStore) PersistObject(_ string, _ interface{}, obj interface{}) error {
	_, err := json.Marshal(obj)
	return err
}

func TestResumeTokenUpdateDoesNotRaceWithTaskDone(t *testing.T) {
	c := NewSimpleCoordinator(1)
	c.Setup(context.Background(), nil, marshalingStore{temp.NewTempStateStore()})
	c.flows["f"] = &FlowDetails{
		FlowID:    "f",
		Options:   iface.FlowOptions{SrcId: "src", DstId: "dst"},
		Resumable: true,
		ReadPlan:  iface.ConnectorReadPlan{Tasks: []iface.ReadPlanTask{{Id: 1}}},
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 200 {
			require.NoError(t, c.NotifyTaskDone("f", "dst", 1, nil))
		}
	}()
	go func() {
		defer wg.Done()
		for range 200 {
			require.NoError(t, c.UpdateCDCResumeToken("f", "dst", []byte("token")))
		}
	}()
	wg.Wait()
}
