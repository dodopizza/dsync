package simple

import (
	"context"
	"errors"
	"testing"

	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/adiom-data/dsync/protocol/iface/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestWaitForFlowDoneReturnsNotifiedError(t *testing.T) {
	c := NewSimpleCoordinator(1)
	c.Setup(context.Background(), nil, nil)
	done := make(chan struct{})
	close(done)
	c.flows["f"] = &FlowDetails{FlowID: "f", flowDone: done}
	historyLost := errors.New("history lost")

	c.NotifyError(historyLost)
	c.NotifyError(errors.New("later"))

	require.ErrorIs(t, c.WaitForFlowDone("f"), historyLost)
}

func TestFlowGetOrCreateFailsWhenFlowStateUnreadable(t *testing.T) {
	throttled := errors.New("throttled")
	s := mocks.NewStatestore(t)
	s.On("RetrieveObject", flowStateMetadataStore, mock.Anything, mock.Anything).Return(throttled)
	c := NewSimpleCoordinator(1)
	c.Setup(context.Background(), nil, s)

	_, err := c.FlowGetOrCreate(iface.FlowOptions{Type: iface.UnidirectionalFlowType})

	require.ErrorIs(t, err, throttled)
}
