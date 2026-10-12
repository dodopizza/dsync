package simple

import (
	"context"
	"errors"
	"testing"

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
