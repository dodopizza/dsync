package common

import (
	"context"
	"errors"
	"testing"

	"github.com/adiom-data/dsync/protocol/iface/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestHandlerErrorNotifiesCoordinator(t *testing.T) {
	coord := mocks.NewCoordinator(t)
	writeErr := errors.New("bulk write failed")
	coord.On("NotifyError", mock.MatchedBy(func(err error) bool { return errors.Is(err, writeErr) })).Once()
	ctx, cancel := context.WithCancel(context.Background())
	c := &connector{coord: coord, flowCtx: ctx, flowCancelFunc: cancel}

	require.ErrorIs(t, c.HandlerError(writeErr), writeErr)
	require.Error(t, ctx.Err())
}
