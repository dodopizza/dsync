package common

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"github.com/adiom-data/dsync/protocol/iface/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.akshayshah.org/memhttp"
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

type streamUpdatesHandler struct {
	adiomv1connect.UnimplementedConnectorServiceHandler
	stream func(*connect.ServerStream[adiomv1.StreamUpdatesResponse]) error
}

func (h streamUpdatesHandler) StreamUpdates(_ context.Context, _ *connect.Request[adiomv1.StreamUpdatesRequest], s *connect.ServerStream[adiomv1.StreamUpdatesResponse]) error {
	return h.stream(s)
}

func connectorWithStream(t *testing.T, coord *mocks.Coordinator, stream func(*connect.ServerStream[adiomv1.StreamUpdatesResponse]) error) *connector {
	_, handler := adiomv1connect.NewConnectorServiceHandler(streamUpdatesHandler{stream: stream})
	srv, err := memhttp.New(handler)
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Close() })
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	return &connector{
		coord:          coord,
		impl:           adiomv1connect.NewConnectorServiceClient(srv.Client(), srv.URL()),
		flowCtx:        ctx,
		flowCancelFunc: cancel,
		settings:       ConnectorSettings{ResumeTokenCheckInterval: 10 * time.Millisecond},
	}
}

func TestResumeTokenWatchFailsFlowWhenHistoryLost(t *testing.T) {
	coord := mocks.NewCoordinator(t)
	coord.On("NotifyError", mock.MatchedBy(func(err error) bool {
		return strings.Contains(err.Error(), "ChangeStreamHistoryLost") && strings.Contains(err.Error(), "rerun it from scratch")
	})).Once()
	c := connectorWithStream(t, coord, func(*connect.ServerStream[adiomv1.StreamUpdatesResponse]) error {
		return connect.NewError(connect.CodeInternal, errors.New("(ChangeStreamHistoryLost) Resume of change stream was not possible"))
	})

	go c.watchResumeToken(make(chan struct{}), []string{"db.col"}, []byte("token"))

	select {
	case <-c.flowCtx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("flow was not failed")
	}
}

func TestResumeTokenCheckIgnoresHealthyAndTransientStreams(t *testing.T) {
	for name, stream := range map[string]func(*connect.ServerStream[adiomv1.StreamUpdatesResponse]) error{
		"opens": func(s *connect.ServerStream[adiomv1.StreamUpdatesResponse]) error {
			return s.Send(&adiomv1.StreamUpdatesResponse{NextCursor: []byte("next")})
		},
		"transient": func(*connect.ServerStream[adiomv1.StreamUpdatesResponse]) error {
			return connect.NewError(connect.CodeUnavailable, errors.New("connection reset"))
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := connectorWithStream(t, mocks.NewCoordinator(t), stream)
			require.NoError(t, c.checkResumeToken([]string{"db.col"}, []byte("token")))
		})
	}
}
