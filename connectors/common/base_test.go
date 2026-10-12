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

type streamLSNHandler struct {
	adiomv1connect.UnimplementedConnectorServiceHandler
	stream func(context.Context, *connect.ServerStream[adiomv1.StreamLSNResponse]) error
}

func (h streamLSNHandler) StreamLSN(ctx context.Context, _ *connect.Request[adiomv1.StreamLSNRequest], s *connect.ServerStream[adiomv1.StreamLSNResponse]) error {
	return h.stream(ctx, s)
}

func connectorWithStream(t *testing.T, coord *mocks.Coordinator, stream func(context.Context, *connect.ServerStream[adiomv1.StreamLSNResponse]) error) *connector {
	_, handler := adiomv1connect.NewConnectorServiceHandler(streamLSNHandler{stream: stream})
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
		settings:       ConnectorSettings{ResumeTokenCheckInterval: time.Hour},
	}
}

func historyLost(context.Context, *connect.ServerStream[adiomv1.StreamLSNResponse]) error {
	return connect.NewError(connect.CodeInternal, errors.New("(ChangeStreamHistoryLost) Resume of change stream was not possible"))
}

func TestResumeTokenWatchFailsFlowOnFirstCheckWhenHistoryLost(t *testing.T) {
	coord := mocks.NewCoordinator(t)
	coord.On("NotifyError", mock.MatchedBy(func(err error) bool {
		return strings.Contains(err.Error(), "ChangeStreamHistoryLost") && strings.Contains(err.Error(), "rerun it from scratch")
	})).Once()
	c := connectorWithStream(t, coord, historyLost)

	go c.watchResumeToken(make(chan struct{}), []string{"db.col"}, []byte("token"))

	select {
	case <-c.flowCtx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("flow was not failed")
	}
}

func TestResumeTokenWatchIgnoresProbeInFlightWhenInitialSyncEnds(t *testing.T) {
	probeStarted := make(chan struct{})
	c := connectorWithStream(t, mocks.NewCoordinator(t), func(ctx context.Context, s *connect.ServerStream[adiomv1.StreamLSNResponse]) error {
		close(probeStarted)
		<-ctx.Done()
		return historyLost(ctx, s)
	})
	initialSyncDone := make(chan struct{})
	watcherDone := make(chan struct{})

	go func() {
		defer close(watcherDone)
		c.watchResumeToken(initialSyncDone, []string{"db.col"}, []byte("token"))
	}()
	<-probeStarted
	close(initialSyncDone)

	select {
	case <-watcherDone:
	case <-time.After(5 * time.Second):
		t.Fatal("watcher did not stop")
	}
	require.NoError(t, c.flowCtx.Err())
}

func TestResumeTokenCheckIgnoresHealthyAndTransientStreams(t *testing.T) {
	for name, stream := range map[string]func(context.Context, *connect.ServerStream[adiomv1.StreamLSNResponse]) error{
		"opens": func(_ context.Context, s *connect.ServerStream[adiomv1.StreamLSNResponse]) error {
			return s.Send(&adiomv1.StreamLSNResponse{NextCursor: []byte("next")})
		},
		"transient": func(context.Context, *connect.ServerStream[adiomv1.StreamLSNResponse]) error {
			return connect.NewError(connect.CodeUnavailable, errors.New("connection reset"))
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := connectorWithStream(t, mocks.NewCoordinator(t), stream)
			require.NoError(t, c.checkResumeToken(c.flowCtx, []string{"db.col"}, []byte("token")))
		})
	}
}
