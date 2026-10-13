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
	"github.com/adiom-data/dsync/internal/util"
	"github.com/adiom-data/dsync/protocol/iface"
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
		"empty": func(context.Context, *connect.ServerStream[adiomv1.StreamLSNResponse]) error {
			return nil
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

type listDataHandler struct {
	adiomv1connect.UnimplementedConnectorServiceHandler
	list func(*adiomv1.ListDataRequest) (*adiomv1.ListDataResponse, error)
}

func (h listDataHandler) ListData(_ context.Context, r *connect.Request[adiomv1.ListDataRequest]) (*connect.Response[adiomv1.ListDataResponse], error) {
	res, err := h.list(r.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(res), nil
}

func (listDataHandler) GetNamespaceMetadata(context.Context, *connect.Request[adiomv1.GetNamespaceMetadataRequest]) (*connect.Response[adiomv1.GetNamespaceMetadataResponse], error) {
	return connect.NewResponse(&adiomv1.GetNamespaceMetadataResponse{}), nil
}

func runInitialSync(t *testing.T, coord *mocks.Coordinator, list func(*adiomv1.ListDataRequest) (*adiomv1.ListDataResponse, error)) *connector {
	_, handler := adiomv1connect.NewConnectorServiceHandler(listDataHandler{list: list})
	srv, err := memhttp.New(handler)
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Close() })
	client := adiomv1connect.NewConnectorServiceClient(srv.Client(), srv.URL())
	dataChannel := make(chan iface.DataMessage, 16)
	transport := mocks.NewTransport(t)
	transport.On("GetDataChannelEndpoint", mock.Anything).Return(dataChannel, nil)
	done := make(chan struct{})
	coord.On("NotifyDone", mock.Anything, mock.Anything).Return(nil).Once().Run(func(mock.Arguments) { close(done) })
	var status iface.ConnectorStatus
	c := &connector{
		ctx:                t.Context(),
		coord:              coord,
		t:                  transport,
		impl:               client,
		maybeOptimizedImpl: client,
		progressTracker:    NewProgressTracker(&status, t.Context()),
		settings:           ConnectorSettings{NumParallelCopiers: 1, ResumeTokenUpdateInterval: time.Hour, ReadRetry: util.RetryPolicy{Retries: 3, MaxInterval: time.Millisecond}},
	}
	task := iface.ReadPlanTask{Id: 1}
	task.Def.Db, task.Def.Col = "db", "db.col"
	options := iface.ConnectorOptions{Mode: iface.SyncModeInitialSync}
	require.NoError(t, c.StartReadToChannel("flow", options, iface.ConnectorReadPlan{Tasks: []iface.ReadPlanTask{task}}, "data"))
	for range dataChannel {
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("NotifyDone not called")
	}
	return c
}

func TestInitialSyncModeFailsFlowWhenTaskFails(t *testing.T) {
	coord := mocks.NewCoordinator(t)
	coord.On("NotifyError", mock.Anything).Once()

	c := runInitialSync(t, coord, func(*adiomv1.ListDataRequest) (*adiomv1.ListDataResponse, error) {
		return nil, connect.NewError(connect.CodeInternal, errors.New("unauthorized"))
	})

	require.Error(t, c.flowCtx.Err())
}

func TestInitialSyncRetriesTaskFromItsStartAfterUnavailable(t *testing.T) {
	coord := mocks.NewCoordinator(t)
	coord.On("NotifyTaskDone", mock.Anything, mock.Anything, iface.ReadPlanTaskID(1), mock.Anything).Return(nil).Once()
	var cursors [][]byte

	c := runInitialSync(t, coord, func(r *adiomv1.ListDataRequest) (*adiomv1.ListDataResponse, error) {
		cursors = append(cursors, r.GetCursor())
		switch len(cursors) {
		case 1:
			return &adiomv1.ListDataResponse{Data: [][]byte{{1}}, NextCursor: []byte("page2")}, nil
		case 2:
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("connection reset"))
		}
		return &adiomv1.ListDataResponse{Data: [][]byte{{1}}}, nil
	})

	require.Equal(t, [][]byte{nil, []byte("page2"), nil}, cursors)
	require.NoError(t, c.flowCtx.Err())
}

type writeUpdatesHandler struct {
	adiomv1connect.UnimplementedConnectorServiceHandler
	calls *[][]*adiomv1.Update
}

func (h writeUpdatesHandler) WriteUpdates(_ context.Context, r *connect.Request[adiomv1.WriteUpdatesRequest]) (*connect.Response[adiomv1.WriteUpdatesResponse], error) {
	for _, u := range r.Msg.GetUpdates() {
		if u.GetType() == adiomv1.UpdateType_UPDATE_TYPE_PARTIAL_UPDATE {
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("partial updates are not supported"))
		}
	}
	*h.calls = append(*h.calls, r.Msg.GetUpdates())
	return connect.NewResponse(&adiomv1.WriteUpdatesResponse{}), nil
}

func writerConnector(t *testing.T) (*connector, *[][]*adiomv1.Update) {
	var calls [][]*adiomv1.Update
	_, handler := adiomv1connect.NewConnectorServiceHandler(writeUpdatesHandler{calls: &calls})
	srv, err := memhttp.New(handler)
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Close() })
	client := adiomv1connect.NewConnectorServiceClient(srv.Client(), srv.URL())
	var status iface.ConnectorStatus
	return &connector{
		flowCtx:            t.Context(),
		impl:               client,
		maybeOptimizedImpl: client,
		progressTracker:    NewProgressTracker(&status, t.Context()),
	}, &calls
}

func TestProcessDataMessagesSkipsEmptyPartialAndAdvancesWriteLSN(t *testing.T) {
	c, calls := writerConnector(t)
	var empty []byte

	require.NoError(t, c.ProcessDataMessages([]iface.DataMessage{{MutationType: iface.MutationType_Apply, Data: &empty, Loc: "db.col", SeqNum: 7}}))

	require.Empty(t, *calls)
	require.Equal(t, int64(7), c.progressTracker.CopyStatus().WriteLSN)
}

func TestProcessDataMessagesDropsEmptyPartialBetweenWrites(t *testing.T) {
	c, calls := writerConnector(t)
	a := []byte("a")
	var empty []byte

	require.NoError(t, c.ProcessDataMessages([]iface.DataMessage{
		{MutationType: iface.MutationType_Insert, Data: &a, Loc: "db.col", SeqNum: 1},
		{MutationType: iface.MutationType_Apply, Data: &empty, Loc: "db.col", SeqNum: 2},
		{MutationType: iface.MutationType_Delete, Loc: "db.col", SeqNum: 3},
	}))

	require.Len(t, *calls, 1)
	var types []adiomv1.UpdateType
	for _, u := range (*calls)[0] {
		types = append(types, u.GetType())
	}
	require.Equal(t, []adiomv1.UpdateType{adiomv1.UpdateType_UPDATE_TYPE_INSERT, adiomv1.UpdateType_UPDATE_TYPE_DELETE}, types)
	require.Equal(t, int64(3), c.progressTracker.CopyStatus().WriteLSN)
}

type cdcSourceHandler struct {
	adiomv1connect.UnimplementedConnectorServiceHandler
	updates []*adiomv1.Update
}

func (cdcSourceHandler) StreamLSN(context.Context, *connect.Request[adiomv1.StreamLSNRequest], *connect.ServerStream[adiomv1.StreamLSNResponse]) error {
	return nil
}

func (h cdcSourceHandler) StreamUpdates(_ context.Context, _ *connect.Request[adiomv1.StreamUpdatesRequest], s *connect.ServerStream[adiomv1.StreamUpdatesResponse]) error {
	return s.Send(&adiomv1.StreamUpdatesResponse{Namespace: "db.col", Updates: h.updates})
}

type recordingTransform struct {
	adiomv1connect.UnimplementedTransformServiceHandler
	requests chan<- []*adiomv1.Update
}

func (h recordingTransform) GetTransform(_ context.Context, r *connect.Request[adiomv1.GetTransformRequest]) (*connect.Response[adiomv1.GetTransformResponse], error) {
	h.requests <- r.Msg.GetUpdates()
	return connect.NewResponse(&adiomv1.GetTransformResponse{Namespace: r.Msg.GetNamespace(), Updates: r.Msg.GetUpdates()}), nil
}

func runCDCWithTransform(t *testing.T, updates []*adiomv1.Update) ([]iface.DataMessage, [][]*adiomv1.Update) {
	_, handler := adiomv1connect.NewConnectorServiceHandler(cdcSourceHandler{updates: updates})
	srv, err := memhttp.New(handler)
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Close() })
	requests := make(chan []*adiomv1.Update, 4)
	_, transformHandler := adiomv1connect.NewTransformServiceHandler(recordingTransform{requests: requests})
	transformSrv, err := memhttp.New(transformHandler)
	require.NoError(t, err)
	t.Cleanup(func() { _ = transformSrv.Close() })
	client := adiomv1connect.NewConnectorServiceClient(srv.Client(), srv.URL())
	dataChannel := make(chan iface.DataMessage, 16)
	transport := mocks.NewTransport(t)
	transport.On("GetDataChannelEndpoint", mock.Anything).Return(dataChannel, nil)
	coord := mocks.NewCoordinator(t)
	done := make(chan struct{})
	coord.On("NotifyDone", mock.Anything, mock.Anything).Return(nil).Once().Run(func(mock.Arguments) { close(done) })
	var status iface.ConnectorStatus
	c := &connector{
		ctx:                t.Context(),
		coord:              coord,
		t:                  transport,
		impl:               client,
		maybeOptimizedImpl: client,
		progressTracker:    NewProgressTracker(&status, t.Context()),
		settings: ConnectorSettings{
			ResumeTokenUpdateInterval: time.Hour,
			TransformClient:           adiomv1connect.NewTransformServiceClient(transformSrv.Client(), transformSrv.URL()),
		},
	}
	require.NoError(t, c.StartReadToChannel("flow", iface.ConnectorOptions{Mode: iface.SyncModeCDC}, iface.ConnectorReadPlan{}, "data"))
	var msgs []iface.DataMessage
	for m := range dataChannel {
		if m.MutationType != iface.MutationType_Barrier {
			msgs = append(msgs, m)
		}
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("NotifyDone not called")
	}
	close(requests)
	var transformed [][]*adiomv1.Update
	for r := range requests {
		transformed = append(transformed, r)
	}
	return msgs, transformed
}

func TestCDCTransformSkipsEmptyPartialButCountsItInLSN(t *testing.T) {
	msgs, transformed := runCDCWithTransform(t, []*adiomv1.Update{
		{Id: []*adiomv1.BsonValue{{Data: []byte("a")}}, Type: adiomv1.UpdateType_UPDATE_TYPE_INSERT, Data: []byte("a")},
		{Id: []*adiomv1.BsonValue{{Data: []byte("b")}}, Type: adiomv1.UpdateType_UPDATE_TYPE_PARTIAL_UPDATE},
		{Id: []*adiomv1.BsonValue{{Data: []byte("c")}}, Type: adiomv1.UpdateType_UPDATE_TYPE_INSERT, Data: []byte("c")},
	})

	require.Len(t, transformed, 1)
	require.Len(t, transformed[0], 2)
	require.Len(t, msgs, 2)
	for _, m := range msgs {
		require.Equal(t, int64(3), m.SeqNum)
	}
}

func TestCDCTransformNotCalledForOnlyEmptyPartials(t *testing.T) {
	msgs, transformed := runCDCWithTransform(t, []*adiomv1.Update{
		{Id: []*adiomv1.BsonValue{{Data: []byte("b")}}, Type: adiomv1.UpdateType_UPDATE_TYPE_PARTIAL_UPDATE},
	})

	require.Empty(t, transformed)
	require.Len(t, msgs, 1)
	require.Equal(t, iface.MutationType_Apply, msgs[0].MutationType)
	require.Equal(t, int64(1), msgs[0].SeqNum)
}
