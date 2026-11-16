package dsync

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/adiom-data/dsync/connectors/common"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"github.com/stretchr/testify/require"
	"go.akshayshah.org/memhttp"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type updatesSource struct {
	adiomv1connect.UnimplementedConnectorServiceHandler
	updates []*adiomv1.Update
}

func (h updatesSource) StreamUpdates(_ context.Context, _ *connect.Request[adiomv1.StreamUpdatesRequest], s *connect.ServerStream[adiomv1.StreamUpdatesResponse]) error {
	return s.Send(&adiomv1.StreamUpdatesResponse{Namespace: "db.col", Updates: h.updates})
}

func readUpdates(t *testing.T, updates ...*adiomv1.Update) ([]string, error) {
	_, handler := adiomv1connect.NewConnectorServiceHandler(updatesSource{updates: updates})
	srv, err := memhttp.New(handler)
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	s := &source{c: adiomv1connect.NewConnectorServiceClient(srv.Client(), srv.URL())}
	ch := make(chan Update, len(updates))

	err = s.readUpdates(ctx, []*adiomv1.UpdatesPartition{{Namespaces: []string{"db.col"}}}, ch)
	close(ch)

	var ids []string
	for u := range ch {
		ids = append(ids, string(u.ID[0].Value))
	}
	return ids, err
}

func doc(t *testing.T, v int) []byte {
	b, err := bson.Marshal(bson.D{{Key: "f", Value: v}})
	require.NoError(t, err)
	return b
}

func TestReadUpdatesSkipsEmptyPartialUpdate(t *testing.T) {
	ids, err := readUpdates(t,
		&adiomv1.Update{Id: []*adiomv1.BsonValue{{Data: []byte("a")}}, Type: adiomv1.UpdateType_UPDATE_TYPE_INSERT, Data: doc(t, 1)},
		&adiomv1.Update{Id: []*adiomv1.BsonValue{{Data: []byte("b")}}, Type: adiomv1.UpdateType_UPDATE_TYPE_PARTIAL_UPDATE},
		&adiomv1.Update{Id: []*adiomv1.BsonValue{{Data: []byte("c")}}, Type: adiomv1.UpdateType_UPDATE_TYPE_DELETE},
	)

	require.NoError(t, err)
	require.Equal(t, []string{"a", "c"}, ids)
}

func TestReadUpdatesRejectsPartialUpdates(t *testing.T) {
	for name, u := range map[string]*adiomv1.Update{
		"set":       {Data: doc(t, 2)},
		"unset":     {PartialUpdateUnset: []string{"f"}},
		"set+unset": {Data: doc(t, 2), PartialUpdateUnset: []string{"g"}},
	} {
		t.Run(name, func(t *testing.T) {
			u.Id = []*adiomv1.BsonValue{{Data: []byte("b")}}
			u.Type = adiomv1.UpdateType_UPDATE_TYPE_PARTIAL_UPDATE

			_, err := readUpdates(t, u)

			require.ErrorContains(t, err, "partial update")
		})
	}
}

type listingSource struct {
	adiomv1connect.UnimplementedConnectorServiceHandler
	docs    [][]byte
	updates []*adiomv1.Update

	skipInitialSync bool
	skipStream      bool

	listErr     error
	streamBlock bool
	streamErr   error
	started     *atomic.Int32
	returned    *atomic.Int32
}

func (h listingSource) GetInfo(context.Context, *connect.Request[adiomv1.GetInfoRequest]) (*connect.Response[adiomv1.GetInfoResponse], error) {
	return connect.NewResponse(&adiomv1.GetInfoResponse{Capabilities: &adiomv1.Capabilities{Source: &adiomv1.Capabilities_Source{
		SupportedDataTypes: []adiomv1.DataType{adiomv1.DataType_DATA_TYPE_MONGO_BSON},
		DefaultPlan:        true,
	}}}), nil
}

func (h listingSource) GeneratePlan(context.Context, *connect.Request[adiomv1.GeneratePlanRequest]) (*connect.Response[adiomv1.GeneratePlanResponse], error) {
	return connect.NewResponse(&adiomv1.GeneratePlanResponse{
		Partitions:        []*adiomv1.Partition{{Namespace: "db.col"}},
		UpdatesPartitions: []*adiomv1.UpdatesPartition{{Namespaces: []string{"db.col"}}},
	}), nil
}

func (h listingSource) ListData(context.Context, *connect.Request[adiomv1.ListDataRequest]) (*connect.Response[adiomv1.ListDataResponse], error) {
	if h.listErr != nil {
		return nil, h.listErr
	}
	return connect.NewResponse(&adiomv1.ListDataResponse{Data: h.docs}), nil
}

func (h listingSource) StreamUpdates(ctx context.Context, _ *connect.Request[adiomv1.StreamUpdatesRequest], s *connect.ServerStream[adiomv1.StreamUpdatesResponse]) error {
	if h.started != nil {
		h.started.Add(1)
	}
	if h.returned != nil {
		defer h.returned.Add(1)
	}
	if err := s.Send(&adiomv1.StreamUpdatesResponse{Namespace: "db.col", Updates: h.updates}); err != nil {
		return err
	}
	if h.streamBlock {
		<-ctx.Done()
	}
	return h.streamErr
}

func newTestSource(t *testing.T, h listingSource, total, partition int) *source {
	_, handler := adiomv1connect.NewConnectorServiceHandler(h)
	srv, err := memhttp.New(handler)
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Close() })
	return &source{
		c:               adiomv1connect.NewConnectorServiceClient(srv.Client(), srv.URL()),
		initialSyncDone: make(chan struct{}),
		parallelism:     1,
		totalPartitions: total,
		partition:       partition,
		skipInitialSync: h.skipInitialSync,
		skipStream:      h.skipStream,
	}
}

func runWithDeadline(t *testing.T, run func(context.Context) error) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- run(ctx) }()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("did not return in time")
		return nil
	}
}

func processSource(t *testing.T, h listingSource, total, partition int) ([]Update, error) {
	var got []Update
	err := processSourceWith(t, h, total, partition, func(_ context.Context, u Update) error {
		got = append(got, u)
		return nil
	})
	return got, err
}

func processSourceWith(t *testing.T, h listingSource, total, partition int, process func(context.Context, Update) error) error {
	s := newTestSource(t, h, total, partition)
	return runWithDeadline(t, func(ctx context.Context) error { return s.ProcessSource(ctx, process) })
}

func idPart(t *testing.T, name string, v any) *adiomv1.BsonValue {
	typ, data, err := bson.MarshalValue(v)
	require.NoError(t, err)
	return &adiomv1.BsonValue{Name: name, Type: uint32(typ), Data: data}
}

func TestUpdateKey(t *testing.T) {
	id := idPart(t, "_id", "k")
	shard := idPart(t, "tenant", "t")
	unnamedA := idPart(t, "", "a")
	unnamedB := idPart(t, "", "b")
	want := func(parts ...*adiomv1.BsonValue) []bson.RawValue {
		var out []bson.RawValue
		for _, p := range parts {
			out = append(out, bson.RawValue{Type: bson.Type(p.Type), Value: p.Data})
		}
		return out
	}

	require.Equal(t, want(id), updateKey([]*adiomv1.BsonValue{id}))
	require.Equal(t, want(id), updateKey([]*adiomv1.BsonValue{shard, id}))
	require.Equal(t, want(id), updateKey([]*adiomv1.BsonValue{id, shard}))
	require.Equal(t, want(unnamedA, unnamedB), updateKey([]*adiomv1.BsonValue{unnamedA, unnamedB}))
	require.Nil(t, updateKey(nil))
}

func TestStreamAndInitialPassShareKeyAndPartition(t *testing.T) {
	body, err := bson.Marshal(bson.D{{Key: "_id", Value: "k"}, {Key: "tenant", Value: "t"}})
	require.NoError(t, err)
	for name, parts := range map[string][]*adiomv1.BsonValue{
		"shard first": {idPart(t, "tenant", "t"), idPart(t, "_id", "k")},
		"id first":    {idPart(t, "_id", "k"), idPart(t, "tenant", "t")},
	} {
		t.Run(name, func(t *testing.T) {
			h := listingSource{docs: [][]byte{body}, updates: []*adiomv1.Update{{Id: parts, Type: adiomv1.UpdateType_UPDATE_TYPE_UPDATE, Data: body}}}
			all, err := processSource(t, h, 0, 0)
			require.NoError(t, err)
			require.Len(t, all, 2)
			initialKey, err := common.IDToString(all[0].ID)
			require.NoError(t, err)
			streamKey, err := common.IDToString(all[1].ID)
			require.NoError(t, err)
			require.Equal(t, initialKey, streamKey)

			counts := make([]int, 2)
			for p := range counts {
				got, err := processSource(t, h, 2, p)
				require.NoError(t, err)
				counts[p] = len(got)
			}
			require.ElementsMatch(t, []int{0, 2}, counts)
		})
	}
}

func TestInitialPassRejectsDocumentWithoutID(t *testing.T) {
	body, err := bson.Marshal(bson.D{{Key: "secret", Value: "content"}})
	require.NoError(t, err)

	_, err = processSource(t, listingSource{docs: [][]byte{body}}, 0, 0)

	require.ErrorContains(t, err, "db.col")
	require.NotContains(t, err.Error(), "content")
}

func TestProcessSourceReturnsProcessErrorInsteadOfHanging(t *testing.T) {
	a, err := bson.Marshal(bson.D{{Key: "_id", Value: "a"}})
	require.NoError(t, err)
	b, err := bson.Marshal(bson.D{{Key: "_id", Value: "b"}})
	require.NoError(t, err)
	failing := errors.New("process failed")
	for name, h := range map[string]listingSource{
		"initial pass": {docs: [][]byte{a, b}},
		"stream": {updates: []*adiomv1.Update{
			{Id: []*adiomv1.BsonValue{idPart(t, "_id", "a")}, Type: adiomv1.UpdateType_UPDATE_TYPE_UPDATE, Data: a},
			{Id: []*adiomv1.BsonValue{idPart(t, "_id", "b")}, Type: adiomv1.UpdateType_UPDATE_TYPE_UPDATE, Data: b},
		}},
	} {
		t.Run(name, func(t *testing.T) {
			h.skipInitialSync = name == "stream"
			h.skipStream = name == "initial pass"
			err := processSourceWith(t, h, 0, 0, func(context.Context, Update) error { return failing })

			require.ErrorIs(t, err, failing)
		})
	}
}

func TestStreamUpdateWithoutIDIsAnErrorEvenWhenPartitioned(t *testing.T) {
	body, err := bson.Marshal(bson.D{{Key: "_id", Value: "k"}})
	require.NoError(t, err)
	h := listingSource{skipInitialSync: true, updates: []*adiomv1.Update{{Type: adiomv1.UpdateType_UPDATE_TYPE_UPDATE, Data: body}}}

	for _, total := range []int{0, 2} {
		_, err := processSource(t, h, total, 0)

		require.ErrorContains(t, err, "db.col")
	}
}

func TestTailKeepsUpdatesDifferingOnlyInTheSecondIDPartApart(t *testing.T) {
	std := &SimpleTailDiffer{m: map[uint64]tailDiff{}}
	process := processTail(std, nil, nil, true)
	first := idPart(t, "", "shared")
	for _, second := range []string{"x", "y"} {
		ids := updateKey([]*adiomv1.BsonValue{first, idPart(t, "", second)})
		require.NoError(t, process(t.Context(), Update{Namespace: "db.col", ID: ids}))
	}

	require.Len(t, std.GetAndClean(time.Now().Add(time.Hour)), 2)
}
