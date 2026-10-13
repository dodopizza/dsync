package dsync

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"github.com/stretchr/testify/require"
)

type updatesSource struct {
	adiomv1connect.UnimplementedConnectorServiceHandler
	updates []*adiomv1.Update
}

func (h updatesSource) StreamUpdates(_ context.Context, _ *connect.Request[adiomv1.StreamUpdatesRequest], s *connect.ServerStream[adiomv1.StreamUpdatesResponse]) error {
	return s.Send(&adiomv1.StreamUpdatesResponse{Namespace: "db.col", Updates: h.updates})
}

func TestReadUpdatesSkipsPartialUpdates(t *testing.T) {
	s := &source{c: connClient(updatesSource{updates: []*adiomv1.Update{
		{Id: []*adiomv1.BsonValue{{Data: []byte("a")}}, Type: adiomv1.UpdateType_UPDATE_TYPE_INSERT, Data: []byte("a")},
		{Id: []*adiomv1.BsonValue{{Data: []byte("b")}}, Type: adiomv1.UpdateType_UPDATE_TYPE_PARTIAL_UPDATE, PartialUpdateUnset: []string{"f"}},
		{Id: []*adiomv1.BsonValue{{Data: []byte("c")}}, Type: adiomv1.UpdateType_UPDATE_TYPE_PARTIAL_UPDATE, Data: []byte("set")},
		{Id: []*adiomv1.BsonValue{{Data: []byte("d")}}, Type: adiomv1.UpdateType_UPDATE_TYPE_DELETE},
	}})}
	ch := make(chan Update, 8)

	require.NoError(t, s.readUpdates(t.Context(), []*adiomv1.UpdatesPartition{{Namespaces: []string{"db.col"}}}, ch))
	close(ch)

	var ids []string
	for u := range ch {
		ids = append(ids, string(u.ID[0].Value))
	}
	require.Equal(t, []string{"a", "d"}, ids)
}
