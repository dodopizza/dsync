package dsync

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"
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
