package cosmos

import (
	"errors"
	"testing"

	mongoconn "github.com/adiom-data/dsync/connectors/mongo"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestUpdateFromEventFailsOnDecodeError(t *testing.T) {
	update, err := updateFromEvent(func(any) error { return errors.New("bad bson") }, nil)

	require.ErrorContains(t, err, "bad bson")
	require.Nil(t, update)
}

func TestUpdateFromEventFailsOnMissingDocumentKey(t *testing.T) {
	update, err := updateFromEvent(func(v any) error {
		*(v.(*mongoconn.MongoUpdate)) = mongoconn.MongoUpdate{FullDocument: bson.Raw{}}
		return nil
	}, nil)

	require.Error(t, err)
	require.Nil(t, update)
}

func TestUpdateFromEventWithoutFullDocumentIsEmptyPartialUpdate(t *testing.T) {
	update, err := updateFromEvent(func(v any) error {
		*(v.(*mongoconn.MongoUpdate)) = mongoconn.MongoUpdate{DocumentKey: bson.D{{Key: "_id", Value: "a"}}}
		return nil
	}, nil)

	require.NoError(t, err)
	require.NotNil(t, update)
	require.Equal(t, adiomv1.UpdateType_UPDATE_TYPE_PARTIAL_UPDATE, update.GetType())
	require.Len(t, update.GetId(), 1)
	require.Empty(t, update.GetData())
}

func TestDeleteUpdateRejectsUnmarshalableID(t *testing.T) {
	update, err := deleteUpdate(make(chan int))

	require.Error(t, err)
	require.Nil(t, update)

	update, err = deleteUpdate("a")
	require.NoError(t, err)
	require.Equal(t, adiomv1.UpdateType_UPDATE_TYPE_DELETE, update.GetType())
}
