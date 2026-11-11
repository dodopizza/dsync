package mongo

import (
	"context"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// The mocked tests cannot show that a real server reports the conflicting key
// in its write errors or that its index listing matches it.
func TestMongoServerDeleteUniqueConflicts(t *testing.T) {
	uri := os.Getenv("MONGO_UNIQUE_CONFLICT_TEST_URI")
	if uri == "" {
		t.Skip("set MONGO_UNIQUE_CONFLICT_TEST_URI to run")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetServerSelectionTimeout(10 * time.Second))
	require.NoError(t, err)
	defer client.Disconnect(context.Background())
	db := client.Database(fmt.Sprintf("dsync_unique_conflict_%d", time.Now().UnixNano()))
	defer db.Drop(context.Background())

	sink := func(deleteUniqueConflicts bool) *conn {
		handler, err := NewConnWithClient(client, ConnectorSettings{DeleteUniqueConflicts: deleteUniqueConflicts})
		require.NoError(t, err)
		return handler.(*conn)
	}
	seed := func(t *testing.T, index mongo.IndexModel, documents ...bson.D) *mongo.Collection {
		col := db.Collection(fmt.Sprintf("c%d", time.Now().UnixNano()))
		_, err := col.Indexes().CreateOne(ctx, index)
		require.NoError(t, err)
		_, err = col.InsertMany(ctx, documents)
		require.NoError(t, err)
		return col
	}
	ids := func(t *testing.T, col *mongo.Collection) []string {
		cursor, err := col.Find(ctx, bson.D{})
		require.NoError(t, err)
		var documents []struct {
			ID string `bson:"_id"`
		}
		require.NoError(t, cursor.All(ctx, &documents))
		found := make([]string, len(documents))
		for i, document := range documents {
			found[i] = document.ID
		}
		slices.Sort(found)
		return found
	}
	namespace := func(col *mongo.Collection) string { return db.Name() + "." + col.Name() }
	streamWrite := func(c *conn, col *mongo.Collection, document bson.M) error {
		_, err := c.WriteUpdates(ctx, connect.NewRequest(&adiomv1.WriteUpdatesRequest{
			Namespace: namespace(col), Updates: []*adiomv1.Update{insertU(t, document["_id"].(string), document)},
		}))
		return err
	}
	copyWrite := func(c *conn, col *mongo.Collection, document bson.M) error {
		_, err := c.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
			Namespace: namespace(col), Data: [][]byte{mustMarshal(t, document)},
		}))
		return err
	}

	uniqueClientID := mongo.IndexModel{Keys: bson.D{{Key: "ClientId", Value: 1}}, Options: options.Index().SetUnique(true)}
	stale := bson.D{{Key: "_id", Value: "stale"}, {Key: "ClientId", Value: "c-1"}}
	bystander := bson.D{{Key: "_id", Value: "bystander"}, {Key: "ClientId", Value: "c-2"}}
	fresh := bson.M{"_id": "fresh", "ClientId": "c-1"}

	t.Run("stream write replaces the stale holder", func(t *testing.T) {
		col := seed(t, uniqueClientID, stale, bystander)
		require.NoError(t, streamWrite(sink(true), col, fresh))
		require.Equal(t, []string{"bystander", "fresh"}, ids(t, col))
	})

	t.Run("copy overwrite replaces the stale holder", func(t *testing.T) {
		col := seed(t, uniqueClientID, stale, bystander)
		require.NoError(t, copyWrite(sink(true), col, fresh))
		require.Equal(t, []string{"bystander", "fresh"}, ids(t, col))
	})

	t.Run("without the flag the conflict fails and nothing is deleted", func(t *testing.T) {
		col := seed(t, uniqueClientID, stale, bystander)
		require.ErrorContains(t, streamWrite(sink(false), col, fresh), "E11000")
		require.ErrorContains(t, copyWrite(sink(false), col, fresh), "E11000")
		require.Equal(t, []string{"bystander", "stale"}, ids(t, col))
	})

	t.Run("same _id is overwritten without deleting anything", func(t *testing.T) {
		col := seed(t, uniqueClientID, bson.D{{Key: "_id", Value: "fresh"}, {Key: "ClientId", Value: "c-1"}, {Key: "Name", Value: "old"}}, bystander)
		require.NoError(t, copyWrite(sink(true), col, bson.M{"_id": "fresh", "ClientId": "c-1", "Name": "new"}))
		require.Equal(t, []string{"bystander", "fresh"}, ids(t, col))
		var written struct{ Name string }
		require.NoError(t, col.FindOne(ctx, bson.D{{Key: "_id", Value: "fresh"}}).Decode(&written))
		require.Equal(t, "new", written.Name)
	})

	t.Run("compound unique index", func(t *testing.T) {
		index := mongo.IndexModel{Keys: bson.D{{Key: "Tenant", Value: 1}, {Key: "Code", Value: -1}}, Options: options.Index().SetUnique(true)}
		col := seed(t, index,
			bson.D{{Key: "_id", Value: "stale"}, {Key: "Tenant", Value: "t"}, {Key: "Code", Value: "x"}},
			bson.D{{Key: "_id", Value: "bystander"}, {Key: "Tenant", Value: "t"}, {Key: "Code", Value: "y"}})
		require.NoError(t, streamWrite(sink(true), col, bson.M{"_id": "fresh", "Tenant": "t", "Code": "x"}))
		require.Equal(t, []string{"bystander", "fresh"}, ids(t, col))
	})

	t.Run("partial unique index is left alone", func(t *testing.T) {
		index := mongo.IndexModel{Keys: bson.D{{Key: "ClientId", Value: 1}}, Options: options.Index().SetUnique(true).
			SetPartialFilterExpression(bson.D{{Key: "Active", Value: true}})}
		col := seed(t, index,
			bson.D{{Key: "_id", Value: "stale"}, {Key: "ClientId", Value: "c-1"}, {Key: "Active", Value: true}},
			bson.D{{Key: "_id", Value: "inactive"}, {Key: "ClientId", Value: "c-1"}, {Key: "Active", Value: false}})
		require.ErrorContains(t, streamWrite(sink(true), col, bson.M{"_id": "fresh", "ClientId": "c-1", "Active": true}), "E11000")
		require.Equal(t, []string{"inactive", "stale"}, ids(t, col))
	})

	t.Run("unique index with a collation is left alone", func(t *testing.T) {
		index := mongo.IndexModel{Keys: bson.D{{Key: "ClientId", Value: 1}}, Options: options.Index().SetUnique(true).
			SetCollation(&options.Collation{Locale: "en", Strength: 2})}
		col := seed(t, index, bson.D{{Key: "_id", Value: "stale"}, {Key: "ClientId", Value: "C-1"}})
		require.ErrorContains(t, streamWrite(sink(true), col, fresh), "E11000")
		require.Equal(t, []string{"stale"}, ids(t, col))
	})
}
