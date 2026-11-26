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

// The mocked tests show the commands sent; only a real server shows what the
// collection holds after a write that missed a document under another shard key value.
func TestMongoServerReplaceIDConflicts(t *testing.T) {
	uri := os.Getenv("MONGO_UNIQUE_CONFLICT_TEST_URI")
	if uri == "" {
		t.Skip("set MONGO_UNIQUE_CONFLICT_TEST_URI to run")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetServerSelectionTimeout(10 * time.Second))
	require.NoError(t, err)
	defer client.Disconnect(context.Background())
	db := client.Database(fmt.Sprintf("dsync_id_conflict_%d", time.Now().UnixNano()))
	defer db.Drop(context.Background())

	type stored struct {
		ID          string `bson:"_id"`
		PartitionId int    `bson:"PartitionId"`
		V           string `bson:"V"`
	}
	sink := func(settings ConnectorSettings) *conn {
		handler, err := NewConnWithClient(client, settings)
		require.NoError(t, err)
		return handler.(*conn)
	}
	seed := func(t *testing.T, index *mongo.IndexModel, documents ...any) *mongo.Collection {
		col := db.Collection(fmt.Sprintf("c%d", time.Now().UnixNano()))
		require.NoError(t, db.CreateCollection(ctx, col.Name()))
		if index != nil {
			_, err := col.Indexes().CreateOne(ctx, *index)
			require.NoError(t, err)
		}
		if len(documents) > 0 {
			_, err := col.InsertMany(ctx, documents)
			require.NoError(t, err)
		}
		return col
	}
	contents := func(t *testing.T, col *mongo.Collection) []stored {
		cursor, err := col.Find(ctx, bson.D{}, options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}))
		require.NoError(t, err)
		var documents []stored
		require.NoError(t, cursor.All(ctx, &documents))
		return documents
	}
	write := func(c *conn, col *mongo.Collection, updates ...*adiomv1.Update) error {
		_, err := c.WriteUpdates(ctx, connect.NewRequest(&adiomv1.WriteUpdatesRequest{Namespace: db.Name() + "." + col.Name(), Updates: updates}))
		return err
	}
	full := func(id string, partition int, v string) *adiomv1.Update {
		return &adiomv1.Update{
			Id:   []*adiomv1.BsonValue{bsonID(t, "_id", id), bsonID(t, "PartitionId", partition)},
			Type: adiomv1.UpdateType_UPDATE_TYPE_INSERT,
			Data: mustMarshal(t, bson.M{"_id": id, "PartitionId": partition, "V": v}),
		}
	}
	doc := func(id string, partition int, v string) bson.D {
		return bson.D{{Key: "_id", Value: id}, {Key: "PartitionId", Value: partition}, {Key: "V", Value: v}}
	}
	on := ConnectorSettings{FullDocumentKey: true, ReplaceIDConflicts: true}

	t.Run("a write under a new shard key value replaces the holder", func(t *testing.T) {
		col := seed(t, nil, doc("a", 1, "old"), doc("b", 1, "bystander"))
		require.NoError(t, write(sink(on), col, full("a", 2, "new")))
		require.Equal(t, []stored{{"a", 2, "new"}, {"b", 1, "bystander"}}, contents(t, col))
	})

	t.Run("the newest write of an _id in a batch wins", func(t *testing.T) {
		col := seed(t, nil)
		require.NoError(t, write(sink(on), col, full("a", 1, "older"), full("a", 2, "newer")))
		require.Equal(t, []stored{{"a", 2, "newer"}}, contents(t, col))
	})

	t.Run("the newest write wins over a third copy", func(t *testing.T) {
		col := seed(t, nil, doc("a", 3, "third"))
		require.NoError(t, write(sink(on), col, full("a", 1, "older"), full("a", 2, "newer")))
		require.Equal(t, []stored{{"a", 2, "newer"}}, contents(t, col))
	})

	t.Run("a partial update leaves no fragment", func(t *testing.T) {
		col := seed(t, nil, doc("a", 1, "old"))
		partial := &adiomv1.Update{
			Id:   []*adiomv1.BsonValue{bsonID(t, "_id", "a"), bsonID(t, "PartitionId", 2)},
			Type: adiomv1.UpdateType_UPDATE_TYPE_PARTIAL_UPDATE,
			Data: mustMarshal(t, bson.M{"V": "new"}),
		}
		require.ErrorContains(t, write(sink(on), col, partial), "E11000")
		require.Equal(t, []stored{{"a", 1, "old"}}, contents(t, col))
	})

	t.Run("without the flag the conflict fails and nothing changes", func(t *testing.T) {
		col := seed(t, nil, doc("a", 1, "old"))
		require.ErrorContains(t, write(sink(ConnectorSettings{FullDocumentKey: true}), col, full("a", 2, "new")), "E11000")
		require.Equal(t, []stored{{"a", 1, "old"}}, contents(t, col))
	})

	t.Run("an _id conflict and a unique key conflict in one batch", func(t *testing.T) {
		index := mongo.IndexModel{Keys: bson.D{{Key: "ClientId", Value: 1}}, Options: options.Index().SetUnique(true)}
		col := seed(t, &index, doc("a", 1, "old"), bson.D{{Key: "_id", Value: "stale"}, {Key: "ClientId", Value: "c-1"}})
		settings := on
		settings.DeleteUniqueConflicts = true
		other := insertU(t, "other", bson.M{"_id": "other", "ClientId": "c-1"})
		require.NoError(t, write(sink(settings), col, full("a", 2, "new"), other))
		require.Equal(t, []stored{{"a", 2, "new"}, {ID: "other"}}, contents(t, col))
	})
}
