package cosmos

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"

	mongoconn "github.com/adiom-data/dsync/connectors/mongo"
	"github.com/adiom-data/dsync/connectors/util"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func eventUpdate(t *testing.T, op string, doc bson.D, fields []string) (*adiomv1.Update, error) {
	t.Helper()
	change := mongoconn.MongoUpdate{
		OperationType: op,
		DocumentKey:   bson.D{{Key: "_id", Value: "doc1"}},
	}
	if doc != nil {
		change.FullDocument = mustMarshalBSON(t, doc)
	}
	return updateFromEvent(func(v any) error {
		*(v.(*mongoconn.MongoUpdate)) = change
		return nil
	}, fields)
}

func idNames(update *adiomv1.Update) []string {
	var names []string
	for _, part := range update.GetId() {
		names = append(names, part.GetName())
	}
	return names
}

func idValue(t *testing.T, part *adiomv1.BsonValue) any {
	t.Helper()
	var v any
	require.NoError(t, bson.UnmarshalValue(bson.Type(part.GetType()), part.GetData(), &v))
	return v
}

var shardedDoc = bson.D{
	{Key: "_id", Value: "doc1"},
	{Key: "tenant", Value: "t1"},
	{Key: "region", Value: int32(7)},
	{Key: "n", Value: bson.D{{Key: "k", Value: "deep"}}},
}

func TestEventsWithoutShardKeyFieldsKeepIdOnly(t *testing.T) {
	for _, op := range []string{"insert", "update", "replace"} {
		update, err := eventUpdate(t, op, shardedDoc, nil)

		require.NoError(t, err)
		require.Equal(t, []string{"_id"}, idNames(update))
		require.Equal(t, mustMarshalBSON(t, shardedDoc), bson.Raw(update.GetData()))
	}
}

func TestEventsWithShardKeyFieldsPutIdFirstThenShardKeyInOrder(t *testing.T) {
	for _, op := range []string{"insert", "update", "replace"} {
		update, err := eventUpdate(t, op, shardedDoc, []string{"region", "tenant"})

		require.NoError(t, err)
		require.Equal(t, []string{"_id", "region", "tenant"}, idNames(update))
		require.Equal(t, "doc1", idValue(t, update.GetId()[0]))
		require.Equal(t, int32(7), idValue(t, update.GetId()[1]))
		require.Equal(t, "t1", idValue(t, update.GetId()[2]))
	}
}

func TestNestedShardKeyPathIsLookedUpAndNamedByItsFullPath(t *testing.T) {
	update, err := eventUpdate(t, "update", shardedDoc, []string{"n.k"})

	require.NoError(t, err)
	require.Equal(t, []string{"_id", "n.k"}, idNames(update))
	require.Equal(t, "deep", idValue(t, update.GetId()[1]))
}

func TestMissingShardKeyFieldFailsNamingTheField(t *testing.T) {
	update, err := eventUpdate(t, "insert", bson.D{{Key: "_id", Value: "doc1"}}, []string{"tenant"})

	require.ErrorContains(t, err, "tenant")
	require.Nil(t, update)
}

func eventWithKey(t *testing.T, op string, doc bson.D, key bson.D, fields []string) (*adiomv1.Update, error) {
	t.Helper()
	change := mongoconn.MongoUpdate{OperationType: op, DocumentKey: key}
	if doc != nil {
		change.FullDocument = mustMarshalBSON(t, doc)
	}
	return updateFromEvent(func(v any) error {
		*(v.(*mongoconn.MongoUpdate)) = change
		return nil
	}, fields)
}

var shardedKey = bson.D{{Key: "_id", Value: "doc1"}, {Key: "tenant", Value: "t1"}, {Key: "region", Value: int32(7)}}

func TestUpsertAndEventWithoutFullDocumentCarryTheSameId(t *testing.T) {
	fields := []string{"region", "tenant"}
	upsert, err := eventWithKey(t, "update", shardedDoc, shardedKey, fields)
	require.NoError(t, err)

	for _, op := range []string{"update", "replace", "delete"} {
		other, err := eventWithKey(t, op, nil, shardedKey, fields)

		require.NoError(t, err)
		require.Equal(t, util.BsonIdKey(upsert.GetId()), util.BsonIdKey(other.GetId()), op)
		require.Equal(t, []string{"_id", "region", "tenant"}, idNames(other))
	}
}

func TestShardKeyFieldMissingFromFullDocumentComesFromDocumentKey(t *testing.T) {
	update, err := eventWithKey(t, "update", bson.D{{Key: "_id", Value: "doc1"}}, shardedKey, []string{"tenant"})

	require.NoError(t, err)
	require.Equal(t, "t1", idValue(t, update.GetId()[1]))
}

func TestShardKeyFieldMissingEverywhereFailsNamingTheField(t *testing.T) {
	for _, doc := range []bson.D{nil, {{Key: "_id", Value: "doc1"}}} {
		update, err := eventWithKey(t, "update", doc, bson.D{{Key: "_id", Value: "doc1"}}, []string{"tenant"})

		require.ErrorContains(t, err, "tenant")
		require.Nil(t, update)
	}
}

func TestShardKeyEqualToIdYieldsNoExtraPart(t *testing.T) {
	collectionInfo := mustMarshalBSON(t, bson.D{{Key: "shardKeyDefinition", Value: bson.D{{Key: "_id", Value: "Hash"}}}})
	fields, err := extractShardKeyFields(collectionInfo)
	require.NoError(t, err)

	update, err := eventUpdate(t, "insert", shardedDoc, fields)

	require.NoError(t, err)
	require.Equal(t, []string{"_id"}, idNames(update))
}

func TestShardKeyFieldsAreFetchedForEachNamespaceOnlyWhenTheFlagIsOn(t *testing.T) {
	reply := bson.D{{Key: "ok", Value: 1}, {Key: "shardKeyDefinition", Value: bson.D{
		{Key: "tenant", Value: "Hash"}, {Key: "_id", Value: "Hash"}, {Key: "region", Value: "Hash"},
	}}}
	ns := iface.Namespace{Db: "db", Col: "col"}
	client, commands := recordingClient(t, reply)

	fields, err := streamShardKeyFields(context.Background(), client, []iface.Namespace{ns}, withFlags(false, false))
	require.NoError(t, err)
	require.Empty(t, fields)
	require.Empty(t, *commands)

	fields, err = streamShardKeyFields(context.Background(), client, []iface.Namespace{ns}, withFlags(true, false))
	require.NoError(t, err)
	require.Equal(t, []string{"tenant", "region"}, fields[ns])
	require.Equal(t, []string{"customAction"}, *commands)
}

var shardedReply = bson.D{{Key: "ok", Value: 1}, {Key: "shardKeyDefinition", Value: bson.D{{Key: "tenant", Value: "Hash"}}}}

var unshardedReply = bson.D{{Key: "ok", Value: 1}}

var failedReply = bson.D{{Key: "ok", Value: 0}, {Key: "code", Value: 2}, {Key: "errmsg", Value: "no collection info"}}

func emptyCursor(t *testing.T) []byte {
	t.Helper()
	token, err := NewTokenMap().encodeMap()
	require.NoError(t, err)
	cursor, err := encodeResumeToken(1234, token)
	require.NoError(t, err)
	return cursor
}

func streamUpdatesFor(t *testing.T, settings ConnectorSettings, response bson.D) (error, []string) {
	t.Helper()
	client, commands := recordingClient(t, response)
	connector := &conn{client: client, connectorSettings: settings}
	err := connector.StreamUpdates(context.Background(), connect.NewRequest(&adiomv1.StreamUpdatesRequest{
		Namespaces: []string{"db.col"}, Cursor: emptyCursor(t),
	}), nil)
	return err, *commands
}

func withFlags(fullDocumentKey, emulateDeletes bool) ConnectorSettings {
	settings := ConnectorSettings{EmulateDeletes: emulateDeletes, DeletesCheckInterval: time.Hour}
	settings.FullDocumentKey = fullDocumentKey
	return settings
}

func TestStreamUpdatesStopsWhenTheShardKeyCannotBeRead(t *testing.T) {
	err, commands := streamUpdatesFor(t, withFlags(true, false), failedReply)

	require.Equal(t, connect.CodeInternal, connect.CodeOf(err))
	require.ErrorContains(t, err, "no collection info")
	require.Equal(t, []string{"customAction"}, commands)
}

func TestStreamUpdatesRejectsEmulatedDeletesWithFullDocumentKeyOnShardedNamespace(t *testing.T) {
	err, commands := streamUpdatesFor(t, withFlags(true, true), shardedReply)

	require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
	require.ErrorContains(t, err, "full-document-key")
	require.ErrorContains(t, err, "cosmos-deletes-cdc")
	require.Equal(t, []string{"customAction"}, commands)
}

func TestEmulatedDeletesWithFullDocumentKeyAreAllowedOnUnshardedNamespace(t *testing.T) {
	client, _ := recordingClient(t, unshardedReply)
	ns := iface.Namespace{Db: "db", Col: "col"}

	fields, err := streamShardKeyFields(context.Background(), client, []iface.Namespace{ns}, withFlags(true, true))

	require.NoError(t, err)
	require.Empty(t, fields[ns])
}

func TestShardKeyFieldsAreNotReadWhenEmulatedDeletesRunWithoutFullDocumentKey(t *testing.T) {
	client, commands := recordingClient(t, shardedReply)

	fields, err := streamShardKeyFields(context.Background(), client, []iface.Namespace{{Db: "db", Col: "col"}}, withFlags(false, true))

	require.NoError(t, err)
	require.Empty(t, fields)
	require.Empty(t, *commands)
}

func TestFetchShardKeyFieldsReturnsTheCommandError(t *testing.T) {
	client, _ := recordingClient(t, failedReply)

	fields, err := fetchShardKeyFields(context.Background(), client, testLoc)

	var commandErr mongo.CommandError
	require.ErrorAs(t, err, &commandErr)
	require.Nil(t, fields)
}

func decodedKey(t *testing.T, key bson.D) bson.D {
	t.Helper()
	var decoded bson.D
	require.NoError(t, bson.Unmarshal(mustMarshalBSON(t, key), &decoded))
	return decoded
}

func TestShardKeyPartIsIdenticalFromFullDocumentAndFromDocumentKey(t *testing.T) {
	for name, value := range map[string]any{
		"string":   "t1",
		"int32":    int32(7),
		"int64":    int64(1 << 40),
		"objectid": bson.NewObjectID(),
		"null":     nil,
		"array":    bson.A{"a", int32(1)},
		"document": bson.D{{Key: "k", Value: "v"}},
	} {
		t.Run(name, func(t *testing.T) {
			idOnly := bson.D{{Key: "_id", Value: "doc1"}}
			withKey := decodedKey(t, bson.D{{Key: "_id", Value: "doc1"}, {Key: "tenant", Value: value}})
			fromDoc, err := eventWithKey(t, "update", bson.D{{Key: "_id", Value: "doc1"}, {Key: "tenant", Value: value}}, idOnly, []string{"tenant"})
			require.NoError(t, err)

			fromKey, err := eventWithKey(t, "update", nil, withKey, []string{"tenant"})

			require.NoError(t, err)
			require.Equal(t, fromDoc.GetId()[1].GetType(), fromKey.GetId()[1].GetType())
			require.Equal(t, util.BsonIdKey(fromDoc.GetId()), util.BsonIdKey(fromKey.GetId()))
		})
	}
}

func TestFullDocumentWinsWhenItDisagreesWithDocumentKey(t *testing.T) {
	key := bson.D{{Key: "_id", Value: "doc1"}, {Key: "tenant", Value: "fromKey"}}

	update, err := eventWithKey(t, "update", bson.D{{Key: "_id", Value: "doc1"}, {Key: "tenant", Value: "fromDoc"}}, key, []string{"tenant"})

	require.NoError(t, err)
	require.Equal(t, "fromDoc", idValue(t, update.GetId()[1]))
}

func TestShardKeyFieldsAreReadForEveryNamespace(t *testing.T) {
	first := iface.Namespace{Db: "db", Col: "a"}
	second := iface.Namespace{Db: "db", Col: "b"}
	reply := func(field string) bson.D {
		return bson.D{{Key: "ok", Value: 1}, {Key: "shardKeyDefinition", Value: bson.D{{Key: field, Value: "Hash"}}}}
	}
	client, commands := recordingClient(t, reply("tenant"), reply("region"))

	fields, err := streamShardKeyFields(context.Background(), client, []iface.Namespace{first, second}, withFlags(true, false))

	require.NoError(t, err)
	require.Equal(t, map[iface.Namespace][]string{first: {"tenant"}, second: {"region"}}, fields)
	require.Equal(t, []string{"customAction", "customAction"}, *commands)
}
