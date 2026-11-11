package mongo

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/drivertest"
)

const conflictNamespace = "db.col"

var (
	idIndexKey       = bson.D{{Key: "_id", Value: 1}}
	clientIDIndexKey = bson.D{{Key: "ClientId", Value: 1}}
	idIndex          = bson.D{{Key: "v", Value: 2}, {Key: "key", Value: idIndexKey}, {Key: "name", Value: "_id_"}}
	writeOKReply     = bson.D{{Key: "ok", Value: 1}, {Key: "n", Value: 1}, {Key: "nModified", Value: 1}}

	conflictRetried = []string{"update", "listIndexes", "findAndModify", "update"}
)

type sentCommand struct {
	name    string
	command bson.Raw
}

func recordingConn(t *testing.T, settings ConnectorSettings, responses ...bson.D) (*conn, *[]sentCommand) {
	t.Helper()
	var commands []sentCommand
	opts := options.Client().SetRetryReads(false).SetRetryWrites(false).SetMonitor(&event.CommandMonitor{
		Started: func(_ context.Context, started *event.CommandStartedEvent) {
			commands = append(commands, sentCommand{name: started.CommandName, command: bytes.Clone(started.Command)})
		},
	})
	opts.Deployment = drivertest.NewMockDeployment(responses...)
	client, err := mongo.Connect(opts)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Disconnect(context.Background())) })
	return &conn{client: client, settings: settings}, &commands
}

func commandNames(commands []sentCommand) []string {
	names := make([]string, len(commands))
	for i, command := range commands {
		names[i] = command.name
	}
	return names
}

func duplicateKey(index int, indexName, dupKey string, keyPattern, keyValue bson.D) bson.D {
	return bson.D{
		{Key: "index", Value: index},
		{Key: "code", Value: 11000},
		{Key: "errmsg", Value: "E11000 duplicate key error collection: db.col index: " + indexName + " dup key: " + dupKey},
		{Key: "keyPattern", Value: keyPattern},
		{Key: "keyValue", Value: keyValue},
	}
}

func clientIDConflict(index int) bson.D {
	return duplicateKey(index, "ClientId_1", `{ ClientId: "c-1" }`, clientIDIndexKey, bson.D{{Key: "ClientId", Value: "c-1"}})
}

func failedWriteReply(writeErrors ...bson.D) bson.D {
	errs := make(bson.A, len(writeErrors))
	for i, writeError := range writeErrors {
		errs[i] = writeError
	}
	return bson.D{{Key: "ok", Value: 1}, {Key: "n", Value: 0}, {Key: "nModified", Value: 0}, {Key: "writeErrors", Value: errs}}
}

func uniqueIndex(name string, key bson.D, extra ...bson.E) bson.D {
	return append(bson.D{{Key: "v", Value: 2}, {Key: "key", Value: key}, {Key: "name", Value: name}, {Key: "unique", Value: true}}, extra...)
}

func indexesReply(indexes ...bson.D) bson.D {
	batch := make(bson.A, len(indexes))
	for i, index := range indexes {
		batch[i] = index
	}
	return bson.D{{Key: "ok", Value: 1}, {Key: "cursor", Value: bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: conflictNamespace}, {Key: "firstBatch", Value: batch}}}}
}

func plainClientIDIndexes() bson.D {
	return indexesReply(idIndex, uniqueIndex("ClientId_1", clientIDIndexKey))
}

func staleHolderReply(id any) bson.D {
	if id == nil {
		return bson.D{{Key: "ok", Value: 1}, {Key: "lastErrorObject", Value: bson.D{{Key: "n", Value: 0}}}, {Key: "value", Value: nil}}
	}
	return bson.D{{Key: "ok", Value: 1}, {Key: "lastErrorObject", Value: bson.D{{Key: "n", Value: 1}}}, {Key: "value", Value: bson.D{{Key: "_id", Value: id}}}}
}

func clientUpdate(t *testing.T, id string) *adiomv1.Update {
	return insertU(t, id, bson.M{"_id": id, "ClientId": "c-1"})
}

func writeUpdates(c *conn, partialResults bool, updates ...*adiomv1.Update) (*connect.Response[adiomv1.WriteUpdatesResponse], error) {
	return c.WriteUpdates(context.Background(), connect.NewRequest(&adiomv1.WriteUpdatesRequest{
		Namespace: conflictNamespace, Updates: updates, PartialWriteResults: partialResults,
	}))
}

func writeData(t *testing.T, c *conn, documents ...bson.D) (*connect.Response[adiomv1.WriteDataResponse], error) {
	data := make([][]byte, len(documents))
	for i, document := range documents {
		data[i] = mustMarshal(t, document)
	}
	return c.WriteData(context.Background(), connect.NewRequest(&adiomv1.WriteDataRequest{Namespace: conflictNamespace, Data: data}))
}

func requireDeletesOnlyOtherHolderOfClientID(t *testing.T, deletion sentCommand, newID string) {
	t.Helper()
	require.Equal(t, "findAndModify", deletion.name)
	require.True(t, deletion.command.Lookup("remove").Boolean())
	want := mustMarshal(t, bson.D{{Key: "_id", Value: bson.D{{Key: "$ne", Value: newID}}}, {Key: "ClientId", Value: "c-1"}})
	require.Equal(t, bson.Raw(want), deletion.command.Lookup("query").Document())
}

func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &logs
}

func TestWriteUpdatesDeletesStaleUniqueConflictAndRetries(t *testing.T) {
	logs := captureLogs(t)
	c, commands := recordingConn(t, ConnectorSettings{DeleteUniqueConflicts: true},
		failedWriteReply(clientIDConflict(0)), plainClientIDIndexes(), staleHolderReply("stale"), writeOKReply)

	_, err := writeUpdates(c, false, clientUpdate(t, "fresh"))

	require.NoError(t, err)
	require.Equal(t, conflictRetried, commandNames(*commands))
	requireDeletesOnlyOtherHolderOfClientID(t, (*commands)[2], "fresh")
	require.Contains(t, logs.String(), "level=WARN")
	require.Contains(t, logs.String(), conflictNamespace)
	require.Contains(t, logs.String(), "ClientId_1")
	require.Contains(t, logs.String(), "stale")
	require.Contains(t, logs.String(), "fresh")
	require.NotContains(t, logs.String(), "c-1", "key values are personal identifiers")
}

func TestWriteUpdatesKeepsUniqueConflictErrorWhenFlagIsOff(t *testing.T) {
	c, commands := recordingConn(t, ConnectorSettings{}, failedWriteReply(clientIDConflict(0)))

	_, err := writeUpdates(c, false, clientUpdate(t, "fresh"))

	require.Equal(t, connect.CodeInternal, connect.CodeOf(err))
	require.ErrorContains(t, err, "E11000")
	require.Equal(t, []string{"update"}, commandNames(*commands))
}

func TestWriteUpdatesRetriesUniqueConflictOnlyOnce(t *testing.T) {
	c, commands := recordingConn(t, ConnectorSettings{DeleteUniqueConflicts: true},
		failedWriteReply(clientIDConflict(0)), plainClientIDIndexes(), staleHolderReply("stale"), failedWriteReply(clientIDConflict(0)))

	_, err := writeUpdates(c, false, clientUpdate(t, "fresh"))

	require.Equal(t, connect.CodeInternal, connect.CodeOf(err))
	require.ErrorContains(t, err, "E11000")
	require.Equal(t, conflictRetried, commandNames(*commands))
}

func TestWriteUpdatesRetriesEvenWhenNoStaleHolderIsLeft(t *testing.T) {
	c, commands := recordingConn(t, ConnectorSettings{DeleteUniqueConflicts: true},
		failedWriteReply(clientIDConflict(0)), plainClientIDIndexes(), staleHolderReply(nil), writeOKReply)

	_, err := writeUpdates(c, false, clientUpdate(t, "fresh"))

	require.NoError(t, err)
	require.Equal(t, conflictRetried, commandNames(*commands))
}

func TestWriteUpdatesDeletesNothingForOtherFailures(t *testing.T) {
	for name, test := range map[string]struct {
		responses []bson.D
		commands  []string
	}{
		"duplicate _id": {
			responses: []bson.D{failedWriteReply(duplicateKey(1, "_id_", `{ _id: "fresh" }`, idIndexKey, bson.D{{Key: "_id", Value: "fresh"}}))},
			commands:  []string{"update"},
		},
		"server reports no key details": {
			responses: []bson.D{failedWriteReply(bson.D{{Key: "index", Value: 1}, {Key: "code", Value: 11000}, {Key: "errmsg", Value: "E11000 duplicate key error"}})},
			commands:  []string{"update"},
		},
		"another write failed for a different reason": {
			responses: []bson.D{failedWriteReply(bson.D{{Key: "index", Value: 0}, {Key: "code", Value: 121}, {Key: "errmsg", Value: "Document failed validation"}}, clientIDConflict(1))},
			commands:  []string{"update"},
		},
		"partial unique index": {
			responses: []bson.D{failedWriteReply(clientIDConflict(1)), indexesReply(idIndex, uniqueIndex("ClientId_1", clientIDIndexKey,
				bson.E{Key: "partialFilterExpression", Value: bson.D{{Key: "Active", Value: true}}}))},
			commands: []string{"update", "listIndexes"},
		},
		"unique index with a collation": {
			responses: []bson.D{failedWriteReply(clientIDConflict(1)), indexesReply(idIndex, uniqueIndex("ClientId_1", clientIDIndexKey,
				bson.E{Key: "collation", Value: bson.D{{Key: "locale", Value: "en"}, {Key: "strength", Value: 2}}}))},
			commands: []string{"update", "listIndexes"},
		},
		"conflicting index is not listed": {
			responses: []bson.D{failedWriteReply(clientIDConflict(1)), indexesReply(idIndex)},
			commands:  []string{"update", "listIndexes"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			c, commands := recordingConn(t, ConnectorSettings{DeleteUniqueConflicts: true}, test.responses...)

			// Unordered models are built from the last update backwards, so model 1 is "fresh".
			_, err := writeUpdates(c, false, clientUpdate(t, "fresh"), insertU(t, "other", bson.M{"_id": "other", "ClientId": "c-2"}))

			require.Equal(t, connect.CodeInternal, connect.CodeOf(err))
			require.Equal(t, test.commands, commandNames(*commands))
		})
	}
}

func TestWriteUpdatesRetriesOnlyTheConflictingWrite(t *testing.T) {
	c, commands := recordingConn(t, ConnectorSettings{DeleteUniqueConflicts: true},
		failedWriteReply(clientIDConflict(1)), plainClientIDIndexes(), staleHolderReply("stale"), writeOKReply)

	// Unordered models are built from the last update backwards, so model 1 is "fresh".
	_, err := writeUpdates(c, false,
		insertU(t, "first", bson.M{"_id": "first", "ClientId": "c-0"}), clientUpdate(t, "fresh"), insertU(t, "last", bson.M{"_id": "last", "ClientId": "c-2"}))

	require.NoError(t, err)
	require.Equal(t, conflictRetried, commandNames(*commands))
	requireDeletesOnlyOtherHolderOfClientID(t, (*commands)[2], "fresh")
	retried, err := (*commands)[3].command.Lookup("updates").Array().Values()
	require.NoError(t, err)
	require.Len(t, retried, 1)
	require.Equal(t, bson.Raw(mustMarshal(t, bson.D{{Key: "_id", Value: "fresh"}})), retried[0].Document().Lookup("q").Document())
}

func TestWriteUpdatesOrderedRetryResumesFromTheFailedWrite(t *testing.T) {
	c, commands := recordingConn(t, ConnectorSettings{DeleteUniqueConflicts: true},
		failedWriteReply(clientIDConflict(1)), plainClientIDIndexes(), staleHolderReply("stale"), writeOKReply)

	// A trailing partial update makes the batch ordered: the server stopped at
	// write 1, so write 2 was never attempted and has to run after the retry.
	_, err := writeUpdates(c, false,
		insertU(t, "fresh", bson.M{"_id": "fresh", "ClientId": "c-0"}),
		partialU(t, "fresh", bson.M{"ClientId": "c-1"}),
		partialU(t, "fresh", bson.M{"Name": "new"}))

	require.NoError(t, err)
	require.Equal(t, conflictRetried, commandNames(*commands))
	retry := (*commands)[3].command
	require.True(t, retry.Lookup("ordered").Boolean())
	retried, err := retry.Lookup("updates").Array().Values()
	require.NoError(t, err)
	require.Len(t, retried, 2)
}

func TestWriteUpdatesReportsFailedRetryAtTheOriginalInput(t *testing.T) {
	c, commands := recordingConn(t, ConnectorSettings{DeleteUniqueConflicts: true},
		failedWriteReply(clientIDConflict(2)), plainClientIDIndexes(), staleHolderReply("stale"), failedWriteReply(clientIDConflict(0)))

	// Model 2 is the first input; in the one-write retry it is write 0.
	res, err := writeUpdates(c, true,
		clientUpdate(t, "fresh"), insertU(t, "middle", bson.M{"_id": "middle", "ClientId": "c-2"}), insertU(t, "last", bson.M{"_id": "last", "ClientId": "c-3"}))

	require.NoError(t, err)
	require.Equal(t, conflictRetried, commandNames(*commands))
	require.Len(t, res.Msg.GetFailures(), 1)
	require.Equal(t, uint32(0), res.Msg.GetFailures()[0].GetIndex())
}

func TestWriteDataDeletesStaleUniqueConflictOnOverwrite(t *testing.T) {
	c, commands := recordingConn(t, ConnectorSettings{DeleteUniqueConflicts: true},
		failedWriteReply(clientIDConflict(0)), failedWriteReply(clientIDConflict(0)), plainClientIDIndexes(), staleHolderReply("stale"), writeOKReply)

	res, err := writeData(t, c, bson.D{{Key: "_id", Value: "fresh"}, {Key: "ClientId", Value: "c-1"}})

	require.NoError(t, err)
	require.Empty(t, res.Msg.GetFailures())
	require.Equal(t, append([]string{"insert"}, conflictRetried...), commandNames(*commands))
	requireDeletesOnlyOtherHolderOfClientID(t, (*commands)[3], "fresh")
}

func TestWriteDataKeepsUniqueConflictErrorWhenFlagIsOff(t *testing.T) {
	c, commands := recordingConn(t, ConnectorSettings{}, failedWriteReply(clientIDConflict(0)), failedWriteReply(clientIDConflict(0)))

	_, err := writeData(t, c, bson.D{{Key: "_id", Value: "fresh"}, {Key: "ClientId", Value: "c-1"}})

	require.Equal(t, connect.CodeInternal, connect.CodeOf(err))
	require.ErrorContains(t, err, "E11000")
	require.Equal(t, []string{"insert", "update"}, commandNames(*commands))
}

func TestWriteDataOverwritesDuplicateIDWithoutDeleting(t *testing.T) {
	c, commands := recordingConn(t, ConnectorSettings{DeleteUniqueConflicts: true},
		failedWriteReply(duplicateKey(0, "_id_", `{ _id: "fresh" }`, idIndexKey, bson.D{{Key: "_id", Value: "fresh"}})), writeOKReply)

	res, err := writeData(t, c, bson.D{{Key: "_id", Value: "fresh"}, {Key: "ClientId", Value: "c-1"}})

	require.NoError(t, err)
	require.Empty(t, res.Msg.GetFailures())
	require.Equal(t, []string{"insert", "update"}, commandNames(*commands))
}
