package cosmos

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/drivertest"
)

var (
	cursorToken = bson.D{{Key: "_data", Value: bson.Binary{Data: []byte(`{"Rid":"collection","at":"cursor"}`)}}}
	eventToken  = bson.D{{Key: "_data", Value: bson.Binary{Data: []byte(`{"Rid":"collection","at":"event"}`)}}}

	collectionInfoReply = bson.D{{Key: "ok", Value: 1}, {Key: "shardKeyDefinition", Value: bson.D{{Key: "partitionKey", Value: "Hash"}}}}
	insertDeniedReply   = bson.D{{Key: "ok", Value: 0}, {Key: "code", Value: 13}, {Key: "errmsg", Value: "not authorized to insert"}}
	readFailedReply     = bson.D{{Key: "ok", Value: 0}, {Key: "code", Value: 2}, {Key: "errmsg", Value: "cursor read rejected"}}
	cursorKilledReply   = bson.D{{Key: "ok", Value: 1}, {Key: "cursorsKilled", Value: bson.A{int64(7)}}, {Key: "cursorsNotFound", Value: bson.A{}}, {Key: "cursorsAlive", Value: bson.A{}}, {Key: "cursorsUnknown", Value: bson.A{}}}

	deniedThenClosed = []string{"aggregate", "customAction", "insert", "killCursors"}
	deniedThenRead   = []string{"aggregate", "customAction", "insert", "getMore", "killCursors"}
)

// The cursor stays open (non-zero id), as a live change stream does.
func cursorReply(batchField string, events bson.A, postBatchResumeToken bson.D) bson.D {
	cursor := bson.D{{Key: "id", Value: int64(7)}, {Key: "ns", Value: "db.col"}, {Key: batchField, Value: events}}
	if postBatchResumeToken != nil {
		cursor = append(cursor, bson.E{Key: "postBatchResumeToken", Value: postBatchResumeToken})
	}
	return bson.D{{Key: "ok", Value: 1}, {Key: "cursor", Value: cursor}}
}

func recordingClient(t *testing.T, responses ...bson.D) (*mongo.Client, *[]string) {
	t.Helper()
	var commands []string
	opts := options.Client().SetRetryReads(false).SetRetryWrites(false).SetMonitor(&event.CommandMonitor{
		Started: func(_ context.Context, started *event.CommandStartedEvent) {
			commands = append(commands, started.CommandName)
		},
	})
	opts.Deployment = drivertest.NewMockDeployment(responses...)
	client, err := mongo.Connect(opts)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Disconnect(context.Background())) })
	return client, &commands
}

func TestLatestResumeTokenWithReadOnlyKeyComesFromOpenCursor(t *testing.T) {
	for name, test := range map[string]struct {
		responses []bson.D
		want      bson.D
		commands  []string
	}{
		"token arrives with the empty first batch": {
			responses: []bson.D{cursorReply("firstBatch", bson.A{}, cursorToken), collectionInfoReply, insertDeniedReply, cursorKilledReply},
			want:      cursorToken,
			commands:  deniedThenClosed,
		},
		"token arrives with the next empty batch": {
			responses: []bson.D{cursorReply("firstBatch", bson.A{}, nil), collectionInfoReply, insertDeniedReply, cursorReply("nextBatch", bson.A{}, cursorToken), cursorKilledReply},
			want:      cursorToken,
			commands:  deniedThenRead,
		},
		"an event arrives before any token": {
			responses: []bson.D{cursorReply("firstBatch", bson.A{}, nil), collectionInfoReply, insertDeniedReply, cursorReply("nextBatch", bson.A{bson.D{{Key: "_id", Value: eventToken}}}, nil), cursorKilledReply},
			want:      eventToken,
			commands:  deniedThenRead,
		},
	} {
		t.Run(name, func(t *testing.T) {
			client, commands := recordingClient(t, test.responses...)

			token, err := getLatestResumeToken(context.Background(), client, iface.Location{Database: "db", Collection: "col"}, false)

			require.NoError(t, err)
			require.Equal(t, mustMarshalBSON(t, test.want), token)
			require.Equal(t, test.commands, *commands, "nothing may be written or cleaned up after the denied insert")
		})
	}
}

func TestLatestResumeTokenWithReadOnlyKeyAndNoCursorTokenStaysEmpty(t *testing.T) {
	for name, read := range map[string]bson.D{
		"cursor has no token":      cursorReply("nextBatch", bson.A{}, nil),
		"reading the cursor fails": readFailedReply,
	} {
		t.Run(name, func(t *testing.T) {
			client, commands := recordingClient(t, cursorReply("firstBatch", bson.A{}, nil), collectionInfoReply, insertDeniedReply, read, cursorKilledReply)

			token, err := getLatestResumeToken(context.Background(), client, iface.Location{Database: "db", Collection: "col"}, false)

			require.NoError(t, err)
			require.Nil(t, token)
			require.Equal(t, deniedThenRead, *commands)
		})
	}
}

func TestLatestResumeTokenWithWritableKeyStillUsesDummyRecord(t *testing.T) {
	client, commands := recordingClient(t, resumePlanningResponses(bson.D{{Key: "ok", Value: 1}, {Key: "n", Value: 1}})...)

	token, err := getLatestResumeToken(context.Background(), client, iface.Location{Database: "db", Collection: "col"}, false)

	require.NoError(t, err)
	require.Equal(t, mustMarshalBSON(t, bson.D{{Key: "_data", Value: bson.Binary{Data: []byte(`{"Rid":"collection"}`)}}}), token)
	require.Equal(t, []string{"aggregate", "customAction", "insert", "delete"}, *commands)
}

func TestGeneratePlanWithReadOnlyKeyResumesAfterCursorToken(t *testing.T) {
	client, _ := recordingClient(t, cursorReply("firstBatch", bson.A{}, nil), collectionInfoReply, insertDeniedReply, cursorReply("nextBatch", bson.A{}, cursorToken), cursorKilledReply)
	connector := &conn{client: client, connectorSettings: ConnectorSettings{MaxNumNamespaces: 1}}

	plan, err := connector.GeneratePlan(context.Background(), connect.NewRequest(&adiomv1.GeneratePlanRequest{
		Namespaces: []string{"db.col"}, Updates: true,
	}))

	require.NoError(t, err)
	require.Len(t, plan.Msg.GetUpdatesPartitions(), 1)
	epoch, encodedTokens, err := decodeResumeToken(plan.Msg.GetUpdatesPartitions()[0].GetCursor())
	require.NoError(t, err)
	tokens := NewTokenMap()
	require.NoError(t, tokens.decodeMap(encodedTokens))
	token, _ := tokens.GetToken(iface.Location{Database: "db", Collection: "col"})
	require.Equal(t, mustMarshalBSON(t, cursorToken), token)

	// Both the update stream and the LSN tracker open their change stream with these options.
	streamOptions := &options.ChangeStreamOptions{}
	for _, apply := range resumeChangeStreamOptions(epoch, token).List() {
		require.NoError(t, apply(streamOptions))
	}
	require.Equal(t, mustMarshalBSON(t, cursorToken), streamOptions.ResumeAfter)
	require.Nil(t, streamOptions.StartAtOperationTime)
}
