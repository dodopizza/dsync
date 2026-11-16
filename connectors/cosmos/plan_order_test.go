package cosmos

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

var countReply = bson.D{{Key: "ok", Value: 1}, {Key: "n", Value: 5}}

func planSettings() ConnectorSettings {
	return ConnectorSettings{MaxNumNamespaces: 1, NumParallelPartitionWorkers: 1, TargetDocCountPerPartition: 1000}
}

func TestGeneratePlanWithoutUpdatesNeverTouchesChangeStream(t *testing.T) {
	client, commands := recordingClient(t, countReply)
	connector := &conn{client: client, connectorSettings: planSettings()}

	plan, err := connector.GeneratePlan(context.Background(), connect.NewRequest(&adiomv1.GeneratePlanRequest{
		Namespaces: []string{"db.col"}, InitialSync: true,
	}))

	require.NoError(t, err)
	require.Len(t, plan.Msg.GetPartitions(), 1)
	require.Equal(t, []string{"count"}, *commands)
}

func TestGeneratePlanTakesResumeTokenBeforePartitioning(t *testing.T) {
	responses := append(resumePlanningResponses(bson.D{{Key: "ok", Value: 1}, {Key: "n", Value: 1}}), countReply)
	client, commands := recordingClient(t, responses...)
	connector := &conn{client: client, connectorSettings: planSettings()}

	plan, err := connector.GeneratePlan(context.Background(), connect.NewRequest(&adiomv1.GeneratePlanRequest{
		Namespaces: []string{"db.col"}, InitialSync: true, Updates: true,
	}))

	require.NoError(t, err)
	require.Len(t, plan.Msg.GetPartitions(), 1)
	require.Equal(t, []string{"aggregate", "customAction", "insert", "delete", "count"}, *commands)
}

func TestGeneratePlanTokenFailureStopsBeforePartitioning(t *testing.T) {
	client, commands := recordingClient(t, bson.D{{Key: "ok", Value: 0}, {Key: "code", Value: 13}, {Key: "errmsg", Value: "change stream unavailable"}})
	connector := &conn{client: client, connectorSettings: planSettings()}

	plan, err := connector.GeneratePlan(context.Background(), connect.NewRequest(&adiomv1.GeneratePlanRequest{
		Namespaces: []string{"db.col"}, InitialSync: true, Updates: true,
	}))

	require.Nil(t, plan)
	require.Equal(t, connect.CodeInternal, connect.CodeOf(err))
	require.NotContains(t, *commands, "count")
}

func findReply(ts uint32) bson.D {
	id := objectIDValue(ts, 1).ObjectID()
	return bson.D{{Key: "ok", Value: 1}, {Key: "cursor", Value: bson.D{
		{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.col"},
		{Key: "firstBatch", Value: bson.A{bson.D{{Key: "_id", Value: id}}}},
	}}}
}

func boundBytes(v any) string {
	raw, _ := v.(bson.RawValue)
	return string(raw.Value)
}

func TestPartitionedTasksFormAnUnbrokenChainWithoutExtraLookups(t *testing.T) {
	client, commands := recordingClient(t,
		bson.D{{Key: "ok", Value: 1}, {Key: "n", Value: 4000}},
		findReply(2000), findReply(1000),
	)
	settings := planSettings()
	settings.partitionKey = "_id"
	p := &planner{Ctx: context.Background(), Client: client, settings: settings}

	tasks := p.partitionTasksIfNecessary([]iface.Namespace{{Db: "db", Col: "col"}})

	require.Len(t, tasks, 6)
	byLow := map[string]iface.ReadPlanTask{}
	for _, task := range tasks {
		byLow[boundBytes(task.Def.Low)] = task
	}
	require.Len(t, byLow, len(tasks))
	chain := []iface.ReadPlanTask{byLow[""]}
	for high := boundBytes(chain[len(chain)-1].Def.High); high != ""; high = boundBytes(chain[len(chain)-1].Def.High) {
		require.LessOrEqual(t, len(chain), len(tasks), "chain is longer than the task list")
		next, ok := byLow[high]
		require.True(t, ok, "no task starts where the previous one ends")
		chain = append(chain, next)
	}
	require.Len(t, chain, len(tasks))
	require.Equal(t, string(objectIDValue(1000, 1).Value), boundBytes(chain[0].Def.High))
	require.Equal(t, string(objectIDValue(2000, 1).Value), boundBytes(chain[len(chain)-1].Def.Low))
	require.Equal(t, []string{"count", "find", "find"}, *commands)
}

func planWithUpdates(t *testing.T, settings ConnectorSettings, updates bool, responses ...bson.D) (error, []string) {
	t.Helper()
	client, commands := recordingClient(t, responses...)
	settings.MaxNumNamespaces = 1
	settings.NumParallelPartitionWorkers = 1
	settings.TargetDocCountPerPartition = 1000
	connector := &conn{client: client, connectorSettings: settings}
	_, err := connector.GeneratePlan(context.Background(), connect.NewRequest(&adiomv1.GeneratePlanRequest{
		Namespaces: []string{"db.col"}, InitialSync: true, Updates: updates,
	}))
	return err, *commands
}

func TestGeneratePlanRejectsEmulatedDeletesWithFullDocumentKeyBeforeAnyOtherCommand(t *testing.T) {
	err, commands := planWithUpdates(t, withFlags(true, true), true, shardedReply, countReply)

	require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
	require.ErrorContains(t, err, "cosmos-deletes-cdc")
	require.Equal(t, []string{"customAction"}, commands)
}

func TestGeneratePlanFailsWhenTheShardKeyCannotBeRead(t *testing.T) {
	err, commands := planWithUpdates(t, withFlags(true, false), true, failedReply, countReply)

	require.Equal(t, connect.CodeInternal, connect.CodeOf(err))
	require.ErrorContains(t, err, "no collection info")
	require.Equal(t, []string{"customAction"}, commands)
}

func TestGeneratePlanWithoutUpdatesDoesNotReadTheShardKey(t *testing.T) {
	err, commands := planWithUpdates(t, withFlags(true, true), false, countReply)

	require.NoError(t, err)
	require.Equal(t, []string{"count"}, commands)
}
