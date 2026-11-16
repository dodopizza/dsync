package cosmos

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/adiom-data/dsync/connectors/common"
	mongoconn "github.com/adiom-data/dsync/connectors/mongo"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestPlannerSplitsNonObjectIdCollectionsWithoutGapsOrDuplicates(t *testing.T) {
	uri := os.Getenv("COSMOS_PLAN_TEST_URI")
	if uri == "" {
		t.Skip("set COSMOS_PLAN_TEST_URI to run")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
	defer cancel()
	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetServerSelectionTimeout(10 * time.Second))
	require.NoError(t, err)
	defer client.Disconnect(context.Background())

	uuidID := func(int) any {
		data := make([]byte, 16)
		_, err := rand.Read(data)
		require.NoError(t, err)
		return bson.Binary{Subtype: bson.TypeBinaryUUID, Data: data}
	}
	for name, tc := range map[string]struct {
		id         func(i int) any
		wantSplits bool
	}{
		"string": {func(i int) any { return fmt.Sprintf("doc-%05d", i) }, true},
		"int64":  {func(i int) any { return int64(i) * 7 }, true},
		"uuid":   {uuidID, true},
		"mixed": {func(i int) any {
			if i%2 == 0 {
				return int32(i)
			}
			return fmt.Sprintf("doc-%05d", i)
		}, false},
	} {
		t.Run(name, func(t *testing.T) {
			col := client.Database("dsync_plan_test").Collection(name + "_" + bson.NewObjectID().Hex())
			t.Cleanup(func() {
				dropCtx, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer dropCancel()
				require.NoError(t, col.Drop(dropCtx))
			})
			var docs []any
			for i := range 3000 {
				docs = append(docs, bson.D{{Key: "_id", Value: tc.id(i)}})
			}
			_, err := col.InsertMany(ctx, docs)
			require.NoError(t, err)

			c := NewConn(ConnectorSettings{ConnectionString: uri, TargetDocCountPerPartition: 300, NumParallelPartitionWorkers: 1})
			t.Cleanup(c.(common.Teardownable).Teardown)
			plan, err := c.GeneratePlan(ctx, connect.NewRequest(&adiomv1.GeneratePlanRequest{
				Namespaces: []string{col.Database().Name() + "." + col.Name()}, InitialSync: true,
			}))
			require.NoError(t, err)
			tasks := plan.Msg.GetPartitions()

			got := map[string]int{}
			for _, partition := range tasks {
				var cursor []byte
				for {
					res, err := c.ListData(ctx, connect.NewRequest(&adiomv1.ListDataRequest{Partition: partition, Cursor: cursor}))
					require.NoError(t, err)
					for _, d := range res.Msg.GetData() {
						got[bson.Raw(d).Lookup("_id").String()]++
					}
					cursor = res.Msg.GetNextCursor()
					if len(cursor) == 0 {
						break
					}
				}
			}
			require.Len(t, got, 3000)
			for id, n := range got {
				require.Equal(t, 1, n, "id %s read %d times", id, n)
			}
			if tc.wantSplits {
				require.Greater(t, len(tasks), 1)
			} else {
				require.Len(t, tasks, 1)
				low, high := mongoconn.DecodeCursor(tasks[0].GetCursor())
				require.True(t, low.IsZero() && high.IsZero())
			}
			t.Logf("%s: %d partitions, %d docs read, each exactly once", name, len(tasks), len(got))
		})
	}
}
