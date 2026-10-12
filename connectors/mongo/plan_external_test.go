package mongo

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestMongoServerPlanCoversMixedIDTypes(t *testing.T) {
	uri := os.Getenv("MONGO_PLAN_TEST_URI")
	if uri == "" {
		t.Skip("set MONGO_PLAN_TEST_URI to run")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetServerSelectionTimeout(10 * time.Second))
	require.NoError(t, err)
	defer client.Disconnect(context.Background())

	col := client.Database("dsync_plan_test").Collection("mixed_ids_" + bson.NewObjectID().Hex())
	defer func() {
		require.NoError(t, col.Drop(context.Background()))
	}()

	var docs []any
	for i := range 300 {
		docs = append(docs, bson.D{{"_id", fmt.Sprintf("s%04d", i)}})
	}
	for range 600 {
		docs = append(docs, bson.D{{"_id", bson.NewObjectID()}})
	}
	for i := range 50 {
		docs = append(docs, bson.D{{"_id", int32(i)}}, bson.D{{"_id", int64(1000 + i)}})
	}
	docs = append(docs, bson.D{{"_id", bson.D{{"k", "v"}}}})
	_, err = col.InsertMany(ctx, docs)
	require.NoError(t, err)

	namespace := col.Database().Name() + "." + col.Name()

	for _, tc := range []struct {
		name  string
		query string
		want  func(bson.RawValue) bool
	}{
		{"no query", "", func(bson.RawValue) bool { return true }},
		{"string query", `{"_id":{"$type":"string"}}`, func(v bson.RawValue) bool { return v.Type == bson.TypeString }},
		{"objectId and number query", `{"_id":{"$type":["objectId","number"]}}`, func(v bson.RawValue) bool {
			return v.Type == bson.TypeObjectID || bsonTypeSortOrder(v.Type) == bsonTypeOrderNumber
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewConnWithClient(client, ConnectorSettings{TargetDocCountPerPartition: 50, SampleFactor: 1, Query: tc.query})
			plan, err := c.GeneratePlan(ctx, connect.NewRequest(&adiomv1.GeneratePlanRequest{Namespaces: []string{namespace}, InitialSync: true}))
			require.NoError(t, err)
			require.Greater(t, len(plan.Msg.GetPartitions()), 4)

			want := map[string]int{}
			for _, d := range docs {
				id := bsonRawValue(t, d.(bson.D)[0].Value)
				if tc.want(id) {
					want[id.String()] = 1
				}
			}

			got := map[string]int{}
			for _, p := range plan.Msg.GetPartitions() {
				var cursor []byte
				for {
					res, err := c.ListData(ctx, connect.NewRequest(&adiomv1.ListDataRequest{Partition: p, Cursor: cursor}))
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
			require.Equal(t, want, got)
		})
	}
}
