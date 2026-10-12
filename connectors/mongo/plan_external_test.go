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
		name   string
		query  string
		target int64
		want   func(bson.RawValue) bool
	}{
		{"no query", "", 50, func(bson.RawValue) bool { return true }},
		{"string query", `{"_id":{"$type":"string"}}`, 50, func(v bson.RawValue) bool { return v.Type == bson.TypeString }},
		{"small collection without sampling", "", 10000, func(bson.RawValue) bool { return true }},
		{"query matching nothing", `{"_id":{"$type":"date"}}`, 50, func(bson.RawValue) bool { return false }},
		{"objectId and number query", `{"_id":{"$type":["objectId","number"]}}`, 50, func(v bson.RawValue) bool {
			return v.Type == bson.TypeObjectID || bsonTypeSortOrder(v.Type) == bsonTypeOrderNumber
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := NewConnWithClient(client, ConnectorSettings{TargetDocCountPerPartition: tc.target, SampleFactor: 1, Query: tc.query})
			require.NoError(t, err)
			plan, err := c.GeneratePlan(ctx, connect.NewRequest(&adiomv1.GeneratePlanRequest{Namespaces: []string{namespace}, InitialSync: true}))
			require.NoError(t, err)

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

func TestMongoServerPlanRejectsUnsupportedMixedIDs(t *testing.T) {
	uri := os.Getenv("MONGO_PLAN_TEST_URI")
	if uri == "" {
		t.Skip("set MONGO_PLAN_TEST_URI to run")
	}

	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetServerSelectionTimeout(10 * time.Second))
	require.NoError(t, err)
	defer client.Disconnect(context.Background())

	for name, ids := range map[string][]any{
		"string and maxKey":           {"a", bson.MaxKey{}},
		"dbPointer and codeWithScope": {bson.DBPointer{DB: "x", Pointer: bson.NewObjectID()}, bson.CodeWithScope{Code: "f", Scope: bson.D{}}},
	} {
		t.Run(name, func(t *testing.T) {
			col := client.Database("dsync_plan_test").Collection("unsupported_ids_" + bson.NewObjectID().Hex())
			defer func() {
				require.NoError(t, col.Drop(context.Background()))
			}()
			for _, id := range ids {
				_, err := col.InsertOne(t.Context(), bson.D{{"_id", id}})
				require.NoError(t, err)
			}

			c, err := NewConnWithClient(client, ConnectorSettings{TargetDocCountPerPartition: 50, SampleFactor: 1})
			require.NoError(t, err)
			_, err = c.GeneratePlan(t.Context(), connect.NewRequest(&adiomv1.GeneratePlanRequest{Namespaces: []string{"dsync_plan_test." + col.Name()}, InitialSync: true}))
			require.ErrorContains(t, err, "not supported")
		})
	}
}
