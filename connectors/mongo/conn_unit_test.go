/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package mongo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/topology"
)

// helpers

func mustMarshal(t *testing.T, v interface{}) []byte {
	t.Helper()
	b, err := bson.Marshal(v)
	require.NoError(t, err)
	return b
}

func bsonID(t *testing.T, name string, v interface{}) *adiomv1.BsonValue {
	t.Helper()
	if name == "" {
		name = "_id"
	}
	typ, data, err := bson.MarshalValue(v)
	require.NoError(t, err)
	return &adiomv1.BsonValue{Name: name, Type: uint32(typ), Data: data}
}

func TestPlanningFanoutLimit(t *testing.T) {
	assert.Equal(t, defaultNamespaceFanoutLimit, (&conn{flavor: FlavorMongoDB}).planningFanoutLimit())
	assert.Equal(t, defaultNamespaceFanoutLimit, (&conn{flavor: FlavorDocumentDB}).planningFanoutLimit())
	assert.Equal(t, 12, (&conn{settings: ConnectorSettings{NamespaceFanout: 12}, flavor: FlavorDocumentDB}).planningFanoutLimit())
}

func TestDocumentDBSamplingFanout(t *testing.T) {
	assert.Equal(t, defaultDocumentDBSamplingFanoutLimit, (&conn{}).documentDBSamplingFanout())
	assert.Equal(t, 17, (&conn{settings: ConnectorSettings{DocumentDBSamplingFanout: 17}}).documentDBSamplingFanout())
}

// buildIdFilter

func TestBuildIdFilter_EmptyId(t *testing.T) {
	c := &conn{}
	_, _, err := c.buildIdFilter(&adiomv1.Update{})
	assert.ErrorContains(t, err, "unexpected empty id")
}

func TestBuildIdFilter_BackwardsCompatNoName(t *testing.T) {
	c := &conn{}
	// A single id part without a name should be treated as _id.
	part := bsonID(t, "", "abc")
	part.Name = ""
	filter, key, err := c.buildIdFilter(&adiomv1.Update{Id: []*adiomv1.BsonValue{part}})
	require.NoError(t, err)
	require.Len(t, filter, 1)
	assert.Equal(t, "_id", filter[0].Key)
	assert.NotEmpty(t, key)
}

func TestBuildIdFilter_NoIdWhenFullDocKeyDisabled(t *testing.T) {
	c := &conn{settings: ConnectorSettings{FullDocumentKey: false}}
	// Non-_id parts get filtered, leaving empty idFilter.
	_, _, err := c.buildIdFilter(&adiomv1.Update{Id: []*adiomv1.BsonValue{bsonID(t, "shard", "s1")}})
	assert.ErrorContains(t, err, "_id not found")
}

func TestBuildIdFilter_FullDocumentKeyKeepsAllParts(t *testing.T) {
	c := &conn{settings: ConnectorSettings{FullDocumentKey: true}}
	filter, key, err := c.buildIdFilter(&adiomv1.Update{Id: []*adiomv1.BsonValue{
		bsonID(t, "_id", "doc1"),
		bsonID(t, "shard", "s1"),
	}})
	require.NoError(t, err)
	require.Len(t, filter, 2)
	assert.Equal(t, "_id", filter[0].Key)
	assert.Equal(t, "shard", filter[1].Key)
	assert.NotEmpty(t, key)
}

func TestBuildIdFilter_DedupKeyDiffersByNormalizedId(t *testing.T) {
	c := &conn{settings: ConnectorSettings{FullDocumentKey: false}}
	_, k1, err := c.buildIdFilter(&adiomv1.Update{Id: []*adiomv1.BsonValue{
		bsonID(t, "_id", "doc1"),
		bsonID(t, "shard", "s1"),
	}})
	require.NoError(t, err)
	_, k2, err := c.buildIdFilter(&adiomv1.Update{Id: []*adiomv1.BsonValue{
		bsonID(t, "_id", "doc1"),
		bsonID(t, "shard", "s2"),
	}})
	require.NoError(t, err)
	// FullDocumentKey=false -> both collapse to just _id=doc1, so keys match.
	assert.Equal(t, k1, k2)

	c.settings.FullDocumentKey = true
	_, k3, err := c.buildIdFilter(&adiomv1.Update{Id: []*adiomv1.BsonValue{
		bsonID(t, "_id", "doc1"),
		bsonID(t, "shard", "s1"),
	}})
	require.NoError(t, err)
	_, k4, err := c.buildIdFilter(&adiomv1.Update{Id: []*adiomv1.BsonValue{
		bsonID(t, "_id", "doc1"),
		bsonID(t, "shard", "s2"),
	}})
	require.NoError(t, err)
	// With FullDocumentKey=true the shard value must be part of the key.
	assert.NotEqual(t, k3, k4)
}

// stripIdFields

func TestStripIdFields_NoStripNeeded(t *testing.T) {
	raw := bson.Raw(mustMarshal(t, bson.M{"a": 1, "b": 2}))
	idFilter := bson.D{{Key: "_id", Value: "x"}}
	out, err := stripIdFields(raw, idFilter)
	require.NoError(t, err)
	// Bytes are unchanged when no id fields are present.
	assert.Equal(t, []byte(raw), []byte(out))
}

func TestStripIdFields_StripsIdField(t *testing.T) {
	raw := bson.Raw(mustMarshal(t, bson.M{"_id": "x", "a": 1}))
	idFilter := bson.D{{Key: "_id", Value: "x"}}
	out, err := stripIdFields(raw, idFilter)
	require.NoError(t, err)
	var decoded bson.M
	require.NoError(t, bson.Unmarshal(out, &decoded))
	assert.NotContains(t, decoded, "_id")
	assert.Equal(t, int32(1), decoded["a"])
}

func TestStripIdFields_OnlyIdReturnsNil(t *testing.T) {
	raw := bson.Raw(mustMarshal(t, bson.M{"_id": "x"}))
	idFilter := bson.D{{Key: "_id", Value: "x"}}
	out, err := stripIdFields(raw, idFilter)
	require.NoError(t, err)
	assert.Nil(t, out)
}

func TestStripIdFields_StripsCompositeIdFields(t *testing.T) {
	raw := bson.Raw(mustMarshal(t, bson.M{"_id": "x", "shard": "s", "a": 1}))
	idFilter := bson.D{
		{Key: "_id", Value: "x"},
		{Key: "shard", Value: "s"},
	}
	out, err := stripIdFields(raw, idFilter)
	require.NoError(t, err)
	var decoded bson.M
	require.NoError(t, bson.Unmarshal(out, &decoded))
	assert.NotContains(t, decoded, "_id")
	assert.NotContains(t, decoded, "shard")
	assert.Equal(t, int32(1), decoded["a"])
}

func TestStripIdFields_MalformedBsonReturnsError(t *testing.T) {
	_, err := stripIdFields(bson.Raw([]byte{0x01, 0x02, 0x03}), bson.D{{Key: "_id", Value: "x"}})
	assert.Error(t, err)
}

// buildBulkModels

func insertU(t *testing.T, id string, data bson.M) *adiomv1.Update {
	return &adiomv1.Update{
		Id:   []*adiomv1.BsonValue{bsonID(t, "_id", id)},
		Type: adiomv1.UpdateType_UPDATE_TYPE_INSERT,
		Data: mustMarshal(t, data),
	}
}

func deleteU(t *testing.T, id string) *adiomv1.Update {
	return &adiomv1.Update{
		Id:   []*adiomv1.BsonValue{bsonID(t, "_id", id)},
		Type: adiomv1.UpdateType_UPDATE_TYPE_DELETE,
	}
}

func partialU(t *testing.T, id string, data bson.M, unset ...string) *adiomv1.Update {
	u := &adiomv1.Update{
		Id:                 []*adiomv1.BsonValue{bsonID(t, "_id", id)},
		Type:               adiomv1.UpdateType_UPDATE_TYPE_PARTIAL_UPDATE,
		PartialUpdateUnset: unset,
	}
	if data != nil {
		u.Data = mustMarshal(t, data)
	}
	return u
}

func TestBuildBulkModels_Empty(t *testing.T) {
	c := &conn{}
	models, ordered, err := c.buildBulkModels(nil)
	require.NoError(t, err)
	assert.Empty(t, models)
	assert.False(t, ordered)
}

func TestBuildBulkModels_AllUniqueStaysUnordered(t *testing.T) {
	c := &conn{}
	models, ordered, err := c.buildBulkModels([]*adiomv1.Update{
		insertU(t, "a", bson.M{"x": 1}),
		insertU(t, "b", bson.M{"x": 2}),
		deleteU(t, "c"),
	})
	require.NoError(t, err)
	assert.False(t, ordered)
	assert.Len(t, models, 3)
}

func TestBuildBulkModels_DedupKeepsLast(t *testing.T) {
	c := &conn{}
	models, ordered, err := c.buildBulkModels([]*adiomv1.Update{
		insertU(t, "a", bson.M{"v": 1}),
		insertU(t, "a", bson.M{"v": 2}),
		insertU(t, "a", bson.M{"v": 3}),
	})
	require.NoError(t, err)
	assert.False(t, ordered)
	require.Len(t, models, 1)
	replace, ok := models[0].(*mongo.ReplaceOneModel)
	require.True(t, ok)
	var doc bson.M
	require.NoError(t, bson.Unmarshal(replace.Replacement.(bson.Raw), &doc))
	assert.Equal(t, int32(3), doc["v"])
}

func TestBuildBulkModels_DeleteAfterInsertsKeepsOnlyDelete(t *testing.T) {
	c := &conn{}
	models, ordered, err := c.buildBulkModels([]*adiomv1.Update{
		insertU(t, "a", bson.M{"v": 1}),
		insertU(t, "a", bson.M{"v": 2}),
		deleteU(t, "a"),
	})
	require.NoError(t, err)
	assert.False(t, ordered)
	require.Len(t, models, 1)
	_, ok := models[0].(*mongo.DeleteOneModel)
	assert.True(t, ok)
}

func TestBuildBulkModels_PartialLastTriggersOrdered(t *testing.T) {
	c := &conn{}
	models, ordered, err := c.buildBulkModels([]*adiomv1.Update{
		insertU(t, "a", bson.M{"v": 1}),
		partialU(t, "a", bson.M{"w": 2}),
	})
	require.NoError(t, err)
	assert.True(t, ordered)
	require.Len(t, models, 2)
	// Ordered prefix runs first: the original insert, then the partial.
	_, isReplace := models[0].(*mongo.ReplaceOneModel)
	assert.True(t, isReplace)
	_, isUpdate := models[1].(*mongo.UpdateOneModel)
	assert.True(t, isUpdate)
}

func TestBuildBulkModels_PartialLastOrderedPreservesChronology(t *testing.T) {
	c := &conn{}
	models, ordered, err := c.buildBulkModels([]*adiomv1.Update{
		insertU(t, "a", bson.M{"v": 1}),
		partialU(t, "a", bson.M{"w": 2}),
		insertU(t, "a", bson.M{"v": 3}),
		partialU(t, "a", bson.M{"x": 4}),
	})
	require.NoError(t, err)
	assert.True(t, ordered)
	// Four ops, all for the same id; ordered must preserve chronological order.
	require.Len(t, models, 4)
	_, m0 := models[0].(*mongo.ReplaceOneModel)
	_, m1 := models[1].(*mongo.UpdateOneModel)
	_, m2 := models[2].(*mongo.ReplaceOneModel)
	_, m3 := models[3].(*mongo.UpdateOneModel)
	assert.True(t, m0 && m1 && m2 && m3)
}

func TestBuildBulkModels_PartialOnlyIdInDataSkipped(t *testing.T) {
	c := &conn{}
	// Partial with data that contains only _id (stripped to nil) and no unset -> no-op.
	u := partialU(t, "a", bson.M{"_id": "a"})
	models, ordered, err := c.buildBulkModels([]*adiomv1.Update{u})
	require.NoError(t, err)
	assert.False(t, ordered)
	assert.Empty(t, models)
}

func TestBuildBulkModels_PartialUnsetOnly(t *testing.T) {
	c := &conn{}
	u := partialU(t, "a", nil, "foo", "bar")
	models, ordered, err := c.buildBulkModels([]*adiomv1.Update{u})
	require.NoError(t, err)
	assert.False(t, ordered)
	require.Len(t, models, 1)
	upd, ok := models[0].(*mongo.UpdateOneModel)
	require.True(t, ok)
	body := upd.Update.(bson.M)
	assert.NotContains(t, body, "$set")
	unset, ok := body["$unset"].(bson.M)
	require.True(t, ok)
	assert.Contains(t, unset, "foo")
	assert.Contains(t, unset, "bar")
}

func TestBuildBulkModels_PartialUnsetFiltersIdFields(t *testing.T) {
	c := &conn{}
	u := partialU(t, "a", bson.M{"x": 1}, "_id", "foo")
	models, ordered, err := c.buildBulkModels([]*adiomv1.Update{u})
	require.NoError(t, err)
	assert.False(t, ordered)
	require.Len(t, models, 1)
	upd, ok := models[0].(*mongo.UpdateOneModel)
	require.True(t, ok)
	body := upd.Update.(bson.M)
	unset, ok := body["$unset"].(bson.M)
	require.True(t, ok)
	assert.NotContains(t, unset, "_id")
	assert.Contains(t, unset, "foo")
}

func TestBuildBulkModels_PartialUnsetOnlyIdBecomesNoop(t *testing.T) {
	c := &conn{}
	// Unset only contains _id which gets filtered -> empty update -> skipped.
	u := partialU(t, "a", nil, "_id")
	models, ordered, err := c.buildBulkModels([]*adiomv1.Update{u})
	require.NoError(t, err)
	assert.False(t, ordered)
	assert.Empty(t, models)
}

func TestBuildBulkModels_FullDocumentKeyDedupesByComposite(t *testing.T) {
	c := &conn{settings: ConnectorSettings{FullDocumentKey: true}}
	mk := func(id, shard string, v int) *adiomv1.Update {
		return &adiomv1.Update{
			Id: []*adiomv1.BsonValue{
				bsonID(t, "_id", id),
				bsonID(t, "shard", shard),
			},
			Type: adiomv1.UpdateType_UPDATE_TYPE_INSERT,
			Data: mustMarshal(t, bson.M{"v": v}),
		}
	}
	models, ordered, err := c.buildBulkModels([]*adiomv1.Update{
		mk("a", "s1", 1),
		mk("a", "s2", 2), // same _id, different shard -> distinct doc
		mk("a", "s1", 3), // same composite as first -> dedup replaces it
	})
	require.NoError(t, err)
	assert.False(t, ordered)
	// Two unique docs after dedup.
	assert.Len(t, models, 2)
}

func TestBuildBulkModels_ErrorPropagates(t *testing.T) {
	c := &conn{}
	_, _, err := c.buildBulkModels([]*adiomv1.Update{{Type: adiomv1.UpdateType_UPDATE_TYPE_INSERT}})
	assert.ErrorContains(t, err, "unexpected empty id")
}

func TestSamplePipeline_MatchesQueryAfterSample(t *testing.T) {
	q := bson.D{{"_id", bson.D{{"$type", "string"}}}}
	assert.Equal(t, mongo.Pipeline{
		{{"$sample", bson.D{{"size", int64(10)}}}},
		{{"$match", q}},
		{{"$project", bson.D{{"_id", 1}}}},
	}, samplePipeline(10, q))
}

func TestSamplePipeline_NoQuery(t *testing.T) {
	assert.Equal(t, mongo.Pipeline{
		{{"$sample", bson.D{{"size", int64(1)}}}},
		{{"$project", bson.D{{"_id", 1}}}},
	}, samplePipeline(1, nil))
}

func TestIDTypeAliasesCoverOrdinaryIDTypes(t *testing.T) {
	for _, typ := range []bson.Type{
		bson.TypeNull, bson.TypeUndefined, bson.TypeDouble, bson.TypeInt32, bson.TypeInt64, bson.TypeDecimal128,
		bson.TypeString, bson.TypeSymbol, bson.TypeEmbeddedDocument, bson.TypeBinary, bson.TypeObjectID,
		bson.TypeBoolean, bson.TypeDateTime, bson.TypeTimestamp,
	} {
		assert.Contains(t, idTypeAliases, bsonTypeSortOrder(typ), typ.String())
	}
	for _, typ := range []bson.Type{bson.TypeMinKey, bson.TypeMaxKey, bson.TypeRegex, bson.TypeDBPointer, bson.TypeJavaScript, bson.TypeCodeWithScope} {
		assert.NotContains(t, idTypeAliases, bsonTypeSortOrder(typ), typ.String())
	}
}

func TestNewConnWithClientRejectsInvalidQuery(t *testing.T) {
	_, err := NewConnWithClient(nil, ConnectorSettings{Query: "{bad"})
	assert.Error(t, err)
}

func TestNewConnWithClientPageBufferTTL(t *testing.T) {
	c, err := NewConnWithClient(nil, ConnectorSettings{})
	require.NoError(t, err)
	assert.Equal(t, time.Hour, c.(*conn).settings.PageBufferTTL)
	c, err = NewConnWithClient(nil, ConnectorSettings{PageBufferTTL: time.Minute})
	require.NoError(t, err)
	assert.Equal(t, time.Minute, c.(*conn).settings.PageBufferTTL)
}

func TestAndFilter(t *testing.T) {
	a := bson.D{{"_id", bson.D{{"$gt", 1}}}}
	b := bson.D{{"_id", bson.D{{"$type", "string"}}}}
	assert.Equal(t, a, andFilter(a, nil))
	assert.Equal(t, b, andFilter(bson.D{}, b))
	assert.Equal(t, bson.D{{"$and", bson.A{a, b}}}, andFilter(a, b))
	assert.NotNil(t, andFilter(bson.D{}, nil))
}

func TestMaybeUnavailableErrorClassifiesSourceFailures(t *testing.T) {
	dnsTimeout := &net.OpError{Op: "dial", Net: "tcp", Err: &net.DNSError{Err: "client timed out waiting for server response", Name: "mongos", IsTimeout: true}}
	cases := map[string]struct {
		err  error
		code connect.Code
	}{
		"labeled network error":           {mongo.CommandError{Labels: []string{"NetworkError"}, Wrapped: io.EOF}, connect.CodeUnavailable},
		"handshake dns timeout":           {topology.ConnectionError{Wrapped: dnsTimeout}, connect.CodeUnavailable},
		"unlabeled handshake eof":         {topology.ConnectionError{Wrapped: io.ErrUnexpectedEOF}, connect.CodeUnavailable},
		"unlabeled eof":                   {topology.ConnectionError{Wrapped: io.EOF}, connect.CodeUnavailable},
		"server selection timeout":        {topology.ServerSelectionError{Wrapped: context.DeadlineExceeded}, connect.CodeUnavailable},
		"driver deadline":                 {fmt.Errorf("find: %w", context.DeadlineExceeded), connect.CodeUnavailable},
		"server maxTimeMS":                {mongo.CommandError{Code: 50, Name: "MaxTimeMSExpired"}, connect.CodeUnavailable},
		"cancelled flow":                  {topology.ConnectionError{Wrapped: context.Canceled}, connect.CodeCanceled},
		"cosmos throttled command":        {mongo.CommandError{Code: 16500, Name: "TooManyRequests"}, connect.CodeUnavailable},
		"cosmos throttled bulk":           {mongo.BulkWriteException{WriteErrors: []mongo.BulkWriteError{{WriteError: mongo.WriteError{Code: 16500}}}}, connect.CodeUnavailable},
		"throttled bulk with duplicates":  {mongo.BulkWriteException{WriteErrors: []mongo.BulkWriteError{{WriteError: mongo.WriteError{Code: 11000}}, {WriteError: mongo.WriteError{Code: 16500}}}}, connect.CodeUnavailable},
		"throttled bulk with invalid doc": {mongo.BulkWriteException{WriteErrors: []mongo.BulkWriteError{{WriteError: mongo.WriteError{Code: 16500}}, {WriteError: mongo.WriteError{Code: 2}}}}, connect.CodeInternal},
		"bulk with only duplicates":       {mongo.BulkWriteException{WriteErrors: []mongo.BulkWriteError{{WriteError: mongo.WriteError{Code: 11000}}}}, connect.CodeInternal},
		"bulk write concern shutdown":     {mongo.BulkWriteException{WriteConcernError: &mongo.WriteConcernError{Code: 91}}, connect.CodeUnavailable},
		"bulk write concern failed":       {mongo.BulkWriteException{WriteConcernError: &mongo.WriteConcernError{Code: 64}}, connect.CodeInternal},
		"primary stepped down":            {mongo.CommandError{Code: 189, Name: "PrimarySteppedDown"}, connect.CodeUnavailable},
		"retryable write label":           {mongo.CommandError{Code: 1, Labels: []string{"RetryableWriteError"}}, connect.CodeUnavailable},
		"auth failure":                    {mongo.CommandError{Code: 13, Name: "Unauthorized"}, connect.CodeInternal},
		"unknown":                         {errors.New("boom"), connect.CodeInternal},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.code, connect.CodeOf(maybeUnavailableError(tc.err)))
		})
	}
}
