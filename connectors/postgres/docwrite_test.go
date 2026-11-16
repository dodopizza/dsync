package postgres

import (
	"errors"
	"fmt"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

const marker = "SECRETMARKER"

func rawValue(t *testing.T, v any) bson.RawValue {
	t.Helper()
	return bson.Raw(rawDoc(t, bson.D{{Key: "v", Value: v}})).Lookup("v")
}

func testTable() docTable {
	return docTable{
		Columns: []docColumn{
			{Name: "Region", Type: "text"},
			{Name: "MixedCase", Type: "integer"},
			{Name: "updated_at", Type: "timestamp with time zone"},
		},
		PrimaryKey: []string{"id", "Region"},
	}
}

func testPlan(t *testing.T, settings PostgresSettings) *docPlan {
	t.Helper()
	plan, err := buildDocPlan("app.items", testTable(), settings)
	require.NoError(t, err)
	return plan
}

func TestScalarValueMapsBSONTypesToGoValuesPgxAccepts(t *testing.T) {
	when := time.Date(2024, 5, 6, 7, 8, 9, 0, time.FixedZone("x", 3600))
	oid := bson.NewObjectID()
	dec, err := bson.ParseDecimal128("1.50")
	require.NoError(t, err)
	cases := []struct {
		name  string
		value any
		want  any
	}{
		{"string to text", "x", "x"},
		{"int32 to integer", int32(7), int32(7)},
		{"int64 to bigint", int64(8), int64(8)},
		{"double to double precision", 1.5, 1.5},
		{"decimal128 to numeric text", dec, "1.50"},
		{"bool to boolean", true, true},
		{"date to timestamptz in UTC", bson.NewDateTimeFromTime(when), when.UTC()},
		{"objectid to hex text", oid, oid.Hex()},
		{"binary to bytea", bson.Binary{Subtype: 0, Data: []byte{1, 2}}, []byte{1, 2}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := scalarValue(rawValue(t, c.value))
			require.NoError(t, err)
			assert.Equal(t, c.want, got)
		})
	}
}

func TestScalarValueRejectsEveryOtherTypeNamingTheTypeNotTheValue(t *testing.T) {
	values := []any{
		bson.D{{Key: "k", Value: marker}},
		bson.A{marker},
		bson.Regex{Pattern: marker},
		bson.Timestamp{T: 1, I: 2},
		bson.MinKey{},
		bson.MaxKey{},
		bson.JavaScript(marker),
		bson.Symbol(marker),
		bson.DBPointer{DB: marker, Pointer: bson.NewObjectID()},
	}
	for _, v := range values {
		rv := rawValue(t, v)
		_, err := scalarValue(rv)
		require.Error(t, err, rv.Type.String())
		assert.Contains(t, err.Error(), rv.Type.String())
		assert.NotContains(t, err.Error(), marker)
	}
}

func TestBuildDocPlanChecksUpdatedAtAtShapeLoad(t *testing.T) {
	cases := []struct {
		name     string
		settings PostgresSettings
		table    docTable
		contains string
	}{
		{"field without column", PostgresSettings{UpdatedAtField: "ts"}, testTable(), "updated-at column"},
		{"missing column", PostgresSettings{UpdatedAtColumn: "gone"}, testTable(), "gone"},
		{"wrong type", PostgresSettings{UpdatedAtColumn: "MixedCase"}, testTable(), "MixedCase"},
		{"primary key column not writable", PostgresSettings{}, docTable{PrimaryKey: []string{"id", "Region"}}, "Region"},
		{"updated-at column in primary key", PostgresSettings{UpdatedAtColumn: "updated_at"}, docTable{
			Columns:    []docColumn{{Name: "updated_at", Type: "timestamp with time zone"}},
			PrimaryKey: []string{"id", "updated_at"},
		}, "updated_at"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := buildDocPlan("app.items", c.table, c.settings)
			require.Error(t, err)
			assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
			assert.Contains(t, err.Error(), "app.items")
			assert.Contains(t, err.Error(), c.contains)
		})
	}
	for _, typ := range []string{"timestamp with time zone", "timestamp without time zone"} {
		table := testTable()
		table.Columns[2].Type = typ
		_, err := buildDocPlan("app.items", table, PostgresSettings{UpdatedAtColumn: "updated_at"})
		require.NoError(t, err, typ)
	}
}

func TestDocUpsertSQLQuotesIdentifiersAndSkipsUnchangedRows(t *testing.T) {
	plan, err := buildDocPlan(`App Schema.items`, docTable{
		Columns:    []docColumn{{Name: "Region", Type: "text"}, {Name: `we"ird`, Type: "integer"}, {Name: "Stamp", Type: "timestamp with time zone"}},
		PrimaryKey: []string{"id", "Region"},
		Constraint: `items "pk"`,
	}, PostgresSettings{UpdatedAtColumn: "Stamp"})
	require.NoError(t, err)

	want := `INSERT INTO "App Schema"."items" AS t ("id", "doc", "Region", "we""ird", "Stamp") VALUES ($1, $2, $3, $4, $5) ` +
		`ON CONFLICT ON CONSTRAINT "items ""pk""" DO UPDATE SET "doc" = EXCLUDED."doc", "we""ird" = EXCLUDED."we""ird", "Stamp" = EXCLUDED."Stamp" ` +
		`WHERE (t."doc", t."Region", t."we""ird") IS DISTINCT FROM (EXCLUDED."doc", EXCLUDED."Region", EXCLUDED."we""ird")`
	assert.Equal(t, want, plan.query)
}

func TestDocBatchArgsStoresWholeDocumentWithEncodedIDAndSelectedColumns(t *testing.T) {
	plan := testPlan(t, PostgresSettings{UpdatedAtColumn: "updated_at"})
	raw := rawDoc(t, bson.D{{Key: "_id", Value: "k1"}, {Key: "Region", Value: "eu"}, {Key: "MixedCase", Value: int32(4)}, {Key: "extra", Value: true}})
	when := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)

	batch, err := docBatchArgs(plan, [][]byte{raw}, when)

	require.NoError(t, err)
	require.Len(t, batch, 1)
	require.Len(t, batch[0], 5)
	assert.Equal(t, "s:k1", batch[0][0])
	assert.JSONEq(t, `{"_id":"k1","Region":"eu","MixedCase":{"$numberInt":"4"},"extra":true}`, string(batch[0][1].([]byte)))
	assert.Equal(t, "eu", batch[0][2])
	assert.Equal(t, int32(4), batch[0][3])
	assert.Equal(t, when, batch[0][4])
}

func TestDocBatchArgsAbsentAndNullColumnsAreNull(t *testing.T) {
	plan := testPlan(t, PostgresSettings{})
	raw := rawDoc(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "Region", Value: "eu"}, {Key: "MixedCase", Value: nil}})

	batch, err := docBatchArgs(plan, [][]byte{raw}, time.Time{})

	require.NoError(t, err)
	assert.Nil(t, batch[0][3])
}

func TestDocBatchArgsUpdatedAtFieldMustBeADateElseClock(t *testing.T) {
	plan := testPlan(t, PostgresSettings{UpdatedAtColumn: "updated_at", UpdatedAtField: "ts"})
	clock := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	stamp := time.Date(2020, 2, 3, 4, 5, 6, 0, time.UTC)
	base := bson.D{{Key: "_id", Value: int32(1)}, {Key: "Region", Value: "eu"}}

	fromField, err := docBatchArgs(plan, [][]byte{rawDoc(t, append(base, bson.E{Key: "ts", Value: bson.NewDateTimeFromTime(stamp)}))}, clock)
	require.NoError(t, err)
	assert.Equal(t, stamp, fromField[0][4])

	fromClock, err := docBatchArgs(plan, [][]byte{rawDoc(t, base)}, clock)
	require.NoError(t, err)
	assert.Equal(t, clock, fromClock[0][4])

	for _, bad := range []any{"2020-01-01", int64(5)} {
		_, err = docBatchArgs(plan, [][]byte{rawDoc(t, append(base, bson.E{Key: "ts", Value: bad}))}, clock)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ts")
		assert.Contains(t, err.Error(), rawValue(t, bad).Type.String())
		assert.NotContains(t, err.Error(), "2020-01-01")
	}
}

func TestDocBatchArgsChecksEncodingBeforeID(t *testing.T) {
	plan := testPlan(t, PostgresSettings{})
	duplicate := bsoncore.NewDocumentBuilder().AppendString("a", "x").AppendString("a", "y").Build()
	for name, raw := range map[string][]byte{
		"U+0000":         rawDoc(t, bson.D{{Key: "a", Value: "x\x00y"}}),
		"duplicate keys": duplicate,
	} {
		_, err := docBatchArgs(plan, [][]byte{raw}, time.Time{})
		require.Error(t, err, name)
		assert.NotContains(t, err.Error(), "_id", name)
	}
}

func TestDocBatchArgsRejectsMissingIDAndBadColumnsNamingDocumentPosition(t *testing.T) {
	plan := testPlan(t, PostgresSettings{})
	good := rawDoc(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "Region", Value: "eu"}})
	cases := map[string]struct {
		raw      []byte
		contains string
	}{
		"missing _id":     {rawDoc(t, bson.D{{Key: "Region", Value: "eu"}}), "_id"},
		"non-scalar":      {rawDoc(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "Region", Value: "eu"}, {Key: "MixedCase", Value: bson.A{marker}}}), "MixedCase"},
		"key column null": {rawDoc(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "Region", Value: nil}}), "Region"},
		"key column gone": {rawDoc(t, bson.D{{Key: "_id", Value: int32(1)}}), "Region"},
	}
	for name, c := range cases {
		_, err := docBatchArgs(plan, [][]byte{good, c.raw}, time.Time{})
		require.Error(t, err, name)
		assert.Contains(t, err.Error(), "document 1", name)
		assert.Contains(t, err.Error(), c.contains, name)
		assert.NotContains(t, err.Error(), marker, name)
	}
}

func TestDocBatchArgsRejectionsAreNonRetryableForTheSharedRetryLoop(t *testing.T) {
	plan := testPlan(t, PostgresSettings{})
	_, err := docBatchArgs(plan, [][]byte{rawDoc(t, bson.D{{Key: "Region", Value: "eu"}})}, time.Time{})
	require.Error(t, err)
	code := connect.CodeOf(err)
	assert.Equal(t, connect.CodeInvalidArgument, code)
	for _, retryable := range []connect.Code{connect.CodeAborted, connect.CodeResourceExhausted, connect.CodeUnavailable, connect.CodeDeadlineExceeded} {
		assert.NotEqual(t, retryable, code)
	}
}

func TestKeyValuesReturnsIDThenKeyColumnsInPrimaryKeyOrder(t *testing.T) {
	got, err := keyValues([]string{"id", "Region", "Tenant"}, "s:k", map[string]any{"Tenant": int32(3), "Region": "eu", "other": 1})

	require.NoError(t, err)
	assert.Equal(t, []any{"s:k", "eu", int32(3)}, got)

	_, err = keyValues([]string{"id", "Region"}, "s:k", map[string]any{"Region": nil})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Region")
}

func TestDocStateLoadsOncePerNamespaceUnderParallelFirstWritesAndDoesNotCacheFailures(t *testing.T) {
	var state docState
	var loads atomic.Int32
	plan := &docPlan{query: "q"}
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			got, err := state.get("app.items", func() (*docPlan, error) {
				loads.Add(1)
				return plan, nil
			})
			assert.NoError(t, err)
			assert.Same(t, plan, got)
		})
	}
	wg.Wait()
	assert.EqualValues(t, 1, loads.Load())

	var failures int
	for range 2 {
		_, err := state.get("app.other", func() (*docPlan, error) {
			failures++
			return nil, errors.New("not yet")
		})
		require.Error(t, err)
	}
	assert.Equal(t, 2, failures)
}

func TestDocWriteErrorKeepsPositionColumnAndSQLStateButNeverTheServerMessage(t *testing.T) {
	pgErr := &pgconn.PgError{Code: "22P02", Message: `invalid input syntax for type integer: "` + marker + `"`, ColumnName: "MixedCase"}
	err := docWriteError(3, fmt.Errorf("wrapped: %w", pgErr))
	assert.Contains(t, err.Error(), "document 3")
	assert.Contains(t, err.Error(), "MixedCase")
	assert.Contains(t, err.Error(), "22P02")
	assert.NotContains(t, err.Error(), marker)

	clientErr := docWriteError(1, fmt.Errorf("unable to encode %s into binary format", marker))
	assert.NotContains(t, clientErr.Error(), marker)
	assert.Contains(t, clientErr.Error(), "document 1")

	serialization := docWriteError(0, &pgconn.PgError{Code: "40001", Message: marker})
	assert.Equal(t, connect.CodeAborted, connect.CodeOf(serialization))
	assert.NotContains(t, serialization.Error(), marker)

	unavailable := docWriteError(-1, &net.OpError{Op: "read", Err: errors.New("reset")})
	assert.Equal(t, connect.CodeUnavailable, connect.CodeOf(unavailable))
}

func TestDocBatchArgsNullUpdatedAtFieldFallsBackToTheClock(t *testing.T) {
	plan := testPlan(t, PostgresSettings{UpdatedAtColumn: "updated_at", UpdatedAtField: "ts"})
	clock := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)

	batch, err := docBatchArgs(plan, [][]byte{rawDoc(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "Region", Value: "eu"}, {Key: "ts", Value: nil}})}, clock)

	require.NoError(t, err)
	assert.Equal(t, clock, batch[0][4])
}

func TestBuildDocPlanAllowsOnlyComparableSelectedColumnTypes(t *testing.T) {
	allowed := []string{"text", "character varying", "character", "smallint", "integer", "bigint", "real", "double precision",
		"numeric", "boolean", "timestamp with time zone", "timestamp without time zone", "date", "bytea", "uuid"}
	for _, typ := range allowed {
		table := docTable{Columns: []docColumn{{Name: "extra", Type: typ}}, PrimaryKey: []string{"id"}}
		_, err := buildDocPlan("app.items", table, PostgresSettings{})
		require.NoError(t, err, typ)
	}
	for _, typ := range []string{"json", "jsonb", "xml", "point", "text[]", "inet"} {
		table := docTable{Columns: []docColumn{{Name: "extra", Type: typ}}, PrimaryKey: []string{"id"}}
		_, err := buildDocPlan("app.items", table, PostgresSettings{})
		require.Error(t, err, typ)
		assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
		for _, part := range []string{"app.items", "extra", typ} {
			assert.Contains(t, err.Error(), part)
		}
	}
	keyed := docTable{Columns: []docColumn{{Name: "net", Type: "inet"}}, PrimaryKey: []string{"id", "net"}}
	_, err := buildDocPlan("app.items", keyed, PostgresSettings{})
	require.NoError(t, err)
}

func TestDocBatchArgsRejectsNonIntegralDoublesForIntegerColumnsOnly(t *testing.T) {
	table := docTable{
		Columns:    []docColumn{{Name: "small", Type: "smallint"}, {Name: "big", Type: "bigint"}, {Name: "ratio", Type: "double precision"}},
		PrimaryKey: []string{"id"},
	}
	plan, err := buildDocPlan("app.items", table, PostgresSettings{})
	require.NoError(t, err)
	doc := func(field string, v float64) []byte {
		return rawDoc(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: field, Value: v}})
	}

	for _, bad := range []struct {
		field string
		value float64
	}{{"small", 1.5}, {"big", 2.0000001}, {"big", math.NaN()}, {"big", math.Inf(1)}} {
		_, err := docBatchArgs(plan, [][]byte{doc(bad.field, bad.value)}, time.Time{})
		require.Error(t, err, bad.field)
		assert.Contains(t, err.Error(), bad.field)
		assert.NotContains(t, err.Error(), "1.5")
	}

	whole, err := docBatchArgs(plan, [][]byte{doc("big", 3)}, time.Time{})
	require.NoError(t, err)
	assert.Equal(t, int64(3), whole[0][3])

	fraction, err := docBatchArgs(plan, [][]byte{doc("ratio", 1.5)}, time.Time{})
	require.NoError(t, err)
	assert.Equal(t, 1.5, fraction[0][4])
}
