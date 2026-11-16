//go:build external

package postgres

import (
	"context"
	"sync"
	"testing"
	"time"

	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func writeDocs(ctx context.Context, c *conn, namespace string, docs ...[]byte) error {
	_, err := c.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{Namespace: namespace, Data: docs}))
	return err
}

type docRow struct {
	ID        string
	Doc       string
	Mixed     *int32
	UpdatedAt time.Time
	Xmin      string
}

func readRow(ctx context.Context, t *testing.T, f *docFixture, id string) docRow {
	t.Helper()
	var row docRow
	err := f.pool.QueryRow(ctx, `SELECT id, doc::text, "MixedCase", updated_at, xmin::text FROM `+f.schema+`."Items" WHERE id = $1`, id).
		Scan(&row.ID, &row.Doc, &row.Mixed, &row.UpdatedAt, &row.Xmin)
	must(t, err, "read row")
	return row
}

func newItemsTable(ctx context.Context, t *testing.T, f *docFixture) string {
	f.exec(ctx, t, `CREATE TABLE %s."Items" (id text NOT NULL, doc jsonb NOT NULL, "Region" text NOT NULL, "MixedCase" integer,
		updated_at timestamptz, PRIMARY KEY (id, "Region"))`, f.schema)
	return f.schema + ".Items"
}

func TestDocumentModeCopySkipsUnchangedRowsAndUpdatesChangedOnes(t *testing.T) {
	ctx := testContext(t)
	f := newDocFixture(ctx, t)
	ns := newItemsTable(ctx, t, f)
	clock := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	c := &conn{c: f.pool, settings: PostgresSettings{DocumentMode: true, UpdatedAtColumn: "updated_at"}, now: func() time.Time { return clock }}
	doc := func(mixed int32, extra string) []byte {
		return rawDoc(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "Region", Value: "eu"}, {Key: "MixedCase", Value: mixed}, {Key: "extra", Value: extra}})
	}

	must(t, writeDocs(ctx, c, ns, doc(5, "a")), "first copy")
	first := readRow(ctx, t, f, "n:1")
	assert.Equal(t, clock, first.UpdatedAt.UTC())
	assert.Equal(t, int32(5), *first.Mixed)
	assert.JSONEq(t, `{"_id":{"$numberInt":"1"},"Region":"eu","MixedCase":{"$numberInt":"5"},"extra":"a"}`, first.Doc)

	clock = clock.Add(time.Hour)
	must(t, writeDocs(ctx, c, ns, doc(5, "a")), "identical copy")
	same := readRow(ctx, t, f, "n:1")
	assert.Equal(t, first.Xmin, same.Xmin)
	assert.Equal(t, first.UpdatedAt, same.UpdatedAt)

	must(t, writeDocs(ctx, c, ns, doc(5, "b")), "doc-only change")
	docChanged := readRow(ctx, t, f, "n:1")
	assert.NotEqual(t, first.Xmin, docChanged.Xmin)
	assert.Equal(t, clock, docChanged.UpdatedAt.UTC())

	clock = clock.Add(time.Hour)
	must(t, writeDocs(ctx, c, ns, doc(6, "b")), "column change")
	colChanged := readRow(ctx, t, f, "n:1")
	assert.NotEqual(t, docChanged.Xmin, colChanged.Xmin)
	assert.Equal(t, clock, colChanged.UpdatedAt.UTC())
	assert.Equal(t, int32(6), *colChanged.Mixed)
}

func TestDocumentModeWritesEveryScalarTypeAndNullsAbsentColumns(t *testing.T) {
	ctx := testContext(t)
	f := newDocFixture(ctx, t)
	f.exec(ctx, t, `CREATE TABLE %s.kinds (id text PRIMARY KEY, doc jsonb NOT NULL, s text, i32 integer, i64 bigint, d double precision,
		dec numeric, b boolean, ts timestamptz, oid text, bin bytea, missing text)`, f.schema)
	c := &conn{c: f.pool, settings: PostgresSettings{DocumentMode: true}}
	when := time.Date(2024, 5, 6, 7, 8, 9, 0, time.UTC)
	oid := bson.NewObjectID()
	dec, err := bson.ParseDecimal128("1.50")
	require.NoError(t, err)

	must(t, writeDocs(ctx, c, f.schema+".kinds", rawDoc(t, bson.D{
		{Key: "_id", Value: oid}, {Key: "s", Value: "x"}, {Key: "i32", Value: int32(7)}, {Key: "i64", Value: int64(8)}, {Key: "d", Value: 1.5},
		{Key: "dec", Value: dec}, {Key: "b", Value: true}, {Key: "ts", Value: bson.NewDateTimeFromTime(when)}, {Key: "oid", Value: oid},
		{Key: "bin", Value: bson.Binary{Data: []byte{1, 2}}}, {Key: "missing", Value: nil},
	})), "write")

	var s, decText, oidText string
	var i32 int32
	var i64 int64
	var d float64
	var b bool
	var ts time.Time
	var bin []byte
	var missing *string
	err = f.pool.QueryRow(ctx, `SELECT s, i32, i64, d, dec::text, b, ts, oid, bin, missing FROM `+f.schema+`.kinds`).
		Scan(&s, &i32, &i64, &d, &decText, &b, &ts, &oidText, &bin, &missing)
	must(t, err, "read")
	assert.Equal(t, "x", s)
	assert.Equal(t, int32(7), i32)
	assert.Equal(t, int64(8), i64)
	assert.Equal(t, 1.5, d)
	assert.Equal(t, "1.50", decText)
	assert.True(t, b)
	assert.Equal(t, when, ts.UTC())
	assert.Equal(t, oid.Hex(), oidText)
	assert.Equal(t, []byte{1, 2}, bin)
	assert.Nil(t, missing)
}

func TestDocumentModeServerErrorsNeverCarryDocumentValuesInErrorOrLogs(t *testing.T) {
	ctx := testContext(t)
	f := newDocFixture(ctx, t)
	f.exec(ctx, t, `CREATE TABLE %s.numbers (id text PRIMARY KEY, doc jsonb NOT NULL, n integer)`, f.schema)
	c := &conn{c: f.pool, settings: PostgresSettings{DocumentMode: true}}
	logs := captureLogs(t)

	err := writeDocs(ctx, c, f.schema+".numbers",
		rawDoc(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "n", Value: int32(1)}}),
		rawDoc(t, bson.D{{Key: "_id", Value: int32(2)}, {Key: "n", Value: marker}}),
	)

	require.Error(t, err)
	assert.NotContains(t, err.Error(), marker)
	assert.Contains(t, err.Error(), "document 1")
	assert.Contains(t, err.Error(), "22P02")
	assert.NotContains(t, logs.String(), marker)
	assert.NotEqual(t, connect.CodeUnavailable, connect.CodeOf(err))

	f.exec(ctx, t, `CREATE TABLE %s.tags (id text PRIMARY KEY, doc jsonb NOT NULL, tag text UNIQUE)`, f.schema)
	must(t, writeDocs(ctx, c, f.schema+".tags", rawDoc(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "tag", Value: marker}})), "first tag")
	err = writeDocs(ctx, c, f.schema+".tags", rawDoc(t, bson.D{{Key: "_id", Value: int32(2)}, {Key: "tag", Value: marker}}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "document 0: constraint tags_tag_key: SQLSTATE 23505")
	assert.NotContains(t, err.Error(), marker)
	assert.NotContains(t, logs.String(), marker)
}

func TestDocumentModeClientEncodeFailuresClaimNoBatchPositionAndAreNotRetried(t *testing.T) {
	ctx := testContext(t)
	f := newDocFixture(ctx, t)
	f.exec(ctx, t, `CREATE TABLE %s.numbers (id text PRIMARY KEY, doc jsonb NOT NULL, n bigint)`, f.schema)
	c := &conn{c: f.pool, settings: PostgresSettings{DocumentMode: true}}
	ns := f.schema + ".numbers"
	doc := func(id int32, n any) []byte { return rawDoc(t, bson.D{{Key: "_id", Value: id}, {Key: "n", Value: n}}) }
	insert := func(raw []byte) *adiomv1.Update { return upsertUpdate(adiomv1.UpdateType_UPDATE_TYPE_INSERT, nil, raw) }

	failures := map[string]error{
		"document": writeDocs(ctx, c, ns, doc(1, int32(1)), doc(2, int32(2)), doc(3, true)),
		"update":   writeUpdates(ctx, c, ns, insert(doc(1, int32(1))), insert(doc(2, int32(2))), insert(doc(3, true))),
	}

	for name, err := range failures {
		require.Error(t, err, name)
		assert.NotContains(t, err.Error(), "document ", name)
		assert.NotContains(t, err.Error(), "update ", name)
		assert.Contains(t, err.Error(), "write failed", name)
		for _, retryable := range []connect.Code{connect.CodeAborted, connect.CodeResourceExhausted, connect.CodeUnavailable, connect.CodeDeadlineExceeded} {
			assert.NotEqual(t, retryable, connect.CodeOf(err), name)
		}
	}
	assert.Zero(t, countRows(ctx, t, f, "numbers"))
}

func TestDocumentModeParallelFirstWritesLoadTheShapeSafely(t *testing.T) {
	ctx := testContext(t)
	f := newDocFixture(ctx, t)
	ns := newItemsTable(ctx, t, f)
	c := &conn{c: f.pool, settings: PostgresSettings{DocumentMode: true, UpdatedAtColumn: "updated_at"}}

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			assert.NoError(t, writeDocs(ctx, c, ns, rawDoc(t, bson.D{{Key: "_id", Value: int32(i)}, {Key: "Region", Value: "eu"}})))
		})
	}
	wg.Wait()

	var count int
	must(t, f.pool.QueryRow(ctx, `SELECT count(*) FROM `+f.schema+`."Items"`).Scan(&count), "count")
	assert.Equal(t, 8, count)
}

func TestDocumentModeUpdatedAtColumnOfWrongTypeFailsAtShapeLoad(t *testing.T) {
	ctx := testContext(t)
	f := newDocFixture(ctx, t)
	ns := newItemsTable(ctx, t, f)
	c := &conn{c: f.pool, settings: PostgresSettings{DocumentMode: true, UpdatedAtColumn: "MixedCase"}}

	err := writeDocs(ctx, c, ns, rawDoc(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "Region", Value: "eu"}}))

	require.Error(t, err)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
	assert.Contains(t, err.Error(), "MixedCase")
}

func TestDocumentModeFallbackUpdatedAtIsUTCOnTimestampWithoutTimeZone(t *testing.T) {
	ctx := testContext(t)
	f := newDocFixture(ctx, t)
	f.exec(ctx, t, `CREATE TABLE %s.naive (id text PRIMARY KEY, doc jsonb NOT NULL, updated_at timestamp)`, f.schema)
	local := time.Date(2025, 1, 1, 12, 0, 0, 0, time.FixedZone("x", 3*3600))
	c := &conn{c: f.pool, settings: PostgresSettings{DocumentMode: true, UpdatedAtColumn: "updated_at"}, now: func() time.Time { return local }}

	must(t, writeDocs(ctx, c, f.schema+".naive", rawDoc(t, bson.D{{Key: "_id", Value: int32(1)}})), "write")

	var stored time.Time
	must(t, f.pool.QueryRow(ctx, `SELECT updated_at FROM `+f.schema+`.naive`).Scan(&stored), "read")
	assert.Equal(t, time.Date(2025, 1, 1, 9, 0, 0, 0, time.UTC), stored)
}

func TestDocumentModeCommitTimeErrorsNeverLogOrReturnDocumentValues(t *testing.T) {
	ctx := testContext(t)
	f := newDocFixture(ctx, t)
	f.exec(ctx, t, `CREATE TABLE %[1]s.guarded (id text PRIMARY KEY, doc jsonb NOT NULL, tag text);
		CREATE FUNCTION %[1]s.guard() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'rejected %%', NEW.tag; END $$;
		CREATE CONSTRAINT TRIGGER guard AFTER INSERT ON %[1]s.guarded DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION %[1]s.guard()`, f.schema)
	c := &conn{c: f.pool, settings: PostgresSettings{DocumentMode: true}}
	logs := captureLogs(t)

	err := writeDocs(ctx, c, f.schema+".guarded", rawDoc(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "tag", Value: marker}}))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "P0001")
	assert.NotContains(t, err.Error(), marker)
	assert.NotContains(t, logs.String(), marker)
}

func TestDocumentModeRejectsColumnTypesWithoutEqualityAtShapeLoad(t *testing.T) {
	ctx := testContext(t)
	f := newDocFixture(ctx, t)
	f.exec(ctx, t, `CREATE TABLE %s.blobs (id text PRIMARY KEY, doc jsonb NOT NULL, payload json)`, f.schema)
	c := &conn{c: f.pool, settings: PostgresSettings{DocumentMode: true}}

	err := writeDocs(ctx, c, f.schema+".blobs", rawDoc(t, bson.D{{Key: "_id", Value: int32(1)}}))

	require.Error(t, err)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
	for _, part := range []string{f.schema + ".blobs", "payload", "json"} {
		assert.Contains(t, err.Error(), part)
	}
}

func TestDocumentModeRejectsFractionalDoublesAndStoresWholeOnesInIntegerColumns(t *testing.T) {
	ctx := testContext(t)
	f := newDocFixture(ctx, t)
	f.exec(ctx, t, `CREATE TABLE %s.counts (id text PRIMARY KEY, doc jsonb NOT NULL, n integer)`, f.schema)
	c := &conn{c: f.pool, settings: PostgresSettings{DocumentMode: true}}

	err := writeDocs(ctx, c, f.schema+".counts", rawDoc(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "n", Value: 1.5}}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "n")
	assert.NotContains(t, err.Error(), "1.5")

	must(t, writeDocs(ctx, c, f.schema+".counts", rawDoc(t, bson.D{{Key: "_id", Value: int32(2)}, {Key: "n", Value: 3.0}})), "whole double")
	var n int32
	must(t, f.pool.QueryRow(ctx, `SELECT n FROM `+f.schema+`.counts`).Scan(&n), "read")
	assert.Equal(t, int32(3), n)
}

func TestDocumentModeRestoresSelectedColumnsChangedBehindTheCopy(t *testing.T) {
	ctx := testContext(t)
	f := newDocFixture(ctx, t)
	ns := newItemsTable(ctx, t, f)
	c := &conn{c: f.pool, settings: PostgresSettings{DocumentMode: true}}
	raw := rawDoc(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "Region", Value: "eu"}, {Key: "MixedCase", Value: int32(5)}})
	must(t, writeDocs(ctx, c, ns, raw), "first copy")
	f.exec(ctx, t, `UPDATE %s."Items" SET "MixedCase" = NULL`, f.schema)

	must(t, writeDocs(ctx, c, ns, raw), "second copy")

	var mixed *int32
	must(t, f.pool.QueryRow(ctx, `SELECT "MixedCase" FROM `+f.schema+`."Items"`).Scan(&mixed), "read")
	require.NotNil(t, mixed)
	assert.Equal(t, int32(5), *mixed)
}

func TestDocumentModeStoresUpdatedAtFieldInsteadOfTheClock(t *testing.T) {
	ctx := testContext(t)
	f := newDocFixture(ctx, t)
	ns := newItemsTable(ctx, t, f)
	clock := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	stamp := time.Date(2020, 2, 3, 4, 5, 6, 0, time.UTC)
	c := &conn{c: f.pool, settings: PostgresSettings{DocumentMode: true, UpdatedAtColumn: "updated_at", UpdatedAtField: "ts"}, now: func() time.Time { return clock }}

	must(t, writeDocs(ctx, c, ns,
		rawDoc(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "Region", Value: "eu"}, {Key: "ts", Value: bson.NewDateTimeFromTime(stamp)}}),
		rawDoc(t, bson.D{{Key: "_id", Value: int32(2)}, {Key: "Region", Value: "eu"}}),
	), "write")

	assert.Equal(t, stamp, readRow(ctx, t, f, "n:1").UpdatedAt.UTC())
	assert.Equal(t, clock, readRow(ctx, t, f, "n:2").UpdatedAt.UTC())
}

func TestDocumentModeIgnoresDeferrableUniqueConstraintsBesideThePrimaryKey(t *testing.T) {
	ctx := testContext(t)
	f := newDocFixture(ctx, t)
	f.exec(ctx, t, `CREATE TABLE %s.single (id text, doc jsonb NOT NULL, note text, PRIMARY KEY (id), UNIQUE (id) DEFERRABLE)`, f.schema)
	f.exec(ctx, t, `CREATE TABLE %s.composite (id text, doc jsonb NOT NULL, region text, note text,
		PRIMARY KEY (id, region), UNIQUE (id, region) DEFERRABLE INITIALLY DEFERRED)`, f.schema)
	c := &conn{c: f.pool, settings: PostgresSettings{DocumentMode: true}}

	for _, table := range []string{"single", "composite"} {
		doc := func(id int32, note string) []byte {
			return rawDoc(t, bson.D{{Key: "_id", Value: id}, {Key: "region", Value: "eu"}, {Key: "note", Value: note}})
		}
		ns := f.schema + "." + table
		must(t, writeDocs(ctx, c, ns, doc(1, "a"), doc(2, "a")), table+" first copy")
		must(t, writeDocs(ctx, c, ns, doc(1, "a"), doc(2, "b")), table+" second copy")

		var note string
		must(t, f.pool.QueryRow(ctx, `SELECT note FROM `+ns+` WHERE id = 'n:2'`).Scan(&note), table+" read")
		assert.Equal(t, "b", note, table)
	}
}
