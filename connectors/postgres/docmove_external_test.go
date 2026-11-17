//go:build external

package postgres

import (
	"math"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestDocumentModeMovesColumnsOutOfDocAndListsTheOriginalDocuments(t *testing.T) {
	ctx := testContext(t)
	f := newDocFixture(ctx, t)
	f.exec(ctx, t, `CREATE TABLE %[1]s.moved (id text NOT NULL, doc jsonb, tenant text NOT NULL, "sortKey" bigint, n integer,
		ratio double precision, "on" boolean, at timestamptz, uid uuid, bin bytea, value jsonb, kept bigint, amount numeric,
		PRIMARY KEY (id, tenant));
		COMMENT ON COLUMN %[1]s.moved.kept IS 'dsync:{"mode":"copy"}'`, f.schema)
	ns := f.schema + ".moved"
	c := &conn{c: f.pool, settings: PostgresSettings{DocumentMode: true}}
	dec, err := bson.ParseDecimal128("1.50")
	require.NoError(t, err)
	full := rawDoc(t, bson.D{
		{Key: "_id", Value: "a"}, {Key: "tenant", Value: "t1"}, {Key: "sortKey", Value: int64(42)}, {Key: "n", Value: int32(7)},
		{Key: "ratio", Value: 3.0}, {Key: "on", Value: true}, {Key: "at", Value: bson.NewDateTimeFromTime(movedAt)},
		{Key: "uid", Value: bson.Binary{Subtype: 4, Data: seqBytes(16)}}, {Key: "bin", Value: bson.Binary{Data: []byte{1, 2}}},
		{Key: "value", Value: bson.D{{Key: "state", Value: int32(2)}, {Key: "payload", Value: bson.A{int64(1), "x"}}}},
		{Key: "kept", Value: int64(9)}, {Key: "amount", Value: dec}, {Key: "rest", Value: "x"},
	})
	sparse := rawDoc(t, bson.D{{Key: "_id", Value: "b"}, {Key: "tenant", Value: "t1"}, {Key: "n", Value: nil}})

	must(t, writeDocs(ctx, c, ns, full, sparse), "write")

	var doc, value, uid string
	var sortKey int64
	err = f.pool.QueryRow(ctx, `SELECT doc::text, "sortKey", value::text, uid::text FROM `+ns+` WHERE id = 's:a'`).Scan(&doc, &sortKey, &value, &uid)
	must(t, err, "read columns")
	assert.JSONEq(t, `{"_id":"a","tenant":"t1","kept":{"$numberLong":"9"},"amount":{"$numberDecimal":"1.50"},"rest":"x"}`, doc)
	assert.Equal(t, int64(42), sortKey)
	assert.JSONEq(t, `{"state":{"$numberInt":"2"},"payload":[{"$numberLong":"1"},"x"]}`, value)
	assert.Equal(t, "00010203-0405-0607-0809-0a0b0c0d0e0f", uid)

	rows, err := c.listDocRows(ctx, ns, []string{"id", "tenant"}, `"id", "tenant"`, "", nil, 10)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, hashOf(t, full), hashOf(t, rows[0].doc))
	assert.Equal(t, hashOf(t, sparse), hashOf(t, rows[1].doc))

	_, err = f.admin.Exec(ctx, `UPDATE `+ns+` SET at = '2024-05-06 07:08:09.123987+00' WHERE id = 's:a'`)
	must(t, err, "set microseconds")
	c = &conn{c: f.pool, settings: PostgresSettings{DocumentMode: true}}
	rows, err = c.listDocRows(ctx, ns, []string{"id", "tenant"}, `"id", "tenant"`, "", nil, 10)
	require.NoError(t, err)
	assert.Equal(t, movedAt.UnixMilli(), bson.Raw(rows[0].doc).Lookup("at").DateTime())
}

func TestDocumentModeTakesTheMappingCommentOfAPartitionFromItsParent(t *testing.T) {
	ctx := testContext(t)
	f := newDocFixture(ctx, t)
	f.exec(ctx, t, `CREATE TABLE %[1]s.parted (id text, region text, doc jsonb, n integer, own integer, PRIMARY KEY (id, region)) PARTITION BY LIST (region);
		CREATE TABLE %[1]s.parted_eu (own integer, doc jsonb, n integer, region text NOT NULL, id text NOT NULL);
		ALTER TABLE %[1]s.parted ATTACH PARTITION %[1]s.parted_eu FOR VALUES IN ('eu');
		COMMENT ON COLUMN %[1]s.parted.n IS 'dsync:{"mode":"copy"}';
		COMMENT ON COLUMN %[1]s.parted_eu.own IS 'dsync:{"mode":"copy"}'`, f.schema)

	parent, err := loadDocTable(ctx, f.pool, f.schema+".parted")
	require.NoError(t, err)
	partition, err := loadDocTable(ctx, f.pool, f.schema+".parted_eu")
	require.NoError(t, err)

	moves := func(table docTable) map[string]bool {
		got := map[string]bool{}
		for _, col := range table.Columns {
			got[col.Name] = col.Move
		}
		return got
	}
	assert.Equal(t, map[string]bool{"region": false, "n": false, "own": true}, moves(parent))
	assert.Equal(t, map[string]bool{"region": false, "n": false, "own": true}, moves(partition))
}

func TestDocumentModeReplaysAMovedDoubleThatOnlyChangedItsZeroSign(t *testing.T) {
	ctx := testContext(t)
	f := newDocFixture(ctx, t)
	f.exec(ctx, t, `CREATE TABLE %s.zeros (id text PRIMARY KEY, doc jsonb, ratio double precision)`, f.schema)
	ns := f.schema + ".zeros"
	c := &conn{c: f.pool, settings: PostgresSettings{DocumentMode: true}}
	negative := rawDoc(t, bson.D{{Key: "_id", Value: "a"}, {Key: "ratio", Value: math.Copysign(0, -1)}})

	must(t, writeDocs(ctx, c, ns, rawDoc(t, bson.D{{Key: "_id", Value: "a"}, {Key: "ratio", Value: 0.0}})), "write positive")
	must(t, writeDocs(ctx, c, ns, negative), "write negative")

	rows, err := c.listDocRows(ctx, ns, []string{"id"}, `"id"`, "", nil, 10)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, hashOf(t, negative), hashOf(t, rows[0].doc))
}

func TestDocumentModeReplaysAMovedDoubleThatChangedToTheAdjacentValueUnderReducedFloatDigits(t *testing.T) {
	ctx := testContext(t)
	f := newDocFixture(ctx, t)
	f.exec(ctx, t, `CREATE TABLE %s.adjacent (id text PRIMARY KEY, doc jsonb, ratio double precision)`, f.schema)
	ns := f.schema + ".adjacent"
	config := f.pool.Config()
	config.ConnConfig.RuntimeParams["extra_float_digits"] = "0"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	must(t, err, "pool")
	t.Cleanup(pool.Close)
	c := &conn{c: pool, settings: PostgresSettings{DocumentMode: true}}
	next := rawDoc(t, bson.D{{Key: "_id", Value: "a"}, {Key: "ratio", Value: math.Nextafter(1, 2)}})

	must(t, writeDocs(ctx, c, ns, rawDoc(t, bson.D{{Key: "_id", Value: "a"}, {Key: "ratio", Value: 1.0}})), "write one")
	must(t, writeDocs(ctx, c, ns, next), "write next")

	rows, err := c.listDocRows(ctx, ns, []string{"id"}, `"id"`, "", nil, 10)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, hashOf(t, next), hashOf(t, rows[0].doc))
}

func TestDocumentModeKeepsEmptyValuesInMovedColumnsAsNonNull(t *testing.T) {
	ctx := testContext(t)
	f := newDocFixture(ctx, t)
	f.exec(ctx, t, `CREATE TABLE %s.empties (id text PRIMARY KEY, doc jsonb, bin bytea, s text, obj jsonb, arr jsonb)`, f.schema)
	ns := f.schema + ".empties"
	c := &conn{c: f.pool, settings: PostgresSettings{DocumentMode: true}}
	empty := rawDoc(t, bson.D{
		{Key: "_id", Value: "a"}, {Key: "bin", Value: bson.Binary{Subtype: 0, Data: []byte{}}}, {Key: "s", Value: ""},
		{Key: "obj", Value: bson.D{}}, {Key: "arr", Value: bson.A{}},
	})

	must(t, writeDocs(ctx, c, ns, empty), "write")

	var bin, s, obj, arr bool
	var doc string
	err := f.pool.QueryRow(ctx, `SELECT bin IS NOT NULL, s IS NOT NULL, obj IS NOT NULL, arr IS NOT NULL, doc::text FROM `+ns+` WHERE id = 's:a'`).Scan(&bin, &s, &obj, &arr, &doc)
	must(t, err, "read columns")
	assert.True(t, bin, "bin")
	assert.True(t, s, "s")
	assert.True(t, obj, "obj")
	assert.True(t, arr, "arr")
	assert.JSONEq(t, `{"_id":"a"}`, doc)

	rows, err := c.listDocRows(ctx, ns, []string{"id"}, `"id"`, "", nil, 10)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, hashOf(t, empty), hashOf(t, rows[0].doc))
}
