//go:build external

package postgres

import (
	"context"
	"testing"
	"time"

	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func writeUpdates(ctx context.Context, c *conn, namespace string, updates ...*adiomv1.Update) error {
	_, err := c.WriteUpdates(ctx, connect.NewRequest(&adiomv1.WriteUpdatesRequest{Namespace: namespace, Updates: updates}))
	return err
}

func countRows(ctx context.Context, t *testing.T, f *docFixture, table string) int {
	t.Helper()
	var n int
	must(t, f.pool.QueryRow(ctx, `SELECT count(*) FROM `+f.schema+`.`+table).Scan(&n), "count")
	return n
}

func rowFingerprint(ctx context.Context, t *testing.T, f *docFixture) string {
	t.Helper()
	var fingerprint string
	must(t, f.pool.QueryRow(ctx, `SELECT xmin::text || doc::text FROM `+f.schema+`."Items" WHERE id = 'n:1'`).Scan(&fingerprint), "fingerprint")
	return fingerprint
}

func TestDocumentModeDeleteRemovesRowsWrittenByTheCopyForEveryIDType(t *testing.T) {
	ctx := testContext(t)
	f := newDocFixture(ctx, t)
	f.exec(ctx, t, `CREATE TABLE %s.things (id text PRIMARY KEY, doc jsonb NOT NULL)`, f.schema)
	c := &conn{c: f.pool, settings: PostgresSettings{DocumentMode: true}}
	ns := f.schema + ".things"
	binaryID := bson.Binary{Subtype: bson.TypeBinaryUUID}
	binaryID.Data = []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	cases := map[string][2]any{
		"int32 copied, int64 deleted": {int32(5), int64(5)},
		"object id":                   {bson.NewObjectID(), nil},
		"string":                      {"abc", nil},
		"uuid binary":                 {binaryID, nil},
		"embedded document":           {bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: "x"}}, nil},
	}
	for name, ids := range cases {
		deleted := ids[1]
		if deleted == nil {
			deleted = ids[0]
		}
		must(t, writeDocs(ctx, c, ns, rawDoc(t, bson.D{{Key: "_id", Value: ids[0]}})), name)
		require.Equal(t, 1, countRows(ctx, t, f, "things"), name)

		must(t, writeUpdates(ctx, c, ns, deleteUpdate(idPart(t, "", deleted))), name)

		assert.Equal(t, 0, countRows(ctx, t, f, "things"), name)
	}
}

func TestDocumentModeDeleteOfMissingRowIsNotAnError(t *testing.T) {
	ctx := testContext(t)
	f := newDocFixture(ctx, t)
	f.exec(ctx, t, `CREATE TABLE %s.things (id text PRIMARY KEY, doc jsonb NOT NULL)`, f.schema)
	c := &conn{c: f.pool, settings: PostgresSettings{DocumentMode: true}}

	must(t, writeUpdates(ctx, c, f.schema+".things", deleteUpdate(idPart(t, "", int32(404)))), "delete missing")
}

func TestDocumentModeDeleteOnCompositeKeyNeedsTheNamedPart(t *testing.T) {
	ctx := testContext(t)
	f := newDocFixture(ctx, t)
	ns := newItemsTable(ctx, t, f)
	c := &conn{c: f.pool, settings: PostgresSettings{DocumentMode: true}}
	must(t, writeDocs(ctx, c, ns,
		rawDoc(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "Region", Value: "eu"}}),
		rawDoc(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "Region", Value: "us"}}),
	), "copy")

	err := writeUpdates(ctx, c, ns, deleteUpdate(idPart(t, "", int32(1))))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	assert.Contains(t, err.Error(), "Region")
	assert.Equal(t, 2, countRows(ctx, t, f, `"Items"`))

	must(t, writeUpdates(ctx, c, ns, deleteUpdate(idPart(t, "", int64(1)), idPart(t, "Region", "eu"))), "named delete")

	assert.Equal(t, 1, countRows(ctx, t, f, `"Items"`))
	var region string
	must(t, f.pool.QueryRow(ctx, `SELECT "Region" FROM `+f.schema+`."Items"`).Scan(&region), "remaining")
	assert.Equal(t, "us", region)
}

func TestDocumentModeInsertUpdateDeleteInOneRequestLeavesNoRow(t *testing.T) {
	ctx := testContext(t)
	f := newDocFixture(ctx, t)
	f.exec(ctx, t, `CREATE TABLE %s.things (id text PRIMARY KEY, doc jsonb NOT NULL)`, f.schema)
	c := &conn{c: f.pool, settings: PostgresSettings{DocumentMode: true}}
	id := []*adiomv1.BsonValue{idPart(t, "", int32(1))}
	doc := func(v string) []byte { return rawDoc(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "v", Value: v}}) }

	err := writeUpdates(ctx, c, f.schema+".things",
		upsertUpdate(adiomv1.UpdateType_UPDATE_TYPE_INSERT, id, doc("a")),
		upsertUpdate(adiomv1.UpdateType_UPDATE_TYPE_UPDATE, id, doc("b")),
		deleteUpdate(id...),
	)

	must(t, err, "request")
	assert.Equal(t, 0, countRows(ctx, t, f, "things"))
}

func TestDocumentModeUpdateOfAnUnchangedDocumentLeavesXminAndUpdatedAtUntouched(t *testing.T) {
	ctx := testContext(t)
	f := newDocFixture(ctx, t)
	ns := newItemsTable(ctx, t, f)
	clock := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	c := &conn{c: f.pool, settings: PostgresSettings{DocumentMode: true, UpdatedAtColumn: "updated_at"}, now: func() time.Time { return clock }}
	raw := rawDoc(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "Region", Value: "eu"}, {Key: "v", Value: "a"}})
	id := []*adiomv1.BsonValue{idPart(t, "", int32(1))}
	must(t, writeDocs(ctx, c, ns, raw), "copy")
	before := readRow(ctx, t, f, "n:1")

	clock = clock.Add(time.Hour)
	must(t, writeUpdates(ctx, c, ns, upsertUpdate(adiomv1.UpdateType_UPDATE_TYPE_UPDATE, id, raw)), "unchanged update")
	same := readRow(ctx, t, f, "n:1")
	assert.Equal(t, before.Xmin, same.Xmin)
	assert.Equal(t, before.UpdatedAt, same.UpdatedAt)

	changed := rawDoc(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "Region", Value: "eu"}, {Key: "v", Value: "b"}})
	must(t, writeUpdates(ctx, c, ns, upsertUpdate(adiomv1.UpdateType_UPDATE_TYPE_UPDATE, id, changed)), "changed update")
	after := readRow(ctx, t, f, "n:1")
	assert.NotEqual(t, before.Xmin, after.Xmin)
	assert.Equal(t, clock, after.UpdatedAt.UTC())
}

func TestDocumentModeEmptyPartialUpdateLeavesTheRowUntouched(t *testing.T) {
	ctx := testContext(t)
	f := newDocFixture(ctx, t)
	ns := newItemsTable(ctx, t, f)
	c := &conn{c: f.pool, settings: PostgresSettings{DocumentMode: true}}
	must(t, writeDocs(ctx, c, ns, rawDoc(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "Region", Value: "eu"}})), "copy")
	before := rowFingerprint(ctx, t, f)
	empty := &adiomv1.Update{Type: adiomv1.UpdateType_UPDATE_TYPE_PARTIAL_UPDATE, Id: []*adiomv1.BsonValue{idPart(t, "", int32(1)), idPart(t, "Region", "eu")}}

	must(t, writeUpdates(ctx, c, ns, empty), "empty partial update")

	assert.Equal(t, before, rowFingerprint(ctx, t, f))
}

func TestDocumentModeUpdateServerErrorsNeverCarryValuesInErrorOrLogs(t *testing.T) {
	ctx := testContext(t)
	f := newDocFixture(ctx, t)
	f.exec(ctx, t, `CREATE TABLE %s.numbers (id text PRIMARY KEY, doc jsonb NOT NULL, n integer)`, f.schema)
	c := &conn{c: f.pool, settings: PostgresSettings{DocumentMode: true}}
	logs := captureLogs(t)

	err := writeUpdates(ctx, c, f.schema+".numbers",
		upsertUpdate(adiomv1.UpdateType_UPDATE_TYPE_INSERT, nil, rawDoc(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "n", Value: int32(1)}})),
		upsertUpdate(adiomv1.UpdateType_UPDATE_TYPE_UPDATE, nil, rawDoc(t, bson.D{{Key: "_id", Value: int32(2)}, {Key: "n", Value: marker}})),
	)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "update 1")
	assert.Contains(t, err.Error(), "22P02")
	assert.NotContains(t, err.Error(), marker)
	assert.NotContains(t, logs.String(), marker)
}

func TestDocumentModeDeleteServerErrorsNeverCarryValuesInErrorOrLogs(t *testing.T) {
	ctx := testContext(t)
	f := newDocFixture(ctx, t)
	f.exec(ctx, t, `CREATE TABLE %[1]s.guarded (id text PRIMARY KEY, doc jsonb NOT NULL, tag text);
		INSERT INTO %[1]s.guarded VALUES ('n:1', '{}', '`+marker+`');
		CREATE FUNCTION %[1]s.guard() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'rejected %%', OLD.tag; END $$;
		CREATE TRIGGER guard BEFORE DELETE ON %[1]s.guarded FOR EACH ROW EXECUTE FUNCTION %[1]s.guard()`, f.schema)
	c := &conn{c: f.pool, settings: PostgresSettings{DocumentMode: true}}
	logs := captureLogs(t)

	err := writeUpdates(ctx, c, f.schema+".guarded", deleteUpdate(idPart(t, "", int32(1))))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "update 0")
	assert.Contains(t, err.Error(), "P0001")
	assert.NotContains(t, err.Error(), marker)
	assert.NotContains(t, logs.String(), marker)
}

func TestDocumentModeDeleteAndUpdateAddressTheRowByTheNamedIDPart(t *testing.T) {
	ctx := testContext(t)
	f := newDocFixture(ctx, t)
	ns := newItemsTable(ctx, t, f)
	f.exec(ctx, t, `CREATE TABLE %s.things (id text PRIMARY KEY, doc jsonb NOT NULL)`, f.schema)
	c := &conn{c: f.pool, settings: PostgresSettings{DocumentMode: true}}
	row := func(region string) []byte {
		return rawDoc(t, bson.D{{Key: "_id", Value: int32(3)}, {Key: "Region", Value: region}})
	}
	must(t, writeDocs(ctx, c, ns, row("eu"), row("us")), "copy")

	parts := []*adiomv1.BsonValue{idPart(t, "Region", "us"), idPart(t, "_id", int32(3))}
	changed := rawDoc(t, bson.D{{Key: "_id", Value: int32(3)}, {Key: "Region", Value: "us"}, {Key: "v", Value: "x"}})
	must(t, writeUpdates(ctx, c, ns, upsertUpdate(adiomv1.UpdateType_UPDATE_TYPE_UPDATE, parts, changed)), "update")
	var docs int
	must(t, f.pool.QueryRow(ctx, `SELECT count(*) FROM `+f.schema+`."Items" WHERE doc ? 'v'`).Scan(&docs), "changed rows")
	assert.Equal(t, 1, docs)

	must(t, writeUpdates(ctx, c, ns, deleteUpdate(parts...)), "delete")
	var region string
	must(t, f.pool.QueryRow(ctx, `SELECT "Region" FROM `+f.schema+`."Items"`).Scan(&region), "remaining")
	assert.Equal(t, "eu", region)

	must(t, writeDocs(ctx, c, f.schema+".things", rawDoc(t, bson.D{{Key: "_id", Value: int32(1)}}), rawDoc(t, bson.D{{Key: "_id", Value: int32(2)}})), "copy single")
	must(t, writeUpdates(ctx, c, f.schema+".things", deleteUpdate(idPart(t, "Other", "zzz"), idPart(t, "_id", int32(1)))), "delete single")
	var left string
	must(t, f.pool.QueryRow(ctx, `SELECT id FROM `+f.schema+`.things`).Scan(&left), "remaining single")
	assert.Equal(t, "n:2", left)
}

func TestDocumentModeUpdateWithKeyPartThatDiffersFromTheDocumentChangesNoRow(t *testing.T) {
	ctx := testContext(t)
	f := newDocFixture(ctx, t)
	ns := newItemsTable(ctx, t, f)
	c := &conn{c: f.pool, settings: PostgresSettings{DocumentMode: true}}
	row := func(region, v string) []byte {
		return rawDoc(t, bson.D{{Key: "_id", Value: int32(3)}, {Key: "Region", Value: region}, {Key: "v", Value: v}})
	}
	must(t, writeDocs(ctx, c, ns, row("eu", "a"), row("us", "a")), "copy")
	fingerprints := func() string {
		var out string
		must(t, f.pool.QueryRow(ctx, `SELECT string_agg("Region" || xmin::text || doc::text, ',' ORDER BY "Region") FROM `+f.schema+`."Items"`).Scan(&out), "fingerprints")
		return out
	}
	before := fingerprints()
	parts := []*adiomv1.BsonValue{idPart(t, "_id", int32(3)), idPart(t, "Region", "us")}

	err := writeUpdates(ctx, c, ns, upsertUpdate(adiomv1.UpdateType_UPDATE_TYPE_UPDATE, parts, row("eu", "b")))

	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	assert.Contains(t, err.Error(), "key column Region differs from the document")
	assert.Equal(t, before, fingerprints())

	must(t, writeUpdates(ctx, c, ns, upsertUpdate(adiomv1.UpdateType_UPDATE_TYPE_UPDATE, parts, row("us", "b"))), "agreeing update")
	var euDoc, usDoc string
	must(t, f.pool.QueryRow(ctx, `SELECT doc::text FROM `+f.schema+`."Items" WHERE "Region" = 'eu'`).Scan(&euDoc), "eu")
	must(t, f.pool.QueryRow(ctx, `SELECT doc::text FROM `+f.schema+`."Items" WHERE "Region" = 'us'`).Scan(&usDoc), "us")
	assert.Contains(t, euDoc, `"a"`)
	assert.Contains(t, usDoc, `"b"`)
}

func TestDocumentModeUpdateErrorNamesTheRequestPositionAfterSkippedEmptyPartials(t *testing.T) {
	ctx := testContext(t)
	f := newDocFixture(ctx, t)
	f.exec(ctx, t, `CREATE TABLE %s.numbers (id text PRIMARY KEY, doc jsonb NOT NULL, n integer)`, f.schema)
	c := &conn{c: f.pool, settings: PostgresSettings{DocumentMode: true}}
	empty := &adiomv1.Update{Type: adiomv1.UpdateType_UPDATE_TYPE_PARTIAL_UPDATE}

	err := writeUpdates(ctx, c, f.schema+".numbers", empty, empty,
		upsertUpdate(adiomv1.UpdateType_UPDATE_TYPE_INSERT, nil, rawDoc(t, bson.D{{Key: "_id", Value: int32(2)}, {Key: "n", Value: marker}})))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "update 2")
}

func TestDocumentModeDeleteConvertsWholeDoublesForIntegerKeyColumnsAndRejectsFractions(t *testing.T) {
	ctx := testContext(t)
	f := newDocFixture(ctx, t)
	f.exec(ctx, t, `CREATE TABLE %s.keyed (id text NOT NULL, doc jsonb NOT NULL, n integer NOT NULL, PRIMARY KEY (id, n))`, f.schema)
	c := &conn{c: f.pool, settings: PostgresSettings{DocumentMode: true}}
	ns := f.schema + ".keyed"
	must(t, writeDocs(ctx, c, ns, rawDoc(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "n", Value: int32(4)}})), "copy")

	err := writeUpdates(ctx, c, ns, deleteUpdate(idPart(t, "_id", int32(1)), idPart(t, "n", 4.5)))
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	assert.Equal(t, 1, countRows(ctx, t, f, "keyed"))

	must(t, writeUpdates(ctx, c, ns, deleteUpdate(idPart(t, "_id", int32(1)), idPart(t, "n", 4.0))), "whole double")
	assert.Equal(t, 0, countRows(ctx, t, f, "keyed"))
}

func TestDocumentModeUpdateCommitTimeErrorsNeverLogOrReturnValues(t *testing.T) {
	ctx := testContext(t)
	f := newDocFixture(ctx, t)
	f.exec(ctx, t, `CREATE TABLE %[1]s.guarded (id text PRIMARY KEY, doc jsonb NOT NULL, tag text);
		CREATE FUNCTION %[1]s.guard() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'rejected %%', NEW.tag; END $$;
		CREATE CONSTRAINT TRIGGER guard AFTER INSERT ON %[1]s.guarded DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION %[1]s.guard()`, f.schema)
	c := &conn{c: f.pool, settings: PostgresSettings{DocumentMode: true}}
	logs := captureLogs(t)

	err := writeUpdates(ctx, c, f.schema+".guarded",
		upsertUpdate(adiomv1.UpdateType_UPDATE_TYPE_INSERT, nil, rawDoc(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "tag", Value: marker}})))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "P0001")
	assert.NotContains(t, err.Error(), marker)
	assert.NotContains(t, logs.String(), marker)
}

func TestDocumentModeAppliesIntermediateOperationsInRequestOrder(t *testing.T) {
	ctx := testContext(t)
	f := newDocFixture(ctx, t)
	f.exec(ctx, t, `CREATE TABLE %[1]s.things (id text PRIMARY KEY, doc jsonb NOT NULL);
		CREATE TABLE %[1]s.audit (id text);
		CREATE FUNCTION %[1]s.note() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO %[1]s.audit VALUES (OLD.id); RETURN OLD; END $$;
		CREATE TRIGGER note AFTER DELETE ON %[1]s.things FOR EACH ROW EXECUTE FUNCTION %[1]s.note()`, f.schema)
	c := &conn{c: f.pool, settings: PostgresSettings{DocumentMode: true}}
	id := []*adiomv1.BsonValue{idPart(t, "_id", int32(1))}
	doc := func(v string) []byte { return rawDoc(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "v", Value: v}}) }

	must(t, writeUpdates(ctx, c, f.schema+".things",
		upsertUpdate(adiomv1.UpdateType_UPDATE_TYPE_INSERT, id, doc("a")),
		deleteUpdate(id...),
		upsertUpdate(adiomv1.UpdateType_UPDATE_TYPE_INSERT, id, doc("b")),
	), "request")

	var audits int
	must(t, f.pool.QueryRow(ctx, `SELECT count(*) FROM `+f.schema+`.audit`).Scan(&audits), "audits")
	assert.Equal(t, 1, audits)
	var stored string
	must(t, f.pool.QueryRow(ctx, `SELECT doc->>'v' FROM `+f.schema+`.things`).Scan(&stored), "stored")
	assert.Equal(t, "b", stored)
}

func TestDocumentModeIDNamedKeyColumnSurvivesCopyUpdateAndDelete(t *testing.T) {
	ctx := testContext(t)
	f := newDocFixture(ctx, t)
	f.exec(ctx, t, `CREATE TABLE %s.underscored (id text NOT NULL, doc jsonb NOT NULL, "_id" text NOT NULL, PRIMARY KEY (id, "_id"))`, f.schema)
	c := &conn{c: f.pool, settings: PostgresSettings{DocumentMode: true}}
	ns := f.schema + ".underscored"
	id := []*adiomv1.BsonValue{idPart(t, "_id", "abc")}
	doc := func(v string) []byte { return rawDoc(t, bson.D{{Key: "_id", Value: "abc"}, {Key: "v", Value: v}}) }

	must(t, writeDocs(ctx, c, ns, doc("a")), "copy")
	must(t, writeUpdates(ctx, c, ns, upsertUpdate(adiomv1.UpdateType_UPDATE_TYPE_UPDATE, id, doc("b"))), "update")
	var stored string
	must(t, f.pool.QueryRow(ctx, `SELECT doc->>'v' FROM `+f.schema+`.underscored`).Scan(&stored), "stored")
	assert.Equal(t, "b", stored)

	must(t, writeUpdates(ctx, c, ns, deleteUpdate(id...)), "delete")

	assert.Equal(t, 0, countRows(ctx, t, f, "underscored"))
}
