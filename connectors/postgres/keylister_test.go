package postgres

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestKeyTableNameMapsNamespacesLikeTheSink(t *testing.T) {
	ns := iface.Namespace{Db: "db", Col: "col"}
	for name, test := range map[string]struct {
		mappings map[string]string
		want     string
	}{
		"mapped namespace":   {map[string]string{"db.col": "public.items"}, "public.items"},
		"mapped database":    {map[string]string{"db": "public"}, "public.col"},
		"unmapped identity":  {nil, "db.col"},
		"other mapping only": {map[string]string{"x.y": "a.b"}, "db.col"},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.want, keyTableName(test.mappings, ns))
		})
	}
}

func TestKeyQueryBuildsOneJSONBObjectPerRowWithFieldPathsAsArguments(t *testing.T) {
	query, args := keyQuery("public.items", []string{"tenant", "a.b", `we"ird`})

	assert.Equal(t, `SELECT "id", jsonb_build_object('_id', "doc"->'_id', 'k0', "doc" #> $1::text[], 'k1', "doc" #> $2::text[], 'k2', "doc" #> $3::text[])::text FROM "public"."items" ORDER BY "id"`, query)
	assert.Equal(t, []any{[]string{"tenant"}, []string{"a", "b"}, []string{`we"ird`}}, args)
	assert.NotContains(t, query, "tenant")
	assert.NotContains(t, query, "we")
}

func TestKeyQueryWithoutKeyFieldsSelectsOnlyTheID(t *testing.T) {
	query, args := keyQuery("public.items", nil)

	assert.Equal(t, `SELECT "id", jsonb_build_object('_id', "doc"->'_id')::text FROM "public"."items" ORDER BY "id"`, query)
	assert.Empty(t, args)
}

func TestCheckKeyShapeRequiresPrimaryKeyOfIDPlusKeyFieldsInAnyOrder(t *testing.T) {
	require.NoError(t, checkKeyShape("public.items", []string{"id", "tenant"}, []string{"tenant"}))
	require.NoError(t, checkKeyShape("public.items", []string{"tenant", "id"}, []string{"tenant"}))
	require.NoError(t, checkKeyShape("public.items", []string{"id"}, nil))
	for name, test := range map[string]struct {
		primaryKey, fields []string
	}{
		"extra primary key column": {[]string{"id", "tenant", "region"}, []string{"tenant"}},
		"missing key column":       {[]string{"id"}, []string{"tenant"}},
		"different column":         {[]string{"id", "region"}, []string{"tenant"}},
	} {
		t.Run(name, func(t *testing.T) {
			err := checkKeyShape("public.items", test.primaryKey, test.fields)

			require.Error(t, err)
			assert.Contains(t, err.Error(), "public.items")
			assert.Contains(t, err.Error(), "tenant")
		})
	}
}

type keyCase struct {
	doc    bson.D
	fields []string
}

func sinkRow(t *testing.T, doc bson.D, fields []string) (idText, keyText string) {
	t.Helper()
	raw := bson.Raw(rawDoc(t, doc))
	encoded, err := encodeDoc(raw)
	require.NoError(t, err)
	id := raw.Lookup("_id")
	idText, err = encodeID(id.Type, id.Value)
	require.NoError(t, err)
	var full map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(jsonbNormalize(t, encoded), &full))
	mini := map[string]json.RawMessage{"_id": full["_id"]}
	for i, field := range fields {
		mini[fmt.Sprintf("k%d", i)] = jsonAt(full, strings.Split(field, "."))
	}
	out, err := json.Marshal(mini)
	require.NoError(t, err)
	return idText, string(out)
}

func jsonAt(obj map[string]json.RawMessage, path []string) json.RawMessage {
	v, ok := obj[path[0]]
	if !ok {
		return json.RawMessage("null")
	}
	if len(path) == 1 {
		return v
	}
	var next map[string]json.RawMessage
	if json.Unmarshal(v, &next) != nil {
		return json.RawMessage("null")
	}
	return jsonAt(next, path[1:])
}

func sourceTuple(t *testing.T, doc bson.D, fields []string) []bson.RawValue {
	t.Helper()
	raw := bson.Raw(rawDoc(t, doc))
	tuple := []bson.RawValue{raw.Lookup("_id")}
	for _, field := range fields {
		rv, err := raw.LookupErr(strings.Split(field, ".")...)
		if err != nil || rv.Type == bson.TypeNull {
			tuple = append(tuple, bson.RawValue{Type: bson.TypeNull})
			continue
		}
		tuple = append(tuple, rv)
	}
	return tuple
}

func assertSameTuple(t *testing.T, want, got []bson.RawValue) {
	t.Helper()
	require.Len(t, got, len(want))
	for i := range want {
		assert.Equal(t, want[i].Type, got[i].Type, "type of part %d", i)
		assert.Equal(t, want[i].Value, got[i].Value, "value of part %d", i)
	}
}

func TestKeyTupleRestoresTheSourceTupleFromTheStoredDocument(t *testing.T) {
	oid, err := bson.ObjectIDFromHex("507f1f77bcf86cd799439011")
	require.NoError(t, err)
	dec, err := bson.ParseDecimal128("5")
	require.NoError(t, err)
	uuid := bson.Binary{Subtype: 4, Data: []byte("0123456789abcdef")}
	for name, test := range map[string]keyCase{
		"decimal128 id":        {bson.D{{Key: "_id", Value: dec}, {Key: "t", Value: "x"}}, []string{"t"}},
		"objectid id":          {bson.D{{Key: "_id", Value: oid}}, nil},
		"objectid shard key":   {bson.D{{Key: "_id", Value: "a"}, {Key: "t", Value: oid}}, []string{"t"}},
		"uuid shard key":       {bson.D{{Key: "_id", Value: "a"}, {Key: "t", Value: uuid}}, []string{"t"}},
		"int32 shard key":      {bson.D{{Key: "_id", Value: "a"}, {Key: "t", Value: int32(7)}}, []string{"t"}},
		"int64 shard key":      {bson.D{{Key: "_id", Value: "a"}, {Key: "t", Value: int64(7)}}, []string{"t"}},
		"nested path":          {bson.D{{Key: "_id", Value: "a"}, {Key: "a", Value: bson.D{{Key: "b", Value: "deep"}}}}, []string{"a.b"}},
		"missing field":        {bson.D{{Key: "_id", Value: "a"}}, []string{"t"}},
		"null field":           {bson.D{{Key: "_id", Value: "a"}, {Key: "t", Value: nil}}, []string{"t"}},
		"structured id":        {bson.D{{Key: "_id", Value: bson.D{{Key: "bb", Value: int32(1)}, {Key: "a", Value: "x"}}}, {Key: "t", Value: "x"}}, []string{"t"}},
		"several fields":       {bson.D{{Key: "_id", Value: "a"}, {Key: "t", Value: "x"}, {Key: "r", Value: int64(2)}}, []string{"t", "r"}},
		"field named like key": {bson.D{{Key: "_id", Value: "a"}, {Key: "k0", Value: "x"}}, []string{"k0"}},
	} {
		t.Run(name, func(t *testing.T) {
			idText, keyText := sinkRow(t, test.doc, test.fields)

			tuple, err := keyTuple(idText, keyText, len(test.fields))

			require.NoError(t, err)
			assertSameTuple(t, sourceTuple(t, test.doc, test.fields), tuple)
		})
	}
}

func TestKeyTupleFailsWhenTheDocumentIDDiffersFromTheIDColumn(t *testing.T) {
	_, keyText := sinkRow(t, bson.D{{Key: "_id", Value: "a"}}, nil)
	otherID, _ := sinkRow(t, bson.D{{Key: "_id", Value: "b"}}, nil)

	_, err := keyTuple(otherID, keyText, 0)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not match")
}

func TestKeyTupleFailsOnTextThatIsNotExtendedJSON(t *testing.T) {
	_, err := keyTuple("s:a", "not json", 0)
	require.Error(t, err)
	_, err = keyTuple("s:a", `{"k0": "x"}`, 1)
	require.Error(t, err)
}

type fakeKeyRows struct {
	rows    [][2]string
	at      int
	err     error
	scanErr error
}

func (f *fakeKeyRows) Next() bool {
	f.at++
	return f.at <= len(f.rows)
}

func (f *fakeKeyRows) Scan(dest ...any) error {
	if f.scanErr != nil {
		return f.scanErr
	}
	*dest[0].(*string) = f.rows[f.at-1][0]
	*dest[1].(*string) = f.rows[f.at-1][1]
	return nil
}

func (f *fakeKeyRows) Err() error { return f.err }

func idRows(ids ...string) *fakeKeyRows {
	f := &fakeKeyRows{}
	for _, id := range ids {
		f.rows = append(f.rows, [2]string{"s:" + id, `{"_id": "` + id + `", "k0": "t"}`})
	}
	return f
}

func TestEmitKeyPagesChunksRowsIntoPagesOfPageSize(t *testing.T) {
	var sizes []int
	err := emitKeyPages(idRows("a", "b", "c", "d", "e"), 1, 2, func(page [][]bson.RawValue) error {
		sizes = append(sizes, len(page))
		assert.Len(t, page[0], 2)
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, []int{2, 2, 1}, sizes)
}

func TestEmitKeyPagesEmitsNothingForNoRows(t *testing.T) {
	err := emitKeyPages(idRows(), 1, 2, func([][]bson.RawValue) error {
		t.Fatal("emitted")
		return nil
	})

	require.NoError(t, err)
}

func TestEmitKeyPagesStopsOnAnEmitError(t *testing.T) {
	boom := errors.New("emit failed")
	rows := idRows("a", "b", "c", "d")
	pages := 0

	err := emitKeyPages(rows, 1, 2, func([][]bson.RawValue) error { pages++; return boom })

	require.ErrorIs(t, err, boom)
	assert.Equal(t, 1, pages)
	assert.Equal(t, 2, rows.at)
}

func TestEmitKeyPagesReportsRowErrorsWithoutEmittingThePartialPage(t *testing.T) {
	boom := errors.New("rows failed")
	rows := idRows("a")
	rows.err = boom

	err := emitKeyPages(rows, 1, 2, func([][]bson.RawValue) error {
		t.Fatal("emitted")
		return nil
	})

	require.ErrorIs(t, err, boom)
}

func TestEmitKeyPagesFailsOnAnUndecodableRow(t *testing.T) {
	rows := &fakeKeyRows{rows: [][2]string{{"bad", "{}"}}}

	err := emitKeyPages(rows, 1, 2, func([][]bson.RawValue) error { return nil })

	require.Error(t, err)
}

func TestNewKeyListerRejectsAMalformedConnectionStringWithoutEchoingIt(t *testing.T) {
	const secret = "s3cr3t-marker"

	lister, err := NewKeyLister(t.Context(), "postgres://u:"+secret+"@host:notaport/db", nil)

	require.Nil(t, lister)
	require.EqualError(t, err, "invalid postgres connection string")
}

func TestDeleteByListedTupleAddressesTheRowTheInsertWrote(t *testing.T) {
	oid, err := bson.ObjectIDFromHex("507f1f77bcf86cd799439011")
	require.NoError(t, err)
	dec, err := bson.ParseDecimal128("5")
	require.NoError(t, err)
	uuid := bson.Binary{Subtype: 4, Data: []byte("0123456789abcdef")}
	for name, test := range map[string]struct {
		id, key  any
		colType  string
		keyField string
	}{
		"objectid in text column":  {"a", oid, "text", "t"},
		"uuid in bytea column":     {"a", uuid, "bytea", "t"},
		"decimal128 id":            {dec, "x", "text", "t"},
		"int32 in bigint column":   {"a", int32(7), "bigint", "t"},
		"objectid id with a field": {oid, "x", "text", "t"},
	} {
		t.Run(name, func(t *testing.T) {
			doc := bson.D{{Key: "_id", Value: test.id}, {Key: test.keyField, Value: test.key}}
			plan := &docPlan{
				table:   docTable{PrimaryKey: []string{docIDColumn, test.keyField}},
				columns: []docColumn{{Name: test.keyField, Type: test.colType}},
			}
			insert, err := plan.args(rawDoc(t, doc), time.Time{})
			require.NoError(t, err)
			idText, keyText := sinkRow(t, doc, []string{test.keyField})
			tuple, err := keyTuple(idText, keyText, 1)
			require.NoError(t, err)

			del, err := plan.deleteArgs([]*adiomv1.BsonValue{
				{Name: idField, Type: uint32(tuple[0].Type), Data: tuple[0].Value},
				{Name: test.keyField, Type: uint32(tuple[1].Type), Data: tuple[1].Value},
			})

			require.NoError(t, err)
			assert.Equal(t, []any{insert[0], insert[2]}, del)
		})
	}
}

func TestClaimTableRefusesASecondNamespaceOnTheSameTable(t *testing.T) {
	for name, test := range map[string]struct {
		mappings map[string]string
		first    iface.Namespace
		second   iface.Namespace
	}{
		"explicit targets":       {map[string]string{"t1.orders": "public.orders", "t2.orders": "public.orders"}, iface.Namespace{Db: "t1", Col: "orders"}, iface.Namespace{Db: "t2", Col: "orders"}},
		"prefix targets":         {map[string]string{"db1": "public", "db2": "public"}, iface.Namespace{Db: "db1", Col: "orders"}, iface.Namespace{Db: "db2", Col: "orders"}},
		"identity versus mapped": {map[string]string{"x.y": "public.orders"}, iface.Namespace{Db: "public", Col: "orders"}, iface.Namespace{Db: "x", Col: "y"}},
	} {
		t.Run(name, func(t *testing.T) {
			lister := &KeyLister{mappings: test.mappings}
			require.NoError(t, lister.claimTable(test.first))

			err := lister.claimTable(test.second)

			require.Error(t, err)
			assert.Contains(t, err.Error(), test.first.Db+"."+test.first.Col)
			assert.Contains(t, err.Error(), test.second.Db+"."+test.second.Col)
			assert.Contains(t, err.Error(), lister.Table(test.first))
		})
	}
}

func TestClaimTableAllowsTheSameNamespaceOnEveryPass(t *testing.T) {
	lister := &KeyLister{mappings: map[string]string{"db.col": "public.items"}}
	ns := iface.Namespace{Db: "db", Col: "col"}

	require.NoError(t, lister.claimTable(ns))
	require.NoError(t, lister.claimTable(ns))
	require.NoError(t, lister.claimTable(iface.Namespace{Db: "other", Col: "thing"}))
}

func TestScanKeysRefusesATableClaimedByAnotherNamespace(t *testing.T) {
	lister := &KeyLister{mappings: map[string]string{"db1": "public", "db2": "public"}}
	require.NoError(t, lister.claimTable(iface.Namespace{Db: "db1", Col: "orders"}))

	err := lister.ScanKeys(t.Context(), iface.Namespace{Db: "db2", Col: "orders"}, nil, 10, func([][]bson.RawValue) error { return nil })

	require.ErrorContains(t, err, "both resolve to table public.orders")
}
