package postgres

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/adiom-data/dsync/connectors/postgres/pglib"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const sentinel = "secret-content"

func nestedID(t *testing.T) bson.D {
	t.Helper()
	oid, err := bson.ObjectIDFromHex("507f1f77bcf86cd799439011")
	require.NoError(t, err)
	dec, err := bson.ParseDecimal128("1234567890.123456789")
	require.NoError(t, err)
	return bson.D{
		{Key: "tenant", Value: "t"},
		{Key: "i32", Value: int32(2)},
		{Key: "i64", Value: int64(1) << 40},
		{Key: "dbl", Value: 7.0},
		{Key: "dec", Value: dec},
		{Key: "oid", Value: oid},
		{Key: "when", Value: bson.NewDateTimeFromTime(time.UnixMilli(1700000000123))},
		{Key: "inner", Value: bson.A{int32(1), bson.D{{Key: "z", Value: int32(1)}, {Key: "a", Value: 2.5}}}},
	}
}

func docIDs(t *testing.T) map[string]any {
	t.Helper()
	oid, err := bson.ObjectIDFromHex("507f1f77bcf86cd799439011")
	require.NoError(t, err)
	dec, err := bson.ParseDecimal128("1234567890.123456789")
	require.NoError(t, err)
	return map[string]any{
		"object id":       oid,
		"string":          "key-1",
		"int32":           int32(7),
		"int64":           int64(1) << 40,
		"double":          7.0,
		"decimal128":      dec,
		"embedded":        bson.D{{Key: "tenant", Value: "t"}, {Key: "n", Value: int32(2)}},
		"nested":          nestedID(t),
		"code with scope": bson.D{{Key: "k", Value: bson.CodeWithScope{Code: "f()", Scope: bson.D{{Key: "bb", Value: int32(1)}, {Key: "a", Value: int32(2)}}}}},
		"array":           bson.A{bson.D{{Key: "tenant", Value: "t"}, {Key: "n", Value: int32(2)}}, int32(3)},
	}
}

func storedRow(t *testing.T, id any) (raw []byte, text string, idText string, doc any) {
	t.Helper()
	raw = rawDoc(t, bson.D{{Key: "_id", Value: id}, {Key: "v", Value: int32(5)}})
	encoded, err := encodeDoc(raw)
	require.NoError(t, err)
	normalized := jsonbNormalize(t, encoded)
	rv, err := bson.Raw(raw).LookupErr("_id")
	require.NoError(t, err)
	idText, err = encodeID(rv.Type, rv.Value)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(normalized, &doc))
	return raw, string(normalized), idText, doc
}

func mongoIDPart(t *testing.T, id any) []*adiomv1.BsonValue {
	t.Helper()
	typ, data, err := bson.MarshalValue(id)
	require.NoError(t, err)
	return []*adiomv1.BsonValue{{Name: "_id", Type: uint32(typ), Data: data}}
}

func assertDocIDBytes(t *testing.T, want, got []byte) {
	t.Helper()
	wantID, err := bson.Raw(want).LookupErr("_id")
	require.NoError(t, err)
	gotID, err := bson.Raw(got).LookupErr("_id")
	require.NoError(t, err)
	assert.Equal(t, wantID.Type, gotID.Type)
	assert.Equal(t, wantID.Value, gotID.Value)
	assert.Equal(t, hashOf(t, want), hashOf(t, got))
}

func TestDecodeIDOfEncodeIDIsByteIdenticalForStructuredIDs(t *testing.T) {
	for name, id := range docIDs(t) {
		t.Run(name, func(t *testing.T) {
			typ, data, err := bson.MarshalValue(id)
			require.NoError(t, err)
			text, err := encodeID(typ, data)
			require.NoError(t, err)

			got, err := decodeID(text)

			require.NoError(t, err)
			if typ == bson.TypeEmbeddedDocument || typ == bson.TypeArray {
				assert.Equal(t, typ, got.Type)
				assert.Equal(t, data, got.Value)
			}
		})
	}
}

func TestListRowsEmitTheOriginalDocumentWithItsOriginalIDBytes(t *testing.T) {
	for name, id := range docIDs(t) {
		t.Run(name, func(t *testing.T) {
			raw, text, idText, _ := storedRow(t, id)
			rows := []sourceRow{{key: []any{"eu", idText}, text: text}}

			require.NoError(t, fillDocs(rows, []string{"region", "id"}, 1))

			assertDocIDBytes(t, raw, rows[0].doc)
		})
	}
}

func TestDocSourceUpdateInsertAndUpdateCarryDocAndMongoIDPart(t *testing.T) {
	for name, id := range docIDs(t) {
		for typ, kind := range map[pglib.UpdateType]adiomv1.UpdateType{
			pglib.UpdateTypeInsert: adiomv1.UpdateType_UPDATE_TYPE_INSERT,
			pglib.UpdateTypeUpdate: adiomv1.UpdateType_UPDATE_TYPE_UPDATE,
		} {
			t.Run(name, func(t *testing.T) {
				raw, _, idText, doc := storedRow(t, id)
				row := map[string]any{"id": idText, "doc": doc, "region": "eu", "updated_at": "ignored"}

				got, err := docSourceUpdate(pglib.Update{Type: typ, New: row})

				require.NoError(t, err)
				assert.Equal(t, kind, got.Type)
				assert.Equal(t, mongoIDPart(t, id), got.Id)
				assertDocIDBytes(t, raw, got.Data)
			})
		}
	}
}

func TestDocSourceUpdateDeleteWithOldDocMatchesMongoIDPart(t *testing.T) {
	for name, id := range docIDs(t) {
		t.Run(name, func(t *testing.T) {
			_, _, idText, doc := storedRow(t, id)
			old := map[string]any{"id": idText, "doc": doc, "region": "eu"}

			got, err := docSourceUpdate(pglib.Update{Type: pglib.UpdateTypeDelete, Old: old})

			require.NoError(t, err)
			assert.Equal(t, adiomv1.UpdateType_UPDATE_TYPE_DELETE, got.Type)
			assert.Equal(t, mongoIDPart(t, id), got.Id)
			assert.Empty(t, got.Data)
		})
	}
}

func TestDocSourceUpdateDeleteWithoutOldDocFallsBackToTheIDColumn(t *testing.T) {
	for name, id := range docIDs(t) {
		t.Run(name, func(t *testing.T) {
			_, _, idText, _ := storedRow(t, id)
			want, err := decodeID(idText)
			require.NoError(t, err)

			for _, old := range []map[string]any{
				{"id": idText, "doc": nil, "region": "eu"},
				{"id": idText, "region": "eu"},
			} {
				got, err := docSourceUpdate(pglib.Update{Type: pglib.UpdateTypeDelete, Old: old})
				require.NoError(t, err)
				require.Len(t, got.Id, 1)
				assert.Equal(t, uint32(want.Type), got.Id[0].Type)
				assert.Equal(t, want.Value, got.Id[0].Data)
			}
			got, err := docSourceUpdate(pglib.Update{Type: pglib.UpdateTypeDelete, Old: map[string]any{"id": idText, "doc": nil}})

			require.NoError(t, err)
			require.Len(t, got.Id, 1)
			assert.Equal(t, "_id", got.Id[0].Name)
			assert.Equal(t, uint32(want.Type), got.Id[0].Type)
			assert.Equal(t, want.Value, got.Id[0].Data)
			if name == "int32" {
				assert.Equal(t, uint32(bson.TypeInt64), got.Id[0].Type)
				assert.NotEqual(t, mongoIDPart(t, id), got.Id)
			}
		})
	}
}

func TestDocSourceUpdateRejectsBadRowsWithoutLeakingContent(t *testing.T) {
	idText := "s:" + sentinel
	good := map[string]any{"_id": sentinel, "v": sentinel}
	for name, update := range map[string]pglib.Update{
		"doc is a json string":   {Type: pglib.UpdateTypeInsert, New: map[string]any{"id": idText, "doc": sentinel}},
		"doc is null":            {Type: pglib.UpdateTypeUpdate, New: map[string]any{"id": idText, "doc": nil}},
		"doc has bad wrapper":    {Type: pglib.UpdateTypeInsert, New: map[string]any{"id": idText, "doc": map[string]any{"_id": map[string]any{"$oid": sentinel}}}},
		"doc without _id":        {Type: pglib.UpdateTypeInsert, New: map[string]any{"id": idText, "doc": map[string]any{"v": sentinel}}},
		"doc id differs":         {Type: pglib.UpdateTypeInsert, New: map[string]any{"id": "s:other-" + sentinel, "doc": good}},
		"doc id type differs":    {Type: pglib.UpdateTypeInsert, New: map[string]any{"id": "n:7", "doc": good}},
		"structured id differs":  {Type: pglib.UpdateTypeInsert, New: map[string]any{"id": `j:{"a":{"$numberInt":"1"}}`, "doc": map[string]any{"_id": map[string]any{"a": map[string]any{"$numberInt": "2"}}}}},
		"missing id column":      {Type: pglib.UpdateTypeInsert, New: map[string]any{"doc": good}},
		"delete without id":      {Type: pglib.UpdateTypeDelete, Old: map[string]any{"region": sentinel}},
		"delete with bad id":     {Type: pglib.UpdateTypeDelete, Old: map[string]any{"id": "zz" + sentinel}},
		"delete non-canonical":   {Type: pglib.UpdateTypeDelete, Old: map[string]any{"id": "n:01"}},
		"delete old doc differs": {Type: pglib.UpdateTypeDelete, Old: map[string]any{"id": "s:other-" + sentinel, "doc": good}},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := docSourceUpdate(update)

			require.Error(t, err)
			assert.Nil(t, got)
			assert.NotContains(t, err.Error(), sentinel)
		})
	}
}

func TestDocSourceUpdateAcceptsDocDecodedLikeTheStream(t *testing.T) {
	oid, err := bson.ObjectIDFromHex("507f1f77bcf86cd799439011")
	require.NoError(t, err)
	dec, err := bson.ParseDecimal128("1234567890.123456789")
	require.NoError(t, err)
	raw := rawDoc(t, bson.D{
		{Key: "_id", Value: bson.D{{Key: "tenant", Value: "t"}, {Key: "n", Value: int32(2)}}},
		{Key: "i32", Value: int32(1)},
		{Key: "i64", Value: int64(1) << 40},
		{Key: "dbl", Value: 7.0},
		{Key: "dec", Value: dec},
		{Key: "oid", Value: oid},
		{Key: "when", Value: bson.NewDateTimeFromTime(time.UnixMilli(1700000000123))},
		{Key: "nested", Value: bson.D{{Key: "z", Value: int32(1)}, {Key: "a", Value: bson.A{int64(2)}}}},
	})
	encoded, err := encodeDoc(raw)
	require.NoError(t, err)
	m := pgtype.NewMap()
	typ, ok := m.TypeForOID(pgtype.JSONBOID)
	require.True(t, ok)
	doc, err := typ.Codec.DecodeValue(m, pgtype.JSONBOID, pgtype.TextFormatCode, jsonbNormalize(t, encoded))
	require.NoError(t, err)
	rv, err := bson.Raw(raw).LookupErr("_id")
	require.NoError(t, err)
	idText, err := encodeID(rv.Type, rv.Value)
	require.NoError(t, err)

	got, err := docSourceUpdate(pglib.Update{Type: pglib.UpdateTypeInsert, New: map[string]any{"id": idText, "doc": doc}})

	require.NoError(t, err)
	assertDocIDBytes(t, raw, got.Data)
	assert.Equal(t, rv.Value, got.Id[0].Data)
}

func TestFillDocsDecodesRowsUpToLimitAndFailsWithoutLeakingContent(t *testing.T) {
	_, text, idText, _ := storedRow(t, "k")
	keys := []string{"region", "id"}
	rows := []sourceRow{
		{key: []any{"eu", idText}, text: text},
		{key: []any{"eu", "s:" + sentinel}, text: "not json " + sentinel},
	}

	require.NoError(t, fillDocs(rows, keys, 1))
	assert.NotEmpty(t, rows[0].doc)
	assert.Nil(t, rows[1].doc)

	err := fillDocs(rows, keys, 2)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), sentinel)
}

func TestFillDocsRejectsDocWithoutIDAndKeysWithoutIDColumn(t *testing.T) {
	_, text, idText, _ := storedRow(t, "k")
	noID := sourceRow{key: []any{idText}, text: `{"v":{"$numberInt":"1"}}`}

	require.Error(t, fillDocs([]sourceRow{noID}, []string{"id"}, 1))
	require.Error(t, fillDocs([]sourceRow{{key: []any{"eu"}, text: text}}, []string{"region"}, 1))
}

func TestToBsonAdiomUpdateUsesTheDocumentSourceInDocumentMode(t *testing.T) {
	raw, _, idText, doc := storedRow(t, "key-1")
	update := pglib.Update{Type: pglib.UpdateTypeInsert, New: map[string]any{"id": idText, "doc": doc}}
	c := &conn{settings: PostgresSettings{DocumentMode: true}}

	got, err := c.toBsonAdiomUpdate(update)

	require.NoError(t, err)
	assert.Equal(t, mongoIDPart(t, "key-1"), got.Id)
	assertDocIDBytes(t, raw, got.Data)
}
