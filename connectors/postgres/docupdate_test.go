package postgres

import (
	"math"
	"testing"
	"time"

	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func idPart(t *testing.T, name string, v any) *adiomv1.BsonValue {
	t.Helper()
	rv := rawValue(t, v)
	return &adiomv1.BsonValue{Name: name, Type: uint32(rv.Type), Data: rv.Value}
}

func upsertUpdate(kind adiomv1.UpdateType, id []*adiomv1.BsonValue, raw []byte) *adiomv1.Update {
	return &adiomv1.Update{Type: kind, Id: id, Data: raw}
}

func deleteUpdate(id ...*adiomv1.BsonValue) *adiomv1.Update {
	return &adiomv1.Update{Type: adiomv1.UpdateType_UPDATE_TYPE_DELETE, Id: id}
}

func TestDocUpdateStatementsUseTheCopyUpsertForInsertAndUpdate(t *testing.T) {
	plan := testPlan(t, PostgresSettings{UpdatedAtColumn: "updated_at"})
	raw := rawDoc(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "Region", Value: "eu"}})
	when := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	want, err := docBatchArgs(plan, [][]byte{raw}, when)
	require.NoError(t, err)

	for _, kind := range []adiomv1.UpdateType{adiomv1.UpdateType_UPDATE_TYPE_INSERT, adiomv1.UpdateType_UPDATE_TYPE_UPDATE} {
		got, err := docUpdateStatements(plan, []*adiomv1.Update{upsertUpdate(kind, []*adiomv1.BsonValue{idPart(t, "", int64(1))}, raw)}, when)

		require.NoError(t, err)
		require.Len(t, got, 1)
		assert.Equal(t, plan.query, got[0].query)
		assert.Equal(t, want[0].args, got[0].args)
	}
}

func TestDocUpdateStatementsRejectIDThatDiffersFromTheDocumentNamingPositionOnly(t *testing.T) {
	plan := testPlan(t, PostgresSettings{})
	good := upsertUpdate(adiomv1.UpdateType_UPDATE_TYPE_INSERT, nil, rawDoc(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "Region", Value: "eu"}}))
	mismatch := upsertUpdate(adiomv1.UpdateType_UPDATE_TYPE_UPDATE, []*adiomv1.BsonValue{idPart(t, "", marker)},
		rawDoc(t, bson.D{{Key: "_id", Value: int32(2)}, {Key: "Region", Value: "eu"}}))

	_, err := docUpdateStatements(plan, []*adiomv1.Update{good, mismatch}, time.Time{})

	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	assert.Contains(t, err.Error(), "update 1")
	assert.NotContains(t, err.Error(), marker)
}

func TestDocUpdateStatementsDeleteByEncodedIDAndNamedCompositeParts(t *testing.T) {
	plan := testPlan(t, PostgresSettings{})
	statements, err := docUpdateStatements(plan, []*adiomv1.Update{
		deleteUpdate(idPart(t, "", int64(7)), idPart(t, "Region", "eu")),
	}, time.Time{})

	require.NoError(t, err)
	assert.Equal(t, `DELETE FROM "app"."items" WHERE "id" = $1 AND "Region" = $2`, statements[0].query)
	assert.Equal(t, []any{"n:7", "eu"}, statements[0].args)
}

func TestDocUpdateStatementsRejectDeleteLackingAKeyPartNamingTheColumn(t *testing.T) {
	plan := testPlan(t, PostgresSettings{})
	for name, parts := range map[string][]*adiomv1.BsonValue{
		"no parts":     nil,
		"id only":      {idPart(t, "", int32(7))},
		"wrong name":   {idPart(t, "", int32(7)), idPart(t, "Other", "eu")},
		"null part":    {idPart(t, "", int32(7)), idPart(t, "Region", nil)},
		"unsupported ": {idPart(t, "", int32(7)), idPart(t, "Region", bson.A{marker})},
	} {
		_, err := docUpdateStatements(plan, []*adiomv1.Update{deleteUpdate(parts...)}, time.Time{})
		require.Error(t, err, name)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), name)
		assert.Contains(t, err.Error(), "update 0", name)
		assert.NotContains(t, err.Error(), marker, name)
	}
	_, err := docUpdateStatements(plan, []*adiomv1.Update{deleteUpdate(idPart(t, "", int32(7)))}, time.Time{})
	assert.Contains(t, err.Error(), "Region")
}

func TestDocUpdateStatementsSkipEmptyPartialUpdatesAndRejectOthersPermanently(t *testing.T) {
	plan := testPlan(t, PostgresSettings{})
	empty := &adiomv1.Update{Type: adiomv1.UpdateType_UPDATE_TYPE_PARTIAL_UPDATE, Id: []*adiomv1.BsonValue{idPart(t, "", int32(1))}}
	del := deleteUpdate(idPart(t, "", int32(1)), idPart(t, "Region", "eu"))

	statements, err := docUpdateStatements(plan, []*adiomv1.Update{empty, del}, time.Time{})
	require.NoError(t, err)
	require.Len(t, statements, 1)
	assert.Equal(t, 1, statements[0].position)

	for _, bad := range []*adiomv1.Update{
		{Type: adiomv1.UpdateType_UPDATE_TYPE_PARTIAL_UPDATE, Data: rawDoc(t, bson.D{{Key: "a", Value: 1}})},
		{Type: adiomv1.UpdateType_UPDATE_TYPE_PARTIAL_UPDATE, PartialUpdateUnset: []string{"a"}},
		{Type: adiomv1.UpdateType(99)},
	} {
		_, err := docUpdateStatements(plan, []*adiomv1.Update{del, bad}, time.Time{})
		require.Error(t, err)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
		assert.Contains(t, err.Error(), "update 1")
	}
}

func TestDocUpdateStatementsKeepRequestOrderWithoutDeduplicating(t *testing.T) {
	plan := testPlan(t, PostgresSettings{})
	row := func(note string) *adiomv1.Update {
		return upsertUpdate(adiomv1.UpdateType_UPDATE_TYPE_UPDATE, nil, rawDoc(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "Region", Value: "eu"}, {Key: "n", Value: note}}))
	}
	del := deleteUpdate(idPart(t, "", int32(1)), idPart(t, "Region", "eu"))

	statements, err := docUpdateStatements(plan, []*adiomv1.Update{row("a"), del, row("b")}, time.Time{})

	require.NoError(t, err)
	require.Len(t, statements, 3)
	assert.Equal(t, plan.query, statements[0].query)
	assert.Equal(t, plan.delete, statements[1].query)
	assert.Equal(t, plan.query, statements[2].query)
	assert.Equal(t, []int{0, 1, 2}, []int{statements[0].position, statements[1].position, statements[2].position})
}

func singleKeyPlan(t *testing.T) *docPlan {
	t.Helper()
	plan, err := buildDocPlan("app.items", docTable{
		Columns:    []docColumn{{Name: "Region", Type: "text"}},
		PrimaryKey: []string{docIDColumn},
		Constraint: "items_pkey",
	}, PostgresSettings{})
	require.NoError(t, err)
	return plan
}

func TestDocUpdateStatementsPickTheIDPartByNameNotPosition(t *testing.T) {
	composite := testPlan(t, PostgresSettings{})
	del, err := docUpdateStatements(composite, []*adiomv1.Update{
		deleteUpdate(idPart(t, "Region", "eu"), idPart(t, "_id", int64(7))),
	}, time.Time{})
	require.NoError(t, err)
	assert.Equal(t, []any{"n:7", "eu"}, del[0].args)

	raw := rawDoc(t, bson.D{{Key: "_id", Value: int32(7)}, {Key: "Region", Value: "eu"}})
	up, err := docUpdateStatements(composite, []*adiomv1.Update{
		upsertUpdate(adiomv1.UpdateType_UPDATE_TYPE_UPDATE, []*adiomv1.BsonValue{idPart(t, "Region", "eu"), idPart(t, "_id", int32(7))}, raw),
	}, time.Time{})
	require.NoError(t, err)
	assert.Equal(t, "n:7", up[0].args[0])

	single, err := docUpdateStatements(singleKeyPlan(t), []*adiomv1.Update{
		deleteUpdate(idPart(t, "Region", "zzz"), idPart(t, "_id", int32(7))),
	}, time.Time{})
	require.NoError(t, err)
	assert.Equal(t, []any{"n:7"}, single[0].args)
}

func TestDocUpdateStatementsRejectMissingOrAmbiguousIDPartWithoutValues(t *testing.T) {
	plan := testPlan(t, PostgresSettings{})
	cases := map[string]struct {
		parts []*adiomv1.BsonValue
		text  string
	}{
		"only named non-id": {[]*adiomv1.BsonValue{idPart(t, "Region", marker)}, "update 0: no _id part"},
		"two _id":           {[]*adiomv1.BsonValue{idPart(t, "_id", marker), idPart(t, "_id", int32(1))}, "update 0: ambiguous _id part"},
		"two unnamed":       {[]*adiomv1.BsonValue{idPart(t, "", marker), idPart(t, "", int32(1))}, "update 0: ambiguous _id part"},
	}
	for name, tc := range cases {
		_, err := docUpdateStatements(plan, []*adiomv1.Update{deleteUpdate(tc.parts...)}, time.Time{})
		require.Error(t, err, name)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), name)
		assert.Contains(t, err.Error(), tc.text, name)
		assert.NotContains(t, err.Error(), marker, name)
	}
}

func TestDocUpdateStatementsRejectKeyPartThatDiffersFromTheDocument(t *testing.T) {
	plan := testPlan(t, PostgresSettings{})
	raw := rawDoc(t, bson.D{{Key: "_id", Value: int32(3)}, {Key: "Region", Value: "eu"}})
	update := func(region string) *adiomv1.Update {
		return upsertUpdate(adiomv1.UpdateType_UPDATE_TYPE_UPDATE, []*adiomv1.BsonValue{idPart(t, "_id", int32(3)), idPart(t, "Region", region)}, raw)
	}

	_, err := docUpdateStatements(plan, []*adiomv1.Update{update("eu")}, time.Time{})
	require.NoError(t, err)

	_, err = docUpdateStatements(plan, []*adiomv1.Update{update(marker)}, time.Time{})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	assert.Contains(t, err.Error(), "update 0: key column Region differs from the document")
	assert.NotContains(t, err.Error(), marker)

	absent := upsertUpdate(adiomv1.UpdateType_UPDATE_TYPE_UPDATE, []*adiomv1.BsonValue{idPart(t, "_id", int32(3))}, raw)
	_, err = docUpdateStatements(plan, []*adiomv1.Update{absent}, time.Time{})
	require.NoError(t, err)
}

func TestDocUpdateStatementsValidateNamedKeyPartsBeforeConversion(t *testing.T) {
	plan := testPlan(t, PostgresSettings{})
	bad := map[string]*adiomv1.BsonValue{
		"malformed boolean": {Name: "Region", Type: uint32(bson.TypeBoolean), Data: []byte{2}},
		"trailing bytes":    {Name: "Region", Type: uint32(bson.TypeInt32), Data: []byte{1, 0, 0, 0, 9}},
		"unterminated":      {Name: "Region", Type: uint32(bson.TypeString), Data: append([]byte{20, 0, 0, 0}, marker...)},
	}
	for name, part := range bad {
		for _, update := range []*adiomv1.Update{
			deleteUpdate(idPart(t, "_id", int32(1)), part),
			upsertUpdate(adiomv1.UpdateType_UPDATE_TYPE_UPDATE, []*adiomv1.BsonValue{idPart(t, "_id", int32(1)), part},
				rawDoc(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "Region", Value: "eu"}})),
		} {
			_, err := docUpdateStatements(plan, []*adiomv1.Update{update}, time.Time{})
			require.Error(t, err, name)
			assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), name)
			assert.Contains(t, err.Error(), "key column Region", name)
			assert.NotContains(t, err.Error(), marker, name)
		}
	}
}

func TestDocUpdateStatementsRejectRepeatedKeyPartButIgnoreMalformedNonKeyParts(t *testing.T) {
	plan := testPlan(t, PostgresSettings{})
	_, err := docUpdateStatements(plan, []*adiomv1.Update{
		deleteUpdate(idPart(t, "_id", int32(1)), idPart(t, "Region", "eu"), idPart(t, "Region", "us")),
	}, time.Time{})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	assert.Contains(t, err.Error(), "update 0: key column Region repeated")

	irrelevant := &adiomv1.BsonValue{Name: "MixedCase", Type: uint32(bson.TypeBoolean), Data: []byte{2}}
	got, err := docUpdateStatements(plan, []*adiomv1.Update{
		deleteUpdate(idPart(t, "_id", int32(1)), idPart(t, "Region", "eu"), irrelevant),
	}, time.Time{})
	require.NoError(t, err)
	assert.Equal(t, []any{"n:1", "eu"}, got[0].args)
}

func keyedPlan(t *testing.T, key docColumn) *docPlan {
	t.Helper()
	plan, err := buildDocPlan("app.items", docTable{
		Columns:    []docColumn{key},
		PrimaryKey: []string{docIDColumn, key.Name},
		Constraint: "items_pkey",
	}, PostgresSettings{})
	require.NoError(t, err)
	return plan
}

func TestDocUpdateStatementsCompareKeyPartsByCanonicalEncoding(t *testing.T) {
	cases := map[string]struct {
		column         docColumn
		part, document any
		accepted       bool
	}{
		"nan equals nan":          {docColumn{Name: "k", Type: "double precision"}, math.NaN(), math.NaN(), true},
		"int64 equals int32":      {docColumn{Name: "k", Type: "numeric"}, int64(5), int32(5), true},
		"int32 equals whole":      {docColumn{Name: "k", Type: "numeric"}, int32(5), 5.0, true},
		"different doubles":       {docColumn{Name: "k", Type: "double precision"}, 1.5, 2.5, false},
		"different integer sizes": {docColumn{Name: "k", Type: "bigint"}, int64(5), int32(6), false},
	}
	for name, tc := range cases {
		plan := keyedPlan(t, tc.column)
		raw := rawDoc(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "k", Value: tc.document}})
		update := upsertUpdate(adiomv1.UpdateType_UPDATE_TYPE_UPDATE, []*adiomv1.BsonValue{idPart(t, "_id", int32(1)), idPart(t, "k", tc.part)}, raw)

		_, err := docUpdateStatements(plan, []*adiomv1.Update{update}, time.Time{})

		if tc.accepted {
			require.NoError(t, err, name)
			continue
		}
		require.Error(t, err, name)
		assert.Contains(t, err.Error(), "key column k differs from the document", name)
	}
}

func TestDocUpdateStatementsDeleteFillsAnIDNamedKeyColumnFromTheIDPart(t *testing.T) {
	plan := keyedPlan(t, docColumn{Name: "_id", Type: "text"})

	got, err := docUpdateStatements(plan, []*adiomv1.Update{deleteUpdate(idPart(t, "_id", "abc"))}, time.Time{})

	require.NoError(t, err)
	assert.Equal(t, []any{"s:abc", "abc"}, got[0].args)
}

func TestDocUpdateStatementsDeleteIgnoresANonKeyIDNamedColumn(t *testing.T) {
	plan, err := buildDocPlan("app.items", docTable{
		Columns:    []docColumn{{Name: "_id", Type: "integer"}},
		PrimaryKey: []string{docIDColumn},
		Constraint: "items_pkey",
	}, PostgresSettings{})
	require.NoError(t, err)

	got, err := docUpdateStatements(plan, []*adiomv1.Update{deleteUpdate(idPart(t, "_id", 1.5))}, time.Time{})

	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Len(t, got[0].args, 1)
}

func TestDocUpdateStatementsRejectNamedKeyPartWhoseFieldIsAbsentFromTheDocument(t *testing.T) {
	plan := keyedPlan(t, docColumn{Name: "k", Type: "text"})
	for name, raw := range map[string][]byte{
		"absent": rawDoc(t, bson.D{{Key: "_id", Value: int32(1)}}),
		"null":   rawDoc(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "k", Value: nil}}),
	} {
		update := upsertUpdate(adiomv1.UpdateType_UPDATE_TYPE_UPDATE, []*adiomv1.BsonValue{idPart(t, "_id", int32(1)), idPart(t, "k", marker)}, raw)

		_, err := docUpdateStatements(plan, []*adiomv1.Update{update}, time.Time{})

		require.Error(t, err, name)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), name)
		assert.Contains(t, err.Error(), "update 0", name)
		assert.NotContains(t, err.Error(), marker, name)
	}
}
