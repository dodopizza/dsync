package postgres

import (
	"encoding/json"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/adiom-data/dsync/connectors/postgres/pglib"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	copyComment = `dsync:{"mode":"copy"}`
	moveComment = `dsync:{"mode":"move"}`
)

var movedAt = time.Date(2024, 5, 6, 7, 8, 9, 123_000_000, time.UTC)

func movePlan(t *testing.T) *docPlan {
	t.Helper()
	plan, err := buildDocPlan("app.items", docTable{
		Columns: []docColumn{
			{Name: "Region", Type: "text"},
			{Name: "n", Type: "integer", Move: true},
			{Name: "big", Type: "bigint", Move: true},
			{Name: "ratio", Type: "double precision", Move: true},
			{Name: "name", Type: "text", Move: true},
			{Name: "on", Type: "boolean", Move: true},
			{Name: "at", Type: typeTimeTZ, Move: true},
			{Name: "uid", Type: "uuid", Move: true},
			{Name: "bin", Type: "bytea", Move: true},
			{Name: "value", Type: "jsonb", Move: true},
			{Name: "list", Type: "jsonb", Move: true},
			{Name: "kept", Type: "bigint"},
			{Name: "blob", Type: "jsonb"},
		},
		PrimaryKey: []string{"id", "Region"},
	}, PostgresSettings{})
	require.NoError(t, err)
	return plan
}

func movedColumnsOf(plan *docPlan) []docColumn {
	var moved []docColumn
	for _, col := range plan.columns {
		if col.Move {
			moved = append(moved, col)
		}
	}
	return moved
}

func fullDoc(t *testing.T) []byte {
	t.Helper()
	return rawDoc(t, bson.D{
		{Key: "_id", Value: "k1"},
		{Key: "Region", Value: "eu"},
		{Key: "n", Value: int32(7)},
		{Key: "big", Value: int64(42)},
		{Key: "ratio", Value: 3.0},
		{Key: "name", Value: "x"},
		{Key: "on", Value: true},
		{Key: "at", Value: bson.NewDateTimeFromTime(movedAt)},
		{Key: "uid", Value: bson.Binary{Subtype: 4, Data: seqBytes(16)}},
		{Key: "bin", Value: bson.Binary{Data: []byte{1, 2}}},
		{Key: "value", Value: bson.D{{Key: "state", Value: int32(2)}, {Key: "payload", Value: bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: "x"}}}}},
		{Key: "list", Value: bson.A{int32(1), "s"}},
		{Key: "kept", Value: int64(9)},
		{Key: "blob", Value: bson.D{{Key: "a", Value: int32(1)}}},
		{Key: "rest", Value: "x"},
	})
}

func readValues(t *testing.T, plan *docPlan, args []any) (string, []any) {
	t.Helper()
	var values []any
	for i, col := range plan.columns {
		value := args[2+i]
		if !col.Move {
			continue
		}
		switch {
		case value == nil:
		case col.Type == "uuid":
			value = [16]byte(value.([]byte))
		case col.Type == "jsonb":
			value = jsonText(jsonbNormalize(t, value.([]byte)))
		}
		values = append(values, value)
	}
	return string(jsonbNormalize(t, args[1].([]byte))), values
}

func TestShapeDocTableMovesPairedNonKeyColumnsUnlessTheCommentSaysCopy(t *testing.T) {
	columns := append(baseColumns(),
		catalogColumn{Name: "plain", Type: "bigint"},
		catalogColumn{Name: "kept", Type: "bigint", Comment: copyComment},
		catalogColumn{Name: "asked", Type: "jsonb", Comment: moveComment},
		catalogColumn{Name: "note", Type: "text", Comment: "owned by billing"},
		catalogColumn{Name: "short", Type: "smallint"},
		catalogColumn{Name: "amount", Type: "numeric"},
		catalogColumn{Name: "rounded", Type: typeTimeTZ, Modifier: true},
		catalogColumn{Name: "_id", Type: "text"},
	)

	got, err := shapeDocTable(shapeNamespace, columns, []string{"id", "Region"})

	require.NoError(t, err)
	moves := map[string]bool{}
	for _, col := range got.Columns {
		moves[col.Name] = col.Move
	}
	assert.Equal(t, map[string]bool{
		"Region": false, "qty": true, "plain": true, "kept": false, "asked": true, "note": true,
		"short": false, "amount": false, "rounded": false, "_id": false,
	}, moves)
}

func TestShapeDocTableRejectsBadMappingCommentsNamingTableAndColumn(t *testing.T) {
	for name, column := range map[string]catalogColumn{
		"unreadable":       {Name: "extra", Type: "bigint", Comment: `dsync:{mode`},
		"unknown key":      {Name: "extra", Type: "bigint", Comment: `dsync:{"mode":"move","other":1}`},
		"unknown mode":     {Name: "extra", Type: "bigint", Comment: `dsync:{"mode":"link"}`},
		"no mode":          {Name: "extra", Type: "bigint", Comment: `dsync:{}`},
		"move a key":       {Name: "Region", Type: "text", Comment: moveComment},
		"move numeric":     {Name: "extra", Type: "numeric", Comment: moveComment},
		"move a modifier":  {Name: "extra", Type: typeTimeTZ, Modifier: true, Comment: moveComment},
		"move the _id":     {Name: "_id", Type: "text", Comment: moveComment},
		"move an array":    {Name: "extra", Type: "text[]", Comment: moveComment},
		"move a short int": {Name: "extra", Type: "smallint", Comment: moveComment},
	} {
		t.Run(name, func(t *testing.T) {
			columns := []catalogColumn{{Name: "id", Type: "text"}, {Name: "doc", Type: "jsonb"}, column}
			if column.Name != "Region" {
				columns = append(columns, catalogColumn{Name: "Region", Type: "text"})
			}

			_, err := shapeDocTable(shapeNamespace, columns, []string{"id", "Region"})

			require.Error(t, err)
			assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
			assert.Contains(t, err.Error(), shapeNamespace)
			assert.Contains(t, err.Error(), column.Name)
		})
	}
}

func TestDocBatchArgsMovesFieldsIntoColumnsAndOutOfDoc(t *testing.T) {
	plan := movePlan(t)

	batch, err := docBatchArgs(plan, [][]byte{fullDoc(t)}, time.Time{})

	require.NoError(t, err)
	args := batch[0].args
	require.Len(t, args, 2+len(plan.columns))
	assert.JSONEq(t, `{"_id":"k1","Region":"eu","kept":{"$numberLong":"9"},"blob":{"a":{"$numberInt":"1"}},"rest":"x"}`, string(args[1].([]byte)))
	assert.Equal(t, []any{"eu", int32(7), int64(42), 3.0, "x", true, movedAt, seqBytes(16), []byte{1, 2}}, args[2:11])
	assert.JSONEq(t, `{"state":{"$numberInt":"2"},"payload":{"a":{"$numberInt":"1"},"b":"x"}}`, string(args[11].([]byte)))
	assert.JSONEq(t, `[{"$numberInt":"1"},"s"]`, string(args[12].([]byte)))
	assert.Equal(t, int64(9), args[13])
	assert.JSONEq(t, `{"a":{"$numberInt":"1"}}`, string(args[14].([]byte)))
}

func TestDocBatchArgsKeepsAnExplicitNullInDocAndLeavesAbsentFieldsOut(t *testing.T) {
	plan := movePlan(t)
	raw := rawDoc(t, bson.D{{Key: "_id", Value: "k1"}, {Key: "Region", Value: "eu"}, {Key: "n", Value: nil}, {Key: "value", Value: nil}})

	batch, err := docBatchArgs(plan, [][]byte{raw}, time.Time{})

	require.NoError(t, err)
	assert.JSONEq(t, `{"_id":"k1","Region":"eu","n":null,"value":null}`, string(batch[0].args[1].([]byte)))
	for _, arg := range batch[0].args[3:] {
		assert.Nil(t, arg)
	}
}

func TestDocBatchArgsRejectsAFieldOfAnotherTypeNamingColumnAndTypeNotTheValue(t *testing.T) {
	plan := movePlan(t)
	for column, value := range map[string]any{
		"n":     int64(7),
		"big":   int32(7),
		"ratio": int32(7),
		"name":  int32(7),
		"on":    marker,
		"at":    marker,
		"uid":   bson.Binary{Subtype: 0, Data: seqBytes(16)},
		"bin":   bson.Binary{Subtype: 4, Data: seqBytes(16)},
		"value": marker,
		"list":  int32(7),
		"blob":  marker,
	} {
		raw := rawDoc(t, bson.D{{Key: "_id", Value: "k1"}, {Key: "Region", Value: "eu"}, {Key: column, Value: value}})

		_, err := docBatchArgs(plan, [][]byte{raw}, time.Time{})

		require.Error(t, err, column)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), column)
		assert.Contains(t, err.Error(), "document 0", column)
		assert.Contains(t, err.Error(), "column "+column, column)
		assert.Contains(t, err.Error(), rawValue(t, value).Type.String(), column)
		assert.NotContains(t, err.Error(), marker, column)
	}
	short := rawDoc(t, bson.D{{Key: "_id", Value: "k1"}, {Key: "Region", Value: "eu"}, {Key: "uid", Value: bson.Binary{Subtype: 4, Data: seqBytes(15)}}})
	_, err := docBatchArgs(plan, [][]byte{short}, time.Time{})
	require.Error(t, err)
}

func TestMovedColumnsRestoreTheDocumentWithTheSameHashOnListAndStream(t *testing.T) {
	plan := movePlan(t)
	moved := movedColumnsOf(plan)
	for name, raw := range map[string][]byte{
		"every column": fullDoc(t),
		"sparse":       rawDoc(t, bson.D{{Key: "_id", Value: "k1"}, {Key: "Region", Value: "eu"}, {Key: "n", Value: nil}, {Key: "rest", Value: int64(1)}}),
	} {
		t.Run(name, func(t *testing.T) {
			batch, err := docBatchArgs(plan, [][]byte{raw}, time.Time{})
			require.NoError(t, err)
			text, values := readValues(t, plan, batch[0].args)

			rows := []sourceRow{{key: []any{"s:k1", "eu"}, text: text, moved: values}}
			require.NoError(t, fillDocs(rows, []string{"id", "Region"}, moved, 1))
			assert.Equal(t, hashOf(t, raw), hashOf(t, rows[0].doc))

			var doc any
			require.NoError(t, json.Unmarshal([]byte(text), &doc))
			row := map[string]any{"id": "s:k1", "doc": doc, "Region": "eu"}
			for i, col := range moved {
				row[col.Name] = values[i]
				if text, ok := values[i].(jsonText); ok {
					var decoded any
					require.NoError(t, json.Unmarshal([]byte(text), &decoded))
					row[col.Name] = decoded
				}
			}
			got, err := docSourceUpdate(pglib.Update{Type: pglib.UpdateTypeUpdate, New: row}, moved)
			require.NoError(t, err)
			assert.Equal(t, hashOf(t, raw), hashOf(t, got.Data))
		})
	}
}

func TestMovedColumnsRestoreValuesDecodedLikeTheStreamAndTruncateMicroseconds(t *testing.T) {
	m := pgtype.NewMap()
	for name, tc := range map[string]struct {
		oid  uint32
		typ  string
		text string
		want any
	}{
		"integer":   {pgtype.Int4OID, "integer", "7", int32(7)},
		"bigint":    {pgtype.Int8OID, "bigint", "42", int64(42)},
		"double":    {pgtype.Float8OID, "double precision", "1.5", 1.5},
		"text":      {pgtype.TextOID, "text", "x", "x"},
		"boolean":   {pgtype.BoolOID, "boolean", "t", true},
		"timestamp": {pgtype.TimestamptzOID, typeTimeTZ, "2024-05-06 07:08:09.123987+00", bson.NewDateTimeFromTime(movedAt)},
		"uuid":      {pgtype.UUIDOID, "uuid", "00010203-0405-0607-0809-0a0b0c0d0e0f", bson.Binary{Subtype: 4, Data: seqBytes(16)}},
		"bytea":     {pgtype.ByteaOID, "bytea", `\x0102`, bson.Binary{Data: []byte{1, 2}}},
		"jsonb":     {pgtype.JSONBOID, "jsonb", `{"a": {"$numberInt": "1"}}`, bson.D{{Key: "a", Value: int32(1)}}},
	} {
		t.Run(name, func(t *testing.T) {
			typ, ok := m.TypeForOID(tc.oid)
			require.True(t, ok)
			value, err := typ.Codec.DecodeValue(m, tc.oid, pgtype.TextFormatCode, []byte(tc.text))
			require.NoError(t, err)
			stored := bson.Raw(rawDoc(t, bson.D{{Key: "_id", Value: "k"}}))

			got, err := restoreMoved(stored, []docColumn{{Name: "v", Type: tc.typ, Move: true}}, []any{value})

			require.NoError(t, err)
			want := rawValue(t, tc.want)
			assert.Equal(t, want.Type, got.Lookup("v").Type)
			assert.Equal(t, want.Value, got.Lookup("v").Value)
		})
	}
}

func TestRestoreMovedReplacesANullAndRejectsConflictsWithoutLeakingContent(t *testing.T) {
	moved := []docColumn{{Name: "n", Type: "integer", Move: true}, {Name: "name", Type: "text", Move: true}}
	withNull := bson.Raw(rawDoc(t, bson.D{{Key: "_id", Value: "k"}, {Key: "n", Value: nil}}))

	got, err := restoreMoved(withNull, moved, []any{int32(5), nil})

	require.NoError(t, err)
	assert.Equal(t, hashOf(t, rawDoc(t, bson.D{{Key: "_id", Value: "k"}, {Key: "n", Value: int32(5)}})), hashOf(t, got))

	both := bson.Raw(rawDoc(t, bson.D{{Key: "_id", Value: "k"}, {Key: "name", Value: sentinel}}))
	for name, values := range map[string][]any{
		"field in doc and column": {nil, sentinel},
		"unexpected value":        {sentinel, nil},
		"unchanged marker":        {struct{}{}, nil},
	} {
		_, err := restoreMoved(both, moved, values)
		require.Error(t, err, name)
		assert.Contains(t, err.Error(), "column", name)
		assert.NotContains(t, err.Error(), sentinel, name)
	}
}

func TestDocSourceUpdateRejectsARowWithoutAMovedColumn(t *testing.T) {
	_, _, idText, doc := storedRow(t, "k")
	moved := []docColumn{{Name: "n", Type: "integer", Move: true}}

	_, err := docSourceUpdate(pglib.Update{Type: pglib.UpdateTypeInsert, New: map[string]any{"id": idText, "doc": doc}}, moved)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "column n")

	deleted, err := docSourceUpdate(pglib.Update{Type: pglib.UpdateTypeDelete, Old: map[string]any{"id": idText}}, moved)
	require.NoError(t, err)
	assert.NotEmpty(t, deleted.Id)
}

func TestDocBatchArgsKeepsAFieldNamedLikeTheUpdatedAtColumnInDoc(t *testing.T) {
	settings := PostgresSettings{UpdatedAtColumn: "at"}
	plan, err := buildDocPlan("app.items", docTable{
		Columns:    []docColumn{{Name: "at", Type: typeTimeTZ, Move: true}, {Name: "n", Type: "integer", Move: true}},
		PrimaryKey: []string{docIDColumn},
	}, settings)
	require.NoError(t, err)
	doc := rawDoc(t, bson.D{{Key: "_id", Value: "k1"}, {Key: "at", Value: "text"}, {Key: "n", Value: int32(7)}})

	batch, err := docBatchArgs(plan, [][]byte{doc}, movedAt)

	require.NoError(t, err)
	assert.JSONEq(t, `{"_id":"k1","at":"text"}`, string(batch[0].args[1].([]byte)))
}

func TestDocBatchArgsRejectsAMovedDateOutsideThePostgresRange(t *testing.T) {
	plan := movePlan(t)
	first := time.Date(-4713, 11, 24, 0, 0, 0, 0, time.UTC).UnixMilli()
	last := time.Date(294276, 12, 31, 23, 59, 59, 999e6, time.UTC).UnixMilli()
	write := func(millis int64) error {
		raw := rawDoc(t, bson.D{{Key: "_id", Value: "k1"}, {Key: "Region", Value: "eu"}, {Key: "at", Value: bson.DateTime(millis)}})
		_, err := docBatchArgs(plan, [][]byte{raw}, time.Time{})
		return err
	}

	for _, millis := range []int64{1 << 61, -(1 << 61), last + 1, first - 1} {
		err := write(millis)
		require.Error(t, err, millis)
		assert.Contains(t, err.Error(), "column at")
	}
	require.NoError(t, write(first))
	require.NoError(t, write(last))
}

func TestDocUpsertSQLComparesMovedDoublesByTheirBytes(t *testing.T) {
	query := movePlan(t).query

	assert.Contains(t, query, `pg_catalog.float8send(t."ratio")`)
	assert.Contains(t, query, `pg_catalog.float8send(EXCLUDED."ratio")`)
	assert.NotContains(t, query, `float8send(t."big")`)
}

func TestBuildDocPlanRejectsAJSONBPrimaryKeyColumn(t *testing.T) {
	_, err := buildDocPlan("app.items", docTable{
		Columns:    []docColumn{{Name: "tenant", Type: typeJSONB}},
		PrimaryKey: []string{docIDColumn, "tenant"},
	}, PostgresSettings{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "tenant")
}

func TestRestoreMovedTakesJSONBAsTextAndHidesABadValue(t *testing.T) {
	moved := []docColumn{{Name: "v", Type: typeJSONB, Move: true}}
	stored := rawDoc(t, bson.D{{Key: "_id", Value: "k1"}})

	got, err := restoreMoved(stored, moved, []any{jsonText(`{"a": {"$numberInt": "1"}}`)})
	require.NoError(t, err)
	assert.Equal(t, hashOf(t, rawDoc(t, bson.D{{Key: "_id", Value: "k1"}, {Key: "v", Value: bson.D{{Key: "a", Value: int32(1)}}}})), hashOf(t, got))

	for _, text := range []jsonText{jsonText(`{"a": {"$numberInt": "` + marker + `"}}`), jsonText(`"` + marker + `"`)} {
		_, err = restoreMoved(stored, moved, []any{text})
		require.Error(t, err)
		assert.NotContains(t, err.Error(), marker)
	}
}
