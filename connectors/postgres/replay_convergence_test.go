package postgres

import (
	"strings"
	"testing"
	"time"

	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func replayMetadata() sinkTableMetadata {
	return sinkTableMetadata{
		Columns: []sinkColumnMetadata{
			{Name: "id"},
			{Name: "name", Nullable: true},
			{Name: "note", Nullable: true},
			{Name: "status", HasDefault: true},
		},
		PrimaryKey: []string{"id"},
	}
}

func replayQuery(t *testing.T, document map[string]interface{}) string {
	t.Helper()
	metadata := replayMetadata()
	op, err := planPostgresUpsert(metadata, document, 0)
	require.NoError(t, err)
	return op.query("public.items", metadata.PrimaryKey)
}

func TestColumnModeReplayedInsertUpdatesInsteadOfFailingOnDuplicateKey(t *testing.T) {
	query := replayQuery(t, map[string]interface{}{"id": int32(1), "name": "a", "note": "n"})

	assert.Contains(t, query, `ON CONFLICT ("id") DO UPDATE SET`)
	assert.NotContains(t, query, "DO NOTHING")
}

func TestColumnModeKeyOnlyTableReplaysAsDoNothing(t *testing.T) {
	query := buildUpsertQuery("public.keys", []string{"id"}, []string{"id"}, nil, false)

	assert.True(t, strings.HasSuffix(query, `ON CONFLICT ("id") DO NOTHING`), query)
}

func TestColumnModeSubsetWriteConvergesOnTheSameRowForDeterministicDefaults(t *testing.T) {
	const assignments = `DO UPDATE SET "name" = EXCLUDED."name", "note" = EXCLUDED."note", "status" = DEFAULT`
	full := replayQuery(t, map[string]interface{}{"id": int32(1), "name": "a", "note": "n"})
	subset := replayQuery(t, map[string]interface{}{"id": int32(1), "name": "b"})

	assert.Contains(t, full, assignments)
	assert.Contains(t, subset, assignments)
}

func TestColumnModeOmittedDefaultColumnIsReevaluatedOnEveryReplayedWrite(t *testing.T) {
	query := replayQuery(t, map[string]interface{}{"id": int32(1), "name": "b"})

	assert.Contains(t, query, `"status" = DEFAULT`)
	assert.NotContains(t, query, "IS DISTINCT FROM")
	assert.NotContains(t, query, "WHERE")
}

func TestColumnModeUpdateWindowForOneIDKeepsOrderKindsAndOverwritesTheKeyFromTheID(t *testing.T) {
	id := []*adiomv1.BsonValue{idPart(t, "", int32(1))}
	upsert := func(kind adiomv1.UpdateType, doc bson.D) *adiomv1.Update {
		return upsertUpdate(kind, id, rawDoc(t, doc))
	}
	updates := []*adiomv1.Update{
		upsert(adiomv1.UpdateType_UPDATE_TYPE_INSERT, bson.D{{Key: "id", Value: int32(99)}, {Key: "name", Value: "v1"}, {Key: "note", Value: "n"}}),
		upsert(adiomv1.UpdateType_UPDATE_TYPE_UPDATE, bson.D{{Key: "name", Value: "v2"}}),
		{Type: adiomv1.UpdateType_UPDATE_TYPE_DELETE, Id: id},
		upsert(adiomv1.UpdateType_UPDATE_TYPE_INSERT, bson.D{{Key: "name", Value: "v3"}}),
	}
	metadata := replayMetadata()

	ops, err := planPostgresUpdates(NewBSONRegistry(), metadata, false, append(updates, updates...))

	require.NoError(t, err)
	require.Len(t, ops, 8)
	for i, op := range ops {
		assert.Equal(t, i%4 == 2, op.delete, "operation %d", i)
		assert.Equal(t, i, op.requestIndex)
		assert.EqualValues(t, 1, op.values[0], "operation %d key", i)
		assert.True(t, strings.HasPrefix(op.query("public.items", metadata.PrimaryKey), map[bool]string{true: "DELETE FROM", false: "INSERT INTO"}[op.delete]))
	}
	assert.Equal(t, []interface{}{"v1", "n"}, ops[0].values[1:])
	assert.Equal(t, []interface{}{"v2", nil}, ops[1].values[1:])
	assert.Equal(t, []interface{}{"v3", nil}, ops[7].values[1:])
	assert.Len(t, ops[6].values, 1)
}

func replayDoc(t *testing.T, version string, extra ...bson.E) []byte {
	t.Helper()
	return rawDoc(t, append(bson.D{{Key: "_id", Value: int32(1)}, {Key: "Region", Value: "eu"}, {Key: "n", Value: version}}, extra...))
}

func replayUpsert(t *testing.T, version string, extra ...bson.E) *adiomv1.Update {
	return upsertUpdate(adiomv1.UpdateType_UPDATE_TYPE_UPDATE, nil, replayDoc(t, version, extra...))
}

func TestDocumentModeIdenticalConsecutiveWriteIsSkippedByTheDistinctGuardIgnoringUpdatedAt(t *testing.T) {
	plan := testPlan(t, PostgresSettings{UpdatedAtColumn: "updated_at"})

	_, guard, found := strings.Cut(plan.query, " WHERE ")

	require.True(t, found)
	assert.Contains(t, guard, `t."doc"`)
	assert.NotContains(t, guard, "updated_at")
}

func TestDocumentModeReplayWindowKeepsRequestOrderAndEndsOnTheNewestDocument(t *testing.T) {
	plan := testPlan(t, PostgresSettings{})
	del := deleteUpdate(idPart(t, "_id", int32(1)), idPart(t, "Region", "eu"))
	window := []*adiomv1.Update{replayUpsert(t, "v1"), replayUpsert(t, "v2")}

	statements, err := docUpdateStatements(plan, append(window, window...), time.Time{})

	require.NoError(t, err)
	require.Len(t, statements, 4)
	for i, st := range statements {
		assert.Equal(t, plan.query, st.query)
		assert.Equal(t, i, st.position)
		assert.Equal(t, "n:1", st.args[0])
		assert.Equal(t, "eu", st.args[2])
		want := `{"_id":{"$numberInt":"1"},"Region":"eu","n":"v` + string(rune('1'+i%2)) + `"}`
		assert.JSONEq(t, want, string(st.args[1].([]byte)))
	}

	statements, err = docUpdateStatements(plan, []*adiomv1.Update{del, replayUpsert(t, "v1"), replayUpsert(t, "v2"), del, del}, time.Time{})

	require.NoError(t, err)
	require.Len(t, statements, 5)
	assert.Equal(t, []string{plan.delete, plan.query, plan.query, plan.delete, plan.delete}, []string{statements[0].query, statements[1].query, statements[2].query, statements[3].query, statements[4].query})
	assert.Equal(t, []any{"n:1", "eu"}, statements[4].args)
}

func TestDocumentModeUpdatedAtColumnWithoutFieldTakesTheReplayTimeForEveryVersionInTheWindow(t *testing.T) {
	plan := testPlan(t, PostgresSettings{UpdatedAtColumn: "updated_at"})
	original := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	replay := original.Add(time.Hour)
	window := []*adiomv1.Update{replayUpsert(t, "v1"), replayUpsert(t, "v2")}

	first, err := docUpdateStatements(plan, window, original)
	require.NoError(t, err)
	replayed, err := docUpdateStatements(plan, window, replay)
	require.NoError(t, err)

	for i := range window {
		assert.Equal(t, original, first[i].args[len(first[i].args)-1])
		assert.Equal(t, replay, replayed[i].args[len(replayed[i].args)-1])
		assert.Equal(t, first[i].args[:len(first[i].args)-1], replayed[i].args[:len(replayed[i].args)-1])
	}
}

func TestDocumentModeUpdatedAtFieldTakesTheStampFromTheDocumentWhateverTheReplayTime(t *testing.T) {
	plan := testPlan(t, PostgresSettings{UpdatedAtColumn: "updated_at", UpdatedAtField: "ts"})
	stamp := time.Date(2024, 5, 6, 7, 8, 9, 0, time.UTC)
	update := replayUpsert(t, "v1", bson.E{Key: "ts", Value: bson.NewDateTimeFromTime(stamp)})

	first, err := docUpdateStatements(plan, []*adiomv1.Update{update}, time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	replayed, err := docUpdateStatements(plan, []*adiomv1.Update{update}, time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	assert.Equal(t, stamp, first[0].args[len(first[0].args)-1])
	assert.Equal(t, first, replayed)
}
