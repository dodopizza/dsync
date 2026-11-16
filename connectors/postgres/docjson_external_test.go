//go:build external

package postgres

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestEncodeDocThroughJSONBColumn(t *testing.T) {
	ctx := testContext(t)
	admin := adminConn(ctx, t)
	_, err := admin.Exec(ctx, `CREATE TEMP TABLE doc_probe (n int PRIMARY KEY, doc jsonb)`)
	must(t, err, "probe table")
	next := 0
	roundTrip := func(t *testing.T, text string) string {
		next++
		_, err := admin.Exec(ctx, `INSERT INTO doc_probe VALUES ($1, $2::jsonb)`, next, text)
		must(t, err, "insert")
		var back string
		must(t, admin.QueryRow(ctx, `SELECT doc::text FROM doc_probe WHERE n = $1`, next).Scan(&back), "select")
		return back
	}

	t.Run("document survives with the same content but keys reordered and spaces added", func(t *testing.T) {
		raw := rawDoc(t, bson.D{
			{Key: "_id", Value: "k"},
			{Key: "zz", Value: int64(9007199254740993)},
			{Key: "b", Value: bson.A{int32(1), bson.D{{Key: "y", Value: 1.5}, {Key: "x", Value: nil}}}},
			{Key: "a", Value: math10p400(t)},
			{Key: "s", Value: "hé <&> \U0001F600"},
		})
		sent, err := encodeDoc(raw)
		require.NoError(t, err)

		back := roundTrip(t, string(sent))

		assert.NotEqual(t, string(sent), back)
		var want, got any
		require.NoError(t, json.Unmarshal(sent, &want))
		require.NoError(t, json.Unmarshal([]byte(back), &got))
		assert.Equal(t, want, got)
		t.Logf("sent: %s", sent)
		t.Logf("back: %s", back)
	})

	t.Run("jsonb orders keys by length then bytes", func(t *testing.T) {
		sent, err := encodeDoc(rawDoc(t, bson.D{{Key: "b", Value: "1"}, {Key: "aa", Value: "2"}, {Key: "a", Value: "3"}, {Key: "_id", Value: "4"}}))
		require.NoError(t, err)

		back := roundTrip(t, string(sent))

		assert.Equal(t, `{"a": "3", "b": "1", "aa": "2", "_id": "4"}`, back)
	})

	t.Run("bare json numbers are not used because every number is a tagged string", func(t *testing.T) {
		sent, err := encodeDoc(rawDoc(t, bson.D{{Key: "d", Value: 0.1}, {Key: "l", Value: int64(-5)}}))
		require.NoError(t, err)

		back := roundTrip(t, string(sent))

		assert.Equal(t, `{"d": {"$numberDouble": "0.1"}, "l": {"$numberLong": "-5"}}`, back)
	})

	t.Run("jsonb itself rewrites bare numbers and drops duplicate keys", func(t *testing.T) {
		assert.Equal(t, `{"a": 3}`, roundTrip(t, `{"a":1,"a":2,"a":3}`))
		assert.Equal(t, `{"m": 1.50, "n": 100}`, roundTrip(t, `{"m":1.50,"n":1e2}`))
	})

	t.Run("jsonb rejects U+0000 that encodeDoc already rejects", func(t *testing.T) {
		_, err := encodeDoc(rawDoc(t, bson.D{{Key: "a", Value: "x\x00y"}}))
		require.Error(t, err)
		_, dbErr := admin.Exec(ctx, `SELECT $1::jsonb`, `{"a":"x\u0000y"}`)
		assert.Error(t, dbErr)
	})
}

func math10p400(t *testing.T) bson.Decimal128 {
	d, err := bson.ParseDecimal128("1.0E+400")
	require.NoError(t, err)
	return d
}
