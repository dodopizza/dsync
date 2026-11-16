package postgres

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

func rawDoc(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := bson.Marshal(v)
	require.NoError(t, err)
	return raw
}

func TestEncodeDocCanonicalExtendedJSON(t *testing.T) {
	oid, err := bson.ObjectIDFromHex("507f1f77bcf86cd799439011")
	require.NoError(t, err)
	for name, tc := range map[string]struct {
		doc  any
		want string
	}{
		"scalars in order": {
			bson.D{{Key: "_id", Value: "k"}, {Key: "z", Value: int32(1)}, {Key: "a", Value: int64(2)}, {Key: "m", Value: 1.5}},
			`{"_id":"k","z":{"$numberInt":"1"},"a":{"$numberLong":"2"},"m":{"$numberDouble":"1.5"}}`,
		},
		"object id and date": {
			bson.D{{Key: "_id", Value: oid}, {Key: "d", Value: bson.DateTime(1700000000123)}},
			`{"_id":{"$oid":"507f1f77bcf86cd799439011"},"d":{"$date":{"$numberLong":"1700000000123"}}}`,
		},
		"nested": {
			bson.D{{Key: "o", Value: bson.D{{Key: "b", Value: bson.A{int32(1), nil, true}}}}},
			`{"o":{"b":[{"$numberInt":"1"},null,true]}}`,
		},
		"special doubles": {
			bson.D{{Key: "n", Value: math.NaN()}, {Key: "z", Value: math.Copysign(0, -1)}},
			`{"n":{"$numberDouble":"NaN"},"z":{"$numberDouble":"-0.0"}}`,
		},
		"html characters are not escaped": {
			bson.D{{Key: "s", Value: "<a>&"}},
			`{"s":"<a>&"}`,
		},
		"unicode": {
			bson.D{{Key: "s", Value: "hé\U0001F600"}},
			"{\"s\":\"hé\U0001F600\"}",
		},
		"empty": {bson.D{}, `{}`},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := encodeDoc(rawDoc(t, tc.doc))

			require.NoError(t, err)
			assert.Equal(t, tc.want, string(got))
		})
	}
}

func TestEncodeDocIsDeterministicAndRoundTrips(t *testing.T) {
	doc := rawDoc(t, bson.D{{Key: "b", Value: int32(1)}, {Key: "a", Value: bson.D{{Key: "y", Value: bson.Decimal128{}}, {Key: "x", Value: bson.Binary{Subtype: 4, Data: seqBytes(16)}}}}})

	first, err := encodeDoc(doc)
	require.NoError(t, err)
	second, err := encodeDoc(doc)
	require.NoError(t, err)
	var back bson.Raw
	require.NoError(t, bson.UnmarshalExtJSON(first, true, &back))

	assert.Equal(t, first, second)
	assert.Equal(t, doc, []byte(back))
}

func TestEncodeDocRejectsNULNamingTheFieldPath(t *testing.T) {
	for name, tc := range map[string]struct {
		doc  []byte
		path string
	}{
		"top level string": {rawDoc(t, bson.D{{Key: "a", Value: "x\x00y"}}), "a"},
		"nested array":     {rawDoc(t, bson.D{{Key: "a", Value: bson.D{{Key: "b", Value: bson.A{"ok", "p\x00q"}}}}}), "a.b[1]"},
		"javascript":       {rawDoc(t, bson.D{{Key: "c", Value: bson.JavaScript("f\x00")}}), "c"},
		"symbol":           {rawDoc(t, bson.D{{Key: "s", Value: bson.Symbol("\x00")}}), "s"},
		"code scope": {rawDoc(t, bson.D{{Key: "w", Value: bson.CodeWithScope{Code: "ok", Scope: bson.D{{Key: "v", Value: "\x00"}}}}}),
			"w.$scope.v"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := encodeDoc(tc.doc)

			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.path)
			assert.Contains(t, err.Error(), "U+0000")
		})
	}
}

func TestEncodeDocRejectsInvalidUTF8NamingTheFieldPath(t *testing.T) {
	value := bsoncore.NewDocumentBuilder().AppendDocument("o", bsoncore.NewDocumentBuilder().AppendString("s", "se\xffcret").Build()).Build()
	key := bsoncore.NewDocumentBuilder().AppendDocument("o", bsoncore.NewDocumentBuilder().AppendString("k\xff", "v").Build()).Build()

	_, valueErr := encodeDoc(value)
	_, keyErr := encodeDoc(key)

	require.Error(t, valueErr)
	assert.Contains(t, valueErr.Error(), "o.s")
	assert.Contains(t, valueErr.Error(), "UTF-8")
	assert.NotContains(t, valueErr.Error(), "cret")
	require.Error(t, keyErr)
	assert.Contains(t, keyErr.Error(), "UTF-8")
	assert.Contains(t, keyErr.Error(), "o")
}

func TestEncodeDocRejectsDuplicateKeysNamingThePath(t *testing.T) {
	top := bsoncore.NewDocumentBuilder().AppendString("a", "secret1").AppendString("a", "secret2").Build()
	nested := bsoncore.NewDocumentBuilder().AppendDocument("o", bsoncore.NewDocumentBuilder().AppendInt32("k", 1).AppendInt32("k", 2).Build()).Build()

	_, topErr := encodeDoc(top)
	_, nestedErr := encodeDoc(nested)

	require.Error(t, topErr)
	assert.Contains(t, topErr.Error(), "duplicate")
	assert.Contains(t, topErr.Error(), "a")
	assert.NotContains(t, topErr.Error(), "secret")
	require.Error(t, nestedErr)
	assert.Contains(t, nestedErr.Error(), "o.k")
}

func TestEncodeDocRejectsMalformedBSONWithoutEchoingIt(t *testing.T) {
	_, err := encodeDoc([]byte("secret-bytes"))

	require.Error(t, err)
	assert.NotContains(t, err.Error(), "secret")
}

func TestEncodeDocRejectsExtendedJSONWrapperKeysNamingThePath(t *testing.T) {
	for _, key := range []string{"$numberInt", "$numberLong", "$numberDouble", "$numberDecimal", "$oid", "$date", "$binary", "$uuid",
		"$regularExpression", "$timestamp", "$symbol", "$code", "$scope", "$minKey", "$maxKey", "$undefined", "$dbPointer"} {
		t.Run(key, func(t *testing.T) {
			top := rawDoc(t, bson.D{{Key: key, Value: "secret"}})
			nested := rawDoc(t, bson.D{{Key: "o", Value: bson.D{{Key: "p", Value: bson.A{"x", bson.D{{Key: key, Value: "secret"}}}}}}})

			_, topErr := encodeDoc(top)
			_, nestedErr := encodeDoc(nested)

			require.Error(t, topErr)
			assert.Contains(t, topErr.Error(), key)
			assert.NotContains(t, topErr.Error(), "secret")
			require.Error(t, nestedErr)
			assert.Contains(t, nestedErr.Error(), "o.p[1]."+key)
		})
	}
}

func TestEncodeDocAllowsDBRefAndOtherDollarKeys(t *testing.T) {
	doc := rawDoc(t, bson.D{
		{Key: "r", Value: bson.D{{Key: "$ref", Value: "c"}, {Key: "$id", Value: int32(1)}, {Key: "$db", Value: "d"}}},
		{Key: "$custom", Value: int32(2)},
		{Key: "n", Value: bson.D{{Key: "$gt", Value: int32(3)}}},
	})

	got, err := encodeDoc(doc)

	require.NoError(t, err)
	assert.Contains(t, string(got), `"$ref":"c"`)
	assert.Contains(t, string(got), `"$custom"`)
}
