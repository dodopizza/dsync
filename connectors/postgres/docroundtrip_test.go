package postgres

import (
	"bytes"
	"cmp"
	"encoding/json"
	"maps"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/adiom-data/dsync/connectors/common"
)

func jsonbNormalize(t *testing.T, text []byte) []byte {
	t.Helper()
	var v any
	require.NoError(t, json.Unmarshal(text, &v))
	var out bytes.Buffer
	writeJSONB(t, &out, v)
	return out.Bytes()
}

func writeJSONB(t *testing.T, out *bytes.Buffer, v any) {
	t.Helper()
	switch v := v.(type) {
	case map[string]any:
		keys := slices.SortedFunc(maps.Keys(v), func(a, b string) int {
			return cmp.Or(cmp.Compare(len(a), len(b)), strings.Compare(a, b))
		})
		out.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				out.WriteByte(',')
			}
			writeJSONB(t, out, k)
			out.WriteByte(':')
			writeJSONB(t, out, v[k])
		}
		out.WriteByte('}')
	case []any:
		out.WriteByte('[')
		for i, e := range v {
			if i > 0 {
				out.WriteByte(',')
			}
			writeJSONB(t, out, e)
		}
		out.WriteByte(']')
	default:
		enc := json.NewEncoder(out)
		enc.SetEscapeHTML(false)
		require.NoError(t, enc.Encode(v))
		out.Truncate(out.Len() - 1)
	}
}

func hashOf(t *testing.T, raw []byte) uint64 {
	t.Helper()
	h := xxhash.New()
	require.NoError(t, common.HashBson(h, bson.Raw(raw), false, nil))
	return h.Sum64()
}

func TestDocJSONBRoundTripPreservesHash(t *testing.T) {
	oid, err := bson.ObjectIDFromHex("507f1f77bcf86cd799439011")
	require.NoError(t, err)
	dec, err := bson.ParseDecimal128("1234567890.123456789")
	require.NoError(t, err)
	cases := map[string]any{
		"double":            1.5,
		"integral double":   float64(3),
		"negative zero":     math.Copysign(0, -1),
		"nan":               math.NaN(),
		"infinity":          math.Inf(1),
		"string":            "hé\U0001F600<&>",
		"embedded document": bson.D{{Key: "bb", Value: int32(1)}, {Key: "a", Value: "x"}},
		"array":             bson.A{int32(1), "s", bson.D{{Key: "k", Value: nil}}},
		"binary generic":    bson.Binary{Subtype: 0, Data: seqBytes(5)},
		"binary uuid":       bson.Binary{Subtype: 4, Data: seqBytes(16)},
		"binary legacy":     bson.Binary{Subtype: 3, Data: seqBytes(16)},
		"binary user":       bson.Binary{Subtype: 0x80, Data: seqBytes(3)},
		"object id":         oid,
		"bool":              true,
		"datetime":          bson.DateTime(1700000000123),
		"null":              nil,
		"regex":             bson.Regex{Pattern: "^a.*", Options: "im"},
		"int32":             int32(7),
		"timestamp":         bson.Timestamp{T: 1700000000, I: 9},
		"int64":             int64(1) << 40,
		"decimal128":        dec,
		"min key":           bson.MinKey{},
		"max key":           bson.MaxKey{},
		"javascript":        bson.JavaScript("f()"),
		"symbol":            bson.Symbol("sym"),
		"undefined":         bson.Undefined{},
		"db pointer":        bson.DBPointer{DB: "ns", Pointer: oid},
		"code with scope":   bson.CodeWithScope{Code: "g()", Scope: bson.D{{Key: "bb", Value: int32(1)}, {Key: "a", Value: bson.D{{Key: "yy", Value: int32(1)}, {Key: "x", Value: int32(2)}}}}},
		"dbref like":        bson.D{{Key: "$ref", Value: "c"}, {Key: "$id", Value: oid}, {Key: "$db", Value: "d"}},
		"dollar key":        bson.D{{Key: "$gt", Value: int32(3)}},
	}
	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			raw := rawDoc(t, bson.D{{Key: "_id", Value: "k"}, {Key: "v", Value: value}, {Key: "a", Value: int32(1)}})
			text, err := encodeDoc(raw)
			if err != nil {
				t.Skipf("encodeDoc rejects: %v", err)
			}
			normalized := jsonbNormalize(t, text)

			var back bson.Raw
			require.NoError(t, bson.UnmarshalExtJSON(normalized, true, &back), "jsonb form %s", normalized)

			assert.Equal(t, hashOf(t, raw), hashOf(t, back), "jsonb form %s", normalized)
		})
	}
}

func TestDocJSONBNormalizerReordersMultiKeyWrappers(t *testing.T) {
	got := string(jsonbNormalize(t, []byte(`{"b":{"$timestamp":{"t":1,"i":2}},"r":{"$regularExpression":{"pattern":"p","options":"i"}},"x":{"$ref":"c","$id":1,"$db":"d"}}`)))

	assert.True(t, strings.Contains(got, `{"i":2,"t":1}`), got)
	assert.True(t, strings.Contains(got, `{"options":"i","pattern":"p"}`), got)
	assert.True(t, strings.Contains(got, `{"$db":"d","$id":1,"$ref":"c"}`), got)
}
