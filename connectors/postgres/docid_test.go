package postgres

import (
	"math"
	"runtime"
	"strings"
	"testing"

	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func idText(t *testing.T, v any) string {
	t.Helper()
	typ, data := bson.TypeNull, []byte(nil)
	if v != nil {
		var err error
		typ, data, err = bson.MarshalValue(v)
		require.NoError(t, err)
	}
	text, err := encodeID(typ, data)
	require.NoError(t, err)
	return text
}

func dec128(t *testing.T, s string) bson.Decimal128 {
	t.Helper()
	d, err := bson.ParseDecimal128(s)
	require.NoError(t, err)
	return d
}

func seqBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}

func TestEncodeIDTextForm(t *testing.T) {
	oid, err := bson.ObjectIDFromHex("507f1f77bcf86cd799439011")
	require.NoError(t, err)
	for name, tc := range map[string]struct {
		id   any
		want string
	}{
		"string":                 {"abc", "s:abc"},
		"empty string":           {"", "s:"},
		"string with tag prefix": {"n:1", "s:n:1"},
		"objectid":               {oid, "o:507f1f77bcf86cd799439011"},
		"true":                   {true, "b:true"},
		"false":                  {false, "b:false"},
		"date":                   {bson.DateTime(1700000000123), "d:1700000000123"},
		"negative date":          {bson.DateTime(-5), "d:-5"},
		"null":                   {nil, "z:"},
		"int32":                  {int32(-7), "n:-7"},
		"int64 max":              {int64(math.MaxInt64), "n:9223372036854775807"},
		"int64 min":              {int64(math.MinInt64), "n:-9223372036854775808"},
		"double fraction":        {0.1, "f:0.1"},
		"double half":            {0.5, "f:0.5"},
		"double negative":        {-2.5, "f:-2.5"},
		"double tiny":            {5e-324, "f:5e-324"},
		"double integral":        {1e22, "n:10000000000000000000000"},
		"decimal half":           {dec128(t, "0.5"), "n:0.5"},
		"decimal 1E+39 plain":    {dec128(t, "1E+39"), "n:1" + strings.Repeat("0", 39)},
		"decimal 1E+40 exponent": {dec128(t, "1E+40"), "n:1e40"},
		"decimal 1E-40 plain":    {dec128(t, "1E-40"), "n:0." + strings.Repeat("0", 39) + "1"},
		"decimal 1E-41 exponent": {dec128(t, "1E-41"), "n:1e-41"},
		"decimal max exponent":   {dec128(t, "1E+6111"), "n:1e6111"},
		"decimal min exponent":   {dec128(t, "1E-6176"), "n:1e-6176"},
		"decimal long exponent":  {dec128(t, "1.234E+100"), "n:1234e97"},
		"decimal negative large": {dec128(t, "-5E+50"), "n:-5e50"},
		"double 2^63":            {math.Ldexp(1, 63), "n:9223372036854775808"},
		"double nan":             {math.NaN(), "n:NaN"},
		"double +inf":            {math.Inf(1), "n:Infinity"},
		"double -inf":            {math.Inf(-1), "n:-Infinity"},
		"double -0":              {math.Copysign(0, -1), "n:0"},
		"decimal trailing zeros": {dec128(t, "5.00"), "n:5"},
		"decimal fraction":       {dec128(t, "-0.0120"), "n:-0.012"},
		"decimal exponent":       {dec128(t, "1E+3"), "n:1000"},
		"decimal -0":             {dec128(t, "-0"), "n:0"},
		"decimal nan":            {dec128(t, "NaN"), "n:NaN"},
		"decimal -inf":           {dec128(t, "-Infinity"), "n:-Infinity"},
		"uuid":                   {bson.Binary{Subtype: 4, Data: seqBytes(16)}, "u:00010203-0405-0607-0809-0a0b0c0d0e0f"},
		"legacy uuid":            {bson.Binary{Subtype: 3, Data: seqBytes(16)}, "l:03020100-0504-0706-0809-0a0b0c0d0e0f"},
		"generic binary":         {bson.Binary{Subtype: 0, Data: []byte{0xde, 0xad}}, "y:00:dead"},
		"empty binary":           {bson.Binary{Subtype: 0}, "y:00:"},
		"short uuid subtype":     {bson.Binary{Subtype: 4, Data: []byte{1, 2, 3}}, "y:04:010203"},
		"short legacy subtype":   {bson.Binary{Subtype: 3, Data: []byte{1, 2, 3}}, "y:03:010203"},
		"custom binary":          {bson.Binary{Subtype: 0x80, Data: []byte{1}}, "y:80:01"},
		"document":               {bson.D{{Key: "b", Value: int32(1)}, {Key: "a", Value: "x"}}, `j:{"b":{"$numberInt":"1"},"a":"x"}`},
		"array":                  {bson.A{int32(1), "x"}, `a:[{"$numberInt":"1"},"x"]`},
		"empty array":            {bson.A{}, `a:[]`},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, idText(t, tc.id))
		})
	}
}

func TestEncodeIDNumericEqualityAcrossTypes(t *testing.T) {
	for name, group := range map[string][]any{
		"five":            {int32(5), int64(5), 5.0, dec128(t, "5"), dec128(t, "5.000"), dec128(t, "0.5E+1")},
		"zero":            {int32(0), int64(0), 0.0, math.Copysign(0, -1), dec128(t, "0"), dec128(t, "-0.00")},
		"tenth":           {dec128(t, "0.1"), dec128(t, "0.10")},
		"double one":      {1.0, int32(1), int64(1), dec128(t, "1"), dec128(t, "1.0")},
		"huge":            {dec128(t, "1E+6111"), dec128(t, "10E+6110")},
		"beyond 2^53":     {int64(9007199254740993), dec128(t, "9007199254740993")},
		"2^63":            {math.Ldexp(1, 63), dec128(t, "9223372036854775808")},
		"nan":             {math.NaN(), dec128(t, "NaN")},
		"infinity":        {math.Inf(1), dec128(t, "Infinity")},
		"negative number": {int32(-3), int64(-3), -3.0, dec128(t, "-3.0")},
	} {
		t.Run(name, func(t *testing.T) {
			want := idText(t, group[0])
			for _, id := range group[1:] {
				assert.Equal(t, want, idText(t, id))
			}
		})
	}
}

func TestEncodeIDKeepsDoubleAndDecimalFractionsApart(t *testing.T) {
	double := idText(t, 0.5)
	decimal := idText(t, dec128(t, "0.5"))

	assert.Equal(t, "f:0.5", double)
	assert.Equal(t, "n:0.5", decimal)
	assert.NotEqual(t, idText(t, 0.1), idText(t, dec128(t, "0.1")))
}

func TestEncodeIDLongNumbersStayShortAndRoundTrip(t *testing.T) {
	for _, d := range []bson.Decimal128{dec128(t, "1E+6111"), dec128(t, "1E-6176"), dec128(t, "-9.999999999999999999999999999999999E+6144")} {
		text := idText(t, d)

		decoded, err := decodeID(text)

		assert.Less(t, len(text), 60)
		require.NoError(t, err)
		assert.Equal(t, bson.TypeDecimal128, decoded.Type)
		again, err := encodeID(decoded.Type, decoded.Value)
		require.NoError(t, err)
		assert.Equal(t, text, again)
	}
}

func TestEncodeIDKeepsExactIntegersApart(t *testing.T) {
	a := idText(t, int64(9007199254740993))
	b := idText(t, math.Ldexp(1, 53))
	c := idText(t, int64(9007199254740992))

	assert.NotEqual(t, a, b)
	assert.Equal(t, b, c)
}

func TestEncodeIDNeverCollidesAcrossTypes(t *testing.T) {
	oid := bson.NewObjectID()
	ids := []any{
		"1", int32(1), true, "true", "", nil, bson.DateTime(1), "n:1", "z:", "o:" + oid.Hex(), oid,
		bson.Binary{Subtype: 4, Data: seqBytes(16)}, bson.Binary{Subtype: 3, Data: seqBytes(16)}, bson.Binary{Subtype: 0, Data: seqBytes(16)},
		bson.D{{Key: "a", Value: int32(1)}}, bson.A{int32(1)}, bson.D{}, bson.A{}, "j:{}", "a:[]", "false", false,
		"u:00010203-0405-0607-0809-0a0b0c0d0e0f", 1.5, "1.5", 0.5, dec128(t, "0.5"), "f:0.5", "n:0.5",
	}
	seen := map[string]int{}
	for i, id := range ids {
		text := idText(t, id)
		if j, dup := seen[text]; dup {
			t.Fatalf("ids %d and %d share the text form of type %T", j, i, id)
		}
		seen[text] = i
	}
}

func TestEncodeIDLegacyUUIDDiffersFromStandardUUID(t *testing.T) {
	std := idText(t, bson.Binary{Subtype: 4, Data: seqBytes(16)})
	legacy := idText(t, bson.Binary{Subtype: 3, Data: seqBytes(16)})

	assert.NotEqual(t, std[2:], legacy[2:])
	assert.Equal(t, "u:", std[:2])
	assert.Equal(t, "l:", legacy[:2])
}

func TestEncodeIDFromElementAndUpdatePartAgree(t *testing.T) {
	for _, v := range []any{"x", int32(3), 3.0, dec128(t, "3"), bson.Binary{Subtype: 3, Data: seqBytes(16)}, bson.D{{Key: "k", Value: "v"}}, bson.NewObjectID()} {
		doc, err := bson.Marshal(bson.D{{Key: "_id", Value: v}})
		require.NoError(t, err)
		element := bson.Raw(doc).Lookup("_id")
		fromElement, err := encodeID(element.Type, element.Value)
		require.NoError(t, err)

		typ, data, err := bson.MarshalValue(v)
		require.NoError(t, err)
		part := &adiomv1.BsonValue{Type: uint32(typ), Data: data}
		fromPart, err := encodeID(bson.Type(part.GetType()), part.GetData())
		require.NoError(t, err)

		assert.Equal(t, fromElement, fromPart)
	}
}

func TestDecodeIDRoundTrip(t *testing.T) {
	oid := bson.NewObjectID()
	for name, tc := range map[string]struct {
		id       any
		wantType bson.Type
	}{
		"string":            {"abc", bson.TypeString},
		"string with colon": {"a:b:c", bson.TypeString},
		"objectid":          {oid, bson.TypeObjectID},
		"bool":              {true, bson.TypeBoolean},
		"date":              {bson.DateTime(99), bson.TypeDateTime},
		"null":              {nil, bson.TypeNull},
		"int32":             {int32(5), bson.TypeInt64},
		"int64":             {int64(math.MinInt64), bson.TypeInt64},
		"double integral":   {3.0, bson.TypeInt64},
		"double fraction":   {0.1, bson.TypeDouble},
		"double 1e300":      {1e300, bson.TypeDouble},
		"decimal half":      {dec128(t, "0.5"), bson.TypeDecimal128},
		"decimal 1E+6111":   {dec128(t, "1E+6111"), bson.TypeDecimal128},
		"decimal 1E-6176":   {dec128(t, "1E-6176"), bson.TypeDecimal128},
		"decimal 1E+40":     {dec128(t, "1E+40"), bson.TypeDecimal128},
		"double 2^70":       {math.Ldexp(1, 70), bson.TypeDouble},
		"double tiny":       {5e-324, bson.TypeDouble},
		"double nan":        {math.NaN(), bson.TypeDouble},
		"double -inf":       {math.Inf(-1), bson.TypeDouble},
		"double 1.5":        {1.5, bson.TypeDouble},
		"decimal fraction":  {dec128(t, "1.5"), bson.TypeDecimal128},
		"decimal 23 digits": {dec128(t, "12345678901234567890123"), bson.TypeDecimal128},
		"decimal precise":   {dec128(t, "0.1000000000000000055511151231257827"), bson.TypeDecimal128},
		"uuid":              {bson.Binary{Subtype: 4, Data: seqBytes(16)}, bson.TypeBinary},
		"legacy uuid":       {bson.Binary{Subtype: 3, Data: seqBytes(16)}, bson.TypeBinary},
		"generic binary":    {bson.Binary{Subtype: 0, Data: []byte{1, 2}}, bson.TypeBinary},
		"document":          {bson.D{{Key: "z", Value: int64(1)}, {Key: "a", Value: bson.A{"q"}}}, bson.TypeEmbeddedDocument},
		"array":             {bson.A{int32(1), bson.D{{Key: "k", Value: nil}}}, bson.TypeArray},
	} {
		t.Run(name, func(t *testing.T) {
			text := idText(t, tc.id)

			decoded, err := decodeID(text)

			require.NoError(t, err)
			assert.Equal(t, tc.wantType, decoded.Type)
			again, err := encodeID(decoded.Type, decoded.Value)
			require.NoError(t, err)
			assert.Equal(t, text, again)
		})
	}
}

func TestDecodeIDRestoresBinarySubtypeAndBytes(t *testing.T) {
	for _, subtype := range []byte{3, 4, 0, 0x80} {
		data := seqBytes(16)
		decoded, err := decodeID(idText(t, bson.Binary{Subtype: subtype, Data: data}))

		require.NoError(t, err)
		gotSubtype, gotData := decoded.Binary()
		assert.Equal(t, subtype, gotSubtype)
		assert.Equal(t, data, gotData)
	}
}

func TestEncodeIDRejectsUnsupportedTypesNamingOnlyTheType(t *testing.T) {
	for name, v := range map[string]any{
		"minkey":     bson.MinKey{},
		"maxkey":     bson.MaxKey{},
		"regex":      bson.Regex{Pattern: "secret-pattern", Options: "i"},
		"timestamp":  bson.Timestamp{T: 1, I: 2},
		"javascript": bson.JavaScript("secret-code"),
		"symbol":     bson.Symbol("secret-symbol"),
		"undefined":  bson.Undefined{},
		"dbpointer":  bson.DBPointer{DB: "secret-db", Pointer: bson.NewObjectID()},
	} {
		t.Run(name, func(t *testing.T) {
			typ, data, err := bson.MarshalValue(v)
			require.NoError(t, err)

			_, err = encodeID(typ, data)

			require.ErrorContains(t, err, typ.String())
			assert.NotContains(t, err.Error(), "secret")
		})
	}
}

func TestEncodeIDErrorsDoNotContainValues(t *testing.T) {
	for name, tc := range map[string]struct {
		typ  bson.Type
		data []byte
	}{
		"truncated string":   {bson.TypeString, []byte{9, 0, 0, 0, 's', 'e', 'c'}},
		"truncated int32":    {bson.TypeInt32, []byte{1, 2}},
		"truncated objectid": {bson.TypeObjectID, []byte("secret")},
		"string with NUL":    {bson.TypeString, append([]byte{7, 0, 0, 0}, []byte("sec\x00ret\x00")...)},
		"string bad utf8":    {bson.TypeString, append([]byte{7, 0, 0, 0}, []byte("sec\xffret\x00")...)},
		"truncated document": {bson.TypeEmbeddedDocument, []byte("secret")},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := encodeID(tc.typ, tc.data)

			require.Error(t, err)
			assert.NotContains(t, err.Error(), "sec")
		})
	}
}

func TestDecodeIDRejectsMalformedTextWithoutEchoingIt(t *testing.T) {
	for _, text := range []string{
		"", "x", "q:secret", "n:secret", "n:-0", "n:01", "n:1.50", "n:+1", "n:1e5", "n:.5",
		"o:secret", "o:507F1F77BCF86CD799439011", "o:507f1f77bcf86cd7994390", "b:secret", "d:secret", "z:secret",
		"u:secret", "u:00010203-0405-0607-0809-0a0b0c0d0e0", "l:secret", "y:secret", "y:0:00", "y:00:zz", "y:00:abc",
		"j:secret", "j:[]", "a:secret", "a:{}", "n:1" + strings.Repeat("0", 7000),
		`a:[1],"b":[2]`, `j:{"a":1}x`, `j:{ "a" : 1 }`, "s:a\x00b", "s:\xff", "y:04:" + strings.Repeat("00", 16),
		"f:1", "f:0.50", "f:NaN", "f:secret", "f:1e5", "n:1e-3", "n:10e1", "n:1e2000000", "n:0.5e1",
		`j:{"a":{"$numberInt":"1"}}x`, `a:[{"$numberInt":"1"}]x`, `j:{"a":{"$numberInt":"01"}}`,
	} {
		t.Run(text, func(t *testing.T) {
			_, err := decodeID(text)

			require.Error(t, err)
			assert.NotContains(t, err.Error(), "secret")
		})
	}
}

func TestEncodeIDRejectsDollarKeysInStructuredIDs(t *testing.T) {
	plain := bson.D{{Key: "a", Value: int32(1)}}
	wrapper := bson.D{{Key: "a", Value: bson.D{{Key: "$numberInt", Value: "1"}}}}
	_, plainErr := encodeIDValueForTest(plain)
	_, wrapperErr := encodeIDValueForTest(wrapper)

	require.NoError(t, plainErr)
	require.Error(t, wrapperErr)
	assert.Contains(t, wrapperErr.Error(), "a.$numberInt")

	for name, tc := range map[string]struct {
		id   any
		path string
	}{
		"top level":  {bson.D{{Key: "$ref", Value: "secret"}}, "$ref"},
		"nested":     {bson.D{{Key: "a", Value: bson.D{{Key: "b", Value: bson.D{{Key: "$x", Value: "secret"}}}}}}, "a.b.$x"},
		"in array":   {bson.A{"ok", bson.D{{Key: "$oid", Value: "secret"}}}, "[1].$oid"},
		"array deep": {bson.D{{Key: "a", Value: bson.A{bson.A{bson.D{{Key: "$n", Value: 1}}}}}}, "a[0][0].$n"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := encodeIDValueForTest(tc.id)

			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.path)
			assert.NotContains(t, err.Error(), "secret")
		})
	}
}

func encodeIDValueForTest(v any) (string, error) {
	typ, data, err := bson.MarshalValue(v)
	if err != nil {
		return "", err
	}
	return encodeID(typ, data)
}

func TestEncodeIDRejectsTrailingBytes(t *testing.T) {
	oid := bson.NewObjectID()
	for name, tc := range map[string]struct {
		typ  bson.Type
		data []byte
	}{
		"int32":    {bson.TypeInt32, []byte{1, 0, 0, 0, 9, 9}},
		"null":     {bson.TypeNull, []byte{9}},
		"bool":     {bson.TypeBoolean, []byte{1, 9}},
		"objectid": {bson.TypeObjectID, append(oid[:], 9)},
		"string":   {bson.TypeString, []byte{2, 0, 0, 0, 'a', 0, 9}},
		"document": {bson.TypeEmbeddedDocument, []byte{5, 0, 0, 0, 0, 9}},
		"date":     {bson.TypeDateTime, append(seqBytes(8), 9)},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := encodeID(tc.typ, tc.data)

			require.Error(t, err)
		})
	}
}

func TestEncodeIDRejectsRightLengthMalformedScalars(t *testing.T) {
	for name, tc := range map[string]struct {
		typ  bson.Type
		data []byte
	}{
		"boolean 2":                {bson.TypeBoolean, []byte{2}},
		"boolean 255":              {bson.TypeBoolean, []byte{255}},
		"string without NUL":       {bson.TypeString, []byte{2, 0, 0, 0, 'a', 'x'}},
		"string zero length":       {bson.TypeString, []byte{0, 0, 0, 0}},
		"string length too long":   {bson.TypeString, []byte{9, 0, 0, 0, 'a', 0}},
		"string length too short":  {bson.TypeString, []byte{1, 0, 0, 0, 'a', 0}},
		"string negative length":   {bson.TypeString, []byte{255, 255, 255, 255, 'a', 0}},
		"binary length too long":   {bson.TypeBinary, []byte{9, 0, 0, 0, 0, 1}},
		"binary length too short":  {bson.TypeBinary, []byte{0, 0, 0, 0, 0, 1}},
		"binary old subtype split": {bson.TypeBinary, []byte{5, 0, 0, 0, 2, 3, 0, 0, 0}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := encodeID(tc.typ, tc.data)

			require.Error(t, err)
		})
	}
}

func TestDecodeIDNumberExponentCap(t *testing.T) {
	_, err := decodeID("n:1e7001")
	decoded, okErr := decodeID("n:1e6111")

	require.Error(t, err)
	require.NoError(t, okErr)
	assert.Equal(t, bson.TypeDecimal128, decoded.Type)
}

func TestDecodeIDHugeExponentAllocatesNothing(t *testing.T) {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	_, err := decodeID("n:1e2000000")

	runtime.ReadMemStats(&after)
	require.Error(t, err)
	assert.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20))
}
