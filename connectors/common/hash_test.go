package common_test

import (
	"hash"
	"slices"
	"testing"

	"github.com/adiom-data/dsync/connectors/common"
	"github.com/cespare/xxhash"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func compareHash(t *testing.T, hasher hash.Hash64, left []byte, right []byte, projection map[string]interface{}) bool {
	hasher.Reset()
	assert.NoError(t, common.HashBson(hasher, left, false, projection))
	l := hasher.Sum64()
	assert.NotZero(t, l)
	hasher.Reset()
	assert.NoError(t, common.HashBson(hasher, right, false, projection))
	r := hasher.Sum64()
	assert.NotZero(t, r)
	return l == r
}

func Test_HashBson(t *testing.T) {
	// These are all the same, but the documents are reordered in various ways
	b1, _ := bson.Marshal(bson.D{{"a", bson.A{"a", bson.D{{"b", "2"}, {"c", "3"}}}}, {"b", bson.D{{"a", "1"}, {"b", "2"}}}})
	b2, _ := bson.Marshal(bson.D{{"b", bson.D{{"a", "1"}, {"b", "2"}}}, {"a", bson.A{"a", bson.D{{"b", "2"}, {"c", "3"}}}}})
	b3, _ := bson.Marshal(bson.D{{"a", bson.A{"a", bson.D{{"c", "3"}, {"b", "2"}}}}, {"b", bson.D{{"b", "2"}, {"a", "1"}}}})
	b4, _ := bson.Marshal(bson.D{{"b", bson.D{{"b", "2"}, {"a", "1"}}}, {"a", bson.A{"a", bson.D{{"b", "2"}, {"c", "3"}}}}})
	same1 := [][]byte{b1, b2, b3, b4}

	// The bson.A is reordered here
	b5, _ := bson.Marshal(bson.D{{"a", bson.A{bson.D{{"b", "2"}, {"c", "3"}}, "a"}}, {"b", bson.D{{"a", "1"}, {"b", "2"}}}})
	b6, _ := bson.Marshal(bson.D{{"b", bson.D{{"a", "1"}, {"b", "2"}}}, {"a", bson.A{bson.D{{"c", "3"}, {"b", "2"}}, "a"}}})
	same2 := [][]byte{b5, b6}

	hasher := xxhash.New()

	// Test hasher is invariant to reordered documents, but not to reordered arrays
	for _, i := range same1 {
		for _, j := range same1 {
			assert.True(t, compareHash(t, hasher, i, j, nil))
		}
		for _, j := range same2 {
			assert.False(t, compareHash(t, hasher, i, j, nil))
		}
	}

	for _, i := range same2 {
		for _, j := range same2 {
			assert.True(t, compareHash(t, hasher, i, j, nil))
		}
		for _, j := range same1 {
			assert.False(t, compareHash(t, hasher, i, j, nil))
		}
	}

	// Sanity check naive hashing
	all := append(same1, same2...)
	for _, i := range all {
		for _, j := range all {
			hasher.Reset()
			hasher.Write(i)
			l := hasher.Sum64()
			hasher.Reset()
			hasher.Write(j)
			r := hasher.Sum64()
			if slices.Equal(i, j) {
				assert.Equal(t, l, r)
			} else {
				assert.NotEqual(t, l, r)
			}
		}
	}

	// Check a projection
	left, _ := bson.Marshal(bson.D{{"a", bson.A{"a", bson.D{{"b", "2"}, {"c", "4"}}}}, {"b", bson.D{{"a", "1"}, {"b", "2"}}}})
	right, _ := bson.Marshal(bson.D{{"a", bson.A{"a", bson.D{{"b", "2"}, {"c", "3"}}}}, {"b", bson.D{{"a", "1"}, {"b", "2"}}}})
	assert.False(t, compareHash(t, hasher, left, right, nil))
	assert.True(t, compareHash(t, hasher, left, right, map[string]interface{}{
		"a": map[string]interface{}{"b": struct{}{}},
		"b": struct{}{},
	}))
	assert.False(t, compareHash(t, hasher, left, right, map[string]interface{}{
		"a": map[string]interface{}{"c": struct{}{}},
	}))

	// Sanity check an error case
	assert.Error(t, common.HashBson(hasher, []byte{1, 2}, false, nil))
}

func Test_HashBsonCodeWithScope(t *testing.T) {
	doc := func(code string, scope bson.D) []byte {
		b, err := bson.Marshal(bson.D{{"k", bson.CodeWithScope{Code: bson.JavaScript(code), Scope: scope}}})
		assert.NoError(t, err)
		return b
	}
	nested := func(inner bson.D) bson.D { return bson.D{{"bb", int32(1)}, {"a", inner}} }
	hasher := xxhash.New()

	assert.True(t, compareHash(t, hasher, doc("f()", bson.D{{"bb", int32(1)}, {"a", int32(2)}}), doc("f()", bson.D{{"a", int32(2)}, {"bb", int32(1)}}), nil))
	assert.True(t, compareHash(t, hasher, doc("f()", nested(bson.D{{"y", int32(1)}, {"x", int32(2)}})), doc("f()", bson.D{{"a", bson.D{{"x", int32(2)}, {"y", int32(1)}}}, {"bb", int32(1)}}), nil))
	assert.False(t, compareHash(t, hasher, doc("//", bson.D{{"a", bson.D{}}}), doc("//a\x03a", bson.D{}), nil))
	assert.False(t, compareHash(t, hasher, doc("f()", bson.D{{"a", int32(2)}}), doc("g()", bson.D{{"a", int32(2)}}), nil))
	assert.False(t, compareHash(t, hasher, doc("f()", bson.D{{"a", int32(2)}}), doc("f()", bson.D{{"a", int32(3)}}), nil))
}

func Test_HashBsonCodeWithScopeMalformedReturnsError(t *testing.T) {
	b := codeWithScopeDoc(t)
	b[11] = 100

	assert.NotPanics(t, func() {
		assert.Error(t, common.HashBson(xxhash.New(), b, false, nil))
	})
}

func codeWithScopeDoc(t *testing.T) []byte {
	b, err := bson.Marshal(bson.D{{"k", bson.CodeWithScope{Code: "f()", Scope: bson.D{{"a", int32(1)}}}}})
	assert.NoError(t, err)
	return b
}
