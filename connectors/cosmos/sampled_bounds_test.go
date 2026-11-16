package cosmos

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func rawOf(t *testing.T, v any) bson.RawValue {
	t.Helper()
	typ, data, err := bson.MarshalValue(v)
	require.NoError(t, err)
	return bson.RawValue{Type: typ, Value: data}
}

func stringIDs(t *testing.T, from, to int) []bson.RawValue {
	var ids []bson.RawValue
	for i := from; i < to; i++ {
		ids = append(ids, rawOf(t, fmt.Sprintf("k%04d", i)))
	}
	return ids
}

type fakeSampler struct {
	ids   []bson.RawValue
	err   error
	sizes []int64
}

func (f *fakeSampler) sample(_ context.Context, size int64) ([]bson.RawValue, error) {
	f.sizes = append(f.sizes, size)
	return f.ids, f.err
}

func requireStrictlyIncreasing(t *testing.T, bounds []bson.RawValue) {
	t.Helper()
	for i := 1; i < len(bounds); i++ {
		require.Negative(t, compareIDs(bounds[i-1], bounds[i]), "bound %d must be above bound %d", i, i-1)
	}
}

func TestSampledBoundsAreStrictlyIncreasingFromMinToMax(t *testing.T) {
	min, max := rawOf(t, "k0000"), rawOf(t, "k0999")
	samples := stringIDs(t, 0, 1000)
	for i, j := 0, len(samples)-1; i < j; i, j = i+1, j-1 {
		samples[i], samples[j] = samples[j], samples[i]
	}
	sampler := &fakeSampler{ids: samples}

	bounds, err := partitionBounds(t.Context(), min, max, 5, sampler.sample)

	require.NoError(t, err)
	require.Equal(t, []int64{50}, sampler.sizes)
	require.Equal(t, min, bounds[0])
	require.Equal(t, max, bounds[len(bounds)-1])
	require.Len(t, bounds, 6)
	requireStrictlyIncreasing(t, bounds)
}

func TestSampledBoundsDropOutOfRangeAndDuplicateSamples(t *testing.T) {
	min, max := rawOf(t, "k0100"), rawOf(t, "k0200")
	var samples []bson.RawValue
	samples = append(samples, stringIDs(t, 0, 101)...)
	samples = append(samples, stringIDs(t, 200, 300)...)
	for range 5 {
		samples = append(samples, stringIDs(t, 120, 130)...)
	}
	sampler := &fakeSampler{ids: samples}

	bounds, err := partitionBounds(t.Context(), min, max, 3, sampler.sample)

	require.NoError(t, err)
	require.Equal(t, min, bounds[0])
	require.Equal(t, max, bounds[len(bounds)-1])
	requireStrictlyIncreasing(t, bounds)
	require.Len(t, bounds, 3)
	require.Equal(t, rawOf(t, "k0129"), bounds[1])
}

func TestSampledBoundsAtMostNumPartsMinusOneInnerBounds(t *testing.T) {
	min, max := rawOf(t, int64(0)), rawOf(t, int64(10000))
	var samples []bson.RawValue
	for i := int64(1); i < 10000; i++ {
		samples = append(samples, rawOf(t, i))
	}
	sampler := &fakeSampler{ids: samples}

	bounds, err := partitionBounds(t.Context(), min, max, 4, sampler.sample)

	require.NoError(t, err)
	require.Len(t, bounds, 5)
	requireStrictlyIncreasing(t, bounds)
}

func TestSampledBoundsSortIntegersNumerically(t *testing.T) {
	min, max := rawOf(t, int32(0)), rawOf(t, int32(1000))
	var samples []bson.RawValue
	for i := int32(1); i < 1000; i++ {
		samples = append(samples, rawOf(t, i))
	}
	sampler := &fakeSampler{ids: samples}

	bounds, err := partitionBounds(t.Context(), min, max, 3, sampler.sample)

	require.NoError(t, err)
	require.Equal(t, rawOf(t, int32(10)), bounds[1])
}

func TestSampledBoundsSortBinaryByBytes(t *testing.T) {
	bin := func(b byte) bson.RawValue {
		return rawOf(t, bson.Binary{Subtype: bson.TypeBinaryUUID, Data: []byte{b, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}})
	}
	min, max := bin(0), bin(255)
	var samples []bson.RawValue
	for i := 254; i > 0; i-- {
		samples = append(samples, bin(byte(i)))
	}
	sampler := &fakeSampler{ids: samples}

	bounds, err := partitionBounds(t.Context(), min, max, 3, sampler.sample)

	require.NoError(t, err)
	require.Len(t, bounds, 4)
	requireStrictlyIncreasing(t, bounds)
}

func TestSampledBoundsFallBackWhenASampleIsOfAnotherKind(t *testing.T) {
	uuid := func(n int) bson.RawValue {
		return rawOf(t, bson.Binary{Subtype: bson.TypeBinaryUUID, Data: []byte{byte(n), 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}})
	}
	for name, tc := range map[string]struct {
		min, max bson.RawValue
		samples  []bson.RawValue
	}{
		"other type": {rawOf(t, "k0000"), rawOf(t, "k0999"), append(stringIDs(t, 1, 500), rawOf(t, int32(5)))},
		"int32 among int64": {rawOf(t, int64(0)), rawOf(t, int64(999)), []bson.RawValue{
			rawOf(t, int64(5)), rawOf(t, int32(7)),
		}},
		"int64 among int32": {rawOf(t, int32(0)), rawOf(t, int32(999)), []bson.RawValue{
			rawOf(t, int32(5)), rawOf(t, int64(7)),
		}},
		"other binary subtype": {uuid(0), uuid(255), []bson.RawValue{
			uuid(5), rawOf(t, bson.Binary{Subtype: bson.TypeBinaryGeneric, Data: []byte{9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}}),
		}},
		"other binary length": {uuid(0), uuid(255), []bson.RawValue{
			uuid(5), rawOf(t, bson.Binary{Subtype: bson.TypeBinaryUUID, Data: []byte{9, 1}}),
		}},
		"binary of other length at the bounds": {uuid(0), rawOf(t, bson.Binary{Subtype: bson.TypeBinaryUUID, Data: []byte{9, 1}}), []bson.RawValue{uuid(5)}},
	} {
		t.Run(name, func(t *testing.T) {
			sampler := &fakeSampler{ids: tc.samples}

			_, err := partitionBounds(t.Context(), tc.min, tc.max, 3, sampler.sample)

			require.Error(t, err)
		})
	}
}

func TestSampledBoundsFallBackWithoutUsableSamples(t *testing.T) {
	min, max := rawOf(t, "k0100"), rawOf(t, "k0200")
	for name, sampler := range map[string]*fakeSampler{
		"no samples":       {},
		"only min and max": {ids: []bson.RawValue{min, max, min}},
		"all outside":      {ids: stringIDs(t, 300, 400)},
		"aggregate error":  {err: errors.New("boom")},
		"fewer than ten":   {ids: stringIDs(t, 150, 159)},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := partitionBounds(t.Context(), min, max, 3, sampler.sample)

			require.Error(t, err)
		})
	}
}

func TestPartitionBoundsFallBackForUnsupportedAndMixedTypes(t *testing.T) {
	for name, tc := range map[string]struct{ min, max bson.RawValue }{
		"mixed int and string": {rawOf(t, int32(1)), rawOf(t, "z")},
		"objectid and string":  {objectIDValue(100, 0), rawOf(t, "z")},
		"int32 and int64":      {rawOf(t, int32(1)), rawOf(t, int64(9))},
		"double":               {rawOf(t, 1.5), rawOf(t, 9.5)},
		"decimal128":           {rawOf(t, bson.NewDecimal128(0, 1)), rawOf(t, bson.NewDecimal128(0, 9))},
		"document":             {rawOf(t, bson.D{{Key: "a", Value: 1}}), rawOf(t, bson.D{{Key: "a", Value: 9}})},
	} {
		t.Run(name, func(t *testing.T) {
			sampler := &fakeSampler{ids: stringIDs(t, 0, 100)}

			_, err := partitionBounds(t.Context(), tc.min, tc.max, 3, sampler.sample)

			require.Error(t, err)
			require.Empty(t, sampler.sizes)
		})
	}
}

func TestPartitionBoundsKeepsTheObjectIdPathWithoutSampling(t *testing.T) {
	low, high := objectIDValue(100, 0), objectIDValue(1000, 0)
	sampler := &fakeSampler{ids: stringIDs(t, 0, 100)}

	bounds, err := partitionBounds(t.Context(), low, high, 4, sampler.sample)

	require.NoError(t, err)
	want, err := splitRangeObjectId(low, high, 4)
	require.NoError(t, err)
	require.Len(t, bounds, len(want))
	for i := range want {
		require.Equal(t, want[i].ObjectID().Timestamp(), bounds[i].ObjectID().Timestamp())
	}
	require.Empty(t, sampler.sizes)
}

func TestSampledBoundsDropASampleEqualToMax(t *testing.T) {
	min, max := rawOf(t, "k0000"), rawOf(t, "k0030")
	samples := append(stringIDs(t, 1, 10), max, max, max)
	sampler := &fakeSampler{ids: samples}

	_, err := partitionBounds(t.Context(), min, max, 2, sampler.sample)

	require.Error(t, err)
}

func TestSampledBoundsFallBackWhenMinAndMaxBinaryDiffer(t *testing.T) {
	bin := func(subtype byte, n int) bson.RawValue {
		return rawOf(t, bson.Binary{Subtype: subtype, Data: make([]byte, n)})
	}
	for name, tc := range map[string]struct{ min, max bson.RawValue }{
		"length":  {bin(bson.TypeBinaryUUID, 16), bin(bson.TypeBinaryUUID, 17)},
		"subtype": {bin(bson.TypeBinaryUUID, 16), bin(bson.TypeBinaryGeneric, 16)},
	} {
		t.Run(name, func(t *testing.T) {
			sampler := &fakeSampler{}

			_, err := partitionBounds(t.Context(), tc.min, tc.max, 3, sampler.sample)

			require.Error(t, err)
			require.Empty(t, sampler.sizes)
		})
	}
}

func TestSampledBoundsErrorsDoNotContainIdValues(t *testing.T) {
	secret := rawOf(t, "secret-id-value")
	sampler := &fakeSampler{ids: []bson.RawValue{rawOf(t, int32(1))}}

	_, mixed := partitionBounds(t.Context(), secret, rawOf(t, int32(1)), 3, sampler.sample)
	_, sample := partitionBounds(t.Context(), secret, rawOf(t, "secret-id-zzz"), 3, sampler.sample)

	require.Error(t, mixed)
	require.Error(t, sample)
	require.NotContains(t, mixed.Error(), "secret")
	require.NotContains(t, sample.Error(), "secret")
	require.ErrorContains(t, sample, "32-bit integer")
}

func TestSampledBoundsOrderAgainstExplicitExpectations(t *testing.T) {
	bin := func(b byte) bson.RawValue {
		return rawOf(t, bson.Binary{Subtype: bson.TypeBinaryUUID, Data: []byte{b, 0}})
	}
	fill := func(values []bson.RawValue, count int) []bson.RawValue {
		var out []bson.RawValue
		for i := range count {
			out = append(out, values[i%len(values)])
		}
		return out
	}
	i64 := func(vs ...int64) []bson.RawValue {
		var out []bson.RawValue
		for _, v := range vs {
			out = append(out, rawOf(t, v))
		}
		return out
	}
	i32 := func(vs ...int32) []bson.RawValue {
		var out []bson.RawValue
		for _, v := range vs {
			out = append(out, rawOf(t, v))
		}
		return out
	}
	str := func(vs ...string) []bson.RawValue {
		var out []bson.RawValue
		for _, v := range vs {
			out = append(out, rawOf(t, v))
		}
		return out
	}
	var binSamples []bson.RawValue
	for b := 1; b < 255; b++ {
		binSamples = append(binSamples, bin(byte(b)))
	}
	for name, tc := range map[string]struct {
		min, max bson.RawValue
		samples  []bson.RawValue
		parts    int
		want     []bson.RawValue
	}{
		"binary high bit": {bin(0), bin(255), binSamples, 2, []bson.RawValue{bin(0), bin(10), bin(255)}},
		"int64 extremes": {rawOf(t, int64(math.MinInt64)), rawOf(t, int64(math.MaxInt64)),
			fill(i64(math.MaxInt64-1, 0, -1, math.MinInt64+1, 5, -7, 9, -9, 3, 1, 2), 11), 2,
			[]bson.RawValue{rawOf(t, int64(math.MinInt64)), rawOf(t, int64(9)), rawOf(t, int64(math.MaxInt64))}},
		"int32 negatives": {rawOf(t, int32(math.MinInt32)), rawOf(t, int32(math.MaxInt32)),
			i32(5, -1, -2, -3, -4, -5, -6, -7, -8, -9, -10, 6), 2,
			[]bson.RawValue{rawOf(t, int32(math.MinInt32)), rawOf(t, int32(-1)), rawOf(t, int32(math.MaxInt32))}},
		"multi-byte strings": {rawOf(t, "a"), rawOf(t, "\U0001F600"),
			str("z", "\u00e9", "\u4e2d", "b", "\u00ff", "c", "d", "e", "f", "g", "h"), 2,
			[]bson.RawValue{rawOf(t, "a"), rawOf(t, "\u00ff"), rawOf(t, "\U0001F600")}},
	} {
		t.Run(name, func(t *testing.T) {
			sampler := &fakeSampler{ids: tc.samples}

			bounds, err := partitionBounds(t.Context(), tc.min, tc.max, tc.parts, sampler.sample)

			require.NoError(t, err)
			require.Equal(t, tc.want, bounds)
		})
	}
}

func TestSampledBoundsCapTheSampleSize(t *testing.T) {
	sampler := &fakeSampler{ids: stringIDs(t, 0, 10000)}

	bounds, err := partitionBounds(t.Context(), rawOf(t, "k"), rawOf(t, "l"), 5000, sampler.sample)

	require.NoError(t, err)
	require.Equal(t, []int64{10000}, sampler.sizes)
	require.Len(t, bounds, 5001)
	requireStrictlyIncreasing(t, bounds)
}

func TestCappedSampledBoundsSpreadEvenlyWhenNumPartsDoesNotDivideTheCap(t *testing.T) {
	sampler := &fakeSampler{ids: stringIDs(t, 0, maxIDSamples)}

	bounds, err := partitionBounds(t.Context(), rawOf(t, "k"), rawOf(t, "l"), 3000, sampler.sample)

	require.NoError(t, err)
	require.Len(t, bounds, 3001)
	previous := -1
	sizes := make([]int, 0, 3000)
	for _, bound := range bounds[1 : len(bounds)-1] {
		var index int
		_, err := fmt.Sscanf(bound.StringValue(), "k%d", &index)
		require.NoError(t, err)
		sizes = append(sizes, index-previous)
		previous = index
	}
	sizes = append(sizes, maxIDSamples-1-previous)
	require.LessOrEqual(t, slices.Max(sizes), slices.Min(sizes)+1)
}

func TestSampledBoundsWithoutPartsFailsWithoutSampling(t *testing.T) {
	sampler := &fakeSampler{ids: stringIDs(t, 0, 100)}

	_, err := partitionBounds(t.Context(), rawOf(t, "k"), rawOf(t, "l"), 0, sampler.sample)

	require.ErrorContains(t, err, "not enough usable id samples")
	require.Empty(t, sampler.sizes)
}
