package cosmos

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func objectIDValue(ts uint32, tail byte) bson.RawValue {
	var id bson.ObjectID
	id[0], id[1], id[2], id[3] = byte(ts>>24), byte(ts>>16), byte(ts>>8), byte(ts)
	for i := 4; i < 12; i++ {
		id[i] = tail
	}
	return bson.RawValue{Type: bson.TypeObjectID, Value: id[:]}
}

func TestSplitRangeObjectIdBoundariesStayAboveMinWhenPartsAreShorterThanASecond(t *testing.T) {
	low, high := objectIDValue(100, 0xff), objectIDValue(102, 0x55)

	bounds, err := splitRangeObjectId(low, high, 4)

	require.NoError(t, err)
	require.Equal(t, low, bounds[0])
	require.Equal(t, high, bounds[len(bounds)-1])
	for i := 1; i < len(bounds); i++ {
		require.Positive(t, bytes.Compare(bounds[i].Value, bounds[i-1].Value), "boundary %d must be strictly above the previous one", i)
	}
}

func TestSplitRangeObjectIdBoundariesStrictlyIncreasingWithinOneSecond(t *testing.T) {
	low, high := objectIDValue(100, 0xfe), objectIDValue(100, 0xff)

	bounds, err := splitRangeObjectId(low, high, 4)

	require.NoError(t, err)
	require.Equal(t, low, bounds[0])
	require.Equal(t, high, bounds[len(bounds)-1])
	for i := 1; i < len(bounds); i++ {
		require.Positive(t, bytes.Compare(bounds[i].Value, bounds[i-1].Value), "boundary %d must be strictly above the previous one", i)
	}
}

func TestSplitRangeObjectIdWithEqualBoundsStaysASingleRange(t *testing.T) {
	bound := objectIDValue(1000, 0)

	boundaries, err := splitRangeObjectId(bound, bound, 5)

	require.NoError(t, err)
	require.Equal(t, []bson.RawValue{bound, bound}, boundaries)
}
