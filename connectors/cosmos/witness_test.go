package cosmos

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestKeyTupleYieldsNullUnderANonTraversableParent(t *testing.T) {
	for name, parent := range map[string]any{"null": nil, "scalar": int32(5), "string": "x"} {
		t.Run(name, func(t *testing.T) {
			raw, err := bson.Marshal(bson.D{{Key: "_id", Value: int32(1)}, {Key: "a", Value: parent}})
			require.NoError(t, err)
			tuple, err := keyTuple(raw, []string{"a.b"})
			require.NoError(t, err)
			require.Len(t, tuple, 2)
			require.Equal(t, bson.TypeNull, tuple[1].Type)
		})
	}
}

func TestKeyTupleStillRejectsACorruptDocument(t *testing.T) {
	corrupt := bson.Raw{0x0c, 0, 0, 0, 0x03, 'a', 0, 0xff, 0xff, 0xff, 0x7f, 0}
	_, err := keyTuple(corrupt, []string{"a.b"})
	require.Error(t, err)
}
