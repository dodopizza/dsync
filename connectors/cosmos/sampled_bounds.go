package cosmos

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"slices"

	mongoconn "github.com/adiom-data/dsync/connectors/mongo"
	"github.com/adiom-data/dsync/protocol/iface"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

const (
	samplesPerPartition = 10
	maxIDSamples        = 10000
)

type idSampler func(ctx context.Context, size int64) ([]bson.RawValue, error)

func partitionBounds(ctx context.Context, min, max bson.RawValue, numParts int, sample idSampler) ([]bson.RawValue, error) {
	if min.Type == bson.TypeObjectID {
		if max.Type != bson.TypeObjectID {
			return nil, fmt.Errorf("cannot partition range between ids of kind %s and %s", idKind(min), idKind(max))
		}
		return splitRangeObjectId(min, max, numParts)
	}
	return sampledBounds(ctx, min, max, numParts, sample)
}

func sampledBounds(ctx context.Context, min, max bson.RawValue, numParts int, sample idSampler) ([]bson.RawValue, error) {
	if !sameIDKind(min, max) {
		return nil, fmt.Errorf("cannot sample a range between ids of kind %s and %s", idKind(min), idKind(max))
	}
	size := numParts * samplesPerPartition
	if size > maxIDSamples {
		size = maxIDSamples
	}
	samples, err := sample(ctx, int64(size))
	if err != nil {
		return nil, fmt.Errorf("failed to sample ids: %w", err)
	}
	var inner []bson.RawValue
	for _, s := range samples {
		if !sameIDKind(min, s) {
			return nil, fmt.Errorf("sampled id of kind %s differs from the range bounds of kind %s", idKind(s), idKind(min))
		}
		if compareIDs(s, min) > 0 && compareIDs(s, max) < 0 {
			inner = append(inner, s)
		}
	}
	slices.SortFunc(inner, compareIDs)
	inner = slices.CompactFunc(inner, func(a, b bson.RawValue) bool { return compareIDs(a, b) == 0 })

	step := size / numParts
	if step < 1 {
		step = 1
	}
	bounds := []bson.RawValue{min}
	for i := step - 1; i < len(inner) && len(bounds) < numParts; i += step {
		bounds = append(bounds, inner[i])
	}
	if len(bounds) == 1 {
		return nil, fmt.Errorf("not enough usable id samples")
	}
	return append(bounds, max), nil
}

func idKind(v bson.RawValue) string {
	if v.Type != bson.TypeBinary {
		return v.Type.String()
	}
	subtype, data := v.Binary()
	return fmt.Sprintf("binary(subtype %d, length %d)", subtype, len(data))
}

func sameIDKind(a, b bson.RawValue) bool {
	if a.Type != b.Type {
		return false
	}
	switch a.Type {
	case bson.TypeString, bson.TypeInt32, bson.TypeInt64:
		return true
	case bson.TypeBinary:
		subtypeA, dataA := a.Binary()
		subtypeB, dataB := b.Binary()
		return subtypeA == subtypeB && len(dataA) == len(dataB)
	}
	return false
}

func compareIDs(a, b bson.RawValue) int {
	switch a.Type {
	case bson.TypeInt32:
		return cmp.Compare(a.Int32(), b.Int32())
	case bson.TypeInt64:
		return cmp.Compare(a.Int64(), b.Int64())
	case bson.TypeBinary:
		_, dataA := a.Binary()
		_, dataB := b.Binary()
		return bytes.Compare(dataA, dataB)
	}
	return cmp.Compare(a.StringValue(), b.StringValue())
}

func (cc *planner) idSampler(ns iface.Namespace) idSampler {
	return func(ctx context.Context, size int64) ([]bson.RawValue, error) {
		return sampleIDs(ctx, cc.Client.Database(ns.Db).Collection(ns.Col), size)
	}
}

func sampleIDs(ctx context.Context, collection *mongo.Collection, size int64) ([]bson.RawValue, error) {
	cursor, err := collection.Aggregate(ctx, mongoconn.SamplePipeline(size, nil))
	if err != nil {
		return nil, err
	}
	var docs []bson.Raw
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, err
	}
	ids := make([]bson.RawValue, 0, len(docs))
	for _, doc := range docs {
		ids = append(ids, doc.Lookup("_id"))
	}
	return ids, nil
}
