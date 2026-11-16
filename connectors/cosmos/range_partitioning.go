/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package cosmos

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/adiom-data/dsync/protocol/iface"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// splits the range between two objectIds into equal parts based on timestamps
// returns an array of boundaries including the top and bottom boundaries
// value1 is the bottom boundary, value2 is the top boundary (i.e. value1 < value2)
func splitRangeObjectId(value1 bson.RawValue, value2 bson.RawValue, numParts int) ([]bson.RawValue, error) {
	minT := value1.ObjectID().Timestamp()
	maxT := value2.ObjectID().Timestamp()

	if minT.After(maxT) {
		return nil, fmt.Errorf("min value is greater than max value")
	}

	// calculate the time difference between the two boundaries
	diff := maxT.Sub(minT)
	partSize := diff / time.Duration(numParts)

	boundaries := []bson.RawValue{value1}
	for i := 1; i < numParts; i++ {
		t := minT.Add(time.Duration(i) * partSize)
		val := bson.NewObjectIDFromTimestamp(t)
		last := boundaries[len(boundaries)-1].Value
		if bytes.Compare(val[:], last) > 0 && bytes.Compare(val[:], value2.Value) < 0 {
			boundaries = append(boundaries, bson.RawValue{Type: bson.TypeObjectID, Value: val[:]})
		}
	}
	return append(boundaries, value2), nil
}

// get min and max boundaries for a namespace task
func getMinAndMax(ctx context.Context, client *mongo.Client, ns iface.Namespace, partitionKey string) (bson.RawValue, bson.RawValue, error) {
	collection := client.Database(ns.Db).Collection(ns.Col)
	optsMax := options.Find().SetProjection(bson.D{{partitionKey, 1}}).SetLimit(1).SetSort(bson.D{{partitionKey, -1}})
	optsMin := options.Find().SetProjection(bson.D{{partitionKey, 1}}).SetLimit(1).SetSort(bson.D{{partitionKey, 1}})

	//get the top and bottom boundaries
	topCursor, err := collection.Find(ctx, bson.M{}, optsMax)
	if err != nil {
		return bson.RawValue{}, bson.RawValue{}, err
	}
	defer topCursor.Close(ctx)

	bottomCursor, err := collection.Find(ctx, bson.M{}, optsMin)
	if err != nil {
		return bson.RawValue{}, bson.RawValue{}, err
	}
	defer bottomCursor.Close(ctx)

	if !topCursor.Next(ctx) {
		return bson.RawValue{}, bson.RawValue{}, fmt.Errorf("failed to get top boundary")
	}

	if !bottomCursor.Next(ctx) {
		return bson.RawValue{}, bson.RawValue{}, fmt.Errorf("failed to get bottom boundary")
	}

	topId := topCursor.Current.Lookup(partitionKey)
	bottomId := bottomCursor.Current.Lookup(partitionKey)

	return bottomId, topId, nil
}
