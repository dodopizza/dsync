/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package cosmos

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/adiom-data/dsync/protocol/iface"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

type Witness interface {
	ScanKeys(ctx context.Context, ns iface.Namespace, keyFields []string, pageSize int, emit func([][]bson.RawValue) error) error
}

type mongoWitness struct {
	client *mongo.Client
}

func keyProjection(keyFields []string) bson.D {
	projection := bson.D{{Key: "_id", Value: 1}}
	for _, field := range keyFields {
		projection = append(projection, bson.E{Key: field, Value: 1})
	}
	return projection
}

func keyTuple(doc bson.Raw, keyFields []string) ([]bson.RawValue, error) {
	tuple := make([]bson.RawValue, 0, 1+len(keyFields))
	id, err := doc.LookupErr("_id")
	if err != nil {
		return nil, fmt.Errorf("document without _id: %w", err)
	}
	tuple = append(tuple, bson.RawValue{Type: id.Type, Value: slices.Clone(id.Value)})
	for _, field := range keyFields {
		value, err := doc.LookupErr(strings.Split(field, ".")...)
		var untraversable bsoncore.InvalidDepthTraversalError
		if errors.Is(err, bsoncore.ErrElementNotFound) || errors.As(err, &untraversable) {
			tuple = append(tuple, bson.RawValue{Type: bson.TypeNull})
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("key field %s: %w", field, err)
		}
		tuple = append(tuple, bson.RawValue{Type: value.Type, Value: slices.Clone(value.Value)})
	}
	return tuple, nil
}

func (w mongoWitness) ScanKeys(ctx context.Context, ns iface.Namespace, keyFields []string, pageSize int, emit func([][]bson.RawValue) error) error {
	opts := options.Find().
		SetProjection(keyProjection(keyFields)).
		SetSort(bson.D{{Key: "_id", Value: 1}}).
		SetBatchSize(int32(pageSize))
	cur, err := w.client.Database(ns.Db).Collection(ns.Col).Find(ctx, bson.D{}, opts)
	if err != nil {
		return err
	}
	defer cur.Close(ctx)
	page := make([][]bson.RawValue, 0, pageSize)
	for cur.Next(ctx) {
		tuple, err := keyTuple(cur.Current, keyFields)
		if err != nil {
			return err
		}
		page = append(page, tuple)
		if len(page) == pageSize {
			if err := emit(page); err != nil {
				return err
			}
			page = make([][]bson.RawValue, 0, pageSize)
		}
	}
	if err := cur.Err(); err != nil {
		return err
	}
	if len(page) > 0 {
		return emit(page)
	}
	return nil
}

func (w mongoWitness) Close() error {
	return w.client.Disconnect(context.Background())
}
