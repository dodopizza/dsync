/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package cosmos

import (
	"context"
	"fmt"
	"slices"

	"github.com/adiom-data/dsync/protocol/iface"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Witness interface {
	ScanKeys(ctx context.Context, ns iface.Namespace, pageSize int, emit func([]bson.RawValue) error) error
}

type mongoWitness struct {
	client *mongo.Client
}

func (w mongoWitness) ScanKeys(ctx context.Context, ns iface.Namespace, pageSize int, emit func([]bson.RawValue) error) error {
	opts := options.Find().
		SetProjection(bson.D{{Key: "_id", Value: 1}}).
		SetSort(bson.D{{Key: "_id", Value: 1}}).
		SetBatchSize(int32(pageSize))
	cur, err := w.client.Database(ns.Db).Collection(ns.Col).Find(ctx, bson.D{}, opts)
	if err != nil {
		return err
	}
	defer cur.Close(ctx)
	page := make([]bson.RawValue, 0, pageSize)
	for cur.Next(ctx) {
		id, err := cur.Current.LookupErr("_id")
		if err != nil {
			return fmt.Errorf("document without _id: %w", err)
		}
		page = append(page, bson.RawValue{Type: id.Type, Value: slices.Clone(id.Value)})
		if len(page) == pageSize {
			if err := emit(page); err != nil {
				return err
			}
			page = make([]bson.RawValue, 0, pageSize)
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
