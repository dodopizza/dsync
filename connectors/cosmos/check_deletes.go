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
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/protocol/iface"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"golang.org/x/sync/errgroup"
)

const (
	witnessPageSize = 1000
	// number of parallel tasks to scan the witness index
	numParallelWitnessScanTasks = 4
	// number of parallel tasks to scan the source index
	numParallelSourceScanTasks = 4
)

type deleteStages struct {
	scan    func(ctx context.Context, ns iface.Namespace, keyFields []string, emit func([][]bson.RawValue) error) error
	missing func(ctx context.Context, loc iface.Location, keyFields []string, keys [][]bson.RawValue) ([][]bson.RawValue, int, error)
}

type passSummary struct {
	Namespaces int
	Keys       int
	Deletes    int
	Deferred   int
}

func checkForDeletes(ctx context.Context, client *mongo.Client, witness Witness, namespaces []iface.Namespace, shardKeyFields map[iface.Namespace][]string) (map[iface.Location][]*adiomv1.Update, passSummary, error) {
	return runDeletePass(ctx, namespaces, shardKeyFields, deleteStages{
		scan: witnessScan(witness, witnessPageSize),
		missing: func(ctx context.Context, loc iface.Location, keyFields []string, keys [][]bson.RawValue) ([][]bson.RawValue, int, error) {
			return missingSourceKeys(ctx, client, loc, keyFields, keys)
		},
	})
}

func sendOrDone[T any](ctx context.Context, ch chan<- T, v T) error {
	select {
	case ch <- v:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func recvOrDone[T any](ctx context.Context, ch <-chan T) (T, bool, error) {
	select {
	case v, ok := <-ch:
		return v, ok, nil
	case <-ctx.Done():
		var zero T
		return zero, false, ctx.Err()
	}
}

func runStage(g *errgroup.Group, workers int, closeOutput func(), work func() error) {
	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		g.Go(func() error {
			defer wg.Done()
			return work()
		})
	}
	g.Go(func() error {
		wg.Wait()
		closeOutput()
		return nil
	})
}

func runDeletePass(ctx context.Context, namespaces []iface.Namespace, shardKeyFields map[iface.Namespace][]string, st deleteStages) (map[iface.Location][]*adiomv1.Update, passSummary, error) {
	g, gctx := errgroup.WithContext(ctx)
	toScan := make(chan iface.Namespace)
	keysToCheck := make(chan idsWithLocation)
	keysToDelete := make(chan idsWithLocation)
	var scanned, keys, deferred atomic.Int64

	g.Go(func() error {
		defer close(toScan)
		for _, ns := range namespaces {
			if err := sendOrDone(gctx, toScan, ns); err != nil {
				return err
			}
		}
		return nil
	})

	runStage(g, numParallelWitnessScanTasks, func() { close(keysToCheck) }, func() error {
		for {
			ns, ok, err := recvOrDone(gctx, toScan)
			if err != nil || !ok {
				return err
			}
			loc := iface.Location{Database: ns.Db, Collection: ns.Col}
			fields := shardKeyFields[ns]
			err = st.scan(gctx, ns, fields, func(page [][]bson.RawValue) error {
				keys.Add(int64(len(page)))
				return sendOrDone(gctx, keysToCheck, idsWithLocation{loc: loc, fields: fields, ids: page})
			})
			if err != nil {
				return err
			}
			scanned.Add(1)
		}
	})

	runStage(g, numParallelSourceScanTasks, func() { close(keysToDelete) }, func() error {
		for {
			batch, ok, err := recvOrDone(gctx, keysToCheck)
			if err != nil || !ok {
				return err
			}
			missing, undecided, err := st.missing(gctx, batch.loc, batch.fields, batch.ids)
			deferred.Add(int64(undecided))
			if err != nil {
				return err
			}
			if len(missing) == 0 {
				continue
			}
			if err := sendOrDone(gctx, keysToDelete, idsWithLocation{loc: batch.loc, fields: batch.fields, ids: missing}); err != nil {
				return err
			}
		}
	})

	updates, convertErr := deleteUpdatesFromIDs(keysToDelete)
	if err := g.Wait(); err != nil {
		return nil, passSummary{}, err
	}
	if convertErr != nil {
		return nil, passSummary{}, convertErr
	}
	deletes := 0
	for _, u := range updates {
		deletes += len(u)
	}
	return updates, passSummary{Namespaces: int(scanned.Load()), Keys: int(keys.Load()), Deletes: deletes, Deferred: int(deferred.Load())}, nil
}

func witnessScan(w Witness, pageSize int) func(context.Context, iface.Namespace, []string, func([][]bson.RawValue) error) error {
	return func(ctx context.Context, ns iface.Namespace, keyFields []string, emit func([][]bson.RawValue) error) error {
		buffered := make([][]bson.RawValue, 0, pageSize)
		err := w.ScanKeys(ctx, ns, keyFields, pageSize, func(page [][]bson.RawValue) error {
			for _, key := range page {
				if len(buffered) >= pageSize && canonicalKey(key[0]) != canonicalKey(buffered[len(buffered)-1][0]) {
					if err := emit(buffered); err != nil {
						return err
					}
					buffered = make([][]bson.RawValue, 0, pageSize)
				}
				buffered = append(buffered, key)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("witness scan of %s.%s: %w", ns.Db, ns.Col, err)
		}
		if len(buffered) == 0 {
			return nil
		}
		return emit(buffered)
	}
}

func distinctIDs(keys [][]bson.RawValue) []bson.RawValue {
	seen := make(map[string]struct{}, len(keys))
	var ids []bson.RawValue
	for _, key := range keys {
		canonical := canonicalKey(key[0])
		if _, ok := seen[canonical]; ok {
			continue
		}
		seen[canonical] = struct{}{}
		ids = append(ids, key[0])
	}
	return ids
}

func missingSourceKeys(ctx context.Context, client *mongo.Client, loc iface.Location, keyFields []string, keys [][]bson.RawValue) ([][]bson.RawValue, int, error) {
	if len(keys) == 0 {
		return nil, 0, nil
	}
	fail := func(err error) ([][]bson.RawValue, int, error) {
		return nil, 0, fmt.Errorf("source check of %s.%s: %w", loc.Database, loc.Collection, err)
	}
	known := make(map[string]struct{}, len(keys))
	pageIDs := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		known[canonicalTuple(key)] = struct{}{}
		pageIDs[canonicalKey(key[0])] = struct{}{}
	}
	opts := options.Find().SetProjection(keyProjection(keyFields))
	filter := bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: distinctIDs(keys)}}}}
	cur, err := client.Database(loc.Database).Collection(loc.Collection).Find(ctx, filter, opts)
	if err != nil {
		return fail(err)
	}
	defer cur.Close(ctx)
	present := make(map[string]struct{}, len(keys))
	undecided := make(map[string]struct{})
	for cur.Next(ctx) {
		tuple, err := keyTuple(cur.Current, keyFields)
		if err != nil {
			return fail(err)
		}
		id := canonicalKey(tuple[0])
		if _, ok := pageIDs[id]; !ok {
			return fail(errors.New("source returned a document whose _id matches no witness key of the page"))
		}
		canonical := canonicalTuple(tuple)
		if _, ok := known[canonical]; !ok {
			undecided[id] = struct{}{}
			continue
		}
		present[canonical] = struct{}{}
	}
	if err := cur.Err(); err != nil {
		return fail(err)
	}
	var missing [][]bson.RawValue
	deferred := 0
	for _, key := range keys {
		if _, ok := undecided[canonicalKey(key[0])]; ok {
			deferred++
		} else if _, ok := present[canonicalTuple(key)]; !ok {
			missing = append(missing, key)
		}
	}
	return missing, deferred, nil
}

/****************************************************
 * Helper functions
 ****************************************************/

// tuple to store location and a set of id
type idsWithLocation struct {
	loc    iface.Location
	fields []string
	ids    [][]bson.RawValue
}

// get all database names except system databases
func getAllDatabasesWitness(ctx context.Context, witnessClient *mongo.Client) ([]string, error) {
	dbNames, err := witnessClient.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		return nil, err
	}

	dbs := slices.DeleteFunc(dbNames, func(d string) bool {
		return slices.Contains(ExcludedDBListForIC, d)
	})

	return dbs, nil
}

// get all collections in a database except system collections
func getAllCollectionsWitness(ctx context.Context, witnessClient *mongo.Client, dbName string) ([]string, error) {
	collectionsAll, err := witnessClient.Database(dbName).ListCollectionNames(ctx, bson.M{})
	if err != nil {
		return nil, err
	}

	//remove all system collections that match the pattern
	r, _ := regexp.Compile(ExcludedSystemCollPattern)
	collections := slices.DeleteFunc(collectionsAll, func(n string) bool {
		return r.Match([]byte(n))
	})

	return collections, nil
}

// Returns a list of fully qualified namespaces
func getFQNamespaceListWitness(ctx context.Context, witnessClient *mongo.Client, namespacesFilter []string) ([]iface.Namespace, error) {
	var dbsToResolve []string //database names that we need to resolve

	var namespaces []iface.Namespace

	if namespacesFilter == nil {
		var err error
		dbsToResolve, err = getAllDatabasesWitness(ctx, witnessClient)
		if err != nil {
			return nil, err
		}
	} else {
		// iterate over provided namespaces
		// if it has a dot, then it is a fully qualified namespace
		// otherwise, it is a database name to resolve
		for _, ns := range namespacesFilter {
			db, col, isFQN := strings.Cut(ns, ".")
			if isFQN {
				namespaces = append(namespaces, iface.Namespace{Db: db, Col: col})
			} else {
				dbsToResolve = append(dbsToResolve, ns)
			}
		}
	}

	//iterate over unresolved databases and get all collections
	for _, db := range dbsToResolve {
		colls, err := getAllCollectionsWitness(ctx, witnessClient, db)
		if err != nil {
			return nil, err
		}
		//create tasks for these
		for _, coll := range colls {
			namespaces = append(namespaces, iface.Namespace{Db: db, Col: coll})
		}
	}

	return namespaces, nil
}
