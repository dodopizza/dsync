/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package cosmos

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"strings"
	"sync"

	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/protocol/iface"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"golang.org/x/sync/errgroup"
)

const (
	// batch size for ids to check
	idsBatchSize = 1000
	// number of parallel tasks to scan the witness index
	numParallelWitnessScanTasks = 4
	// number of parallel tasks to scan the source index
	numParallelSourceScanTasks = 4
)

type deleteStages struct {
	differs func(ctx context.Context, ns iface.Namespace) (bool, error)
	scan    func(ctx context.Context, ns iface.Namespace, emit func([]any) error) error
	missing func(ctx context.Context, loc iface.Location, ids []any) ([]any, error)
}

func checkForDeletes(ctx context.Context, client *mongo.Client, witnessClient *mongo.Client, namespaces []iface.Namespace) (map[iface.Location][]*adiomv1.Update, error) {
	return runDeletePass(ctx, namespaces, deleteStages{
		differs: func(ctx context.Context, ns iface.Namespace) (bool, error) {
			return namespaceCountsDiffer(ctx, client, witnessClient, ns)
		},
		scan: func(ctx context.Context, ns iface.Namespace, emit func([]any) error) error {
			return scanWitnessNamespace(ctx, witnessClient, ns, emit)
		},
		missing: func(ctx context.Context, loc iface.Location, ids []any) ([]any, error) {
			return missingSourceIds(ctx, client, loc, ids)
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

func runDeletePass(ctx context.Context, namespaces []iface.Namespace, st deleteStages) (map[iface.Location][]*adiomv1.Update, error) {
	g, gctx := errgroup.WithContext(ctx)
	mismatched := make(chan iface.Namespace)
	idsToCheck := make(chan idsWithLocation)
	idsToDelete := make(chan idsWithLocation)

	g.Go(func() error {
		defer close(mismatched)
		for _, ns := range namespaces {
			differs, err := st.differs(gctx, ns)
			if err != nil {
				return err
			}
			if !differs {
				continue
			}
			if err := sendOrDone(gctx, mismatched, ns); err != nil {
				return err
			}
		}
		return nil
	})

	runStage(g, numParallelWitnessScanTasks, func() { close(idsToCheck) }, func() error {
		for {
			ns, ok, err := recvOrDone(gctx, mismatched)
			if err != nil || !ok {
				return err
			}
			loc := iface.Location{Database: ns.Db, Collection: ns.Col}
			err = st.scan(gctx, ns, func(ids []any) error {
				return sendOrDone(gctx, idsToCheck, idsWithLocation{loc: loc, ids: ids})
			})
			if err != nil {
				return err
			}
		}
	})

	runStage(g, numParallelSourceScanTasks, func() { close(idsToDelete) }, func() error {
		for {
			batch, ok, err := recvOrDone(gctx, idsToCheck)
			if err != nil || !ok {
				return err
			}
			missing, err := st.missing(gctx, batch.loc, batch.ids)
			if err != nil {
				return err
			}
			if len(missing) == 0 {
				continue
			}
			if err := sendOrDone(gctx, idsToDelete, idsWithLocation{loc: batch.loc, ids: missing}); err != nil {
				return err
			}
		}
	})

	updates, convertErr := deleteUpdatesFromIDs(idsToDelete)
	if err := g.Wait(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if convertErr != nil {
		return nil, convertErr
	}
	return updates, nil
}

func namespaceCountsDiffer(ctx context.Context, client *mongo.Client, witnessClient *mongo.Client, ns iface.Namespace) (bool, error) {
	sourceCount, err := client.Database(ns.Db).Collection(ns.Col).EstimatedDocumentCount(ctx)
	if err != nil {
		return false, fmt.Errorf("source count for %s.%s: %w", ns.Db, ns.Col, err)
	}
	witnessCount, err := witnessClient.Database(ns.Db).Collection(ns.Col).EstimatedDocumentCount(ctx)
	if err != nil {
		return false, fmt.Errorf("witness count for %s.%s: %w", ns.Db, ns.Col, err)
	}
	if sourceCount != witnessCount {
		slog.Debug(fmt.Sprintf("Mismatched namespace: %v, source count: %v, witness count: %v", ns, sourceCount, witnessCount))
	}
	return sourceCount != witnessCount, nil
}

func scanWitnessNamespace(ctx context.Context, witnessClient *mongo.Client, ns iface.Namespace, emit func([]any) error) error {
	slog.Debug(fmt.Sprintf("Scanning witness index namespace %v", ns))
	opts := options.Find().SetProjection(bson.M{"_id": 1})
	cur, err := witnessClient.Database(ns.Db).Collection(ns.Col).Find(ctx, bson.M{}, opts)
	if err != nil {
		return fmt.Errorf("witness scan of %s.%s: %w", ns.Db, ns.Col, err)
	}
	defer cur.Close(ctx)
	var ids []any
	for cur.Next(ctx) {
		var idDoc bson.M
		if err := cur.Decode(&idDoc); err != nil {
			return fmt.Errorf("witness scan of %s.%s: decode: %w", ns.Db, ns.Col, err)
		}
		id, ok := idDoc["_id"]
		if !ok {
			return fmt.Errorf("witness scan of %s.%s: document without _id", ns.Db, ns.Col)
		}
		ids = append(ids, id)
		if len(ids) == idsBatchSize {
			if err := emit(ids); err != nil {
				return err
			}
			ids = nil
		}
	}
	if err := cur.Err(); err != nil {
		return fmt.Errorf("witness scan of %s.%s: %w", ns.Db, ns.Col, err)
	}
	if len(ids) > 0 {
		return emit(ids)
	}
	return nil
}

func missingSourceIds(ctx context.Context, client *mongo.Client, loc iface.Location, ids []any) ([]any, error) {
	pipeline := mongo.Pipeline{
		{{"$match", bson.D{{"_id", bson.D{{"$in", ids}}}}}},
		{{"$group", bson.D{{"_id", nil}, {"ids", bson.D{{"$addToSet", "$_id"}}}}}},
		{{"$project", bson.D{
			{"missingIds", bson.D{{"$setDifference", bson.A{ids, "$ids"}}}},
			{"_id", 0},
		}}},
	}
	cur, err := client.Database(loc.Database).Collection(loc.Collection).Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("source check of %s.%s: %w", loc.Database, loc.Collection, err)
	}
	defer cur.Close(ctx)
	if !cur.Next(ctx) {
		if err := cur.Err(); err != nil {
			return nil, fmt.Errorf("source check of %s.%s: %w", loc.Database, loc.Collection, err)
		}
		return ids, nil
	}
	var res bson.M
	if err := cur.Decode(&res); err != nil {
		return nil, fmt.Errorf("source check of %s.%s: decode: %w", loc.Database, loc.Collection, err)
	}
	missing, ok := res["missingIds"].(bson.A)
	if !ok {
		return nil, fmt.Errorf("source check of %s.%s: unexpected result shape", loc.Database, loc.Collection)
	}
	return []any(missing), nil
}

/****************************************************
 * Helper functions
 ****************************************************/

// tuple to store location and a set of id
type idsWithLocation struct {
	loc iface.Location
	ids []interface{}
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
