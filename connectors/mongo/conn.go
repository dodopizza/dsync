package mongo

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math"
	"net"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	"github.com/adiom-data/dsync/connectors/util"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"github.com/adiom-data/dsync/protocol/iface"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	moptions "go.mongodb.org/mongo-driver/v2/mongo/options"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ConnectorSettings struct {
	ConnectionString string

	ServerConnectTimeout       time.Duration
	PingTimeout                time.Duration
	WriterMaxBatchSize         int   // applies to batch inserts only; 0 means no limit
	TargetDocCountPerPartition int64 //target number of documents per partition (256k docs is 256MB with 1KB average doc size)
	SampleFactor               int   // a factor to determine how many extra samples per partition are used
	MaxPageSize                int
	NamespaceFanout            int
	DocumentDBSamplingFanout   int
	PerNamespaceStreams        bool
	SkipBatchOverwrite         bool
	FullDocumentKey            bool
	PageBufferTTL              time.Duration
	CosmosSink                 bool
	DeleteUniqueConflicts      bool

	Query string // query filter, as a v2 Extended JSON string, e.g., '{\"x\":{\"$gt\":1}}'"
}

const (
	defaultNamespaceFanoutLimit          = 100
	defaultDocumentDBSamplingFanoutLimit = 100
)

func setDefault[T comparable](field *T, defaultValue T) {
	if *field == *new(T) {
		*field = defaultValue
	}
}

type bufferData struct {
	data [][]byte
	err  error
}

type buffer struct {
	ctr     int64
	ch      <-chan bufferData
	last    *adiomv1.ListDataResponse
	cleanup *time.Timer
	err     error
}

type conn struct {
	adiomv1connect.UnimplementedConnectorServiceHandler
	client *mongo.Client

	settings ConnectorSettings
	flavor   MongoFlavor

	nextCursorID    atomic.Int64
	replacedTTLDocs atomic.Int64
	shardKeyFields  sync.Map
	ctx             context.Context
	cancel          context.CancelFunc
	buffersMutex    sync.RWMutex
	buffers         map[int64]buffer

	query bson.D
}

// get all database names except system databases
func getAllDatabases(ctx context.Context, client *mongo.Client) ([]string, error) {
	dbNames, err := client.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		return nil, err
	}

	dbs := slices.DeleteFunc(dbNames, func(d string) bool {
		return slices.Contains(ExcludedDBListForIC, d)
	})

	return dbs, nil
}

// get all collections in a database except system collections
func getAllCollections(ctx context.Context, client *mongo.Client, dbName string) ([]string, error) {
	collectionsAll, err := client.Database(dbName).ListCollectionNames(ctx, bson.M{})
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

func GetCol(client *mongo.Client, ns string) (*mongo.Collection, iface.Namespace, bool) {
	n, ok := ToNS(ns)
	return client.Database(n.Db).Collection(n.Col), n, ok
}

func ToNS(ns string) (iface.Namespace, bool) {
	db, col, ok := strings.Cut(ns, ".")
	return iface.Namespace{Db: db, Col: col}, ok
}

func NamespacePartitions(ctx context.Context, namespaces []string, client *mongo.Client) ([]*adiomv1.Partition, error) {
	var dbsToResolve []string //database names that we need to resolve
	var partitions []*adiomv1.Partition

	if len(namespaces) < 1 {
		var err error
		dbsToResolve, err = getAllDatabases(ctx, client)
		if err != nil {
			return nil, err
		}
	} else {
		// iterate over provided namespaces
		// if it has a dot, then it is a fully qualified namespace
		// otherwise, it is a database name to resolve
		for _, ns := range namespaces {
			db, _, isFQN := strings.Cut(ns, ".")
			if isFQN {
				partitions = append(partitions, &adiomv1.Partition{
					Namespace: ns,
				})
			} else {
				dbsToResolve = append(dbsToResolve, db)
			}
		}
	}

	slog.Debug(fmt.Sprintf("Databases to resolve: %v", dbsToResolve))

	//iterate over unresolved databases and get all collections
	for _, db := range dbsToResolve {
		cols, err := getAllCollections(ctx, client, db)
		if err != nil {
			return nil, err
		}
		for _, col := range cols {
			partitions = append(partitions, &adiomv1.Partition{
				Namespace: fmt.Sprintf("%v.%v", db, col),
			})
		}
	}

	return partitions, nil
}

func findLargestID(ctx context.Context, col *mongo.Collection, query bson.D) (bson.RawValue, error) {
	res, err := col.Find(ctx, append(bson.D{}, query...), options.Find().SetSort(bson.M{"_id": -1}).SetLimit(1).SetProjection(bson.D{{"_id", 1}}))
	if err != nil {
		return bson.RawValue{}, fmt.Errorf("err in find: %w", err)
	}
	defer res.Close(ctx)
	for res.Next(ctx) {
		return res.Current.Lookup("_id"), nil
	}
	if res.Err() != nil {
		return bson.RawValue{}, fmt.Errorf("err in iterating: %w", res.Err())
	}
	return bson.RawValue{}, nil
}

func findSmallestID(ctx context.Context, col *mongo.Collection, query bson.D) (bson.RawValue, error) {
	res, err := col.Find(ctx, append(bson.D{}, query...), options.Find().SetSort(bson.M{"_id": 1}).SetLimit(1).SetProjection(bson.D{{"_id", 1}}))
	if err != nil {
		return bson.RawValue{}, fmt.Errorf("err in find: %w", err)
	}
	defer res.Close(ctx)
	for res.Next(ctx) {
		return res.Current.Lookup("_id"), nil
	}
	if res.Err() != nil {
		return bson.RawValue{}, fmt.Errorf("err in iterating: %w", res.Err())
	}
	return bson.RawValue{}, nil
}

var idTypeAliases = map[int]bson.A{
	2:  {"null", "undefined"},
	3:  {"number"},
	4:  {"string", "symbol"},
	5:  {"object"},
	7:  {"binData"},
	8:  {"objectId"},
	9:  {"bool"},
	10: {"date"},
	11: {"timestamp"},
}

func andFilter(a, b bson.D) bson.D {
	if len(b) == 0 {
		return a
	}
	if len(a) == 0 {
		return b
	}
	return bson.D{{"$and", bson.A{a, b}}}
}

func (c *conn) planCollection(ctx context.Context, col *mongo.Collection, namespace string) ([]*adiomv1.Partition, error) {
	lowest, err := findSmallestID(ctx, col, c.query)
	if err != nil {
		return nil, fmt.Errorf("err finding smallest id: %w", err)
	}
	high, err := findLargestID(ctx, col, c.query)
	if err != nil {
		return nil, fmt.Errorf("err finding largest id: %w", err)
	}
	if lowest.IsZero() {
		return []*adiomv1.Partition{{Namespace: namespace, Cursor: EncodeCursor(bson.RawValue{}, high)}}, nil
	}
	mixed := bsonTypeSortOrder(lowest.Type) != bsonTypeSortOrder(high.Type)
	if lowest.Type != high.Type && (idTypeAliases[bsonTypeSortOrder(lowest.Type)] == nil || idTypeAliases[bsonTypeSortOrder(high.Type)] == nil) {
		return nil, fmt.Errorf("mixed _id types %v and %v not supported", lowest.Type, high.Type)
	}

	count, err := col.EstimatedDocumentCount(ctx)
	if err != nil {
		return nil, err
	}
	var ids []bson.RawValue
	if count < c.settings.TargetDocCountPerPartition*2 {
		if !mixed {
			return []*adiomv1.Partition{{
				Namespace:      namespace,
				EstimatedCount: uint64(count),
				Cursor:         EncodeCursor(bson.RawValue{}, high),
			}}, nil
		}
	} else {
		numSamples := (count / c.settings.TargetDocCountPerPartition) * int64(c.settings.SampleFactor)
		if numSamples*21 >= count {
			return nil, fmt.Errorf("too many samples requested (%v of %v)- adjust sample factor or target documents per partition", numSamples, count)
		}
		if numSamples > 1000000 {
			slog.Warn("More than 1000000 samples requested", "samples", numSamples)
		}
		slog.Debug("Getting samples for namespace", "namespace", namespace, "samples", numSamples)
		ids, err = c.sampleIDs(ctx, col, numSamples)
		if err != nil {
			return nil, fmt.Errorf("error getting %v samples for partition: %w", numSamples, err)
		}
	}
	if !mixed {
		return c.rangePartitions(namespace, ids, high), nil
	}

	var partitions []*adiomv1.Partition
	for order := bsonTypeSortOrder(lowest.Type); order <= bsonTypeSortOrder(high.Type); order++ {
		aliases, ok := idTypeAliases[order]
		if !ok {
			continue
		}
		typeHigh, err := findLargestID(ctx, col, andFilter(bson.D{{"_id", bson.D{{"$type", aliases}}}}, c.query))
		if err != nil {
			return nil, fmt.Errorf("err finding largest id of types %v: %w", aliases, err)
		}
		if typeHigh.IsZero() {
			continue
		}
		var typeIDs []bson.RawValue
		for _, id := range ids {
			if bsonTypeSortOrder(id.Type) == order {
				typeIDs = append(typeIDs, id)
			}
		}
		partitions = append(partitions, c.rangePartitions(namespace, typeIDs, typeHigh)...)
	}
	return partitions, nil
}

func (c *conn) rangePartitions(namespace string, ids []bson.RawValue, high bson.RawValue) []*adiomv1.Partition {
	var partitions []*adiomv1.Partition
	factorCount := c.settings.SampleFactor / 2
	var low bson.RawValue
	for _, id := range ids {
		if factorCount > 0 {
			factorCount -= 1
			continue
		}
		factorCount = c.settings.SampleFactor - 1
		partitions = append(partitions, &adiomv1.Partition{
			Namespace:      namespace,
			EstimatedCount: uint64(c.settings.TargetDocCountPerPartition),
			Cursor:         EncodeCursor(low, id),
		})
		low = id
	}
	return append(partitions, &adiomv1.Partition{
		Namespace:      namespace,
		EstimatedCount: uint64(c.settings.TargetDocCountPerPartition),
		Cursor:         EncodeCursor(low, high),
	})
}

// GeneratePlan implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GeneratePlan(ctx context.Context, r *connect.Request[adiomv1.GeneratePlanRequest]) (*connect.Response[adiomv1.GeneratePlanResponse], error) {
	partitions, err := NamespacePartitions(ctx, r.Msg.GetNamespaces(), c.client)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	var updatesPartitions []*adiomv1.UpdatesPartition

	if r.Msg.GetUpdates() {
		if c.settings.PerNamespaceStreams {
			// TODO: maybe parallelize
			for _, partition := range partitions {
				ns, _ := ToNS(partition.Namespace)
				col := c.client.Database(ns.Db).Collection(ns.Col)
				resumeToken, err := getLatestResumeToken(ctx, col)
				if err != nil {
					slog.Error(fmt.Sprintf("Failed to get latest resume token for ns %v: %v", partition.GetNamespace(), err))
					return nil, connect.NewError(connect.CodeInternal, err)
				}

				updatesPartitions = append(updatesPartitions, &adiomv1.UpdatesPartition{
					Namespaces: []string{partition.GetNamespace()},
					Cursor:     resumeToken,
				})
			}
		} else {
			resumeToken, err := getLatestResumeToken(ctx, c.client)
			if err != nil {
				slog.Error(fmt.Sprintf("Failed to get latest resume token: %v", err))
				return nil, connect.NewError(connect.CodeInternal, err)
			}

			var updatesNamespaces []string
			if len(r.Msg.GetNamespaces()) > 0 {
				for _, partition := range partitions {
					updatesNamespaces = append(updatesNamespaces, partition.GetNamespace())
				}
			}
			updatesPartitions = []*adiomv1.UpdatesPartition{{Namespaces: updatesNamespaces, Cursor: resumeToken}}
		}
	}

	done := make(chan struct{})
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(c.planningFanoutLimit())
	var finalPartitions []*adiomv1.Partition
	ch := make(chan *adiomv1.Partition)

	go func() {
		defer close(done)
		for p := range ch {
			finalPartitions = append(finalPartitions, p)
		}
	}()

	for _, partition := range partitions {
		if !r.Msg.GetInitialSync() {
			break
		}
		eg.Go(func() error {
			ns, _ := ToNS(partition.Namespace)
			col := c.client.Database(ns.Db).Collection(ns.Col)
			planned, err := c.planCollection(ctx, col, partition.GetNamespace())
			if err != nil {
				return err
			}
			for _, p := range planned {
				ch <- p
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		close(ch)
		return nil, err
	}
	close(ch)
	<-done

	return connect.NewResponse(&adiomv1.GeneratePlanResponse{
		Partitions:        finalPartitions,
		UpdatesPartitions: updatesPartitions,
	}), nil
}

func (c *conn) planningFanoutLimit() int {
	if c.settings.NamespaceFanout > 0 {
		return c.settings.NamespaceFanout
	}
	return defaultNamespaceFanoutLimit
}

// GetInfo implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GetInfo(ctx context.Context, r *connect.Request[adiomv1.GetInfoRequest]) (*connect.Response[adiomv1.GetInfoResponse], error) {
	// Get version of the MongoDB server
	var commandResult bson.M
	err := c.client.Database("admin").RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&commandResult)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	version := commandResult["version"]

	return connect.NewResponse(&adiomv1.GetInfoResponse{
		Id:      string(generateConnectorID(c.settings.ConnectionString + c.settings.Query)),
		DbType:  connectorDBType,
		Version: version.(string),
		Spec:    connectorSpec,
		Capabilities: &adiomv1.Capabilities{
			Source: &adiomv1.Capabilities_Source{
				SupportedDataTypes: []adiomv1.DataType{adiomv1.DataType_DATA_TYPE_MONGO_BSON},
				LsnStream:          true,
				MultiNamespacePlan: true,
				DefaultPlan:        !c.settings.PerNamespaceStreams,
				GetByIds:           true,
			},
			Sink: &adiomv1.Capabilities_Sink{
				SupportedDataTypes:  []adiomv1.DataType{adiomv1.DataType_DATA_TYPE_MONGO_BSON},
				PartialWriteResults: true,
			},
		},
	}), nil
}

// GetNamespaceMetadata implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GetNamespaceMetadata(ctx context.Context, r *connect.Request[adiomv1.GetNamespaceMetadataRequest]) (*connect.Response[adiomv1.GetNamespaceMetadataResponse], error) {
	collection, _, ok := GetCol(c.client, r.Msg.GetNamespace())
	if !ok {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("namespace should be fully qualified"))
	}
	count, err := collection.EstimatedDocumentCount(ctx)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			slog.Error(fmt.Sprintf("Failed to count documents: %v", err))
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&adiomv1.GetNamespaceMetadataResponse{
		Count: uint64(count),
	}), nil
}

func EncodeCursor(low bson.RawValue, high bson.RawValue) []byte {
	if low.IsZero() && high.IsZero() {
		return nil
	}
	var d bson.D
	if !low.IsZero() {
		d = append(d, bson.E{"low", low})
	}
	if !high.IsZero() {
		d = append(d, bson.E{"high", high})
	}
	res, _ := bson.Marshal(d)
	return res
}

func DecodeCursor(cursor []byte) (bson.RawValue, bson.RawValue) {
	if len(cursor) == 0 {
		return bson.RawValue{}, bson.RawValue{}
	}
	raw := bson.Raw(cursor)
	low := raw.Lookup("low")
	high := raw.Lookup("high")
	return low, high
}

func createFindFilterFromCursor(cursor []byte) bson.D {
	low, high := DecodeCursor(cursor)

	if low.IsZero() && high.IsZero() { //no boundaries
		return bson.D{}
	} else if low.IsZero() { //only upper boundary
		return bson.D{
			{"_id", bson.D{
				{"$lte", high},
			}},
		}
	} else if high.IsZero() { //only lower boundary
		return bson.D{
			{"_id", bson.D{
				{"$gt", low},
			}},
		}
	} else { //both boundaries
		return bson.D{
			{"_id", bson.D{
				{"$gt", low},
				{"$lte", high},
			}},
		}
	}
}

// ListData implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) ListData(ctx context.Context, r *connect.Request[adiomv1.ListDataRequest]) (*connect.Response[adiomv1.ListDataResponse], error) {
	partition := r.Msg.GetPartition()
	collection, _, ok := GetCol(c.client, partition.GetNamespace())
	if !ok {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("namespace should be fully qualified"))
	}

	var cursorID, ctr int64

	pageCursor := r.Msg.GetCursor()
	if pageCursor == nil {
		cursorID = c.nextCursorID.Add(1)
		ch := make(chan bufferData, 10)
		c.buffersMutex.Lock()
		c.buffers[cursorID] = buffer{
			ctr:  0,
			ch:   ch,
			last: nil,
			cleanup: time.AfterFunc(c.settings.PageBufferTTL, func() {
				c.buffersMutex.Lock()
				delete(c.buffers, cursorID)
				c.buffersMutex.Unlock()
			}),
		}
		c.buffersMutex.Unlock()

		filter := andFilter(createFindFilterFromCursor(r.Msg.GetPartition().GetCursor()), c.query)

		cursor, err := collection.Find(ctx, filter)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				slog.Debug("Find Error", "filter", filter)
				slog.Error(fmt.Sprintf("Failed to find documents: %v", err))
			}
			return nil, maybeUnavailableError(err)
		}
		go func(ctx context.Context) {
			defer cursor.Close(ctx)
			defer close(ch)
			var dataBatch [][]byte
			for cursor.Next(ctx) {
				dataBatch = append(dataBatch, cursor.Current)
				if cursor.RemainingBatchLength() == 0 || (c.settings.MaxPageSize != 0 && len(dataBatch) == int(c.settings.MaxPageSize)) {
					select {
					case ch <- bufferData{data: dataBatch}:
						dataBatch = nil
					case <-ctx.Done():
						ch <- bufferData{err: ctx.Err()}
						return
					}
				}
			}
			if cursor.Err() != nil {
				if !errors.Is(cursor.Err(), context.Canceled) {
					slog.Error(fmt.Sprintf("Failed to iterate through documents: %v", cursor.Err()))
				}
				ch <- bufferData{err: cursor.Err()}
			}
		}(c.ctx)
	} else {
		var err error
		br := bytes.NewReader(r.Msg.GetCursor())
		cursorID, err = binary.ReadVarint(br)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid cursor: %w", err))
		}
		ctr, err = binary.ReadVarint(br)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid cursor: %w", err))
		}
	}
	c.buffersMutex.RLock()
	buffer, ok := c.buffers[cursorID]
	c.buffersMutex.RUnlock()
	if !ok {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid cursor"))
	}
	buffer.cleanup.Reset(c.settings.PageBufferTTL)
	if buffer.err != nil {
		return nil, maybeUnavailableError(buffer.err)
	}
	if ctr+1 == buffer.ctr && buffer.last != nil {
		return connect.NewResponse(buffer.last), nil
	}
	if ctr != buffer.ctr { // Reject for old or invalid pages
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("no longer available"))
	}

	for {
		select {
		case dataBatch, ok := <-buffer.ch:
			if dataBatch.err != nil {
				buffer.err = dataBatch.err
				c.buffersMutex.Lock()
				c.buffers[cursorID] = buffer
				c.buffersMutex.Unlock()
				return nil, maybeUnavailableError(buffer.err)
			}
			var nextCursor []byte
			if !ok {
				buffer.last = &adiomv1.ListDataResponse{}
				return connect.NewResponse(buffer.last), nil
			}
			buffer.ctr++
			nextCursor = binary.AppendVarint(nil, cursorID)
			nextCursor = binary.AppendVarint(nextCursor, ctr+1)

			resp := &adiomv1.ListDataResponse{
				Data:       dataBatch.data,
				NextCursor: nextCursor,
			}
			buffer.last = resp
			buffer.cleanup.Reset(c.settings.PageBufferTTL)
			c.buffersMutex.Lock()
			c.buffers[cursorID] = buffer
			c.buffersMutex.Unlock()
			return connect.NewResponse(resp), nil
		case <-ctx.Done():
			return nil, connect.NewError(connect.CodeCanceled, ctx.Err())
		}
	}
}

// StreamLSN implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) StreamLSN(ctx context.Context, r *connect.Request[adiomv1.StreamLSNRequest], s *connect.ServerStream[adiomv1.StreamLSNResponse]) error {
	var watcher Watchable
	var pipeline mongo.Pipeline

	var namespaces []iface.Namespace
	for _, namespace := range r.Msg.GetNamespaces() {
		ns, ok := ToNS(namespace)
		if !ok {
			return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("namespace should be fully qualified"))
		}
		namespaces = append(namespaces, ns)
	}

	if c.settings.PerNamespaceStreams {
		if len(namespaces) != 1 {
			return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("expected exactly 1 namespace"))
		}
		watcher = c.client.Database(namespaces[0].Db).Collection(namespaces[0].Col)
		pipeline = mongo.Pipeline{}
	} else {
		watcher = c.client
		nsFilter := createChangeStreamNamespaceFilterFromNamespaces(namespaces)
		slog.Debug(fmt.Sprintf("Change stream namespace filter: %v", nsFilter))
		pipeline = mongo.Pipeline{
			{{"$match", nsFilter}},
		}
	}
	opts := c.changeStreamOpts(r.Msg.GetCursor())

	changeStream, err := watcher.Watch(ctx, pipeline, opts)
	if err != nil {
		slog.Error(fmt.Sprintf("LSN tracker: Failed to open change stream: %v", err))
		return connect.NewError(connect.CodeInternal, err)
	}
	defer changeStream.Close(ctx)

	lsn := 0
	for changeStream.Next(ctx) {
		lsn++
		if changeStream.RemainingBatchLength() == 0 {
			err := s.Send(&adiomv1.StreamLSNResponse{
				Lsn:        uint64(lsn),
				NextCursor: changeStream.ResumeToken(),
			})
			if err != nil {
				if errors.Is(err, context.Canceled) {
					break
				}
			}
		}
	}
	if changeStream.Err() != nil {
		if errors.Is(changeStream.Err(), context.Canceled) || ctx.Err() != nil {
			return nil
		}
		return connect.NewError(connect.CodeInternal, changeStream.Err())
	}
	return nil
}

// creates a filter for the change stream to include only the specified namespaces
func createChangeStreamNamespaceFilterFromNamespaces(namespaces []iface.Namespace) bson.D {
	if len(namespaces) == 0 {
		return createChangeStreamNamespaceFilter()
	}

	var filters []bson.D
	for _, namespace := range namespaces {
		filters = append(filters, bson.D{{"ns.db", namespace.Db}, {"ns.coll", namespace.Col}})
	}
	return bson.D{{"$or", filters}}
}

func convertChangeStreamEventToUpdate(change MongoUpdate, fullDocumentKey bool) (*adiomv1.Update, error) {
	// slog.Debug(fmt.Sprintf("Converting change stream event %v", change))

	optype := change.OperationType
	var update *adiomv1.Update

	var id []*adiomv1.BsonValue
	for _, k := range change.DocumentKey {
		if !fullDocumentKey && k.Key != "_id" {
			continue
		}
		idType, idVal, err := bson.MarshalValue(k.Value)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal %v: %v", k.Key, err)
		}
		id = append(id, &adiomv1.BsonValue{
			Data: idVal,
			Type: uint32(idType),
			Name: k.Key,
		})
	}

	switch optype {
	case "insert":
		update = &adiomv1.Update{
			Id:   id,
			Type: adiomv1.UpdateType_UPDATE_TYPE_INSERT,
			Data: change.FullDocument,
		}
	case "update", "replace":
		if len(change.FullDocument) == 0 {
			return &adiomv1.Update{Id: id, Type: adiomv1.UpdateType_UPDATE_TYPE_PARTIAL_UPDATE}, nil
		}
		update = &adiomv1.Update{
			Id:   id,
			Type: adiomv1.UpdateType_UPDATE_TYPE_UPDATE,
			Data: change.FullDocument,
		}
	case "delete":
		update = &adiomv1.Update{
			Id:   id,
			Type: adiomv1.UpdateType_UPDATE_TYPE_DELETE,
		}
	default:
		return nil, fmt.Errorf("unsupported change event operation type: %v", optype)
	}

	return update, nil
}

func toTimestampPB(t bson.Timestamp) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(time.Unix(int64(t.T), 0))
}

type MongoUpdate struct {
	NS            bson.M          `bson:"ns"`
	ClusterTime   *bson.Timestamp `bson:"clusterTime"`
	DocumentKey   bson.D          `bson:"documentKey"`
	FullDocument  bson.Raw        `bson:"fullDocument"`
	OperationType string          `bson:"operationType"`
}

// StreamUpdates implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) StreamUpdates(ctx context.Context, r *connect.Request[adiomv1.StreamUpdatesRequest], s *connect.ServerStream[adiomv1.StreamUpdatesResponse]) error {
	var watcher Watchable
	var pipeline mongo.Pipeline

	var namespaces []iface.Namespace
	for _, namespace := range r.Msg.GetNamespaces() {
		ns, ok := ToNS(namespace)
		if !ok {
			return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("namespace should be fully qualified"))
		}
		namespaces = append(namespaces, ns)
	}

	if c.settings.PerNamespaceStreams {
		if len(namespaces) != 1 {
			return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("expected exactly 1 namespace"))
		}
		watcher = c.client.Database(namespaces[0].Db).Collection(namespaces[0].Col)
		pipeline = mongo.Pipeline{}
	} else {
		watcher = c.client
		nsFilter := createChangeStreamNamespaceFilterFromNamespaces(namespaces)
		slog.Debug(fmt.Sprintf("Change stream namespace filter: %v", nsFilter))
		pipeline = mongo.Pipeline{
			{{"$match", nsFilter}},
		}
	}
	opts := c.changeStreamOpts(r.Msg.GetCursor()).SetFullDocument("updateLookup")

	changeStream, err := watcher.Watch(ctx, pipeline, opts)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to open change stream: %v", err))
		return connect.NewError(connect.CodeInternal, err)
	}
	defer changeStream.Close(ctx)

	var updates []*adiomv1.Update
	var currentNamespace string
	var lastResumeToken bson.Raw
	var lastTime bson.Timestamp

	for {
		if !changeStream.TryNext(ctx) {
			if changeStream.Err() != nil || changeStream.ID() == 0 {
				break
			}
			if token := changeStream.ResumeToken(); len(updates) > 0 || !bytes.Equal(token, lastResumeToken) {
				lastResumeToken = token
				resp := &adiomv1.StreamUpdatesResponse{Updates: updates, Namespace: currentNamespace, NextCursor: token}
				if len(updates) > 0 {
					resp.Time = toTimestampPB(lastTime)
				}
				if err := s.Send(resp); err != nil {
					return streamSendErr(err)
				}
				updates = nil
			}
			continue
		}
		var change MongoUpdate
		if err := changeStream.Decode(&change); err != nil {
			return connect.NewError(connect.CodeInternal, fmt.Errorf("failed to decode change stream event: %w", err))
		}

		update, err := convertChangeStreamEventToUpdate(change, c.settings.FullDocumentKey)
		if err != nil {
			return connect.NewError(connect.CodeInternal, fmt.Errorf("failed to convert change stream event: %w", err))
		}

		db := change.NS["db"].(string)
		col := change.NS["coll"].(string)
		newNamespace := fmt.Sprintf("%v.%v", db, col)

		if currentNamespace == "" || currentNamespace != newNamespace {
			if len(updates) > 0 {
				err := s.Send(&adiomv1.StreamUpdatesResponse{
					Updates:    updates,
					Namespace:  currentNamespace,
					NextCursor: lastResumeToken,
					Time:       toTimestampPB(lastTime),
				})
				if err != nil {
					return streamSendErr(err)
				}
			}
			currentNamespace = newNamespace
			updates = nil
		}

		updates = append(updates, update)
		lastResumeToken = changeStream.ResumeToken()
		if change.ClusterTime != nil {
			lastTime = *change.ClusterTime
		}

		if changeStream.RemainingBatchLength() == 0 {
			err := s.Send(&adiomv1.StreamUpdatesResponse{
				Updates:    updates,
				Namespace:  currentNamespace,
				NextCursor: lastResumeToken,
				Time:       toTimestampPB(lastTime),
			})
			if err != nil {
				return streamSendErr(err)
			}
			updates = nil
		}
	}
	if changeStream.Err() != nil {
		if errors.Is(changeStream.Err(), context.Canceled) {
			return nil
		}
		return connect.NewError(connect.CodeInternal, changeStream.Err())
	}

	if len(updates) > 0 {
		err := s.Send(&adiomv1.StreamUpdatesResponse{
			Updates:    updates,
			Namespace:  currentNamespace,
			NextCursor: lastResumeToken,
			Time:       toTimestampPB(lastTime),
		})
		if err != nil {
			return streamSendErr(err)
		}
	}

	return connect.NewError(connect.CodeInternal, errors.New("change stream was closed by the server"))
}

func streamSendErr(err error) error {
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return connect.NewError(connect.CodeInternal, err)
}

type indexedDocument struct {
	document bson.Raw
	index    uint32
}

// insertBatchOverwrite inserts a batch and overwrites existing documents on
// duplicate-key errors. When partialResults is true, errors that MongoDB ties
// to specific operations are returned as item failures. Otherwise the first
// such error fails the batch. Errors with ambiguous outcomes always fail the
// batch.
func insertBatchOverwrite(ctx context.Context, collection *mongo.Collection, documents []indexedDocument, skipBatchOverwrite, partialResults, deleteUniqueConflicts bool, shardKeyFields func() ([]string, error)) ([]*adiomv1.ItemFailure, error) {
	if len(documents) == 0 {
		return nil, nil
	}

	rawDocuments := make([]interface{}, len(documents))
	for i, document := range documents {
		rawDocuments[i] = document.document
	}

	_, insertErr := collection.InsertMany(ctx, rawDocuments, options.InsertMany().SetOrdered(false))
	if insertErr == nil {
		return nil, nil
	}

	var bulkErr mongo.BulkWriteException
	if !errors.As(insertErr, &bulkErr) {
		if isBSONObjectTooLargeError(insertErr) {
			if len(documents) == 1 {
				if partialResults {
					return []*adiomv1.ItemFailure{{Index: documents[0].index, Message: insertErr.Error()}}, nil
				}
				return nil, insertErr
			}
			mid := len(documents) / 2
			first, err := insertBatchOverwrite(ctx, collection, documents[:mid], skipBatchOverwrite, partialResults, deleteUniqueConflicts, shardKeyFields)
			if err != nil {
				return nil, err
			}
			second, err := insertBatchOverwrite(ctx, collection, documents[mid:], skipBatchOverwrite, partialResults, deleteUniqueConflicts, shardKeyFields)
			if err != nil {
				return nil, err
			}
			return append(first, second...), nil
		}
		return nil, insertErr
	}
	if bulkErr.WriteConcernError != nil {
		slog.Error(fmt.Sprintf("Write concern error inserting into collection: %v", bulkErr.WriteConcernError))
		return nil, insertErr
	}
	if len(bulkErr.WriteErrors) == 0 {
		return nil, insertErr
	}

	failures := make([]*adiomv1.ItemFailure, 0, len(bulkErr.WriteErrors))
	replacements := make([]mongo.WriteModel, 0, len(bulkErr.WriteErrors))
	replacementDocuments := make([]indexedDocument, 0, len(bulkErr.WriteErrors))
	for _, writeErr := range bulkErr.WriteErrors {
		if writeErr.Index < 0 || writeErr.Index >= len(documents) {
			return nil, fmt.Errorf("MongoDB returned out-of-range insert error index %d for %d documents", writeErr.Index, len(documents))
		}
		document := documents[writeErr.Index]
		if mongo.IsDuplicateKeyError(writeErr) {
			if skipBatchOverwrite {
				continue
			}
			fields, err := shardKeyFields()
			if err != nil {
				return nil, err
			}
			filter, err := shardKeyFilter(document.document, bson.D{{Key: "_id", Value: document.document.Lookup("_id")}}, fields)
			if err != nil {
				return nil, err
			}
			replacements = append(replacements, mongo.NewReplaceOneModel().SetFilter(filter).SetReplacement(document.document).SetUpsert(true))
			replacementDocuments = append(replacementDocuments, document)
			continue
		}
		if !partialResults {
			if slices.Contains(transientServerCodes, writeErr.Code) {
				slog.Warn(fmt.Sprintf("Transient failure to insert document into collection: %v", writeErr.WriteError))
			} else {
				slog.Error(fmt.Sprintf("Failure to insert document into collection: %v", writeErr.WriteError))
			}
			return nil, insertErr
		}
		failures = append(failures, &adiomv1.ItemFailure{Index: document.index, Message: writeErr.Error()})
	}

	if len(replacements) == 0 {
		return failures, nil
	}

	res, replaceErr := collection.BulkWrite(ctx, replacements, options.BulkWrite().SetOrdered(false))
	if res != nil && res.UpsertedCount > 0 {
		slog.Error(fmt.Sprintf("Overwrite inserted %d documents instead of replacing duplicates in %v (shard key mismatch?): %v", res.UpsertedCount, collection.Name(), slices.Collect(maps.Values(res.UpsertedIDs))[:min(10, len(res.UpsertedIDs))]))
	}
	if replaceErr != nil && deleteUniqueConflicts {
		replaceErr = retryAfterDeletingStaleUniqueHolders(ctx, collection, replacements, false, replaceErr)
	}
	if replaceErr == nil {
		return failures, nil
	}
	slog.Error(fmt.Sprintf("Failed to overwrite documents in collection: %v", replaceErr))
	if owErr, ok := errors.AsType[mongo.BulkWriteException](replaceErr); ok {
		for _, we := range owErr.WriteErrors[:min(10, len(owErr.WriteErrors))] {
			if we.Index < 0 || we.Index >= len(replacements) {
				continue
			}
			slog.Error(fmt.Sprintf("Overwrite failed for %v: code %d: %v", replacements[we.Index].(*mongo.ReplaceOneModel).Filter, we.Code, we.Message))
		}
	}
	if !partialResults {
		return nil, replaceErr
	}
	var replaceBulkErr mongo.BulkWriteException
	if !errors.As(replaceErr, &replaceBulkErr) || replaceBulkErr.WriteConcernError != nil || len(replaceBulkErr.WriteErrors) == 0 {
		return nil, replaceErr
	}
	for _, writeErr := range replaceBulkErr.WriteErrors {
		if writeErr.Index < 0 || writeErr.Index >= len(replacementDocuments) {
			return nil, fmt.Errorf("MongoDB returned out-of-range replacement error index %d for %d documents", writeErr.Index, len(replacementDocuments))
		}
		document := replacementDocuments[writeErr.Index]
		failures = append(failures, &adiomv1.ItemFailure{Index: document.index, Message: writeErr.Error()})
	}
	return failures, nil
}

func normalizeWriteFailures(failures []*adiomv1.ItemFailure) []*adiomv1.ItemFailure {
	slices.SortFunc(failures, func(a, b *adiomv1.ItemFailure) int {
		if a.GetIndex() < b.GetIndex() {
			return -1
		}
		if a.GetIndex() > b.GetIndex() {
			return 1
		}
		return 0
	})
	if len(failures) < 2 {
		return failures
	}

	normalized := failures[:1]
	for _, failure := range failures[1:] {
		if failure.GetIndex() == normalized[len(normalized)-1].GetIndex() {
			continue
		}
		normalized = append(normalized, failure)
	}
	return normalized
}

func shardKeyFilter(doc bson.Raw, idFilter bson.D, shardKeyFields []string) (bson.D, error) {
	filter := slices.Clone(idFilter)
	for _, field := range shardKeyFields {
		if slices.ContainsFunc(idFilter, func(e bson.E) bool { return e.Key == field }) {
			continue
		}
		v, err := doc.LookupErr(strings.Split(field, ".")...)
		if err != nil {
			return nil, fmt.Errorf("document %v has no shard key field %v: %w", idFilter, field, err)
		}
		filter = append(filter, bson.E{Key: field, Value: v})
	}
	return filter, nil
}

func (c *conn) cosmosShardKeyFields(ctx context.Context, col *mongo.Collection) ([]string, error) {
	if !c.settings.CosmosSink {
		return nil, nil
	}
	ns := col.Database().Name() + "." + col.Name()
	if fields, ok := c.shardKeyFields.Load(ns); ok {
		return fields.([]string), nil
	}
	var res struct {
		ShardKeyDefinition bson.M `bson:"shardKeyDefinition"`
	}
	if err := col.Database().RunCommand(ctx, bson.D{{Key: "customAction", Value: "GetCollection"}, {Key: "collection", Value: col.Name()}}).Decode(&res); err != nil {
		return nil, fmt.Errorf("get shard key of %v: %w", ns, err)
	}
	fields := slices.DeleteFunc(slices.Sorted(maps.Keys(res.ShardKeyDefinition)), func(f string) bool { return f == "_id" })
	c.shardKeyFields.Store(ns, fields)
	return fields, nil
}

// WriteData implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) WriteData(ctx context.Context, r *connect.Request[adiomv1.WriteDataRequest]) (*connect.Response[adiomv1.WriteDataResponse], error) {
	col, _, ok := GetCol(c.client, r.Msg.GetNamespace())
	if !ok {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("namespace should be fully qualified"))
	}
	partialResults := r.Msg.GetPartialWriteResults()
	shardKeyFields := sync.OnceValues(func() ([]string, error) { return c.cosmosShardKeyFields(ctx, col) })
	var failures []*adiomv1.ItemFailure
	var batch []indexedDocument
	flush := func() error {
		currentBatch := batch
		batch = nil
		batchFailures, err := insertBatchOverwrite(ctx, col, currentBatch, c.settings.SkipBatchOverwrite, partialResults, c.settings.DeleteUniqueConflicts, shardKeyFields)
		if err != nil {
			if !partialResults || ctx.Err() != nil {
				return err
			}
			message := fmt.Sprintf("batch write failed: %v", err)
			for _, document := range currentBatch {
				failures = append(failures, &adiomv1.ItemFailure{Index: document.index, Message: message})
			}
			return nil
		}
		failures = append(failures, batchFailures...)
		return nil
	}

	for i, data := range r.Msg.GetData() {
		doc, err := c.cosmosValidTTL(r.Msg.GetNamespace(), bson.Raw(data), nil)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
		batch = append(batch, indexedDocument{document: doc, index: uint32(i)}) // #nosec G115 -- protobuf messages cannot contain more than uint32 items
		if c.settings.WriterMaxBatchSize > 0 && len(batch) >= c.settings.WriterMaxBatchSize {
			if err := flush(); err != nil {
				if !errors.Is(err, context.Canceled) {
					slog.Error(fmt.Sprintf("Failed to insert batch: %v", err))
				}
				return nil, maybeUnavailableError(err)
			}
		}
	}
	if len(batch) > 0 {
		if err := flush(); err != nil {
			if !errors.Is(err, context.Canceled) {
				slog.Error(fmt.Sprintf("Failed to insert batch: %v", err))
			}
			return nil, maybeUnavailableError(err)
		}
	}
	return connect.NewResponse(&adiomv1.WriteDataResponse{Failures: normalizeWriteFailures(failures)}), nil
}

func (c *conn) buildIdFilter(update *adiomv1.Update) (bson.D, string, error) {
	var idFilter bson.D
	var normalized []*adiomv1.BsonValue
	if len(update.GetId()) == 0 {
		return nil, "", fmt.Errorf("err update with unexpected empty id")
	}
	for _, idPart := range update.GetId() {
		key := idPart.GetName()
		// For backwards compatibility- we used to pass no key
		if key == "" {
			key = "_id"
		}
		if !c.settings.FullDocumentKey && key != "_id" {
			continue
		}
		typ := bson.Type(idPart.GetType())
		idFilter = append(idFilter, bson.E{Key: key, Value: bson.RawValue{Type: typ, Value: idPart.GetData()}})
		normalized = append(normalized, &adiomv1.BsonValue{Name: key, Type: idPart.GetType(), Data: idPart.GetData()})
	}
	if len(idFilter) == 0 {
		return nil, "", fmt.Errorf("err with _id not found- enable full-document-key or ensure an id part has the name _id: %v", update.GetId())
	}
	return idFilter, util.BsonIdKey(normalized), nil
}

func stripIdFields(raw bson.Raw, idFilter bson.D) (bson.Raw, error) {
	idKeys := make(map[string]struct{}, len(idFilter))
	for _, e := range idFilter {
		idKeys[e.Key] = struct{}{}
	}
	elems, err := raw.Elements()
	if err != nil {
		return nil, fmt.Errorf("stripIdFields: malformed bson: %w", err)
	}
	filtered := make(bson.D, 0, len(elems))
	stripped := false
	for _, elem := range elems {
		if _, ok := idKeys[elem.Key()]; ok {
			stripped = true
			continue
		}
		filtered = append(filtered, bson.E{Key: elem.Key(), Value: elem.Value()})
	}
	if !stripped {
		return raw, nil
	}
	if len(filtered) == 0 {
		return nil, nil
	}
	out, err := bson.Marshal(filtered)
	if err != nil {
		return nil, fmt.Errorf("stripIdFields: marshal filtered doc: %w", err)
	}
	return out, nil
}

func (c *conn) cosmosValidTTL(namespace string, raw bson.Raw, idFilter bson.D) (bson.Raw, error) {
	if !c.settings.CosmosSink || raw == nil {
		return raw, nil
	}
	ttl, err := raw.LookupErr("ttl")
	if err != nil {
		return raw, nil
	}
	if n, ok := ttl.AsFloat64OK(); !ok || n != math.Trunc(n) || n > 0 || n == -1 {
		return raw, nil
	}
	elems, err := raw.Elements()
	if err != nil {
		return nil, fmt.Errorf("cosmosValidTTL: malformed bson: %w", err)
	}
	doc := make(bson.D, 0, len(elems))
	for _, elem := range elems {
		if elem.Key() == "ttl" {
			doc = append(doc, bson.E{Key: "ttl", Value: int32(-1)})
			continue
		}
		doc = append(doc, bson.E{Key: elem.Key(), Value: elem.Value()})
	}
	out, err := bson.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("cosmosValidTTL: marshal doc: %w", err)
	}
	id := any(idFilter)
	if idFilter == nil {
		id = raw.Lookup("_id")
	}
	if count := c.replacedTTLDocs.Add(1); count <= 100 || count%10000 == 0 {
		slog.Warn(fmt.Sprintf("Replacing ttl %v rejected by Cosmos DB with -1 in %s, id %v (%d replacements so far)", ttl, namespace, id, count))
	}
	return out, nil
}

type indexedWriteModel struct {
	model   mongo.WriteModel
	indices []uint32
	idKey   string
}

func (c *conn) buildWriteModel(namespace string, update *adiomv1.Update) (mongo.WriteModel, string, error) {
	idFilter, idKey, err := c.buildIdFilter(update)
	if err != nil {
		return nil, "", err
	}

	switch update.GetType() {
	case adiomv1.UpdateType_UPDATE_TYPE_INSERT, adiomv1.UpdateType_UPDATE_TYPE_UPDATE:
		replacement, err := c.cosmosValidTTL(namespace, bson.Raw(update.GetData()), idFilter)
		if err != nil {
			return nil, "", err
		}
		return mongo.NewReplaceOneModel().SetFilter(idFilter).SetReplacement(replacement).SetUpsert(true), idKey, nil
	case adiomv1.UpdateType_UPDATE_TYPE_DELETE:
		return mongo.NewDeleteOneModel().SetFilter(idFilter), idKey, nil
	case adiomv1.UpdateType_UPDATE_TYPE_PARTIAL_UPDATE:
		theUpdate := bson.M{}
		if update.GetData() != nil {
			stripped, err := stripIdFields(bson.Raw(update.GetData()), idFilter)
			if err != nil {
				return nil, "", err
			}
			stripped, err = c.cosmosValidTTL(namespace, stripped, idFilter)
			if err != nil {
				return nil, "", err
			}
			if len(stripped) > 0 {
				theUpdate["$set"] = stripped
			}
		}
		if len(update.GetPartialUpdateUnset()) > 0 {
			idKeys := make(map[string]struct{}, len(idFilter))
			for _, e := range idFilter {
				idKeys[e.Key] = struct{}{}
			}
			inner := bson.M{}
			for _, field := range update.GetPartialUpdateUnset() {
				if _, isID := idKeys[field]; isID {
					continue
				}
				inner[field] = 1
			}
			if len(inner) > 0 {
				theUpdate["$unset"] = inner
			}
		}
		if len(theUpdate) == 0 {
			return nil, idKey, nil
		}
		return mongo.NewUpdateOneModel().SetFilter(idFilter).SetUpdate(theUpdate).SetUpsert(true), idKey, nil
	default:
		return nil, idKey, nil
	}
}

// buildIndexedBulkModels converts updates into models while retaining the
// original request indices represented by each model.
func (c *conn) buildIndexedBulkModels(namespace string, updates []*adiomv1.Update) ([]indexedWriteModel, bool, error) {
	type compiledUpdate struct {
		model mongo.WriteModel
		idKey string
		typ   adiomv1.UpdateType
		index uint32
	}

	compiled := make([]compiledUpdate, 0, len(updates))
	for i, update := range updates {
		model, idKey, err := c.buildWriteModel(namespace, update)
		if err != nil {
			return nil, false, err
		}
		compiled = append(compiled, compiledUpdate{model: model, idKey: idKey, typ: update.GetType(), index: uint32(i)}) // #nosec G115 -- protobuf messages cannot contain more than uint32 items
	}

	var models []indexedWriteModel
	var orderedModels []indexedWriteModel
	seen := map[string]adiomv1.UpdateType{}
	superseded := map[string][]uint32{}

	for i := len(compiled) - 1; i >= 0; i-- {
		update := compiled[i]
		lastType, found := seen[update.idKey]
		isNew := !found
		if isNew {
			seen[update.idKey] = update.typ
			lastType = update.typ
		}

		if update.model == nil {
			continue
		}
		indexed := indexedWriteModel{model: update.model, indices: []uint32{update.index}, idKey: update.idKey}
		if isNew {
			models = append(models, indexed)
		} else if lastType == adiomv1.UpdateType_UPDATE_TYPE_PARTIAL_UPDATE {
			orderedModels = append(orderedModels, indexed)
		} else {
			superseded[update.idKey] = append(superseded[update.idKey], update.index)
		}
	}

	for i := range models {
		models[i].indices = append(models[i].indices, superseded[models[i].idKey]...)
	}
	if len(orderedModels) > 0 {
		slices.Reverse(orderedModels)
		return append(orderedModels, models...), true, nil
	}
	return models, false, nil
}

// buildBulkModels converts a sequence of updates into a slice of mongo.WriteModel
// ready for BulkWrite, along with whether the batch must run ordered.
func (c *conn) buildBulkModels(namespace string, updates []*adiomv1.Update) ([]mongo.WriteModel, bool, error) {
	indexed, ordered, err := c.buildIndexedBulkModels(namespace, updates)
	if err != nil {
		return nil, false, err
	}
	models := make([]mongo.WriteModel, len(indexed))
	for i, model := range indexed {
		models[i] = model.model
	}
	return models, ordered, nil
}

func failuresFromBulkWrite(err error, models []indexedWriteModel, ordered bool) ([]*adiomv1.ItemFailure, bool) {
	var bulkErr mongo.BulkWriteException
	if !errors.As(err, &bulkErr) || bulkErr.WriteConcernError != nil || len(bulkErr.WriteErrors) == 0 {
		return nil, false
	}

	var failures []*adiomv1.ItemFailure
	if ordered {
		firstFailed := len(models)
		message := err.Error()
		for _, writeErr := range bulkErr.WriteErrors {
			if writeErr.Index < 0 || writeErr.Index >= len(models) {
				return nil, false
			}
			if writeErr.Index < firstFailed {
				firstFailed = writeErr.Index
				message = writeErr.Error()
			}
		}
		for i := firstFailed; i < len(models); i++ {
			failureMessage := "not attempted after an earlier ordered write failed"
			if i == firstFailed {
				failureMessage = message
			}
			for _, index := range models[i].indices {
				failures = append(failures, &adiomv1.ItemFailure{Index: index, Message: failureMessage})
			}
		}
		return failures, true
	}

	for _, writeErr := range bulkErr.WriteErrors {
		if writeErr.Index < 0 || writeErr.Index >= len(models) {
			return nil, false
		}
		for _, index := range models[writeErr.Index].indices {
			failures = append(failures, &adiomv1.ItemFailure{Index: index, Message: writeErr.Error()})
		}
	}
	return failures, true
}

type uniqueConflict struct {
	position     int
	newID        any
	keyPattern   bson.Raw
	holderFilter bson.D
	indexName    string
}

// retryAfterDeletingStaleUniqueHolders handles a bulk write that failed only
// because other destination documents hold unique key values its documents
// need. The source enforces the same unique index, so it no longer has those
// documents: each one is deleted and the failed writes run once more. Any
// other failure is returned unchanged, and a failed retry is returned with its
// write error indexes pointing into models.
func retryAfterDeletingStaleUniqueHolders(ctx context.Context, col *mongo.Collection, models []mongo.WriteModel, ordered bool, writeErr error) error {
	bulkErr, ok := errors.AsType[mongo.BulkWriteException](writeErr)
	if !ok || bulkErr.WriteConcernError != nil || len(bulkErr.WriteErrors) == 0 {
		return writeErr
	}
	conflicts := make([]uniqueConflict, 0, len(bulkErr.WriteErrors))
	for _, we := range bulkErr.WriteErrors {
		conflict, ok := uniqueConflictFrom(we, models)
		if !ok {
			return writeErr
		}
		conflicts = append(conflicts, conflict)
	}
	ns := col.Database().Name() + "." + col.Name()
	indexNames, err := plainUniqueIndexNames(ctx, col)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to list indexes of %v to resolve a unique index conflict: %v", ns, err))
		return writeErr
	}
	for i := range conflicts {
		if conflicts[i].indexName, ok = indexNames[string(conflicts[i].keyPattern)]; !ok {
			return writeErr
		}
	}

	positions := make([]int, 0, len(conflicts))
	for _, conflict := range conflicts {
		projection := options.FindOneAndDelete().SetProjection(bson.D{{Key: "_id", Value: 1}})
		deleted, err := col.FindOneAndDelete(ctx, conflict.holderFilter, projection).Raw()
		if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
			slog.Error(fmt.Sprintf("Failed to delete the stale document holding unique index %v in %v: %v", conflict.indexName, ns, err))
			return writeErr
		}
		if err == nil {
			slog.Warn(fmt.Sprintf("Deleted stale document %v from %v to resolve a conflict on unique index %v with document %v", deleted.Lookup("_id"), ns, conflict.indexName, conflict.newID))
		}
		positions = append(positions, conflict.position)
	}
	slices.Sort(positions)
	positions = slices.Compact(positions)
	if ordered {
		// An ordered batch stopped at its first failure, so the writes after it never ran.
		for position := positions[0] + 1; position < len(models); position++ {
			positions = append(positions, position)
		}
		positions = slices.Compact(positions)
	}
	retry := make([]mongo.WriteModel, len(positions))
	for i, position := range positions {
		retry[i] = models[position]
	}
	_, retryErr := col.BulkWrite(ctx, retry, options.BulkWrite().SetOrdered(ordered))
	retryBulkErr, ok := errors.AsType[mongo.BulkWriteException](retryErr)
	if !ok {
		return retryErr
	}
	for i, we := range retryBulkErr.WriteErrors {
		if we.Index >= 0 && we.Index < len(positions) {
			retryBulkErr.WriteErrors[i].Index = positions[we.Index]
		}
	}
	return retryBulkErr
}

func uniqueConflictFrom(we mongo.BulkWriteError, models []mongo.WriteModel) (uniqueConflict, bool) {
	if !mongo.IsDuplicateKeyError(we) || we.Index < 0 || we.Index >= len(models) {
		return uniqueConflict{}, false
	}
	keyPattern, ok := we.Raw.Lookup("keyPattern").DocumentOK()
	if !ok {
		return uniqueConflict{}, false
	}
	if _, err := keyPattern.LookupErr("_id"); err == nil {
		return uniqueConflict{}, false
	}
	// With a collation the server reports a collation key, not a value a filter can match.
	if _, err := we.Raw.LookupErr("collation"); err == nil {
		return uniqueConflict{}, false
	}
	keyValue, ok := we.Raw.Lookup("keyValue").DocumentOK()
	if !ok {
		return uniqueConflict{}, false
	}
	keyElements, err := keyValue.Elements()
	if err != nil || len(keyElements) == 0 {
		return uniqueConflict{}, false
	}
	var filter any
	switch model := models[we.Index].(type) {
	case *mongo.ReplaceOneModel:
		filter = model.Filter
	case *mongo.UpdateOneModel:
		filter = model.Filter
	}
	idFilter, _ := filter.(bson.D)
	idIndex := slices.IndexFunc(idFilter, func(e bson.E) bool { return e.Key == "_id" })
	if idIndex < 0 {
		return uniqueConflict{}, false
	}
	newID := idFilter[idIndex].Value
	holderFilter := bson.D{{Key: "_id", Value: bson.D{{Key: "$ne", Value: newID}}}}
	for _, element := range keyElements {
		holderFilter = append(holderFilter, bson.E{Key: element.Key(), Value: element.Value()})
	}
	return uniqueConflict{position: we.Index, newID: newID, keyPattern: keyPattern, holderFilter: holderFilter}, true
}

// plainUniqueIndexNames maps the key pattern of each unique index to its name,
// leaving out partial and collated indexes: a filter by key value could match
// a document such an index does not hold.
func plainUniqueIndexNames(ctx context.Context, col *mongo.Collection) (map[string]string, error) {
	cursor, err := col.Indexes().List(ctx)
	if err != nil {
		return nil, err
	}
	var indexes []struct {
		Name      string   `bson:"name"`
		Key       bson.Raw `bson:"key"`
		Unique    bool     `bson:"unique"`
		Partial   bson.Raw `bson:"partialFilterExpression"`
		Collation bson.Raw `bson:"collation"`
	}
	if err := cursor.All(ctx, &indexes); err != nil {
		return nil, err
	}
	names := map[string]string{}
	var excluded []string
	for _, index := range indexes {
		if index.Unique && len(index.Partial) == 0 && len(index.Collation) == 0 {
			names[string(index.Key)] = index.Name
		} else {
			excluded = append(excluded, string(index.Key))
		}
	}
	for _, key := range excluded {
		delete(names, key)
	}
	return names, nil
}

// WriteUpdates implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) WriteUpdates(ctx context.Context, r *connect.Request[adiomv1.WriteUpdatesRequest]) (*connect.Response[adiomv1.WriteUpdatesResponse], error) {
	col, _, ok := GetCol(c.client, r.Msg.GetNamespace())
	if !ok {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("namespace should be fully qualified"))
	}
	indexedModels, ordered, err := c.buildIndexedBulkModels(r.Msg.GetNamespace(), r.Msg.GetUpdates())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	if len(indexedModels) == 0 {
		return connect.NewResponse(&adiomv1.WriteUpdatesResponse{}), nil
	}

	models := make([]mongo.WriteModel, len(indexedModels))
	for i, model := range indexedModels {
		models[i] = model.model
	}
	_, err = col.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(ordered))
	if err != nil && c.settings.DeleteUniqueConflicts {
		err = retryAfterDeletingStaleUniqueHolders(ctx, col, models, ordered, err)
	}
	if err != nil {
		if r.Msg.GetPartialWriteResults() {
			if failures, attributable := failuresFromBulkWrite(err, indexedModels, ordered); attributable {
				return connect.NewResponse(&adiomv1.WriteUpdatesResponse{Failures: normalizeWriteFailures(failures)}), nil
			}
		}
		if !errors.Is(err, context.Canceled) {
			slog.Error(fmt.Sprintf("Failed to insert bulk updates: %v", err))
		}
		return nil, maybeUnavailableError(err)
	}

	return connect.NewResponse(&adiomv1.WriteUpdatesResponse{}), nil
}

func MongoClient(ctx context.Context, settings ConnectorSettings) (*mongo.Client, error) {
	setDefault(&settings.ServerConnectTimeout, 10*time.Second)
	setDefault(&settings.PingTimeout, 2*time.Second)

	// Connect to the MongoDB instance
	clientOptions := moptions.Client().SetAppName("dsync").ApplyURI(settings.ConnectionString).SetConnectTimeout(settings.ServerConnectTimeout)
	client, err := mongo.Connect(clientOptions)
	if err != nil {
		return nil, InvalidClientOptionsError(clientOptions)
	}

	// Check the connection
	ctxPing, cancelPing := context.WithTimeout(ctx, settings.PingTimeout)
	defer cancelPing()
	err = client.Ping(ctxPing, nil)
	if err != nil {
		return nil, err
	}

	return client, err
}

func (c *conn) Teardown() {
	c.cancel()
	c.buffersMutex.Lock()
	for id, buf := range c.buffers {
		buf.cleanup.Stop()
		delete(c.buffers, id)
	}
	c.buffersMutex.Unlock()
	_ = c.client.Disconnect(context.Background())
}

func NewConn(connSettings ConnectorSettings) (adiomv1connect.ConnectorServiceHandler, error) {
	setDefault(&connSettings.TargetDocCountPerPartition, 512*1000)
	setDefault(&connSettings.SampleFactor, 1)
	setDefault(&connSettings.NamespaceFanout, defaultNamespaceFanoutLimit)
	setDefault(&connSettings.DocumentDBSamplingFanout, defaultDocumentDBSamplingFanoutLimit)
	client, err := MongoClient(context.Background(), connSettings)
	if err != nil {
		slog.Error(fmt.Sprintf("unable to connect to mongo client: %v", err))
		return nil, err
	}
	return NewConnWithClient(client, connSettings)
}

func NewConnWithClient(client *mongo.Client, settings ConnectorSettings) (adiomv1connect.ConnectorServiceHandler, error) {
	setDefault(&settings.PageBufferTTL, time.Hour)
	query, err := stringToQuery(settings.Query)
	if err != nil {
		return nil, errors.New("invalid --initial-sync-query, expected Extended JSON")
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &conn{
		client:   client,
		settings: settings,
		flavor:   GetMongoFlavor(settings.ConnectionString),
		ctx:      ctx,
		cancel:   cancel,
		buffers:  map[int64]buffer{},
		query:    query,
	}, nil
}

func (c *conn) changeStreamOpts(cursor []byte) *moptions.ChangeStreamOptionsBuilder {
	opts := moptions.ChangeStream()
	if c.flavor == FlavorDocumentDB {
		opts.SetResumeAfter(bson.Raw(cursor))
	} else {
		opts.SetStartAfter(bson.Raw(cursor))
	}
	return opts
}

const cosmosTooManyRequestsCode = 16500

var transientServerCodes = []int{6, 7, 50, 64, 89, 91, 134, 189, 262, 9001, 10107, 11600, 11602, 13435, 13436, cosmosTooManyRequestsCode}

var transientErrorLabels = []string{"RetryableWriteError", "NetworkError"}

func IsTransientError(err error) bool {
	if _, isNetErr := errors.AsType[net.Error](err); isNetErr {
		return true
	}
	if writeErr, ok := errors.AsType[mongo.WriteException](err); ok {
		return isTransientWriteError(writeErr.WriteErrors, writeErr.WriteConcernError, writeErr.Labels)
	}
	if bulkErr, ok := errors.AsType[mongo.BulkWriteException](err); ok {
		writeErrors := make([]mongo.WriteError, len(bulkErr.WriteErrors))
		for i, we := range bulkErr.WriteErrors {
			writeErrors[i] = we.WriteError
		}
		return isTransientWriteError(writeErrors, bulkErr.WriteConcernError, bulkErr.Labels)
	}
	if serverErr, ok := errors.AsType[mongo.ServerError](err); ok && (slices.ContainsFunc(transientErrorLabels, serverErr.HasErrorLabel) || slices.ContainsFunc(transientServerCodes, serverErr.HasErrorCode)) {
		return true
	}
	return mongo.IsNetworkError(err) || mongo.IsTimeout(err) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

func isTransientWriteError(writeErrors []mongo.WriteError, concernErr *mongo.WriteConcernError, labels []string) bool {
	var codes []int
	for _, we := range writeErrors {
		if !mongo.IsDuplicateKeyError(we) {
			codes = append(codes, we.Code)
		}
	}
	if concernErr != nil {
		codes = append(codes, concernErr.Code)
	}
	if slices.ContainsFunc(codes, func(code int) bool { return !slices.Contains(transientServerCodes, code) }) {
		return false
	}
	return len(codes) > 0 || slices.ContainsFunc(labels, func(label string) bool { return slices.Contains(transientErrorLabels, label) })
}

func maybeUnavailableError(err error) error {
	if errors.Is(err, context.Canceled) {
		return connect.NewError(connect.CodeCanceled, err)
	}
	if IsTransientError(err) {
		return connect.NewError(connect.CodeUnavailable, err)
	}
	return connect.NewError(connect.CodeInternal, err)
}

// GetByIds implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GetByIds(ctx context.Context, r *connect.Request[adiomv1.GetByIdsRequest]) (*connect.Response[adiomv1.GetByIdsResponse], error) {
	col, _, ok := GetCol(c.client, r.Msg.GetNamespace())
	if !ok {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("namespace should be fully qualified"))
	}

	// TODO: maybe use batch endpoint if we need to optimize
	res := make([]*adiomv1.GetByIdsResponse_ResponseItem, len(r.Msg.GetIds()))
	var eg errgroup.Group
	for i, id := range r.Msg.GetIds() {
		eg.Go(func() error {
			if len(id.GetId()) < 1 {
				res[i] = &adiomv1.GetByIdsResponse_ResponseItem{}
				return nil
			}
			bv := id.GetId()[0]
			rawVal := bson.RawValue{
				Type:  bson.Type(bv.GetType()),
				Value: bv.GetData(),
			}
			v, err := col.FindOne(ctx, bson.M{"_id": rawVal}).Raw()
			if err != nil {
				if errors.Is(err, mongo.ErrNoDocuments) {
					res[i] = &adiomv1.GetByIdsResponse_ResponseItem{}
					return nil
				}
				return fmt.Errorf("err in findone: %w", err)
			}
			res[i] = &adiomv1.GetByIdsResponse_ResponseItem{
				Data: v,
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("err finding ids: %w", err))
	}

	return connect.NewResponse(&adiomv1.GetByIdsResponse{
		Data: res,
	}), nil
}
