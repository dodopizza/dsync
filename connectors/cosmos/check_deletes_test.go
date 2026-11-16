package cosmos

import (
	"context"
	"errors"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/drivertest"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

var testNs = iface.Namespace{Db: testLoc.Database, Col: testLoc.Collection}

func requireReturns(t *testing.T, what string, f func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Cleanup(func() {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
			}
		})
		t.Fatalf("%s blocked", what)
	}
}

func rawKey(t *testing.T, v any) bson.RawValue {
	t.Helper()
	typ, data, err := bson.MarshalValue(v)
	require.NoError(t, err)
	return bson.RawValue{Type: typ, Value: data}
}

func mustValues(vs ...any) []bson.RawValue {
	var out []bson.RawValue
	for _, v := range vs {
		typ, data, err := bson.MarshalValue(v)
		if err != nil {
			panic(err)
		}
		out = append(out, bson.RawValue{Type: typ, Value: data})
	}
	return out
}

func mustKeys(vs ...any) [][]bson.RawValue {
	var out [][]bson.RawValue
	for _, v := range mustValues(vs...) {
		out = append(out, []bson.RawValue{v})
	}
	return out
}

type fakeWitness struct {
	mu             sync.Mutex
	keys           map[iface.Namespace][][]bson.RawValue
	pageSizes      []int
	keyFields      [][]string
	emitted        int
	failAfterPages int
	err            error
}

func (f *fakeWitness) ScanKeys(_ context.Context, ns iface.Namespace, keyFields []string, pageSize int, emit func([][]bson.RawValue) error) error {
	f.mu.Lock()
	f.pageSizes = append(f.pageSizes, pageSize)
	f.keyFields = append(f.keyFields, keyFields)
	f.mu.Unlock()
	all := f.keys[ns]
	for i := 0; i < len(all); i += pageSize {
		if f.failAfterPages > 0 && i/pageSize == f.failAfterPages {
			return f.err
		}
		if err := emit(all[i:min(i+pageSize, len(all))]); err != nil {
			return err
		}
		f.mu.Lock()
		f.emitted++
		f.mu.Unlock()
	}
	return nil
}

func sourceFindReply(docs ...bson.D) bson.D {
	batch := bson.A{}
	for _, d := range docs {
		batch = append(batch, d)
	}
	return bson.D{{Key: "ok", Value: 1}, {Key: "cursor", Value: bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.col"}, {Key: "firstBatch", Value: batch}}}}
}

func idDoc(v any) bson.D { return bson.D{{Key: "_id", Value: v}} }

func passStages() deleteStages {
	return deleteStages{
		scan: func(_ context.Context, _ iface.Namespace, _ []string, emit func([][]bson.RawValue) error) error {
			return emit(mustKeys(int32(1), int32(2)))
		},
		missing: func(_ context.Context, _ iface.Location, _ []string, keys [][]bson.RawValue) ([][]bson.RawValue, int, error) {
			return keys[:1], 0, nil
		},
	}
}

func TestDeletePassReturnsTheMissingKeysAsDeletes(t *testing.T) {
	updates, summary, err := runDeletePass(t.Context(), []iface.Namespace{testNs}, nil, passStages())

	require.NoError(t, err)
	require.Len(t, updates[testLoc], 1)
	require.Equal(t, passSummary{Namespaces: 1, Keys: 2, Deletes: 1}, summary)
}

func TestDeletePassEmitsTheDeleteWhenCountsAreEqualButKeySetsDiffer(t *testing.T) {
	source, _ := recordingClient(t, sourceFindReply(idDoc(int32(1))))
	witness := &fakeWitness{keys: map[iface.Namespace][][]bson.RawValue{testNs: mustKeys(int32(1), int32(2))}}

	updates, summary, err := checkForDeletes(t.Context(), source, witness, []iface.Namespace{testNs}, nil)

	require.NoError(t, err)
	require.Len(t, updates[testLoc], 1)
	require.Equal(t, uint32(bson.TypeInt32), updates[testLoc][0].Id[0].Type)
	require.Equal(t, mustKeys(int32(2))[0][0].Value, updates[testLoc][0].Id[0].Data)
	require.Equal(t, passSummary{Namespaces: 1, Keys: 2, Deletes: 1}, summary)
}

func TestDeletePassWalksEveryNamespace(t *testing.T) {
	other := iface.Namespace{Db: "db2", Col: "col2"}
	var mu sync.Mutex
	var scanned []iface.Namespace
	st := passStages()
	st.scan = func(_ context.Context, ns iface.Namespace, _ []string, emit func([][]bson.RawValue) error) error {
		mu.Lock()
		scanned = append(scanned, ns)
		mu.Unlock()
		return nil
	}

	_, summary, err := runDeletePass(t.Context(), []iface.Namespace{testNs, other}, nil, st)

	require.NoError(t, err)
	require.ElementsMatch(t, []iface.Namespace{testNs, other}, scanned)
	require.Equal(t, passSummary{Namespaces: 2}, summary)
}

func witnessStages(w Witness, pageSize int, missing func([][]bson.RawValue) [][]bson.RawValue) deleteStages {
	return deleteStages{
		scan: witnessScan(w, pageSize),
		missing: func(_ context.Context, _ iface.Location, _ []string, keys [][]bson.RawValue) ([][]bson.RawValue, int, error) {
			return missing(keys), 0, nil
		},
	}
}

func capturingClient(t *testing.T, responses ...bson.D) (*mongo.Client, *[]bson.Raw) {
	t.Helper()
	var mu sync.Mutex
	var finds []bson.Raw
	opts := options.Client().SetRetryReads(false).SetMonitor(&event.CommandMonitor{
		Started: func(_ context.Context, started *event.CommandStartedEvent) {
			if started.CommandName == "find" {
				mu.Lock()
				finds = append(finds, started.Command)
				mu.Unlock()
			}
		},
	})
	opts.Deployment = drivertest.NewMockDeployment(responses...)
	client, err := mongo.Connect(opts)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Disconnect(context.Background())) })
	return client, &finds
}

func TestMongoWitnessEmitsOneSortedPagedFindWithTheKeyProjection(t *testing.T) {
	oid := bson.NewObjectID()
	lastBatch := bson.D{{Key: "ok", Value: 1}, {Key: "cursor", Value: bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.col"}, {Key: "nextBatch", Value: bson.A{idDoc(oid)}}}}}
	client, finds := capturingClient(t, cursorReply("firstBatch", bson.A{idDoc(int32(1)), idDoc(int32(2)), idDoc("a"), idDoc("b")}, nil), lastBatch)
	var pages [][][]bson.RawValue
	var err error

	requireReturns(t, "witness scan", func() {
		err = mongoWitness{client: client}.ScanKeys(t.Context(), testNs, []string{"tenant"}, 2, func(page [][]bson.RawValue) error {
			pages = append(pages, page)
			return nil
		})
	})

	require.NoError(t, err)
	null := bson.RawValue{Type: bson.TypeNull}
	want := [][][]bson.RawValue{
		{{mustValues(int32(1))[0], null}, {mustValues(int32(2))[0], null}},
		{{mustValues("a")[0], null}, {mustValues("b")[0], null}},
		{{mustValues(oid)[0], null}},
	}
	require.Equal(t, want, pages)
	require.Len(t, *finds, 1)
	find := (*finds)[0]
	require.Equal(t, bson.Raw(mustMarshalBSON(t, bson.D{})), find.Lookup("filter").Document())
	require.Equal(t, bson.Raw(mustMarshalBSON(t, bson.D{{Key: "_id", Value: 1}})), find.Lookup("sort").Document())
	require.Equal(t, bson.Raw(mustMarshalBSON(t, bson.D{{Key: "_id", Value: 1}, {Key: "tenant", Value: 1}})), find.Lookup("projection").Document())
	require.EqualValues(t, 2, find.Lookup("batchSize").AsInt64())
}

func TestMongoWitnessAbortsWhenALaterBatchFails(t *testing.T) {
	client, _ := recordingClient(t, cursorReply("firstBatch", bson.A{idDoc(int32(1)), idDoc(int32(2))}, nil), readFailedReply, cursorKilledReply)
	pages := 0
	var err error

	requireReturns(t, "witness scan", func() {
		err = mongoWitness{client: client}.ScanKeys(t.Context(), testNs, nil, 2, func([][]bson.RawValue) error { pages++; return nil })
	})

	require.Error(t, err)
	require.Equal(t, 1, pages)
}

func TestMongoWitnessStopsWhenEmitFails(t *testing.T) {
	boom := errors.New("emit failed")
	client, _ := recordingClient(t, cursorReply("firstBatch", bson.A{idDoc(int32(1)), idDoc(int32(2)), idDoc(int32(3)), idDoc(int32(4))}, nil), cursorKilledReply)
	pages := 0
	var err error

	requireReturns(t, "witness scan", func() {
		err = mongoWitness{client: client}.ScanKeys(t.Context(), testNs, nil, 2, func([][]bson.RawValue) error { pages++; return boom })
	})

	require.ErrorIs(t, err, boom)
	require.Equal(t, 1, pages)
}

func TestMongoWitnessTakesNestedKeyFieldsAndNullsMissingOnes(t *testing.T) {
	doc := bson.D{{Key: "_id", Value: int32(1)}, {Key: "a", Value: bson.D{{Key: "b", Value: int64(7)}}}}
	client, _ := recordingClient(t, sourceFindReply(doc, idDoc(int32(2))))
	var pages [][][]bson.RawValue
	var err error

	requireReturns(t, "witness scan", func() {
		err = mongoWitness{client: client}.ScanKeys(t.Context(), testNs, []string{"a.b", "c"}, 10, func(page [][]bson.RawValue) error {
			pages = append(pages, page)
			return nil
		})
	})

	require.NoError(t, err)
	null := bson.RawValue{Type: bson.TypeNull}
	want := [][][]bson.RawValue{{
		{mustValues(int32(1))[0], mustValues(int64(7))[0], null},
		{mustValues(int32(2))[0], null, null},
	}}
	require.Equal(t, want, pages)
}

func TestDeletePassChecksEveryKeyOnceWhenPagesCrossBsonTypes(t *testing.T) {
	var all []any
	for i := range 3 {
		all = append(all, int32(i))
	}
	for _, s := range []string{"a", "b", "c"} {
		all = append(all, s)
	}
	for range 3 {
		all = append(all, bson.NewObjectID())
	}
	all = append(all, "z")
	witness := &fakeWitness{keys: map[iface.Namespace][][]bson.RawValue{testNs: mustKeys(all...)}}
	var mu sync.Mutex
	checked := map[string]int{}
	st := witnessStages(witness, 4, func(keys [][]bson.RawValue) [][]bson.RawValue {
		mu.Lock()
		defer mu.Unlock()
		for _, k := range keys {
			checked[canonicalTuple(k)]++
		}
		return nil
	})
	var summary passSummary
	var err error

	requireReturns(t, "delete pass", func() {
		_, summary, err = runDeletePass(t.Context(), []iface.Namespace{testNs}, nil, st)
	})

	require.NoError(t, err)
	require.Len(t, checked, len(all))
	for k, n := range checked {
		require.Equal(t, 1, n, "key %q", k)
	}
	require.Equal(t, len(all), summary.Keys)
	require.Equal(t, []int{4}, witness.pageSizes)
}

func TestDeletePassRequestsWitnessPagesOfTheConfiguredSize(t *testing.T) {
	witness := &fakeWitness{keys: map[iface.Namespace][][]bson.RawValue{testNs: mustKeys(int32(1))}}
	client, _ := recordingClient(t, sourceFindReply(idDoc(int32(1))))

	requireReturns(t, "delete pass", func() {
		_, _, err := checkForDeletes(t.Context(), client, witness, []iface.Namespace{testNs}, nil)
		require.NoError(t, err)
	})

	require.Equal(t, []int{witnessPageSize}, witness.pageSizes)
}

func TestDeletePassAbortsWhenTheWitnessFailsAfterTheFirstPage(t *testing.T) {
	boom := errors.New("witness page failed")
	witness := &fakeWitness{keys: map[iface.Namespace][][]bson.RawValue{testNs: mustKeys(int32(0), int32(1), int32(2), int32(3), int32(4))}, failAfterPages: 1, err: boom}
	st := witnessStages(witness, 2, func(keys [][]bson.RawValue) [][]bson.RawValue { return keys })
	var updates map[iface.Location][]*adiomv1.Update
	var err error

	requireReturns(t, "delete pass", func() {
		updates, _, err = runDeletePass(t.Context(), []iface.Namespace{testNs}, nil, st)
	})

	require.ErrorIs(t, err, boom)
	require.ErrorContains(t, err, "db1.col1")
	require.Nil(t, updates)
}

func TestDeletePassStopsTheWitnessScanWhenTheCheckFails(t *testing.T) {
	boom := errors.New("source failed")
	var all []any
	for i := range 20 {
		all = append(all, int32(i))
	}
	witness := &fakeWitness{keys: map[iface.Namespace][][]bson.RawValue{testNs: mustKeys(all...)}}
	st := witnessStages(witness, 2, nil)
	st.missing = func(context.Context, iface.Location, []string, [][]bson.RawValue) ([][]bson.RawValue, int, error) {
		return nil, 0, boom
	}
	var err error

	requireReturns(t, "delete pass", func() {
		_, _, err = runDeletePass(t.Context(), []iface.Namespace{testNs}, nil, st)
	})

	require.ErrorIs(t, err, boom)
	require.Less(t, witness.emitted, 10)
}

func TestDeletePassAbortsWhenTheSourceFindFails(t *testing.T) {
	source, _ := recordingClient(t, readFailedReply)
	witness := &fakeWitness{keys: map[iface.Namespace][][]bson.RawValue{testNs: mustKeys(int32(1))}}
	var updates map[iface.Location][]*adiomv1.Update
	var err error

	requireReturns(t, "delete pass", func() {
		updates, _, err = checkForDeletes(t.Context(), source, witness, []iface.Namespace{testNs}, nil)
	})

	require.ErrorContains(t, err, "db1.col1")
	require.Nil(t, updates)
}

func TestDeletePassAbortsOnAnyStageError(t *testing.T) {
	boom := errors.New("stage failed")
	for name, mutate := range map[string]func(*deleteStages){
		"witness": func(st *deleteStages) {
			st.scan = func(_ context.Context, _ iface.Namespace, _ []string, emit func([][]bson.RawValue) error) error {
				_ = emit(mustKeys(int32(1)))
				return boom
			}
		},
		"source": func(st *deleteStages) {
			st.missing = func(context.Context, iface.Location, []string, [][]bson.RawValue) ([][]bson.RawValue, int, error) {
				return nil, 0, boom
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			st := passStages()
			mutate(&st)
			var updates map[iface.Location][]*adiomv1.Update
			var err error

			requireReturns(t, "delete pass", func() {
				updates, _, err = runDeletePass(t.Context(), []iface.Namespace{testNs, {Db: "db2", Col: "col2"}}, nil, st)
			})

			require.ErrorIs(t, err, boom)
			require.Nil(t, updates)
		})
	}
}

func TestDeletePassDoesNotBlockWhenAStageFailsWithBackedUpProducers(t *testing.T) {
	boom := errors.New("source failed")
	var namespaces []iface.Namespace
	for i := range 50 {
		namespaces = append(namespaces, iface.Namespace{Db: "db", Col: string(rune('a' + i%26))})
	}
	st := deleteStages{
		scan: func(_ context.Context, _ iface.Namespace, _ []string, emit func([][]bson.RawValue) error) error {
			for range 100 {
				if err := emit(mustKeys(int32(1))); err != nil {
					return err
				}
			}
			return nil
		},
		missing: func(context.Context, iface.Location, []string, [][]bson.RawValue) ([][]bson.RawValue, int, error) {
			return nil, 0, boom
		},
	}
	var err error

	requireReturns(t, "delete pass", func() {
		_, _, err = runDeletePass(t.Context(), namespaces, nil, st)
	})

	require.ErrorIs(t, err, boom)
}

func TestDeletePassReturnsASourceErrorWhileTheWitnessScanIsStillBlocked(t *testing.T) {
	boom := errors.New("source failed")
	st := passStages()
	st.scan = func(ctx context.Context, _ iface.Namespace, _ []string, emit func([][]bson.RawValue) error) error {
		if err := emit(mustKeys(int32(1))); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	}
	st.missing = func(context.Context, iface.Location, []string, [][]bson.RawValue) ([][]bson.RawValue, int, error) {
		return nil, 0, boom
	}
	var err error

	requireReturns(t, "delete pass", func() {
		_, _, err = runDeletePass(t.Context(), []iface.Namespace{testNs}, nil, st)
	})

	require.ErrorIs(t, err, boom)
}

func TestDeletePassDoesNotBlockWhenTheConsumerFailsToConvert(t *testing.T) {
	st := passStages()
	st.missing = func(context.Context, iface.Location, []string, [][]bson.RawValue) ([][]bson.RawValue, int, error) {
		return [][]bson.RawValue{{{}}}, 0, nil
	}
	var err error

	requireReturns(t, "delete pass", func() {
		_, _, err = runDeletePass(t.Context(), []iface.Namespace{testNs}, nil, st)
	})

	require.ErrorContains(t, err, "db1.col1")
}

func TestDeletePassReturnsContextErrorWhenCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	st := passStages()
	st.scan = func(ctx context.Context, _ iface.Namespace, _ []string, _ func([][]bson.RawValue) error) error {
		cancel()
		<-ctx.Done()
		return ctx.Err()
	}
	var updates map[iface.Location][]*adiomv1.Update
	var err error

	requireReturns(t, "delete pass", func() {
		updates, _, err = runDeletePass(ctx, []iface.Namespace{testNs}, nil, st)
	})

	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, updates)
}

func TestSourceCheckTreatsKeysOfEquivalentNumericTypesAsPresent(t *testing.T) {
	for name, test := range map[string]struct{ witness, source any }{
		"int64 against int32":  {int64(5), int32(5)},
		"int32 against int64":  {int32(5), int64(5)},
		"integral double":      {float64(5), int32(5)},
		"int against a double": {int64(5), float64(5)},
	} {
		t.Run(name, func(t *testing.T) {
			client, _ := recordingClient(t, sourceFindReply(idDoc(test.source)))

			missing, _, err := missingSourceKeys(t.Context(), client, testLoc, nil, mustKeys(test.witness))

			require.NoError(t, err)
			require.Empty(t, missing)
		})
	}
}

func TestSourceCheckKeepsTheWitnessBytesOfAMissingKey(t *testing.T) {
	client, _ := recordingClient(t, sourceFindReply(idDoc(int32(1))))
	witnessKeys := mustKeys(int32(1), int64(7))

	missing, _, err := missingSourceKeys(t.Context(), client, testLoc, nil, witnessKeys)

	require.NoError(t, err)
	require.Equal(t, witnessKeys[1:], missing)
}

func TestSourceCheckFailsWhenTheSourceReturnsAnIdTheWitnessDoesNotHave(t *testing.T) {
	dec, err := bson.ParseDecimal128("5")
	require.NoError(t, err)
	for name, test := range map[string]struct{ witness, source any }{
		"number for a string":     {"5", int32(5)},
		"decimal128 for an int32": {int32(5), dec},
		"decimal128 inside a document": {
			bson.D{{Key: "a", Value: int32(5)}},
			bson.D{{Key: "a", Value: dec}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			client, _ := recordingClient(t, sourceFindReply(idDoc(test.source)))

			missing, _, err := missingSourceKeys(t.Context(), client, testLoc, nil, mustKeys(test.witness))

			require.ErrorContains(t, err, "db1.col1")
			require.NotContains(t, err.Error(), "Decimal")
			require.Nil(t, missing)
		})
	}
}

func TestDeletePassSendsNoDeletesWhenTheSourceReturnsAnUnknownId(t *testing.T) {
	dec, err := bson.ParseDecimal128("5")
	require.NoError(t, err)
	client, _ := recordingClient(t, sourceFindReply(idDoc(dec)))
	witness := &fakeWitness{keys: map[iface.Namespace][][]bson.RawValue{testNs: mustKeys(int32(5), int32(6))}}
	var updates map[iface.Location][]*adiomv1.Update

	requireReturns(t, "delete pass", func() {
		updates, _, err = checkForDeletes(t.Context(), client, witness, []iface.Namespace{testNs}, nil)
	})

	require.Error(t, err)
	require.Nil(t, updates)
}

func TestSourceCheckRoundTripsEveryIdKind(t *testing.T) {
	oid := bson.NewObjectID()
	ids := []any{oid, "abc", int32(7), int64(1) << 40, bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: "x"}}}
	var docs []bson.D
	for _, id := range ids {
		docs = append(docs, idDoc(id))
	}
	client, _ := recordingClient(t, sourceFindReply(docs...))

	missing, _, err := missingSourceKeys(t.Context(), client, testLoc, nil, mustKeys(ids...))

	require.NoError(t, err)
	require.Empty(t, missing)
}

func TestSourceCheckTreatsAnEmptyFindAsAllMissing(t *testing.T) {
	client, _ := recordingClient(t, sourceFindReply())
	keys := mustKeys(int32(1), int32(2))

	missing, _, err := missingSourceKeys(t.Context(), client, testLoc, nil, keys)

	require.NoError(t, err)
	require.Equal(t, keys, missing)
}

func TestSourceCheckFailsWhenTheCursorFails(t *testing.T) {
	client, _ := recordingClient(t, cursorReply("firstBatch", bson.A{}, nil), readFailedReply, cursorKilledReply)

	missing, _, err := missingSourceKeys(t.Context(), client, testLoc, nil, mustKeys(int32(1)))

	require.ErrorContains(t, err, "db1.col1")
	require.Nil(t, missing)
}

func TestSourceCheckFailsWhenTheFindFails(t *testing.T) {
	client, _ := recordingClient(t, readFailedReply)

	missing, _, err := missingSourceKeys(t.Context(), client, testLoc, nil, mustKeys(int32(1)))

	require.ErrorContains(t, err, "db1.col1")
	require.Nil(t, missing)
}

func TestSourceCheckFailsOnAResultWithoutAnId(t *testing.T) {
	client, _ := recordingClient(t, sourceFindReply(bson.D{{Key: "other", Value: 1}}), cursorKilledReply)

	missing, _, err := missingSourceKeys(t.Context(), client, testLoc, nil, mustKeys(int32(1)))

	require.ErrorContains(t, err, "db1.col1")
	require.Nil(t, missing)
}

func TestCanonicalKeyComparesNumbersByValueAndEverythingElseByTypeAndBytes(t *testing.T) {
	same := [][]any{
		{int32(5), int64(5), float64(5)},
		{int32(-3), int64(-3), float64(-3)},
		{int32(0), int64(0), float64(0), math.Copysign(0, -1)},
		{"a", "a"},
	}
	for _, group := range same {
		first := canonicalKey(rawKey(t, group[0]))
		for _, v := range group[1:] {
			require.Equal(t, first, canonicalKey(rawKey(t, v)), "%v", v)
		}
	}
	distinct := []any{int32(5), 5.5, "5", int32(6), bson.NewObjectID(), bson.D{{Key: "a", Value: int32(5)}}, bson.A{int32(5)}, int64(1) << 62, float64(1 << 62), float64(1<<62) * 4, math.NaN(), math.Inf(1), math.Inf(-1)}
	seen := map[string]any{}
	for _, v := range distinct {
		key := canonicalKey(rawKey(t, v))
		if other, ok := seen[key]; ok && !(v == int64(1)<<62 || v == float64(1<<62)) {
			t.Fatalf("%v and %v share a canonical key", v, other)
		}
		seen[key] = v
	}
	require.Equal(t, canonicalKey(rawKey(t, int64(1)<<62)), canonicalKey(rawKey(t, float64(1<<62))))
	d1 := bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: int32(2)}}
	d2 := bson.D{{Key: "b", Value: int32(2)}, {Key: "a", Value: int32(1)}}
	require.NotEqual(t, canonicalKey(rawKey(t, d1)), canonicalKey(rawKey(t, d2)))
}

func TestStreamUpdatesSurfacesAWitnessErrorAndSendsNoDeletes(t *testing.T) {
	boom := errors.New("witness read failed")
	st := passStages()
	st.scan = func(_ context.Context, _ iface.Namespace, _ []string, emit func([][]bson.RawValue) error) error {
		_ = emit(mustKeys(int32(1)))
		return boom
	}
	requireStreamFailure(t, st, "witness read failed")
}

func TestStreamUpdatesSurfacesASourceCheckErrorAndSendsNoDeletes(t *testing.T) {
	st := passStages()
	st.missing = func(context.Context, iface.Location, []string, [][]bson.RawValue) ([][]bson.RawValue, int, error) {
		return nil, 0, errors.New("source check failed")
	}
	requireStreamFailure(t, st, "source check failed")
}

func requireStreamFailure(t *testing.T, st deleteStages, want string) {
	t.Helper()
	var sent atomic.Int32
	force := make(chan struct{})
	run := idleRun(
		func(*adiomv1.StreamUpdatesResponse) error { sent.Add(1); return nil },
		func(ctx context.Context) (map[iface.Location][]*adiomv1.Update, error) {
			updates, _, err := runDeletePass(ctx, []iface.Namespace{testNs}, nil, st)
			return updates, err
		},
		force,
	)
	done := make(chan error, 1)
	go func() { done <- run.run(t.Context()) }()

	triggerDelete(t, force)

	select {
	case err := <-done:
		require.ErrorContains(t, err, want)
	case <-time.After(5 * time.Second):
		t.Fatal("stream did not fail")
	}
	require.Zero(t, sent.Load())
}

func TestDeletesDoneRunsAfterTheSendsAndNotAfterAFailedCheck(t *testing.T) {
	var mu sync.Mutex
	var order []string
	record := func(what string) { mu.Lock(); order = append(order, what); mu.Unlock() }
	done := make(chan struct{}, 1)
	force := make(chan struct{})
	run := idleRun(func(*adiomv1.StreamUpdatesResponse) error { record("send"); return nil }, oneDelete, force)
	run.deletesDone = func() { record("done"); done <- struct{}{} }
	ctx, cancel := context.WithCancel(t.Context())
	finished := make(chan error, 1)
	go func() { finished <- run.run(ctx) }()

	triggerDelete(t, force)
	awaitSignal(t, done, "deletes done")
	cancel()
	awaitSignal(t, func() <-chan struct{} {
		c := make(chan struct{})
		go func() { <-finished; close(c) }()
		return c
	}(), "run to finish")

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"send", "done"}, order)
}

func TestDeletesDoneDoesNotRunWhenTheCheckFails(t *testing.T) {
	var called atomic.Bool
	force := make(chan struct{})
	run := idleRun(func(*adiomv1.StreamUpdatesResponse) error { return nil }, func(context.Context) (map[iface.Location][]*adiomv1.Update, error) {
		return nil, errors.New("check failed")
	}, force)
	run.deletesDone = func() { called.Store(true) }
	finished := make(chan error, 1)
	go func() { finished <- run.run(t.Context()) }()

	triggerDelete(t, force)

	select {
	case err := <-finished:
		require.ErrorContains(t, err, "check failed")
	case <-time.After(5 * time.Second):
		t.Fatal("run did not fail")
	}
	require.False(t, called.Load())
}

func shardedTuples(vs ...[]any) [][]bson.RawValue {
	var out [][]bson.RawValue
	for _, v := range vs {
		out = append(out, mustValues(v...))
	}
	return out
}

func tupleDoc(id any, kv ...any) bson.D {
	doc := idDoc(id)
	for i := 0; i < len(kv); i += 2 {
		doc = append(doc, bson.E{Key: kv[i].(string), Value: kv[i+1]})
	}
	return doc
}

func TestCanonicalTupleComparesNumbersByValueAndStringsByBoundary(t *testing.T) {
	require.NotEqual(t, canonicalTuple(mustValues("a", "bc")), canonicalTuple(mustValues("ab", "c")))
	require.NotEqual(t, canonicalTuple(mustValues(int32(1), int32(23))), canonicalTuple(mustValues(int32(12), int32(3))))
	require.Equal(t, canonicalTuple(mustValues(int32(1), int32(5))), canonicalTuple(mustValues(int64(1), int64(5))))
}

func TestWitnessScanKeepsEqualIdRunsInOnePageAcrossThePageSize(t *testing.T) {
	keys := shardedTuples([]any{int32(1), "a"}, []any{int32(2), "a"}, []any{int32(2), "b"}, []any{int32(2), "c"}, []any{int32(3), "a"})
	witness := &fakeWitness{keys: map[iface.Namespace][][]bson.RawValue{testNs: keys}}
	var pages [][][]bson.RawValue
	var err error

	requireReturns(t, "witness scan", func() {
		err = witnessScan(witness, 2)(t.Context(), testNs, []string{"tenant"}, func(page [][]bson.RawValue) error {
			pages = append(pages, page)
			return nil
		})
	})

	require.NoError(t, err)
	require.Equal(t, [][][]bson.RawValue{keys[:4], keys[4:]}, pages)
}

func TestCheckForDeletesPassesTheShardKeyFieldsToTheWitnessAndTheSourceFind(t *testing.T) {
	client, finds := capturingClient(t, sourceFindReply(tupleDoc(int32(1), "tenant", "A")))
	witness := &fakeWitness{keys: map[iface.Namespace][][]bson.RawValue{testNs: shardedTuples([]any{int32(1), "A"})}}

	_, _, err := checkForDeletes(t.Context(), client, witness, []iface.Namespace{testNs}, map[iface.Namespace][]string{testNs: {"tenant"}})

	require.NoError(t, err)
	require.Equal(t, [][]string{{"tenant"}}, witness.keyFields)
	require.Len(t, *finds, 1)
	require.Equal(t, bson.Raw(mustMarshalBSON(t, bson.D{{Key: "_id", Value: 1}, {Key: "tenant", Value: 1}})), (*finds)[0].Lookup("projection").Document())
	require.Equal(t, bson.Raw(mustMarshalBSON(t, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: bson.A{int32(1)}}}}})), (*finds)[0].Lookup("filter").Document())
}

func TestCheckForDeletesEmitsOneDeleteCarryingTheShardKeyPart(t *testing.T) {
	client, _ := recordingClient(t, sourceFindReply(tupleDoc(int32(1), "tenant", "B")))
	witness := &fakeWitness{keys: map[iface.Namespace][][]bson.RawValue{testNs: shardedTuples([]any{int32(1), "A"}, []any{int32(1), "B"})}}

	updates, summary, err := checkForDeletes(t.Context(), client, witness, []iface.Namespace{testNs}, map[iface.Namespace][]string{testNs: {"tenant"}})

	require.NoError(t, err)
	require.Len(t, updates[testLoc], 1)
	id := updates[testLoc][0].Id
	require.Len(t, id, 2)
	require.Equal(t, "_id", id[0].Name)
	require.Equal(t, uint32(bson.TypeInt32), id[0].Type)
	require.Equal(t, mustValues(int32(1))[0].Value, id[0].Data)
	require.Equal(t, "tenant", id[1].Name)
	require.Equal(t, uint32(bson.TypeString), id[1].Type)
	require.Equal(t, mustValues("A")[0].Value, id[1].Data)
	require.Equal(t, adiomv1.UpdateType_UPDATE_TYPE_DELETE, updates[testLoc][0].Type)
	require.Equal(t, passSummary{Namespaces: 1, Keys: 2, Deletes: 1}, summary)
}

func TestSourceCheckTreatsAMissingShardKeyFieldAsNullOnBothSides(t *testing.T) {
	client, _ := recordingClient(t, sourceFindReply(idDoc(int32(1))))
	witnessKeys := [][]bson.RawValue{{mustValues(int32(1))[0], {Type: bson.TypeNull}}}

	missing, _, err := missingSourceKeys(t.Context(), client, testLoc, []string{"tenant"}, witnessKeys)

	require.NoError(t, err)
	require.Empty(t, missing)
}

func TestSourceCheckTreatsEquivalentNumericShardKeyComponentsAsEqual(t *testing.T) {
	client, _ := recordingClient(t, sourceFindReply(tupleDoc(int32(1), "tenant", int64(5))))

	missing, _, err := missingSourceKeys(t.Context(), client, testLoc, []string{"tenant"}, shardedTuples([]any{int32(1), int32(5)}))

	require.NoError(t, err)
	require.Empty(t, missing)
}

func TestSourceCheckReadsANestedShardKeyField(t *testing.T) {
	doc := tupleDoc(int32(1), "a", bson.D{{Key: "b", Value: "x"}})
	client, _ := recordingClient(t, sourceFindReply(doc))

	missing, _, err := missingSourceKeys(t.Context(), client, testLoc, []string{"a.b"}, shardedTuples([]any{int32(1), "x"}, []any{int32(1), "y"}))

	require.NoError(t, err)
	require.Equal(t, shardedTuples([]any{int32(1), "y"}), missing)
}

func TestSourceCheckDefersEveryTupleOfAnIdWhoseSourceTupleIsUnknown(t *testing.T) {
	for name, test := range map[string]struct {
		witness  [][]bson.RawValue
		source   []bson.D
		deferred int
	}{
		"source also holds another shard key value": {
			shardedTuples([]any{int32(1), "A"}),
			[]bson.D{tupleDoc(int32(1), "tenant", "A"), tupleDoc(int32(1), "tenant", "B")},
			1,
		},
		"field missing on the source only": {
			shardedTuples([]any{int32(1), "A"}),
			[]bson.D{idDoc(int32(1))},
			1,
		},
		"field missing on the witness only": {
			[][]bson.RawValue{{mustValues(int32(1))[0], {Type: bson.TypeNull}}},
			[]bson.D{tupleDoc(int32(1), "tenant", "A")},
			1,
		},
		"both tuples of the id are deferred": {
			shardedTuples([]any{int32(1), "A"}, []any{int32(1), "B"}, []any{int32(2), "C"}),
			[]bson.D{tupleDoc(int32(1), "tenant", "B2"), tupleDoc(int32(2), "tenant", "C")},
			2,
		},
	} {
		t.Run(name, func(t *testing.T) {
			client, _ := recordingClient(t, sourceFindReply(test.source...))

			missing, deferred, err := missingSourceKeys(t.Context(), client, testLoc, []string{"tenant"}, test.witness)

			require.NoError(t, err)
			require.Empty(t, missing)
			require.Equal(t, test.deferred, deferred)
		})
	}
}

func TestCheckForDeletesCountsDeferredKeysAndSendsNoDeleteForThem(t *testing.T) {
	client, _ := recordingClient(t, sourceFindReply(tupleDoc(int32(1), "tenant", "B2"), tupleDoc(int32(2), "tenant", "C")))
	witness := &fakeWitness{keys: map[iface.Namespace][][]bson.RawValue{testNs: shardedTuples([]any{int32(1), "A"}, []any{int32(1), "B"}, []any{int32(2), "C"})}}

	updates, summary, err := checkForDeletes(t.Context(), client, witness, []iface.Namespace{testNs}, map[iface.Namespace][]string{testNs: {"tenant"}})

	require.NoError(t, err)
	require.Empty(t, updates)
	require.Equal(t, passSummary{Namespaces: 1, Keys: 3, Deferred: 2}, summary)
}

func TestSourceCheckStillDeletesTheUndecidedIdOnlyWhenTheSourceHasNoDocumentForIt(t *testing.T) {
	client, _ := recordingClient(t, sourceFindReply(tupleDoc(int32(2), "tenant", "C2")))

	missing, deferred, err := missingSourceKeys(t.Context(), client, testLoc, []string{"tenant"}, shardedTuples([]any{int32(1), "A"}, []any{int32(2), "C"}))

	require.NoError(t, err)
	require.Equal(t, shardedTuples([]any{int32(1), "A"}), missing)
	require.Equal(t, 1, deferred)
}
