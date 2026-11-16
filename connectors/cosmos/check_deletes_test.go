package cosmos

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

var testNs = iface.Namespace{Db: testLoc.Database, Col: testLoc.Collection}

func nReply(n int32) bson.D { return bson.D{{Key: "ok", Value: 1}, {Key: "n", Value: n}} }

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

func passStages() deleteStages {
	return deleteStages{
		differs: func(context.Context, iface.Namespace) (bool, error) { return true, nil },
		scan: func(_ context.Context, _ iface.Namespace, emit func([]any) error) error {
			return emit([]any{int32(1), int32(2)})
		},
		missing: func(_ context.Context, _ iface.Location, ids []any) ([]any, error) { return ids[:1], nil },
	}
}

func TestDeletePassReturnsTheMissingIdsAsDeletes(t *testing.T) {
	updates, err := runDeletePass(t.Context(), []iface.Namespace{testNs}, passStages())

	require.NoError(t, err)
	require.Len(t, updates[testLoc], 1)
}

func TestDeletePassSkipsNamespacesWhoseCountsMatch(t *testing.T) {
	st := passStages()
	st.differs = func(context.Context, iface.Namespace) (bool, error) { return false, nil }
	st.scan = func(context.Context, iface.Namespace, func([]any) error) error {
		t.Error("scanned a namespace with equal counts")
		return nil
	}

	updates, err := runDeletePass(t.Context(), []iface.Namespace{testNs}, st)

	require.NoError(t, err)
	require.Empty(t, updates)
}

func TestDeletePassAbortsOnAnyStageError(t *testing.T) {
	boom := errors.New("stage failed")
	for name, mutate := range map[string]func(*deleteStages){
		"count": func(st *deleteStages) {
			st.differs = func(context.Context, iface.Namespace) (bool, error) { return false, boom }
		},
		"witness": func(st *deleteStages) {
			st.scan = func(_ context.Context, _ iface.Namespace, emit func([]any) error) error {
				_ = emit([]any{int32(1)})
				return boom
			}
		},
		"source": func(st *deleteStages) {
			st.missing = func(context.Context, iface.Location, []any) ([]any, error) { return nil, boom }
		},
	} {
		t.Run(name, func(t *testing.T) {
			st := passStages()
			mutate(&st)
			var updates map[iface.Location][]*adiomv1.Update
			var err error

			requireReturns(t, "delete pass", func() {
				updates, err = runDeletePass(t.Context(), []iface.Namespace{testNs, {Db: "db2", Col: "col2"}}, st)
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
	var scans atomic.Int32
	st := deleteStages{
		differs: func(context.Context, iface.Namespace) (bool, error) { return true, nil },
		scan: func(_ context.Context, _ iface.Namespace, emit func([]any) error) error {
			scans.Add(1)
			for range 100 {
				if err := emit([]any{int32(1)}); err != nil {
					return err
				}
			}
			return nil
		},
		missing: func(ctx context.Context, _ iface.Location, _ []any) ([]any, error) {
			return nil, boom
		},
	}
	var err error

	requireReturns(t, "delete pass", func() {
		_, err = runDeletePass(t.Context(), namespaces, st)
	})

	require.ErrorIs(t, err, boom)
}

func TestDeletePassReturnsASourceErrorWhileTheWitnessScanIsStillBlocked(t *testing.T) {
	boom := errors.New("source failed")
	st := passStages()
	st.scan = func(ctx context.Context, _ iface.Namespace, emit func([]any) error) error {
		if err := emit([]any{int32(1)}); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	}
	st.missing = func(context.Context, iface.Location, []any) ([]any, error) { return nil, boom }
	var err error

	requireReturns(t, "delete pass", func() {
		_, err = runDeletePass(t.Context(), []iface.Namespace{testNs}, st)
	})

	require.ErrorIs(t, err, boom)
}

func TestDeletePassDoesNotBlockWhenTheConsumerFailsToConvert(t *testing.T) {
	st := passStages()
	st.missing = func(context.Context, iface.Location, []any) ([]any, error) {
		return []any{make(chan int)}, nil
	}
	var err error

	requireReturns(t, "delete pass", func() {
		_, err = runDeletePass(t.Context(), []iface.Namespace{testNs}, st)
	})

	require.ErrorContains(t, err, "db1.col1")
}

func TestDeletePassReturnsContextErrorWhenCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	st := passStages()
	st.scan = func(ctx context.Context, _ iface.Namespace, _ func([]any) error) error {
		cancel()
		<-ctx.Done()
		return ctx.Err()
	}
	var updates map[iface.Location][]*adiomv1.Update
	var err error

	requireReturns(t, "delete pass", func() {
		updates, err = runDeletePass(ctx, []iface.Namespace{testNs}, st)
	})

	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, updates)
}

func TestNamespaceCountsDifferComparesBothSides(t *testing.T) {
	source, _ := recordingClient(t, nReply(5))
	witness, _ := recordingClient(t, nReply(3))

	differs, err := namespaceCountsDiffer(t.Context(), source, witness, testNs)

	require.NoError(t, err)
	require.True(t, differs)
}

func TestNamespaceCountsDifferReportsEachSideFailure(t *testing.T) {
	failed := bson.D{{Key: "ok", Value: 0}, {Key: "code", Value: 2}, {Key: "errmsg", Value: "count rejected"}}
	for name, test := range map[string]struct{ source, witness bson.D }{
		"source":  {failed, nReply(3)},
		"witness": {nReply(3), failed},
	} {
		t.Run(name, func(t *testing.T) {
			source, _ := recordingClient(t, test.source)
			witness, _ := recordingClient(t, test.witness)

			_, err := namespaceCountsDiffer(t.Context(), source, witness, testNs)

			require.ErrorContains(t, err, name)
			require.ErrorContains(t, err, "db1.col1")
		})
	}
}

func TestWitnessScanFailsWhenTheCursorFailsAfterTheLastBatch(t *testing.T) {
	witness, _ := recordingClient(t, cursorReply("firstBatch", bson.A{bson.D{{Key: "_id", Value: int32(1)}}}, nil), readFailedReply, cursorKilledReply)
	emitted := 0

	err := scanWitnessNamespace(t.Context(), witness, testNs, func([]any) error { emitted++; return nil })

	require.ErrorContains(t, err, "db1.col1")
	require.Zero(t, emitted)
}

func TestWitnessScanFailsOnADocumentWithoutId(t *testing.T) {
	witness, _ := recordingClient(t, cursorReply("firstBatch", bson.A{bson.D{{Key: "other", Value: 1}}}, nil), cursorKilledReply)

	err := scanWitnessNamespace(t.Context(), witness, testNs, func([]any) error { return nil })

	require.ErrorContains(t, err, "_id")
}

func TestWitnessScanEmitsEveryIdInBatches(t *testing.T) {
	var docs bson.A
	for i := range idsBatchSize + 1 {
		docs = append(docs, bson.D{{Key: "_id", Value: int32(i)}})
	}
	witness, _ := recordingClient(t, bson.D{{Key: "ok", Value: 1}, {Key: "cursor", Value: bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.col"}, {Key: "firstBatch", Value: docs}}}})
	var sizes []int

	err := scanWitnessNamespace(t.Context(), witness, testNs, func(ids []any) error { sizes = append(sizes, len(ids)); return nil })

	require.NoError(t, err)
	require.Equal(t, []int{idsBatchSize, 1}, sizes)
}

func TestSourceIdCheckFailsWhenTheCursorFails(t *testing.T) {
	client, _ := recordingClient(t, cursorReply("firstBatch", bson.A{}, nil), readFailedReply, cursorKilledReply)

	ids, err := missingSourceIds(t.Context(), client, testLoc, []any{int32(1)})

	require.ErrorContains(t, err, "db1.col1")
	require.Nil(t, ids)
}

func TestSourceIdCheckFailsOnAnUnexpectedResultShape(t *testing.T) {
	for name, doc := range map[string]bson.D{
		"not an array": {{Key: "missingIds", Value: "oops"}},
		"absent":       {},
		"null":         {{Key: "missingIds", Value: nil}},
	} {
		t.Run(name, func(t *testing.T) {
			client, _ := recordingClient(t, cursorReply("firstBatch", bson.A{doc}, nil), cursorKilledReply)

			ids, err := missingSourceIds(t.Context(), client, testLoc, []any{int32(1)})

			require.ErrorContains(t, err, "db1.col1")
			require.Nil(t, ids)
		})
	}
}

func TestSourceIdCheckTreatsAnEmptyMissingArrayAsNothingMissing(t *testing.T) {
	client, _ := recordingClient(t, cursorReply("firstBatch", bson.A{bson.D{{Key: "missingIds", Value: bson.A{}}}}, nil), cursorKilledReply)

	ids, err := missingSourceIds(t.Context(), client, testLoc, []any{int32(1)})

	require.NoError(t, err)
	require.Empty(t, ids)
}

func TestSourceIdCheckReturnsTheMissingIds(t *testing.T) {
	client, _ := recordingClient(t, cursorReply("firstBatch", bson.A{bson.D{{Key: "missingIds", Value: bson.A{int32(2)}}}}, nil), cursorKilledReply)

	ids, err := missingSourceIds(t.Context(), client, testLoc, []any{int32(1), int32(2)})

	require.NoError(t, err)
	require.Equal(t, []any{int32(2)}, ids)
}

func TestSourceIdCheckTreatsAnEmptyResultAsAllMissing(t *testing.T) {
	client, _ := recordingClient(t, bson.D{{Key: "ok", Value: 1}, {Key: "cursor", Value: bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.col"}, {Key: "firstBatch", Value: bson.A{}}}}})

	ids, err := missingSourceIds(t.Context(), client, testLoc, []any{int32(1), int32(2)})

	require.NoError(t, err)
	require.Equal(t, []any{int32(1), int32(2)}, ids)
}

func TestStreamUpdatesSurfacesAWitnessErrorAndSendsNoDeletes(t *testing.T) {
	boom := errors.New("witness read failed")
	st := passStages()
	st.scan = func(_ context.Context, _ iface.Namespace, emit func([]any) error) error {
		_ = emit([]any{int32(1)})
		return boom
	}
	var sent atomic.Int32
	force := make(chan struct{})
	run := idleRun(
		func(*adiomv1.StreamUpdatesResponse) error { sent.Add(1); return nil },
		func(ctx context.Context) (map[iface.Location][]*adiomv1.Update, error) {
			return runDeletePass(ctx, []iface.Namespace{testNs}, st)
		},
		force,
	)
	done := make(chan error, 1)
	go func() { done <- run.run(t.Context()) }()

	triggerDelete(t, force)

	select {
	case err := <-done:
		require.ErrorContains(t, err, "witness read failed")
	case <-time.After(5 * time.Second):
		t.Fatal("stream did not fail")
	}
	require.Zero(t, sent.Load())
}

func TestStreamUpdatesSurfacesASourceCheckErrorAndSendsNoDeletes(t *testing.T) {
	st := passStages()
	st.missing = func(context.Context, iface.Location, []any) ([]any, error) {
		return nil, errors.New("source check failed")
	}
	var sent atomic.Int32
	force := make(chan struct{})
	run := idleRun(
		func(*adiomv1.StreamUpdatesResponse) error { sent.Add(1); return nil },
		func(ctx context.Context) (map[iface.Location][]*adiomv1.Update, error) {
			return runDeletePass(ctx, []iface.Namespace{testNs}, st)
		},
		force,
	)
	done := make(chan error, 1)
	go func() { done <- run.run(t.Context()) }()

	triggerDelete(t, force)

	select {
	case err := <-done:
		require.ErrorContains(t, err, "source check failed")
	case <-time.After(5 * time.Second):
		t.Fatal("stream did not fail")
	}
	require.Zero(t, sent.Load())
}
