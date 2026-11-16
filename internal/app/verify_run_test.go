package dsync

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"github.com/adiom-data/dsync/internal/app/options"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func idDoc(t *testing.T, id string, v int) []byte {
	b, err := bson.Marshal(bson.D{{Key: "_id", Value: id}, {Key: "v", Value: v}})
	require.NoError(t, err)
	return b
}

func streamOf(t *testing.T, id string, v int) []*adiomv1.Update {
	return []*adiomv1.Update{{Id: []*adiomv1.BsonValue{idPart(t, "_id", id)}, Type: adiomv1.UpdateType_UPDATE_TYPE_UPDATE, Data: idDoc(t, id, v)}}
}

func newMemVerify(t *testing.T, left, right listingSource) *memVerify {
	return &memVerify{
		leftSource:  *newTestSource(t, left, 0, 0),
		rightSource: *newTestSource(t, right, 0, 0),
		limit:       10,
		cooldown:    time.Hour,
		latency:     20 * time.Millisecond,
		poll:        20 * time.Millisecond,
	}
}

func TestOnceVerdict(t *testing.T) {
	a1, a2, b1 := idDoc(t, "a", 1), idDoc(t, "a", 2), idDoc(t, "b", 1)
	for name, tc := range map[string]struct {
		left, right [][]byte
		want        error
	}{
		"equal":                {[][]byte{a1, b1}, [][]byte{a1, b1}, nil},
		"differing document":   {[][]byte{a1}, [][]byte{a2}, ErrVerifyMismatch},
		"missing on one side":  {[][]byte{a1, b1}, [][]byte{a1}, ErrVerifyMismatch},
		"missing on other one": {[][]byte{a1}, [][]byte{a1, b1}, ErrVerifyMismatch},
	} {
		t.Run(name, func(t *testing.T) {
			m := newMemVerify(t, listingSource{docs: tc.left, streamBlock: true}, listingSource{docs: tc.right, streamBlock: true})
			m.once = true

			err := runWithDeadline(t, m.Run)

			if tc.want == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, tc.want)
			require.ErrorContains(t, err, "1 mismatches in")
		})
	}
}

func TestOnceReturnsAfterBothStreamsReturned(t *testing.T) {
	var started, returned atomic.Int32
	h := listingSource{docs: [][]byte{idDoc(t, "a", 1)}, streamBlock: true, started: &started, returned: &returned}
	m := newMemVerify(t, h, h)
	m.once = true

	require.NoError(t, runWithDeadline(t, m.Run))

	require.Eventually(t, func() bool { return started.Load() == returned.Load() }, 2*time.Second, 5*time.Millisecond)
}

func TestOnceInitialSyncFailureIsTheSourceError(t *testing.T) {
	failing := errors.New("list failed")
	m := newMemVerify(t, listingSource{listErr: failing, streamBlock: true}, listingSource{docs: [][]byte{idDoc(t, "a", 1)}, streamBlock: true})
	m.once = true

	err := runWithDeadline(t, m.Run)

	require.ErrorContains(t, err, "list failed")
	require.NotErrorIs(t, err, ErrVerifyMismatch)
}

func TestStreamErrorAfterStopDoesNotChangeTheVerdict(t *testing.T) {
	docs := [][]byte{idDoc(t, "a", 1)}
	h := listingSource{docs: docs, streamBlock: true, streamErr: errors.New("driver closed")}
	m := newMemVerify(t, h, h)
	m.once = true

	require.NoError(t, runWithDeadline(t, m.Run))

	m = newMemVerify(t, h, listingSource{docs: [][]byte{idDoc(t, "a", 2)}, streamBlock: true, streamErr: errors.New("driver closed")})
	m.once = true
	require.ErrorIs(t, runWithDeadline(t, m.Run), ErrVerifyMismatch)
}

func TestStreamErrorBeforeStopIsAFailure(t *testing.T) {
	docs := [][]byte{idDoc(t, "a", 1)}
	m := newMemVerify(t, listingSource{docs: docs, streamErr: errors.New("boom")}, listingSource{docs: docs, streamBlock: true})
	m.once = true
	m.latency = time.Hour

	err := runWithDeadline(t, m.Run)

	require.ErrorContains(t, err, "boom")
	require.NotErrorIs(t, err, ErrVerifyMismatch)
}

func interruptWhen(t *testing.T, cond func(*report) bool, run func(ctx context.Context, onReport func(*report)) error) error {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	return runWithDeadline(t, func(context.Context) error {
		return run(ctx, func(r *report) {
			if cond(r) {
				cancel()
			}
		})
	})
}

func anyReport(*report) bool { return true }

func TestMemoryInterruptVerdict(t *testing.T) {
	failing := errors.New("driver closed")
	for name, tc := range map[string]struct {
		right int
		want  error
	}{
		"clean report":    {1, nil},
		"mismatch report": {2, ErrVerifyMismatch},
	} {
		t.Run(name, func(t *testing.T) {
			m := newMemVerify(t,
				listingSource{docs: [][]byte{idDoc(t, "a", 1)}, streamBlock: true, streamErr: failing},
				listingSource{docs: [][]byte{idDoc(t, "a", tc.right)}, streamBlock: true, streamErr: failing})

			err := interruptWhen(t, anyReport, func(ctx context.Context, onReport func(*report)) error {
				m.onReport = onReport
				return m.Run(ctx)
			})

			if tc.want == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.want)
			}
		})
	}
}

func TestMemoryInterruptBeforeFirstReport(t *testing.T) {
	m := newMemVerify(t, listingSource{docs: [][]byte{idDoc(t, "a", 1)}, streamBlock: true}, listingSource{docs: [][]byte{idDoc(t, "a", 1)}, streamBlock: true})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := runWithDeadline(t, func(context.Context) error { return m.Run(ctx) })

	require.ErrorIs(t, err, errInterrupted)
}

func newTailVerify(t *testing.T, left, right listingSource) *tailVerify {
	left.skipInitialSync, right.skipInitialSync = true, true
	return &tailVerify{
		leftSource:  *newTestSource(t, left, 0, 0),
		rightSource: *newTestSource(t, right, 0, 0),
		limit:       10,
		latency:     10 * time.Millisecond,
		cooldown:    10 * time.Millisecond,
		poll:        10 * time.Millisecond,
	}
}

func hasDiffs(r *report) bool { return r.mismatches > 0 }

func runTailUntil(t *testing.T, v *tailVerify, cond func(*report) bool) error {
	return interruptWhen(t, cond, func(ctx context.Context, onReport func(*report)) error {
		v.onReport = onReport
		return v.Run(ctx)
	})
}

func TestTailInterruptAfterCleanReportIsNil(t *testing.T) {
	failing := errors.New("driver closed")
	v := newTailVerify(t, listingSource{streamBlock: true, streamErr: failing}, listingSource{streamBlock: true, streamErr: failing})

	require.NoError(t, runTailUntil(t, v, anyReport))
}

func TestTailInterruptAfterOneSidedDocumentIsAMismatch(t *testing.T) {
	failing := errors.New("driver closed")
	v := newTailVerify(t,
		listingSource{updates: streamOf(t, "a", 1), streamBlock: true, streamErr: failing},
		listingSource{streamBlock: true, streamErr: failing})

	err := runTailUntil(t, v, hasDiffs)

	require.ErrorIs(t, err, ErrVerifyMismatch)
}

func TestTailInterruptAfterDiffThenCleanReportIsAMismatch(t *testing.T) {
	v := newTailVerify(t, listingSource{updates: streamOf(t, "a", 1), streamBlock: true}, listingSource{streamBlock: true})
	var diffTotal atomic.Int64
	var sawClean atomic.Bool

	err := runTailUntil(t, v, func(r *report) bool {
		if total := diffTotal.Load(); total > 0 {
			if r.total == total {
				sawClean.Store(true)
				return true
			}
			return false
		}
		if r.mismatches > 0 {
			diffTotal.Store(r.total)
		}
		return false
	})

	require.True(t, sawClean.Load())
	require.ErrorIs(t, err, ErrVerifyMismatch)
}

func TestTailInterruptBeforeFirstReport(t *testing.T) {
	v := newTailVerify(t, listingSource{streamBlock: true}, listingSource{streamBlock: true})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := runWithDeadline(t, func(context.Context) error { return v.Run(ctx) })

	require.ErrorIs(t, err, errInterrupted)
}

func TestVerifyFlagsUsageErrors(t *testing.T) {
	const latency = time.Second
	require.Error(t, checkVerifyFlags(true, true, latency))
	require.NoError(t, checkVerifyFlags(true, false, latency))
	require.NoError(t, checkVerifyFlags(false, true, latency))
	require.Error(t, checkVerifyFlags(false, false, 0))
	require.Error(t, checkVerifyFlags(true, false, -time.Second))
	require.NoError(t, checkVerifyFlags(false, true, 0))
}

func TestInitialSyncDoneWaitsForTheConsumerToFinishTheLastDocument(t *testing.T) {
	docs := [][]byte{idDoc(t, "a", 1), idDoc(t, "b", 1)}
	s := newTestSource(t, listingSource{docs: docs}, 0, 0)
	inLast := make(chan struct{})
	release := make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	var seen atomic.Int32
	var processErr error
	finished := make(chan struct{})
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(func() {
		releaseOnce()
		cancel()
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Error("ProcessSource did not return")
		}
	})
	go func() {
		defer close(finished)
		processErr = s.ProcessSource(ctx, func(context.Context, Update) error {
			if seen.Add(1) == int32(len(docs)) {
				close(inLast)
				<-release
			}
			return nil
		})
	}()
	select {
	case <-inLast:
	case <-time.After(3 * time.Second):
		t.Fatal("last document never reached process")
	}

	early, earlyCancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer earlyCancel()
	require.False(t, s.WaitForInitialSync(early))

	releaseOnce()
	late, lateCancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer lateCancel()
	require.True(t, s.WaitForInitialSync(late))
	select {
	case <-finished:
		require.NoError(t, processErr)
	case <-time.After(3 * time.Second):
		t.Fatal("ProcessSource did not return")
	}
}

func TestOnceInterruptWinsOverAReadyTimer(t *testing.T) {
	docs := [][]byte{idDoc(t, "a", 1)}
	for range 50 {
		m := newMemVerify(t, listingSource{docs: docs, streamBlock: true}, listingSource{docs: docs, streamBlock: true})
		m.once = true
		ctx, cancel := context.WithCancel(t.Context())
		m.after = func(time.Duration) <-chan time.Time {
			cancel()
			ch := make(chan time.Time, 1)
			ch <- time.Now()
			return ch
		}

		err := runWithDeadline(t, func(context.Context) error { return m.Run(ctx) })
		cancel()

		require.ErrorIs(t, err, errInterrupted)
	}
}

type fakeDropper struct {
	calls       atomic.Int32
	runFinished *atomic.Bool
	after       atomic.Bool
	ctxErr      error
	hasDeadline bool
	err         error
}

func (f *fakeDropper) DropReplication(ctx context.Context) error {
	f.calls.Add(1)
	f.after.Store(f.runFinished.Load())
	f.ctxErr = ctx.Err()
	_, f.hasDeadline = ctx.Deadline()
	return f.err
}

type localHandler struct {
	adiomv1connect.UnimplementedConnectorServiceHandler
}

func (localHandler) DropReplication(context.Context) error { return nil }

func TestRunThenDropReplication(t *testing.T) {
	boom := errors.New("source failed")
	for name, tc := range map[string]struct {
		runErr    error
		interrupt bool
		dropErr   error
	}{
		"clean":                 {},
		"mismatch":              {runErr: mismatchError(1, 2)},
		"source error":          {runErr: boom},
		"interrupt":             {runErr: errInterrupted, interrupt: true},
		"clean, cleanup fails":  {dropErr: errors.New("slot busy")},
		"source error, cleanup": {runErr: boom, dropErr: errors.New("slot busy")},
	} {
		t.Run(name, func(t *testing.T) {
			var finished atomic.Bool
			left := &fakeDropper{runFinished: &finished, err: tc.dropErr}
			right := &fakeDropper{runFinished: &finished, err: tc.dropErr}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			err := runThenDropReplication(ctx, func(ctx context.Context) error {
				if tc.interrupt {
					cancel()
				}
				finished.Store(true)
				return tc.runErr
			}, left, right)

			require.Equal(t, tc.runErr, err)
			for _, d := range []*fakeDropper{left, right} {
				require.Equal(t, int32(1), d.calls.Load())
				require.True(t, d.after.Load())
				require.NoError(t, d.ctxErr)
				require.True(t, d.hasDeadline)
			}
		})
	}
}

func TestReplicationDroppersPicksLocalConnectorsThatImplementIt(t *testing.T) {
	droppers := replicationDroppers(
		options.ConfiguredConnector{Local: localHandler{}},
		options.ConfiguredConnector{Local: adiomv1connect.UnimplementedConnectorServiceHandler{}},
		options.ConfiguredConnector{},
	)

	require.Len(t, droppers, 1)
}

func TestOnceJudgesAStreamChangeThatArrivedAfterTheInitialRead(t *testing.T) {
	for range 30 {
		sent := make(chan struct{})
		m := newMemVerify(t,
			listingSource{docs: [][]byte{idDoc(t, "a", 1)}, streamBlock: true},
			listingSource{docs: [][]byte{idDoc(t, "a", 2)}, updates: streamOf(t, "a", 2), streamBlock: true, sent: sent})
		m.once = true
		m.poll = time.Hour
		m.after = func(time.Duration) <-chan time.Time {
			<-sent
			time.Sleep(5 * m.latency)
			ch := make(chan time.Time, 1)
			ch <- time.Now()
			return ch
		}

		err := runWithDeadline(t, m.Run)

		require.ErrorIs(t, err, ErrVerifyMismatch)
	}
}
