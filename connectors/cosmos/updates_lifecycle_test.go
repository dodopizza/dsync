package cosmos

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type idleStream struct{}

func (s *idleStream) Next(ctx context.Context) bool {
	<-ctx.Done()
	return false
}
func (s *idleStream) Decode(any) error          { return nil }
func (s *idleStream) RemainingBatchLength() int { return 0 }
func (s *idleStream) ResumeToken() bson.Raw     { return nil }
func (s *idleStream) Err() error                { return nil }

func idleRun(send func(*adiomv1.StreamUpdatesResponse) error, check func(context.Context) (map[iface.Location][]*adiomv1.Update, error), force <-chan struct{}) updatesRun {
	return updatesRun{
		namespaces:     []iface.Namespace{{Db: testLoc.Database, Col: testLoc.Collection}},
		shardKeyFields: map[iface.Namespace][]string{},
		tokenMap:       NewTokenMap(),
		openStream: func(ctx context.Context, loc iface.Location) (eventStream, func(), error) {
			return &idleStream{}, func() {}, nil
		},
		checkDeletes:    check,
		deletesInterval: time.Hour,
		forceDelete:     force,
		send:            send,
		deletesSent:     &atomic.Uint64{},
	}
}

type endedStream struct{ idleStream }

func (s *endedStream) Next(context.Context) bool { return false }

func triggerDelete(t *testing.T, force chan<- struct{}) {
	t.Helper()
	select {
	case force <- struct{}{}:
	case <-time.After(5 * time.Second):
		t.Fatal("delete goroutine did not accept the trigger")
	}
}

func oneDelete(context.Context) (map[iface.Location][]*adiomv1.Update, error) {
	u, err := deleteUpdate(int32(1))
	if err != nil {
		return nil, err
	}
	return map[iface.Location][]*adiomv1.Update{testLoc: {u}}, nil
}

func TestStreamUpdatesDoesNotReturnWhileADeleteSendIsInFlight(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var returned atomic.Bool
	var lateSend atomic.Bool
	var releaseOnce sync.Once
	releaseSend := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseSend)
	send := func(*adiomv1.StreamUpdatesResponse) error {
		close(entered)
		<-release
		if returned.Load() {
			lateSend.Store(true)
		}
		return nil
	}
	force := make(chan struct{})
	run := idleRun(send, oneDelete, force)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	finished := make(chan struct{})
	t.Cleanup(func() {
		releaseSend()
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
		}
	})
	go func() {
		defer close(finished)
		err := run.run(ctx)
		returned.Store(true)
		done <- err
	}()

	triggerDelete(t, force)
	awaitSignal(t, entered, "delete send")
	cancel()

	select {
	case <-done:
		t.Fatal("returned while a delete send was in flight")
	case <-time.After(200 * time.Millisecond):
	}
	releaseSend()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("did not return after the send finished")
	}
	require.False(t, lateSend.Load())
}

func TestStreamUpdatesReturnsAfterTheDeletePassFailed(t *testing.T) {
	passErr := errors.New("pass failed")
	force := make(chan struct{})
	run := idleRun(
		func(*adiomv1.StreamUpdatesResponse) error { return nil },
		func(context.Context) (map[iface.Location][]*adiomv1.Update, error) { return nil, passErr },
		force,
	)
	done := make(chan error, 1)
	go func() { done <- run.run(t.Context()) }()

	triggerDelete(t, force)

	select {
	case err := <-done:
		require.ErrorContains(t, err, "pass failed")
	case <-time.After(5 * time.Second):
		t.Fatal("namespace streams were not cancelled by the pass error")
	}
}

func TestLsnCountsOnlyDeletesSentSinceTheStreamStarted(t *testing.T) {
	var deletes atomic.Uint64
	deletes.Add(7)
	base := deletes.Load()
	deletes.Add(3)

	require.Equal(t, uint64(13), lsnWithDeletes(10, &deletes, base))
	require.Equal(t, uint64(20), lsnWithDeletes(10, &deletes, 0))
}

func TestStreamUpdatesReturnsWhenEveryNamespaceStreamEndedAndNoPassIsRunning(t *testing.T) {
	run := idleRun(
		func(*adiomv1.StreamUpdatesResponse) error { return nil },
		oneDelete,
		make(chan struct{}),
	)
	run.openStream = func(context.Context, iface.Location) (eventStream, func(), error) {
		return &endedStream{}, func() {}, nil
	}
	var err error

	requireReturns(t, "stream updates", func() { err = run.run(t.Context()) })

	require.NoError(t, err)
}
