package cosmos

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const cursorTestTimeout = 5 * time.Second

type signalingLocker struct {
	inner   sync.Mutex
	waiting chan struct{}
}

func (l *signalingLocker) Lock() {
	l.waiting <- struct{}{}
	l.inner.Lock()
}

func (l *signalingLocker) Unlock() { l.inner.Unlock() }

func cursorTokens(t *testing.T, cursor []byte) map[iface.Location]bson.Raw {
	t.Helper()
	_, encoded, err := decodeResumeToken(cursor)
	require.NoError(t, err)
	decoded := NewTokenMap()
	require.NoError(t, decoded.decodeMap(encoded))
	return decoded.Map
}

func awaitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(cursorTestTimeout):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func awaitJoin(t *testing.T, wg *sync.WaitGroup) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	awaitSignal(t, done, "goroutines to finish")
}

func orderTestToken(t *testing.T, data string) bson.Raw {
	return bson.Raw(mustMarshalBSON(t, bson.D{{Key: "_data", Value: data}}))
}

func TestCursorNeverCoversATokenWhoseUpdatesAreNotSentYet(t *testing.T) {
	locA := iface.Location{Database: "db", Collection: "a"}
	locB := iface.Location{Database: "db", Collection: "b"}
	tokenA, tokenB := orderTestToken(t, "a1"), orderTestToken(t, "b1")
	tokenMap := NewTokenMap()
	lock := &signalingLocker{waiting: make(chan struct{}, 2)}
	sender := &cursorSender{mu: lock}

	var sentMu sync.Mutex
	var sent [][]byte
	record := func(cursor []byte) error {
		sentMu.Lock()
		defer sentMu.Unlock()
		sent = append(sent, cursor)
		return nil
	}
	inSend := make(chan struct{})
	releaseSend := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseSend) }) }
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	t.Cleanup(func() {
		release()
		awaitJoin(t, &wg)
	})

	wg.Add(1)
	go func() {
		defer wg.Done()
		errs <- sendWithCursor(sender, tokenMap, 1, locA, tokenA, func(cursor []byte) error {
			close(inSend)
			<-releaseSend
			return record(cursor)
		})
	}()
	awaitSignal(t, lock.waiting, "A to take the lock")
	awaitSignal(t, inSend, "A to start sending")

	wg.Add(1)
	go func() {
		defer wg.Done()
		errs <- sendWithCursor(sender, tokenMap, 1, locB, tokenB, record)
	}()
	awaitSignal(t, lock.waiting, "B to queue on the lock")

	encoded, err := tokenMap.encodeMap()
	require.NoError(t, err)
	decoded := NewTokenMap()
	require.NoError(t, decoded.decodeMap(encoded))
	require.NotContains(t, decoded.Map, locB, "a cursor encoded now would cover a token whose updates are not sent")

	release()
	awaitJoin(t, &wg)
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	require.Len(t, sent, 2)
	first, second := cursorTokens(t, sent[0]), cursorTokens(t, sent[1])
	require.NotContains(t, first, locB)
	require.Contains(t, second, locA)
	require.Contains(t, second, locB)
}

func TestQueuedSendNeverCarriesATokenWhoseSendFailed(t *testing.T) {
	locA := iface.Location{Database: "db", Collection: "a"}
	locB := iface.Location{Database: "db", Collection: "b"}
	tokenA, tokenB := orderTestToken(t, "a1"), orderTestToken(t, "b1")
	tokenMap := NewTokenMap()
	lock := &signalingLocker{waiting: make(chan struct{}, 2)}
	sender := &cursorSender{mu: lock}
	errSend := errors.New("message too large")

	inSend := make(chan struct{})
	failSend := make(chan struct{})
	var failOnce sync.Once
	fail := func() { failOnce.Do(func() { close(failSend) }) }
	var bSent [][]byte
	var wg sync.WaitGroup
	t.Cleanup(func() {
		fail()
		awaitJoin(t, &wg)
	})
	aErr := make(chan error, 1)
	bErr := make(chan error, 1)

	wg.Add(1)
	go func() {
		defer wg.Done()
		aErr <- sendWithCursor(sender, tokenMap, 1, locA, tokenA, func([]byte) error {
			close(inSend)
			<-failSend
			return errSend
		})
	}()
	awaitSignal(t, lock.waiting, "A to take the lock")
	awaitSignal(t, inSend, "A to start sending")

	wg.Add(1)
	go func() {
		defer wg.Done()
		bErr <- sendWithCursor(sender, tokenMap, 1, locB, tokenB, func(cursor []byte) error {
			bSent = append(bSent, cursor)
			return nil
		})
	}()
	awaitSignal(t, lock.waiting, "B to queue on the lock")

	fail()
	awaitJoin(t, &wg)

	require.ErrorIs(t, <-aErr, errSend)
	require.ErrorIs(t, <-bErr, errSend)
	require.Empty(t, bSent)
}
