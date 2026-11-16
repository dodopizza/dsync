package postgres

import (
	"context"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func inUse() error { return &pgconn.PgError{Code: sqlStateObjectInUse} }

var _ interface{ DropReplication(context.Context) error } = (*conn)(nil)

func dropSeen(r *createdReplication) (slot, publication bool, err error) {
	err = r.drop(func(s, p bool) error {
		slot, publication = s, p
		return nil
	})
	return slot, publication, err
}

func TestCreatedReplicationDropReturnsAndClearsTheMarks(t *testing.T) {
	var r createdReplication
	slot, publication, err := dropSeen(&r)
	require.NoError(t, err)
	require.False(t, slot)
	require.False(t, publication)

	var r2 createdReplication
	require.NoError(t, r2.create(func(markPublication, _ func()) error {
		markPublication()
		return nil
	}))
	slot, publication, _ = dropSeen(&r2)
	require.False(t, slot)
	require.True(t, publication)

	var r3 createdReplication
	require.NoError(t, r3.create(func(markPublication, markSlot func()) error {
		markPublication()
		markSlot()
		return nil
	}))
	slot, publication, _ = dropSeen(&r3)
	require.True(t, slot)
	require.True(t, publication)
	slot, publication, _ = dropSeen(&r3)
	require.False(t, slot)
	require.False(t, publication)
}

func TestCreatedReplicationDropWaitsForARunningCreateAndSeesItsMarks(t *testing.T) {
	var r createdReplication
	inCreate := make(chan struct{})
	finishCreate := make(chan struct{})
	createDone := make(chan error, 1)
	go func() {
		createDone <- r.create(func(markPublication, markSlot func()) error {
			markPublication()
			close(inCreate)
			<-finishCreate
			markSlot()
			return nil
		})
	}()
	<-inCreate
	type seen struct{ slot, publication bool }
	dropped := make(chan seen, 1)
	go func() {
		slot, publication, _ := dropSeen(&r)
		dropped <- seen{slot, publication}
	}()

	select {
	case <-dropped:
		t.Fatal("drop did not wait for the running create")
	case <-time.After(100 * time.Millisecond):
	}
	close(finishCreate)

	require.NoError(t, <-createDone)
	select {
	case got := <-dropped:
		require.Equal(t, seen{slot: true, publication: true}, got)
	case <-time.After(3 * time.Second):
		t.Fatal("drop did not finish")
	}
}

func TestCreatedReplicationRefusesACreateAfterTheDrop(t *testing.T) {
	var r createdReplication
	_, _, err := dropSeen(&r)
	require.NoError(t, err)
	ran := false

	err = r.create(func(func(), func()) error {
		ran = true
		return nil
	})

	require.ErrorIs(t, err, errReplicationDropped)
	require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
	require.False(t, ran)
}

func TestCreatedReplicationIsRaceCleanUnderConcurrentUse(t *testing.T) {
	var r createdReplication
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = r.create(func(markPublication, markSlot func()) error {
				markPublication()
				markSlot()
				return nil
			})
		}()
		go func() {
			defer wg.Done()
			_, _, _ = dropSeen(&r)
		}()
	}
	wg.Wait()
}

func TestDropReplicationDoesNothingWhenNothingWasCreated(t *testing.T) {
	require.NoError(t, (&conn{}).DropReplication(t.Context()))
}

func TestRetryWhileSlotInUse(t *testing.T) {
	other := &pgconn.PgError{Code: "42501"}
	for name, tc := range map[string]struct {
		results   []error
		wantErr   error
		wantCalls int
	}{
		"succeeds at once":         {[]error{nil}, nil, 1},
		"succeeds after in use":    {[]error{inUse(), inUse(), nil}, nil, 3},
		"other error returns once": {[]error{inUse(), other, nil}, other, 2},
	} {
		t.Run(name, func(t *testing.T) {
			calls := 0
			err := retryWhileSlotInUse(t.Context(), 5, time.Millisecond, func() error {
				calls++
				return tc.results[calls-1]
			})

			require.Equal(t, tc.wantCalls, calls)
			if tc.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.wantErr)
			}
		})
	}
}

func TestRetryWhileSlotInUseStopsAtTheBound(t *testing.T) {
	calls := 0

	err := retryWhileSlotInUse(t.Context(), 3, time.Millisecond, func() error {
		calls++
		return inUse()
	})

	require.Equal(t, 3, calls)
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
}

func TestRetryWhileSlotInUseStopsWhenTheContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	calls := 0

	err := retryWhileSlotInUse(ctx, 100, time.Hour, func() error {
		calls++
		cancel()
		return inUse()
	})

	require.Equal(t, 1, calls)
	require.ErrorIs(t, err, context.Canceled)
}
