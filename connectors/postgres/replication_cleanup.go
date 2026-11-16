package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"connectrpc.com/connect"
	"github.com/adiom-data/dsync/connectors/postgres/pglib"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	slotDropAttempts = 20
	slotDropPause    = 250 * time.Millisecond
)

var errReplicationDropped = connect.NewError(connect.CodeFailedPrecondition, errors.New("replication resources already dropped"))

type createdReplication struct {
	mu          sync.Mutex
	slot        bool
	publication bool
	closed      bool
}

func (r *createdReplication) create(fn func(markPublication, markSlot func()) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return errReplicationDropped
	}
	return fn(func() { r.publication = true }, func() { r.slot = true })
}

func (r *createdReplication) drop(fn func(slot, publication bool) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	slot, publication := r.slot, r.publication
	r.slot, r.publication = false, false
	return fn(slot, publication)
}

func retryWhileSlotInUse(ctx context.Context, attempts int, pause time.Duration, op func() error) error {
	var err error
	for i := range attempts {
		if err = op(); err == nil {
			return nil
		}
		if pgErr, ok := errors.AsType[*pgconn.PgError](err); !ok || pgErr.Code != sqlStateObjectInUse {
			return err
		}
		if i == attempts-1 {
			break
		}
		select {
		case <-time.After(pause):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

func (c *conn) DropReplication(ctx context.Context) error {
	return c.created.drop(func(slot, publication bool) error {
		var errs []error
		if slot {
			err := retryWhileSlotInUse(ctx, slotDropAttempts, slotDropPause, func() error {
				_, err := c.c.Exec(ctx, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name=$1", c.settings.SlotName)
				return err
			})
			if err != nil {
				errs = append(errs, fmt.Errorf("drop replication slot %q: %w", c.settings.SlotName, pglib.HideConnDetails(err)))
			}
		}
		if publication {
			query := fmt.Sprintf("DROP PUBLICATION IF EXISTS %v", pgx.Identifier([]string{c.settings.PublicationName}).Sanitize())
			if _, err := c.c.Exec(ctx, query); err != nil {
				errs = append(errs, fmt.Errorf("drop publication %q: %w", c.settings.PublicationName, pglib.HideConnDetails(err)))
			}
		}
		return errors.Join(errs...)
	})
}
