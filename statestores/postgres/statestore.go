package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"time"

	connectorPostgres "github.com/adiom-data/dsync/connectors/postgres"
	"github.com/adiom-data/dsync/internal/util"
	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/cenkalti/backoff/v4"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	checkSQL   = "SELECT store, id, data, updated_at FROM dsync_state.objects WHERE false"
	upsertSQL  = "INSERT INTO dsync_state.objects (store, id, data) VALUES ($1, $2, $3) ON CONFLICT (store, id) DO UPDATE SET data = excluded.data, updated_at = now()"
	selectSQL  = "SELECT data FROM dsync_state.objects WHERE store = $1 AND id = $2"
	deleteSQL  = "DELETE FROM dsync_state.objects WHERE store = $1 AND id = $2"
	setupHint  = "check the connection string and that the dsync_state.objects table exists and is accessible (see the PostgreSQL state store setup script in the README)"
	invalidDSN = "invalid postgres state store connection string"
)

type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type StateStoreSettings struct {
	ConnectionString string
	Retry            util.RetryPolicy
}

type StateStore struct {
	settings       StateStoreSettings
	attemptTimeout time.Duration
	open           func(ctx context.Context, connectionString string) (querier, func(), error)
	registry       *bson.Registry
	q              querier
	closePool      func()
	ctx            context.Context
}

func NewPostgresStateStore(settings StateStoreSettings) *StateStore {
	registry := bson.NewRegistry()
	registry.RegisterTypeMapEntry(bson.TypeEmbeddedDocument, reflect.TypeFor[bson.M]())
	return &StateStore{settings: settings, attemptTimeout: 30 * time.Second, open: openPool, registry: registry}
}

func openPool(ctx context.Context, connectionString string) (querier, func(), error) {
	config, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		return nil, nil, errors.New(invalidDSN)
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, nil, sanitize(err)
	}
	return pool, pool.Close, nil
}

func sanitize(err error) error {
	if err == nil {
		return nil
	}
	return classify("postgres state store request failed", err)
}

func classify(msg string, err error) error {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return fmt.Errorf("%s: SQLSTATE %s", msg, pgErr.Code)
	}
	if _, ok := errors.AsType[*pgconn.ConnectError](err); ok {
		msg += ": could not connect"
	}
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		if errors.Is(err, cause) {
			return fmt.Errorf("%s: %w", msg, cause)
		}
	}
	if netErr, ok := errors.AsType[net.Error](err); ok && netErr.Timeout() {
		msg += ": network timeout"
	}
	return errors.New(msg)
}

func (s *StateStore) Setup(ctx context.Context) error {
	s.ctx = ctx
	q, closePool, err := s.open(ctx, s.settings.ConnectionString)
	if err != nil {
		return err
	}
	checkCtx, cancel := context.WithTimeout(ctx, s.attemptTimeout)
	defer cancel()
	if _, err := q.Exec(checkCtx, checkSQL); err != nil {
		closePool()
		return fmt.Errorf("%w: %s", classify("postgres state store is not usable", err), setupHint)
	}
	s.q, s.closePool = q, closePool
	return nil
}

func (s *StateStore) Teardown() {
	if s.closePool != nil {
		s.closePool()
	}
}

func (s *StateStore) withRetry(op func(ctx context.Context) error) error {
	err := backoff.Retry(func() error {
		ctx, cancel := context.WithTimeout(s.ctx, s.attemptTimeout)
		defer cancel()
		err := op(ctx)
		if err != nil && !connectorPostgres.IsTransientError(err) {
			return backoff.Permanent(err)
		}
		return err
	}, s.settings.Retry.Backoff(s.ctx))
	if errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	return sanitize(err)
}

func idKey(id any) (string, error) {
	v := reflect.ValueOf(id)
	if v.Kind() != reflect.String {
		return "", errors.New("state object id must be a string")
	}
	return v.String(), nil
}

func (s *StateStore) PersistObject(storeName string, id any, obj any) error {
	key, err := idKey(id)
	if err != nil {
		return err
	}
	data, err := bson.Marshal(obj)
	if err != nil {
		return fmt.Errorf("encode state object: %w", err)
	}
	return s.withRetry(func(ctx context.Context) error {
		_, err := s.q.Exec(ctx, upsertSQL, storeName, key, data)
		return err
	})
}

func (s *StateStore) RetrieveObject(storeName string, id any, obj any) error {
	key, err := idKey(id)
	if err != nil {
		return err
	}
	var data []byte
	err = s.withRetry(func(ctx context.Context) error {
		return s.q.QueryRow(ctx, selectSQL, storeName, key).Scan(&data)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return iface.ErrNotFound
	}
	if err != nil {
		return err
	}
	dec := bson.NewDecoder(bson.NewDocumentReader(bytes.NewReader(data)))
	dec.SetRegistry(s.registry)
	if err := dec.Decode(obj); err != nil {
		return fmt.Errorf("decode state object: %w", err)
	}
	return nil
}

func (s *StateStore) DeleteObject(storeName string, id any) error {
	key, err := idKey(id)
	if err != nil {
		return err
	}
	return s.withRetry(func(ctx context.Context) error {
		_, err := s.q.Exec(ctx, deleteSQL, storeName, key)
		return err
	})
}
