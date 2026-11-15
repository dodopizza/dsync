//go:build external

package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	postgresEnvironmentVariable = "POSTGRES_TEST"
	pollInterval                = 10 * time.Millisecond
	testTimeout                 = 30 * time.Second
)

type writeFixture struct {
	conn      *conn
	namespace string
	table     string
}

func must(t *testing.T, err error, what string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s failed (%T)", what, err)
	}
}

func testContext(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)
	return ctx
}

func testDSN(t *testing.T) string {
	dsn := os.Getenv(postgresEnvironmentVariable)
	if dsn == "" {
		t.Skip(postgresEnvironmentVariable + " is not set")
	}
	return dsn
}

func cleanupContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

func adminConn(ctx context.Context, t *testing.T) *pgx.Conn {
	c, err := pgx.Connect(ctx, testDSN(t))
	must(t, err, "admin connect")
	t.Cleanup(func() {
		cctx, cancel := cleanupContext()
		defer cancel()
		_ = c.Close(cctx)
	})
	return c
}

func newWriteFixture(ctx context.Context, t *testing.T) *writeFixture {
	admin := adminConn(ctx, t)

	schema := fmt.Sprintf("it_%d", time.Now().UnixNano())
	_, err := admin.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %[1]s;
CREATE TABLE %[1]s.parents (id integer PRIMARY KEY);
CREATE TABLE %[1]s.items (id integer PRIMARY KEY, v text NOT NULL, ref integer REFERENCES %[1]s.parents (id) DEFERRABLE INITIALLY DEFERRED)`, schema))
	must(t, err, "schema setup")
	t.Cleanup(func() {
		cctx, cancel := cleanupContext()
		defer cancel()
		if _, err := admin.Exec(cctx, fmt.Sprintf("DROP SCHEMA %s CASCADE", schema)); err != nil {
			t.Errorf("schema drop failed (%T)", err)
		}
	})

	config, err := pgxpool.ParseConfig(testDSN(t))
	must(t, err, "parse config")
	config.MaxConns = 2
	pool, err := pgxpool.NewWithConfig(ctx, config)
	must(t, err, "pool")
	t.Cleanup(pool.Close)

	namespace := schema + ".items"
	return &writeFixture{
		conn: &conn{
			c:            pool,
			pkeys:        map[string][]string{namespace: {"id"}},
			bsonRegistry: NewBSONRegistry(),
		},
		namespace: namespace,
		table:     namespace,
	}
}

func (f *writeFixture) rows(ctx context.Context, t *testing.T) map[int32]string {
	rows, err := f.conn.c.Query(ctx, fmt.Sprintf("SELECT id, v FROM %s", f.table))
	must(t, err, "select")
	got := map[int32]string{}
	for rows.Next() {
		var id int32
		var v string
		must(t, rows.Scan(&id, &v), "scan")
		got[id] = v
	}
	must(t, rows.Err(), "rows")
	return got
}

func doc(t *testing.T, fields bson.M) []byte {
	b, err := bson.Marshal(fields)
	require.NoError(t, err)
	return b
}

func item(t *testing.T, id int32, v any) []byte {
	return doc(t, bson.M{"id": id, "v": v})
}

func idValue(t *testing.T, id int32) []*adiomv1.BsonValue {
	typ, data, err := marshalValueWithRegistry(NewBSONRegistry(), id)
	require.NoError(t, err)
	return []*adiomv1.BsonValue{{Type: uint32(typ), Data: data}}
}

func upsert(t *testing.T, id int32, v any) *adiomv1.Update {
	return &adiomv1.Update{Id: idValue(t, id), Type: adiomv1.UpdateType_UPDATE_TYPE_UPDATE, Data: item(t, id, v)}
}

func remove(t *testing.T, id int32) *adiomv1.Update {
	return &adiomv1.Update{Id: idValue(t, id), Type: adiomv1.UpdateType_UPDATE_TYPE_DELETE}
}

func (f *writeFixture) writeData(ctx context.Context, docs ...[]byte) error {
	_, err := f.conn.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{Namespace: f.namespace, Data: docs}))
	return err
}

func (f *writeFixture) writeUpdates(ctx context.Context, updates ...*adiomv1.Update) error {
	_, err := f.conn.WriteUpdates(ctx, connect.NewRequest(&adiomv1.WriteUpdatesRequest{Namespace: f.namespace, Updates: updates}))
	return err
}

func sqlState(t *testing.T, err error) string {
	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	require.True(t, ok, "no PgError in chain")
	return pgErr.Code
}

type holder struct {
	f       *writeFixture
	tx      pgx.Tx
	pid     int32
	monitor *pgx.Conn
	known   []int32
}

type blockedWrite struct {
	h         *holder
	writerPID int32
	result    chan error
	cancel    context.CancelFunc
}

func (f *writeFixture) holdRow(ctx context.Context, t *testing.T, id int32) *holder {
	holderConn := adminConn(ctx, t)
	tx, err := holderConn.Begin(ctx)
	must(t, err, "begin")
	t.Cleanup(func() {
		cctx, cancel := cleanupContext()
		defer cancel()
		_ = tx.Rollback(cctx)
	})
	h := &holder{f: f, tx: tx, monitor: adminConn(ctx, t)}
	must(t, tx.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&h.pid), "holder pid")
	h.known = []int32{h.pid}
	_, err = tx.Exec(ctx, fmt.Sprintf("UPDATE %s SET v = 'held' WHERE id = $1", f.table), id)
	must(t, err, "hold row")
	return h
}

func (h *holder) startWriter(ctx, writeCtx context.Context, t *testing.T, write func(context.Context) error) *blockedWrite {
	wctx, cancel := context.WithCancel(writeCtx)
	result := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		result <- write(wctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("writer goroutine did not stop")
		}
	})

	b := &blockedWrite{h: h, result: result, cancel: cancel}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		err := h.monitor.QueryRow(ctx, "SELECT pid FROM pg_stat_activity WHERE datname = current_database() AND pid <> ALL ($1) AND pg_blocking_pids(pid) && $1", h.known).Scan(&b.writerPID)
		if err == nil {
			h.known = append(h.known, b.writerPID)
			return b
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			t.Fatalf("poll blocked writer failed (%T)", err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("writer never became blocked")
		case <-ticker.C:
		}
	}
}

func (f *writeFixture) blockWriter(ctx, writeCtx context.Context, t *testing.T, holdID int32, write func(context.Context) error) *blockedWrite {
	return f.holdRow(ctx, t, holdID).startWriter(ctx, writeCtx, t, write)
}

func (b *blockedWrite) await(ctx context.Context, t *testing.T) error {
	select {
	case err := <-b.result:
		return err
	case <-ctx.Done():
		t.Fatal("write did not return")
		return nil
	}
}

func (b *blockedWrite) waitWriterGone(ctx context.Context, t *testing.T) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		var gone bool
		must(t, b.h.monitor.QueryRow(ctx, "SELECT NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1)", b.writerPID).Scan(&gone), "poll writer backend")
		if gone {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("writer backend never exited")
		case <-ticker.C:
		}
	}
}

func (b *blockedWrite) release(ctx context.Context, t *testing.T) {
	must(t, b.h.tx.Rollback(ctx), "release holder")
}

func TestWriteDataBatchIsAtomic(t *testing.T) {
	ctx := testContext(t)
	f := newWriteFixture(ctx, t)

	err := f.writeData(ctx, item(t, 1, "a"), item(t, 2, "b"), item(t, 3, nil))

	require.Error(t, err)
	assert.Equal(t, connect.CodeInternal, connect.CodeOf(err))
	assert.Equal(t, "23502", sqlState(t, err))
	assert.Empty(t, f.rows(ctx, t))
}

func TestWriteUpdatesBatchIsAtomic(t *testing.T) {
	ctx := testContext(t)
	f := newWriteFixture(ctx, t)
	require.NoError(t, f.writeData(ctx, item(t, 1, "a")))

	nullValue := &adiomv1.Update{Id: idValue(t, 3), Type: adiomv1.UpdateType_UPDATE_TYPE_INSERT, Data: item(t, 3, nil)}
	err := f.writeUpdates(ctx, upsert(t, 1, "changed"), upsert(t, 2, "b"), nullValue)

	require.Error(t, err)
	assert.Equal(t, connect.CodeInternal, connect.CodeOf(err))
	assert.Equal(t, "23502", sqlState(t, err))
	assert.Equal(t, map[int32]string{1: "a"}, f.rows(ctx, t))
}

func TestWriteDataCommitFailureLeavesNothing(t *testing.T) {
	ctx := testContext(t)
	f := newWriteFixture(ctx, t)
	orphanA := doc(t, bson.M{"id": int32(1), "v": "a", "ref": int32(999)})
	orphanB := doc(t, bson.M{"id": int32(2), "v": "b", "ref": int32(999)})

	err := f.writeData(ctx, orphanA, orphanB)

	require.Error(t, err)
	assert.Equal(t, connect.CodeInternal, connect.CodeOf(err))
	assert.Equal(t, "23503", sqlState(t, err))
	assert.Empty(t, f.rows(ctx, t))
	require.NoError(t, f.writeData(ctx, item(t, 3, "c")))
	assert.Equal(t, map[int32]string{3: "c"}, f.rows(ctx, t))
}

func TestWriteDataTerminatedBackendIsUnavailable(t *testing.T) {
	ctx := testContext(t)
	f := newWriteFixture(ctx, t)
	require.NoError(t, f.writeData(ctx, item(t, 1, "a")))
	batch := [][]byte{item(t, 1, "x"), item(t, 2, "y")}
	b := f.blockWriter(ctx, ctx, t, 1, func(wctx context.Context) error { return f.writeData(wctx, batch...) })

	_, err := b.h.monitor.Exec(ctx, "SELECT pg_terminate_backend($1)", b.writerPID)
	must(t, err, "terminate")
	writeErr := b.await(ctx, t)
	b.release(ctx, t)

	require.Error(t, writeErr)
	assert.Equal(t, connect.CodeUnavailable, connect.CodeOf(writeErr))
	require.NoError(t, f.writeData(ctx, batch...))
	assert.Equal(t, map[int32]string{1: "x", 2: "y"}, f.rows(ctx, t))
}

func TestWriteUpdatesCanceledMidBatchWritesNothing(t *testing.T) {
	ctx := testContext(t)
	f := newWriteFixture(ctx, t)
	require.NoError(t, f.writeData(ctx, item(t, 1, "a")))
	updates := []*adiomv1.Update{upsert(t, 2, "y"), upsert(t, 1, "x")}
	b := f.blockWriter(ctx, ctx, t, 1, func(wctx context.Context) error { return f.writeUpdates(wctx, updates...) })

	b.cancel()
	writeErr := b.await(ctx, t)
	b.waitWriterGone(ctx, t)
	b.release(ctx, t)

	require.Error(t, writeErr)
	assert.Equal(t, connect.CodeCanceled, connect.CodeOf(writeErr))
	assert.Equal(t, map[int32]string{1: "a"}, f.rows(ctx, t))
	require.NoError(t, f.writeData(ctx, item(t, 3, "c")))
	assert.Equal(t, map[int32]string{1: "a", 3: "c"}, f.rows(ctx, t))
}

func TestWriteUpdatesDeadlockIsAborted(t *testing.T) {
	ctx := testContext(t)
	f := newWriteFixture(ctx, t)
	require.NoError(t, f.writeData(ctx, item(t, 1, "seed"), item(t, 2, "seed"), item(t, 3, "seed")))
	updatesA := []*adiomv1.Update{upsert(t, 1, "a"), upsert(t, 3, "a"), upsert(t, 2, "a")}
	updatesB := []*adiomv1.Update{upsert(t, 2, "b"), upsert(t, 3, "b"), upsert(t, 1, "b")}
	h := f.holdRow(ctx, t, 3)
	a := h.startWriter(ctx, ctx, t, func(wctx context.Context) error { return f.writeUpdates(wctx, updatesA...) })
	b := h.startWriter(ctx, ctx, t, func(wctx context.Context) error { return f.writeUpdates(wctx, updatesB...) })

	must(t, h.tx.Rollback(ctx), "release holder")
	errA, errB := a.await(ctx, t), b.await(ctx, t)

	require.True(t, (errA == nil) != (errB == nil), "exactly one writer must fail")
	failed, failedUpdates, failedValue := errA, updatesA, "a"
	if errA == nil {
		failed, failedUpdates, failedValue = errB, updatesB, "b"
	}
	assert.Equal(t, connect.CodeAborted, connect.CodeOf(failed))
	assert.Equal(t, "40P01", sqlState(t, failed))
	require.NoError(t, f.writeUpdates(ctx, failedUpdates...))
	assert.Equal(t, map[int32]string{1: failedValue, 2: failedValue, 3: failedValue}, f.rows(ctx, t))
}

func TestWriteDataReplayIsIdempotent(t *testing.T) {
	ctx := testContext(t)
	f := newWriteFixture(ctx, t)
	batch := [][]byte{item(t, 1, "a"), item(t, 2, "b"), item(t, 1, "c")}

	require.NoError(t, f.writeData(ctx, batch...))
	once := f.rows(ctx, t)
	require.NoError(t, f.writeData(ctx, batch...))

	assert.Equal(t, map[int32]string{1: "c", 2: "b"}, once)
	assert.Equal(t, once, f.rows(ctx, t))
}

func TestWriteUpdatesReplayIsIdempotent(t *testing.T) {
	ctx := testContext(t)
	f := newWriteFixture(ctx, t)
	require.NoError(t, f.writeData(ctx, item(t, 1, "a"), item(t, 2, "b")))
	batch := []*adiomv1.Update{upsert(t, 1, "changed"), upsert(t, 10, "new"), remove(t, 2), remove(t, 99)}

	require.NoError(t, f.writeUpdates(ctx, batch...))
	once := f.rows(ctx, t)
	require.NoError(t, f.writeUpdates(ctx, batch...))

	assert.Equal(t, map[int32]string{1: "changed", 10: "new"}, once)
	assert.Equal(t, once, f.rows(ctx, t))
}
