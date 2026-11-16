package postgres

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/adiom-data/dsync/coordinators/simple"
	"github.com/adiom-data/dsync/internal/util"
	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type call struct {
	sql  string
	args []any
}

type fakeRow struct {
	data []byte
	err  error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	*dest[0].(*[]byte) = r.data
	return nil
}

type fakeQuerier struct {
	calls []call
	exec  func(n int) error
	row   func(n int) fakeRow
}

func (f *fakeQuerier) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f.calls = append(f.calls, call{sql, args})
	if f.exec == nil {
		return pgconn.CommandTag{}, nil
	}
	return pgconn.CommandTag{}, f.exec(len(f.calls))
}

func (f *fakeQuerier) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	f.calls = append(f.calls, call{sql, args})
	return f.row(len(f.calls))
}

func newStore(t *testing.T, q querier) *StateStore {
	t.Helper()
	s := NewPostgresStateStore(StateStoreSettings{ConnectionString: "unused", Retry: util.RetryPolicy{Retries: 3, MaxInterval: time.Millisecond}})
	s.open = func(context.Context, string) (querier, func(), error) { return q, func() {}, nil }
	require.NoError(t, s.Setup(t.Context()))
	return s
}

type planLike struct {
	Token []byte
	Count int64
	Low   any
	High  any
	Tasks []taskLike
}

type taskLike struct {
	Id  int64
	Def struct{ Low, High any }
}

func TestRoundTripKeepsBSONTypesOfInterfaceFields(t *testing.T) {
	oid := bson.NewObjectID()
	when := bson.NewDateTimeFromTime(time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC))
	task := taskLike{Id: 1}
	task.Def.Low, task.Def.High = oid, when
	in := planLike{Token: []byte{1, 2, 3}, Count: 1 << 40, Low: int64(7), High: "z", Tasks: []taskLike{task}}
	var stored []byte
	q := &fakeQuerier{
		exec: func(int) error { return nil },
		row:  func(int) fakeRow { return fakeRow{data: stored} },
	}
	s := newStore(t, q)
	require.NoError(t, s.PersistObject("flows", "f1", in))
	stored = q.calls[len(q.calls)-1].args[2].([]byte)

	var out planLike
	require.NoError(t, s.RetrieveObject("flows", "f1", &out))

	assert.Equal(t, in, out)
	assert.IsType(t, int64(0), out.Low)
	assert.IsType(t, bson.ObjectID{}, out.Tasks[0].Def.Low)
	assert.IsType(t, bson.DateTime(0), out.Tasks[0].Def.High)
}

func TestRoundTripDecodesEmbeddedDocumentsAsBsonM(t *testing.T) {
	data, err := bson.Marshal(bson.M{"Low": bson.M{"a": int64(1)}})
	require.NoError(t, err)
	s := newStore(t, &fakeQuerier{row: func(int) fakeRow { return fakeRow{data: data} }})
	var out struct{ Low any }

	require.NoError(t, s.RetrieveObject("flows", "f1", &out))

	assert.Equal(t, bson.M{"a": int64(1)}, out.Low)
}

func TestStatementsBindStoreIDAndDataAsParameters(t *testing.T) {
	q := &fakeQuerier{row: func(int) fakeRow { return fakeRow{err: pgx.ErrNoRows} }}
	s := newStore(t, q)
	q.calls = nil

	require.NoError(t, s.PersistObject("flow_state'; DROP", iface.FlowID("42"), bson.M{"a": 1}))
	require.NoError(t, s.DeleteObject("flows", iface.FlowID("42")))
	_ = s.RetrieveObject("flows", "f1", &bson.M{})

	data, err := bson.Marshal(bson.M{"a": 1})
	require.NoError(t, err)
	assert.Equal(t, []call{
		{"INSERT INTO dsync_state.objects (store, id, data) VALUES ($1, $2, $3) ON CONFLICT (store, id) DO UPDATE SET data = excluded.data, updated_at = now()", []any{"flow_state'; DROP", "42", data}},
		{"DELETE FROM dsync_state.objects WHERE store = $1 AND id = $2", []any{"flows", "42"}},
		{"SELECT data FROM dsync_state.objects WHERE store = $1 AND id = $2", []any{"flows", "f1"}},
	}, q.calls)
}

func TestRetrieveMapsOnlyNoRowsToNotFound(t *testing.T) {
	boom := &pgconn.PgError{Code: "42P01", Message: "relation host=secret missing"}
	for name, tc := range map[string]struct {
		err  error
		want func(error) bool
	}{
		"no rows":   {pgx.ErrNoRows, func(err error) bool { return errors.Is(err, iface.ErrNotFound) }},
		"permanent": {boom, func(err error) bool { return err != nil && !errors.Is(err, iface.ErrNotFound) }},
	} {
		t.Run(name, func(t *testing.T) {
			s := newStore(t, &fakeQuerier{row: func(int) fakeRow { return fakeRow{err: tc.err} }})

			assert.True(t, tc.want(s.RetrieveObject("flows", "f1", &bson.M{})))
		})
	}
}

func TestTransientErrorsAreRetriedAndPermanentOnesAreNot(t *testing.T) {
	transient := &pgconn.PgError{Code: "40001"}
	permanent := &pgconn.PgError{Code: "23505"}
	for name, tc := range map[string]struct {
		first     error
		wantCalls int
		wantErr   bool
	}{
		"transient then ok": {transient, 2, false},
		"network then ok":   {io.ErrUnexpectedEOF, 2, false},
		"permanent":         {permanent, 1, true},
	} {
		t.Run(name, func(t *testing.T) {
			q := &fakeQuerier{}
			s := newStore(t, q)
			q.calls = nil
			q.exec = func(n int) error {
				if n == 1 {
					return tc.first
				}
				return nil
			}

			err := s.DeleteObject("flows", "f1")

			assert.Equal(t, tc.wantErr, err != nil)
			assert.Len(t, q.calls, tc.wantCalls)
		})
	}
}

func TestTransientErrorsGiveUpAfterTheRetryBudget(t *testing.T) {
	q := &fakeQuerier{}
	s := newStore(t, q)
	q.calls = nil
	q.exec = func(int) error { return &pgconn.PgError{Code: "08006"} }

	err := s.DeleteObject("flows", "f1")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "SQLSTATE 08006")
	assert.Len(t, q.calls, 4)
}

func TestAttemptTimeoutBoundsAHungQuery(t *testing.T) {
	q := &hangingQuerier{}
	s := NewPostgresStateStore(StateStoreSettings{Retry: util.RetryPolicy{Retries: 1, MaxInterval: time.Millisecond}})
	s.attemptTimeout = 20 * time.Millisecond
	s.open = func(context.Context, string) (querier, func(), error) { return q, func() {}, nil }
	s.ctx, s.q = t.Context(), q

	start := time.Now()
	err := s.DeleteObject("flows", "f1")

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Equal(t, 2, q.calls)
	assert.Less(t, time.Since(start), 5*time.Second)
}

type hangingQuerier struct{ calls int }

func (h *hangingQuerier) Exec(ctx context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
	h.calls++
	<-ctx.Done()
	return pgconn.CommandTag{}, ctx.Err()
}

func (h *hangingQuerier) QueryRow(context.Context, string, ...any) pgx.Row { return nil }

func TestSetupProbesTheTableColumnsWithASelect(t *testing.T) {
	q := &fakeQuerier{}
	newStore(t, q)

	assert.Equal(t, []call{{"SELECT store, id, data, updated_at FROM dsync_state.objects WHERE false", nil}}, q.calls)
}

const (
	sentinelHost = "db-host-sentinel.invalid"
	sentinelUser = "user-sentinel"
	sentinelPass = "pass-sentinel"
	sentinelDB   = "db-sentinel"
)

func TestErrorsHideConnectionDetails(t *testing.T) {
	dsn := "postgres://" + sentinelUser + ":" + sentinelPass + "@" + sentinelHost + ":5432/" + sentinelDB + "?sslmode=disable"
	leaky := func(err error) error {
		return errors.Join(err, errors.New("dial tcp "+sentinelHost+": user="+sentinelUser+" password="+sentinelPass+" dbname="+sentinelDB+" "+dsn))
	}
	leakyPg := &pgconn.PgError{Code: "28P01", Message: "password " + sentinelPass + " for " + sentinelUser + " at " + sentinelHost + "/" + sentinelDB}
	cases := map[string]func() error{
		"malformed dsn": func() error {
			s := NewPostgresStateStore(StateStoreSettings{ConnectionString: "postgres://" + sentinelUser + ":" + sentinelPass + "@" + sentinelHost + ":badport/" + sentinelDB})
			return s.Setup(t.Context())
		},
		"check fails with pg error": func() error {
			s := NewPostgresStateStore(StateStoreSettings{ConnectionString: dsn})
			s.open = func(context.Context, string) (querier, func(), error) {
				return &fakeQuerier{exec: func(int) error { return leakyPg }}, func() {}, nil
			}
			return s.Setup(t.Context())
		},
		"check fails with network error": func() error {
			s := NewPostgresStateStore(StateStoreSettings{ConnectionString: dsn})
			s.open = func(context.Context, string) (querier, func(), error) {
				return &fakeQuerier{exec: func(int) error { return leaky(io.ErrUnexpectedEOF) }}, func() {}, nil
			}
			return s.Setup(t.Context())
		},
		"operation pg error": func() error {
			return newStore(t, &fakeQuerier{}).withFailure(leakyPg)
		},
		"operation network error": func() error {
			return newStore(t, &fakeQuerier{}).withFailure(leaky(io.ErrUnexpectedEOF))
		},
	}
	for name, run := range cases {
		t.Run(name, func(t *testing.T) {
			err := run()

			require.Error(t, err)
			for _, secret := range []string{sentinelHost, sentinelUser, sentinelPass, sentinelDB, dsn} {
				assert.NotContains(t, err.Error(), secret)
			}
		})
	}
}

func TestErrorsNameTheFailureClassWithoutConnectionDetails(t *testing.T) {
	connectErr := &pgconn.ConnectError{Config: &pgconn.Config{Host: sentinelHost, User: sentinelUser, Password: sentinelPass, Database: sentinelDB}}
	timeoutErr := &net.DNSError{Err: "i/o timeout", Name: sentinelHost, Server: sentinelHost, IsTimeout: true}
	classes := map[string]error{"could not connect": connectErr, "network timeout": timeoutErr}
	for class, cause := range classes {
		paths := map[string]func() error{
			"setup": func() error {
				s := NewPostgresStateStore(StateStoreSettings{})
				s.open = func(context.Context, string) (querier, func(), error) {
					return &fakeQuerier{exec: func(int) error { return cause }}, func() {}, nil
				}
				return s.Setup(t.Context())
			},
			"operation": func() error {
				return newStore(t, &fakeQuerier{}).withFailure(cause)
			},
		}
		for path, run := range paths {
			t.Run(class+"/"+path, func(t *testing.T) {
				err := run()

				require.Error(t, err)
				assert.Contains(t, err.Error(), class)
				for _, secret := range []string{sentinelHost, sentinelUser, sentinelPass, sentinelDB} {
					assert.NotContains(t, err.Error(), secret)
				}
			})
		}
	}
}

func (s *StateStore) withFailure(err error) error {
	s.settings.Retry = util.RetryPolicy{}
	s.q = &fakeQuerier{exec: func(int) error { return err }}
	return s.DeleteObject("flows", "f1")
}

func TestSetupFailureNamesSQLStateAndTheSetupScript(t *testing.T) {
	s := NewPostgresStateStore(StateStoreSettings{})
	s.open = func(context.Context, string) (querier, func(), error) {
		return &fakeQuerier{exec: func(int) error { return &pgconn.PgError{Code: "42P01"} }}, func() {}, nil
	}

	err := s.Setup(t.Context())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "SQLSTATE 42P01")
	assert.Contains(t, err.Error(), "README")
}

func TestNonStringIDsAreRejectedAndFlowIDIsAccepted(t *testing.T) {
	q := &fakeQuerier{row: func(int) fakeRow { return fakeRow{err: pgx.ErrNoRows} }}
	s := newStore(t, q)
	q.calls = nil

	for _, id := range []any{1, int64(1), nil, []byte("1"), struct{}{}} {
		assert.Error(t, s.PersistObject("flows", id, bson.M{}), "%T", id)
		assert.Error(t, s.RetrieveObject("flows", id, &bson.M{}), "%T", id)
		assert.Error(t, s.DeleteObject("flows", id), "%T", id)
	}
	assert.Empty(t, q.calls)

	require.NoError(t, s.PersistObject("flows", iface.FlowID("f1"), bson.M{}))
	require.NoError(t, s.DeleteObject("flows", iface.FlowID("f1")))
	assert.ErrorIs(t, s.RetrieveObject("flows", iface.FlowID("f1"), &bson.M{}), iface.ErrNotFound)
	for _, c := range q.calls {
		assert.Equal(t, "f1", c.args[1])
	}
	assert.Len(t, q.calls, 3)
}

func TestReadPlanRoundTripKeepsEveryFieldAndType(t *testing.T) {
	task := func(id iface.ReadPlanTaskID, status uint, low, high []byte, estimated, copied int64) iface.ReadPlanTask {
		task := iface.ReadPlanTask{Id: id, Status: status, EstimatedDocCount: estimated, DocsCopied: copied}
		task.Def.Db, task.Def.Col, task.Def.PartitionKey = "db", "public.items", "pk"
		task.Def.Low, task.Def.High = bson.Binary{Data: low}, bson.Binary{Subtype: 0x80, Data: high}
		return task
	}
	in := simple.FlowDetails{
		FlowID:  "f1",
		Options: iface.FlowOptions{Type: iface.UnidirectionalFlowType, SrcId: "src", DstId: "dst"},
		ReadPlan: iface.ConnectorReadPlan{
			Tasks:          []iface.ReadPlanTask{task(1, iface.ReadPlanTaskStatus_Completed, []byte{1, 2}, []byte{3}, 1<<40, 17), task(2, iface.ReadPlanTaskStatus_New, []byte{9}, []byte{}, 5, 0)},
			CdcResumeToken: []byte{7, 8, 9},
			CreatedAtEpoch: 1700000000,
		},
	}
	var stored []byte
	q := &fakeQuerier{row: func(int) fakeRow { return fakeRow{data: stored} }}
	s := newStore(t, q)
	require.NoError(t, s.PersistObject("flows", iface.FlowID("f1"), &in))
	stored = q.calls[len(q.calls)-1].args[2].([]byte)

	var out simple.FlowDetails
	require.NoError(t, s.RetrieveObject("flows", iface.FlowID("f1"), &out))

	assert.Equal(t, in.FlowID, out.FlowID)
	assert.Equal(t, in.Options, out.Options)
	require.Len(t, out.ReadPlan.Tasks, 2)
	for i, want := range in.ReadPlan.Tasks {
		got := out.ReadPlan.Tasks[i]
		assert.Equal(t, want.Id, got.Id)
		assert.Equal(t, want.Status, got.Status)
		assert.Equal(t, want.EstimatedDocCount, got.EstimatedDocCount)
		assert.Equal(t, want.DocsCopied, got.DocsCopied)
		assert.Equal(t, want.Def.Db, got.Def.Db)
		assert.Equal(t, want.Def.Col, got.Def.Col)
		assert.Equal(t, want.Def.PartitionKey, got.Def.PartitionKey)
		assert.IsType(t, bson.Binary{}, got.Def.Low)
		assert.IsType(t, bson.Binary{}, got.Def.High)
		assert.Equal(t, want.Def.Low, got.Def.Low)
		assert.Equal(t, want.Def.High, got.Def.High)
	}
	assert.Equal(t, in.ReadPlan.CdcResumeToken, out.ReadPlan.CdcResumeToken)
	assert.Equal(t, in.ReadPlan.CreatedAtEpoch, out.ReadPlan.CreatedAtEpoch)
}

func TestSetupFailureClosesThePoolAndTeardownClosesItOtherwise(t *testing.T) {
	closed := 0
	open := func(exec func(int) error) func(context.Context, string) (querier, func(), error) {
		return func(context.Context, string) (querier, func(), error) {
			return &fakeQuerier{exec: exec}, func() { closed++ }, nil
		}
	}
	failing := NewPostgresStateStore(StateStoreSettings{})
	failing.open = open(func(int) error { return &pgconn.PgError{Code: "42P01"} })

	require.Error(t, failing.Setup(t.Context()))
	assert.Equal(t, 1, closed)
	failing.Teardown()
	assert.Equal(t, 1, closed)

	ok := NewPostgresStateStore(StateStoreSettings{})
	ok.open = open(nil)
	require.NoError(t, ok.Setup(t.Context()))
	assert.Equal(t, 1, closed)
	ok.Teardown()
	assert.Equal(t, 2, closed)
}
