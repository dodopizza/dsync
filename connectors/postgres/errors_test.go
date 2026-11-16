package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type safeToRetryError struct{}

func (safeToRetryError) Error() string     { return "safe to retry" }
func (safeToRetryError) SafeToRetry() bool { return true }

func TestToConnectError(t *testing.T) {
	pgErr := func(code string) error { return &pgconn.PgError{Code: code, Message: "failure"} }
	cases := map[string]struct {
		err  error
		code connect.Code
	}{
		"canceled":                      {context.Canceled, connect.CodeCanceled},
		"wrapped canceled":              {fmt.Errorf("query: %w", context.Canceled), connect.CodeCanceled},
		"serialization failure":         {pgErr("40001"), connect.CodeAborted},
		"deadlock detected":             {pgErr("40P01"), connect.CodeAborted},
		"connection exception":          {pgErr("08000"), connect.CodeUnavailable},
		"connection failure":            {pgErr("08006"), connect.CodeUnavailable},
		"admin shutdown":                {pgErr("57P01"), connect.CodeUnavailable},
		"crash shutdown":                {pgErr("57P02"), connect.CodeUnavailable},
		"cannot connect now":            {pgErr("57P03"), connect.CodeUnavailable},
		"too many connections":          {pgErr("53300"), connect.CodeResourceExhausted},
		"lock not available":            {pgErr("55P03"), connect.CodeAborted},
		"idle session timeout":          {pgErr("57P05"), connect.CodeUnavailable},
		"idle in transaction timeout":   {pgErr("25P03"), connect.CodeUnavailable},
		"out of memory":                 {pgErr("53200"), connect.CodeResourceExhausted},
		"unique violation":              {pgErr("23505"), connect.CodeInternal},
		"invalid password":              {pgErr("28P01"), connect.CodeInternal},
		"invalid catalog name":          {pgErr("3D000"), connect.CodeInternal},
		"undefined table":               {pgErr("42P01"), connect.CodeInternal},
		"wrapped serialization failure": {fmt.Errorf("commit: %w", pgErr("40001")), connect.CodeAborted},
		"wrapped unique violation":      {fmt.Errorf("insert: %w", pgErr("23505")), connect.CodeInternal},
		"pg error beats retryable":      {fmt.Errorf("%w: %w", pgErr("23505"), io.EOF), connect.CodeInternal},
		"deadline exceeded":             {context.DeadlineExceeded, connect.CodeDeadlineExceeded},
		"wrapped deadline exceeded":     {fmt.Errorf("query: %w", context.DeadlineExceeded), connect.CodeDeadlineExceeded},
		"safe to retry":                 {safeToRetryError{}, connect.CodeUnavailable},
		"net op error":                  {&net.OpError{Op: "read", Net: "tcp", Err: errors.New("reset")}, connect.CodeUnavailable},
		"wrapped eof":                   {fmt.Errorf("read: %w", io.EOF), connect.CodeUnavailable},
		"wrapped unexpected eof":        {fmt.Errorf("read: %w", io.ErrUnexpectedEOF), connect.CodeUnavailable},
		"canceled beats pg error":       {errors.Join(context.Canceled, pgErr("40001")), connect.CodeCanceled},
		"pg error beats deadline":       {errors.Join(pgErr("23505"), context.DeadlineExceeded), connect.CodeInternal},
		"query canceled":                {pgErr("57014"), connect.CodeInternal},
		"net timeout":                   {&net.OpError{Op: "read", Net: "tcp", Err: syscall.ETIMEDOUT}, connect.CodeUnavailable},
		"unknown":                       {errors.New("boom"), connect.CodeInternal},
		"connect code beats canceled":   {connect.NewError(connect.CodeUnavailable, fmt.Errorf("send: %w", context.Canceled)), connect.CodeUnavailable},
		"existing connect code":         {connect.NewError(connect.CodeResourceExhausted, errors.New("too large")), connect.CodeResourceExhausted},
		"wrapped connect code":          {fmt.Errorf("send: %w", connect.NewError(connect.CodeResourceExhausted, errors.New("too large"))), connect.CodeResourceExhausted},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := toConnectError(tc.err)
			assert.Equal(t, tc.code, connect.CodeOf(got))
			assert.ErrorIs(t, got, tc.err)
		})
	}
}

func TestIsTransientError(t *testing.T) {
	pgErr := func(code string) error { return &pgconn.PgError{Code: code} }
	cases := map[string]struct {
		err  error
		want bool
	}{
		"serialization failure": {pgErr("40001"), true},
		"connection class":      {pgErr("08006"), true},
		"too many connections":  {pgErr("53300"), true},
		"unexpected eof":        {io.ErrUnexpectedEOF, true},
		"deadline":              {context.DeadlineExceeded, true},
		"unique violation":      {pgErr("23505"), false},
		"missing relation":      {pgErr("42P01"), false},
		"canceled":              {context.Canceled, false},
		"plain":                 {errors.New("boom"), false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) { assert.Equal(t, tc.want, IsTransientError(tc.err)) })
	}
}

const unreachableURL = "postgres://secretuser:secretpass@127.0.0.1:1/secretdb?connect_timeout=1"

func requireNoConnectionDetails(t *testing.T, err error) {
	t.Helper()
	requireNoDetailsInText(t, err.Error())
}

func requireNoDetailsInText(t *testing.T, text string) {
	t.Helper()
	for _, part := range []string{"secretuser", "secretpass", "secretdb", "127.0.0.1"} {
		assert.NotContains(t, text, part)
	}
}

func TestToConnectErrorHidesConnectionDetails(t *testing.T) {
	_, dialErr := pgconn.Connect(t.Context(), unreachableURL)
	_, isConnectError := errors.AsType[*pgconn.ConnectError](dialErr)
	require.True(t, isConnectError)

	got := setReplicaRoleError(dialErr)

	requireNoConnectionDetails(t, got)
	assert.Equal(t, connect.CodeUnavailable, connect.CodeOf(got))
	assert.True(t, IsTransientError(errors.Unwrap(got)))
	requireNoConnectionDetails(t, classifyStreamError(dialErr))
}

func TestToConnectErrorHidesNetworkAddresses(t *testing.T) {
	tcp := func(ip string) *net.TCPAddr { return &net.TCPAddr{IP: net.ParseIP(ip), Port: 5432} }
	cases := map[string]error{
		"op error":  &net.OpError{Op: "read", Net: "tcp", Source: tcp("10.1.2.3"), Addr: tcp("10.9.9.9"), Err: syscall.ECONNRESET},
		"dns error": &net.DNSError{Name: "secrethost.example", Err: "no such host", IsNotFound: true},
	}
	for name, original := range cases {
		t.Run(name, func(t *testing.T) {
			got := toConnectError(fmt.Errorf("query: %w", original))

			assert.Equal(t, connectCode(original), connect.CodeOf(got))
			assert.Contains(t, got.Error(), "network error")
			for _, part := range []string{"10.1.2.3", "10.9.9.9", "secrethost"} {
				assert.NotContains(t, got.Error(), part)
			}
			assert.ErrorIs(t, got, original)
			target, ok := errors.AsType[net.Error](got)
			require.True(t, ok)
			assert.Same(t, original, target)
		})
	}
}

func unreachableConn(t *testing.T) *conn {
	t.Helper()
	pool, err := pgxpool.New(t.Context(), unreachableURL)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return &conn{c: pool}
}

func TestSinkWriteLogHidesConnectionDetails(t *testing.T) {
	logs := captureLogs(t)
	c := unreachableConn(t)

	err := c.executePostgresWrites(t.Context(), "public.t", []string{"id"}, []postgresWriteOperation{{delete: true, values: []any{1}}})

	require.Error(t, err)
	require.Contains(t, logs.String(), "Failed to execute PostgreSQL sink operation")
	requireNoDetailsInText(t, logs.String())
}

func TestDropReplicationHidesConnectionDetails(t *testing.T) {
	c := unreachableConn(t)
	c.settings.SlotName = "dsync_slot"
	c.settings.PublicationName = "dsync_pub"
	c.created.create(func(markPublication, markSlot func()) error { markPublication(); markSlot(); return nil })

	err := c.DropReplication(t.Context())

	require.Error(t, err)
	requireNoConnectionDetails(t, err)
}

func TestToConnectErrorHidesServerMessage(t *testing.T) {
	original := &pgconn.PgError{Code: "42501", Message: `permission denied for database "markerdb"`}

	got := toConnectError(fmt.Errorf("query: %w", original))

	assert.Equal(t, connectCode(original), connect.CodeOf(got))
	assert.EqualError(t, got, "internal: query: postgres server error: SQLSTATE 42501")
	assert.NotContains(t, got.Error(), "markerdb")
	assert.ErrorIs(t, got, original)
}

func TestClassifyStreamErrorHidesSlotInUseMessage(t *testing.T) {
	got := classifyStreamError(&pgconn.PgError{Code: "55006", Message: "slot marker active"})

	assert.Equal(t, connect.CodeUnavailable, connect.CodeOf(got))
	assert.NotContains(t, got.Error(), "marker")
	assert.Contains(t, got.Error(), "SQLSTATE 55006")
}

func TestSetReplicaRoleErrorKeepsHintAndHidesServerMessage(t *testing.T) {
	original := &pgconn.PgError{Code: "42501", Message: "permission marker"}

	got := setReplicaRoleError(original)

	assert.EqualError(t, got, "internal: failed to set session_replication_role: postgres server error: SQLSTATE 42501")
	assert.ErrorIs(t, got, errSetReplicaRole)
	assert.ErrorIs(t, got, original)
}

func TestToConnectErrorKeepsCallerContextOfServerError(t *testing.T) {
	original := &pgconn.PgError{Severity: "ERROR", Code: "42501", Message: "permission marker"}

	got := toConnectError(fmt.Errorf("witness scan of db.col: %w", original))

	assert.EqualError(t, got, "internal: witness scan of db.col: postgres server error: SQLSTATE 42501")
}
