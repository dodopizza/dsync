package pglib

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func parseConfigError(inner error) error {
	return pgconn.NewParseConfigError("postgres://secretuser:secretpass@secrethost:5432/secretdb", "failed", inner)
}

func TestHideConnDetailsFixedTextAndFailureClass(t *testing.T) {
	tcp := func(ip string) *net.TCPAddr { return &net.TCPAddr{IP: net.ParseIP(ip), Port: 5432} }
	reset := &net.OpError{Op: "read", Net: "tcp", Source: tcp("10.1.2.3"), Addr: tcp("10.9.9.9"), Err: syscall.ECONNRESET}
	cases := map[string]struct {
		err  error
		want string
	}{
		"network error":     {reset, "postgres network error"},
		"dns":               {&net.DNSError{Name: "secrethost.example", Err: "no such host", IsNotFound: true}, "postgres network error: host lookup failed"},
		"refused":           {&net.OpError{Op: "dial", Net: "tcp", Addr: tcp("10.9.9.9"), Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}, "postgres network error: connection refused"},
		"net timeout":       {&net.OpError{Op: "read", Net: "tcp", Addr: tcp("10.9.9.9"), Err: os.ErrDeadlineExceeded}, "postgres network error: timeout"},
		"dial deadline":     {parseConfigError(context.DeadlineExceeded), "could not connect to postgres: timeout"},
		"tls verification":  {parseConfigError(&tls.CertificateVerificationError{Err: errors.New("bad")}), "could not connect to postgres: tls"},
		"tls record header": {parseConfigError(tls.RecordHeaderError{Msg: "bad"}), "could not connect to postgres: tls"},
		"sqlstate only":     {parseConfigError(errors.Join(&pgconn.PgError{Code: "28P01"}, context.DeadlineExceeded)), "could not connect to postgres: SQLSTATE 28P01"},
		"no class":          {parseConfigError(errors.New("bad")), "could not connect to postgres"},
		"tls alert":         {parseConfigError(tls.AlertError(40)), "could not connect to postgres: tls"},
		"op error deadline": {&net.OpError{Op: "read", Net: "tcp", Addr: tcp("10.9.9.9"), Err: context.DeadlineExceeded}, "postgres network error: timeout"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := HideConnDetails(tc.err)

			assert.EqualError(t, got, tc.want)
			assert.ErrorIs(t, got, tc.err)
			assert.Same(t, tc.err, errors.Unwrap(got))
		})
	}
}

func TestHideConnDetailsLeavesOtherErrorsAlone(t *testing.T) {
	plain := errors.New("plain")
	assert.Nil(t, HideConnDetails(nil))
	assert.Same(t, plain, HideConnDetails(plain))
	assert.Equal(t, context.DeadlineExceeded, HideConnDetails(context.DeadlineExceeded))
	wrapped := fmt.Errorf("query: %w", context.DeadlineExceeded)
	assert.Same(t, wrapped, HideConnDetails(wrapped))
}

func TestHideConnDetailsReturnsHiddenInputUnchanged(t *testing.T) {
	hidden := HideConnDetails(&net.DNSError{Name: "secrethost.example"})

	require.Same(t, hidden, HideConnDetails(hidden))
}

func TestHideConnDetailsIgnoresHiddenErrorInsideChain(t *testing.T) {
	hidden := HideConnDetails(&net.DNSError{Name: "other.example"})
	raw := &net.OpError{Op: "read", Net: "tcp", Addr: &net.TCPAddr{IP: net.ParseIP("10.9.9.9"), Port: 5432}, Err: syscall.ECONNRESET}

	got := HideConnDetails(errors.Join(hidden, raw))

	assert.NotContains(t, got.Error(), "10.9.9.9")
	assert.ErrorIs(t, got, raw)
}

func TestHideConnDetailsJoinedDeadlineAndAddressError(t *testing.T) {
	raw := &net.OpError{Op: "read", Net: "tcp", Addr: &net.TCPAddr{IP: net.ParseIP("10.9.9.9"), Port: 5432}, Err: syscall.ECONNRESET}
	cases := map[string]error{
		"deadline first": errors.Join(context.DeadlineExceeded, raw),
		"deadline last":  errors.Join(raw, context.DeadlineExceeded),
		"addr error":     &net.AddrError{Err: "bad", Addr: "10.9.9.9"},
	}
	for name, err := range cases {
		t.Run(name, func(t *testing.T) {
			got := HideConnDetails(err)

			assert.NotContains(t, got.Error(), "10.9.9.9")
			assert.ErrorIs(t, got, err)
		})
	}
}

func TestHideConnDetailsServerError(t *testing.T) {
	const message = `permission denied for database "markerdb" to role "markerrole"`
	server := func(e pgconn.PgError) *pgconn.PgError {
		e.Message = message
		e.Detail = "markerdetail"
		e.Hint = "markerhint"
		e.Where = "markerwhere"
		return &e
	}
	full := server(pgconn.PgError{Code: "23505", SchemaName: "s", TableName: "t", ColumnName: "c", ConstraintName: "k"})
	bare := server(pgconn.PgError{Code: "42501"})
	cases := map[string]struct {
		err  error
		want string
	}{
		"all names":            {full, "postgres server error: table s.t: column c: constraint k: SQLSTATE 23505"},
		"no names":             {bare, "postgres server error: SQLSTATE 42501"},
		"table without schema": {server(pgconn.PgError{Code: "23502", TableName: "t"}), "postgres server error: table t: SQLSTATE 23502"},
		"column only":          {server(pgconn.PgError{Code: "22P02", ColumnName: "c"}), "postgres server error: column c: SQLSTATE 22P02"},
		"wrapped":              {fmt.Errorf("ctx: %w", full), "ctx: postgres server error: table s.t: column c: constraint k: SQLSTATE 23505"},
		"joined":               {errors.Join(errors.New("other"), bare), "other\npostgres server error: SQLSTATE 42501"},
		"connect error wins":   {parseConfigError(full), "could not connect to postgres: SQLSTATE 23505"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := HideConnDetails(tc.err)

			assert.EqualError(t, got, tc.want)
			assert.ErrorIs(t, got, tc.err)
			for _, part := range []string{"markerdb", "markerrole", "markerdetail", "markerhint", "markerwhere", "permission"} {
				assert.NotContains(t, got.Error(), part)
			}
			pgErr, ok := errors.AsType[*pgconn.PgError](got)
			require.True(t, ok)
			assert.NotEmpty(t, pgErr.Code)
		})
	}
}

func TestServerErrorNames(t *testing.T) {
	assert.Equal(t, "", ServerErrorNames(&pgconn.PgError{Code: "42501", Message: "markerdb"}))
	assert.Equal(t, ": table s.t: column c: constraint k", ServerErrorNames(&pgconn.PgError{SchemaName: "s", TableName: "t", ColumnName: "c", ConstraintName: "k"}))
}

func TestHideConnDetailsServerErrorKeepsCallerContext(t *testing.T) {
	a := &pgconn.PgError{Severity: "ERROR", Code: "42501", Message: "markera"}
	b := &pgconn.PgError{Severity: "ERROR", Code: "23505", Message: "markerb"}
	raw := &net.OpError{Op: "read", Net: "tcp", Addr: &net.TCPAddr{IP: net.ParseIP("10.9.9.9"), Port: 5432}, Err: syscall.ECONNRESET}
	cases := map[string]struct {
		err  error
		want string
	}{
		"context kept":      {fmt.Errorf("reading keys of ns1: %w", a), "reading keys of ns1: postgres server error: SQLSTATE 42501"},
		"two server errors": {errors.Join(a, b), "postgres server error: SQLSTATE 42501"},
		"in connect error":  {parseConfigError(a), "could not connect to postgres: SQLSTATE 42501"},
		"network wins":      {fmt.Errorf("ctx: %w", errors.Join(raw, a)), "postgres network error: SQLSTATE 42501"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := HideConnDetails(tc.err)

			assert.EqualError(t, got, tc.want)
			for _, part := range []string{"markera", "markerb", "10.9.9.9"} {
				assert.NotContains(t, got.Error(), part)
			}
		})
	}
}

func TestHideConnDetailsServerErrorWithoutOriginalTextFallsBackToSafeText(t *testing.T) {
	original := &pgconn.PgError{Severity: "ERROR", Code: "42501", Message: "markera"}

	got := HideConnDetails(errors.Join(HideConnDetails(original), errors.New("x")))

	assert.EqualError(t, got, "postgres server error: SQLSTATE 42501")
	found, ok := errors.AsType[*pgconn.PgError](got)
	require.True(t, ok)
	assert.Same(t, original, found)
}
