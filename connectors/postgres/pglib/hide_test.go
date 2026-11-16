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
	pgErr := &pgconn.PgError{Code: "23505", Message: "duplicate"}
	assert.Same(t, pgErr, HideConnDetails(pgErr))
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
