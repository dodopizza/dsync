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
	"github.com/stretchr/testify/assert"
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
