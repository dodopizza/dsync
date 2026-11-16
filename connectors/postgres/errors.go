package postgres

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	sqlStateSerializationFailure            = "40001"
	sqlStateDeadlockDetected                = "40P01"
	sqlStateConnectionClass                 = "08"
	sqlStateAdminShutdown                   = "57P01"
	sqlStateCrashShutdown                   = "57P02"
	sqlStateCannotConnectNow                = "57P03"
	sqlStateTooManyConnections              = "53300"
	sqlStateLockNotAvailable                = "55P03"
	sqlStateOutOfMemory                     = "53200"
	sqlStateIdleSessionTimeout              = "57P05"
	sqlStateIdleInTransactionSessionTimeout = "25P03"
	sqlStateObjectInUse                     = "55006"
)

func toConnectError(err error) error {
	if _, ok := errors.AsType[*connect.Error](err); ok {
		return err
	}
	return connect.NewError(connectCode(err), err)
}

func connectCode(err error) connect.Code {
	if errors.Is(err, context.Canceled) {
		return connect.CodeCanceled
	}
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return sqlStateCode(pgErr.Code)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return connect.CodeDeadlineExceeded
	}
	if _, isNetErr := errors.AsType[net.Error](err); isNetErr ||
		pgconn.SafeToRetry(err) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) {
		return connect.CodeUnavailable
	}
	return connect.CodeInternal
}

func sqlStateCode(sqlState string) connect.Code {
	switch sqlState {
	case sqlStateSerializationFailure, sqlStateDeadlockDetected, sqlStateLockNotAvailable:
		return connect.CodeAborted
	case sqlStateAdminShutdown, sqlStateCrashShutdown, sqlStateCannotConnectNow,
		sqlStateIdleSessionTimeout, sqlStateIdleInTransactionSessionTimeout:
		return connect.CodeUnavailable
	case sqlStateTooManyConnections, sqlStateOutOfMemory:
		return connect.CodeResourceExhausted
	}
	if strings.HasPrefix(sqlState, sqlStateConnectionClass) {
		return connect.CodeUnavailable
	}
	return connect.CodeInternal
}

func classifyStreamError(err error) error {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && pgErr.Code == sqlStateObjectInUse {
		return connect.NewError(connect.CodeUnavailable, err)
	}
	return toConnectError(err)
}
