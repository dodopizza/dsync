package pglib

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strings"
	"syscall"

	"github.com/jackc/pgx/v5/pgconn"
)

type hiddenConnError struct {
	text string
	err  error
}

func (e *hiddenConnError) Error() string { return e.text }

func (e *hiddenConnError) Unwrap() error { return e.err }

func HideConnDetails(err error) error {
	if err == nil {
		return nil
	}
	if _, hidden := err.(*hiddenConnError); hidden {
		return err
	}
	text, ok := hiddenText(err)
	if !ok {
		return err
	}
	return &hiddenConnError{text: text, err: err}
}

func hiddenText(err error) (string, bool) {
	if text, ok := connErrorText(err); ok {
		return text + failureSuffix(err), true
	}
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return serverErrorText(err, pgErr), true
	}
	return "", false
}

func serverErrorText(err error, pgErr *pgconn.PgError) string {
	safe := "postgres server error" + ServerErrorNames(pgErr) + ": SQLSTATE " + pgErr.Code
	text := strings.ReplaceAll(err.Error(), pgErr.Error(), safe)
	if text == err.Error() || strings.Contains(text, "(SQLSTATE ") {
		return safe
	}
	return text
}

func connErrorText(err error) (string, bool) {
	_, dialed := errors.AsType[*pgconn.ConnectError](err)
	_, unparsed := errors.AsType[*pgconn.ParseConfigError](err)
	if dialed || unparsed {
		return "could not connect to postgres", true
	}
	_, op := errors.AsType[*net.OpError](err)
	_, dns := errors.AsType[*net.DNSError](err)
	_, addr := errors.AsType[*net.AddrError](err)
	if op || dns || addr {
		return "postgres network error", true
	}
	return "", false
}

func ServerErrorNames(pgErr *pgconn.PgError) string {
	names := ""
	if pgErr.TableName != "" {
		table := pgErr.TableName
		if pgErr.SchemaName != "" {
			table = pgErr.SchemaName + "." + table
		}
		names += ": table " + table
	}
	if pgErr.ColumnName != "" {
		names += ": column " + pgErr.ColumnName
	}
	if pgErr.ConstraintName != "" {
		names += ": constraint " + pgErr.ConstraintName
	}
	return names
}

func failureSuffix(err error) string {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return ": SQLSTATE " + pgErr.Code
	}
	if _, ok := errors.AsType[*net.DNSError](err); ok {
		return ": host lookup failed"
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return ": connection refused"
	}
	if netErr, ok := errors.AsType[net.Error](err); (ok && netErr.Timeout()) || errors.Is(err, context.DeadlineExceeded) {
		return ": timeout"
	}
	_, verification := errors.AsType[*tls.CertificateVerificationError](err)
	_, header := errors.AsType[tls.RecordHeaderError](err)
	_, alert := errors.AsType[tls.AlertError](err)
	if verification || header || alert {
		return ": tls"
	}
	return ""
}
