package pglib

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
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
	text, ok := connErrorText(err)
	if !ok {
		return err
	}
	return &hiddenConnError{text: text + failureSuffix(err), err: err}
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
