package pglib

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrorResponseErrorKeepsPgError(t *testing.T) {
	err := errorResponseError(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "40001", Message: "could not serialize"})

	pgErr, ok := errors.AsType[*pgconn.PgError](err)
	require.True(t, ok)
	assert.Equal(t, "40001", pgErr.Code)
}
