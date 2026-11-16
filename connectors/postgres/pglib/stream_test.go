package pglib

import (
	"context"
	"errors"
	"testing"
	"time"

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

func TestRunErrorsHideConnectionString(t *testing.T) {
	const password = "s3cr3t-marker"
	for name, url := range map[string]string{
		"malformed":      "postgres://u:" + password + "@127.0.0.1:badport/d?replication=database",
		"keyword value":  "host=127.0.0.1 port=1 user=u password=" + password + " dbname=d sslmode=bogus replication=database",
		"query password": "postgres://u@127.0.0.1:1/d?password=" + password + "&sslmode=bogus&replication=database",
		"closed port":    "postgres://u:" + password + "@127.0.0.1:1/d?sslmode=disable&replication=database",
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			err := (&ChangeStream{ReplicationURL: url, ReplicationSlotName: "s", PublicationName: "p"}).Run(ctx, make(chan []Update, 1))

			require.Error(t, err)
			assert.NotContains(t, err.Error(), password)
			assert.NotContains(t, err.Error(), url)
			_, isParseError := errors.AsType[*pgconn.ParseConfigError](err)
			assert.False(t, isParseError)
		})
	}
}
