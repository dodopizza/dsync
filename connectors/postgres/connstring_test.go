package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewConnInvalidConnectionStringErrorHidesCredentials(t *testing.T) {
	for _, uri := range []string{
		"postgres://user:a%PWb@127.0.0.1:1/db",
		"postgres://user:PW@127.0.0.1:1/db?sslmode=bogus",
	} {
		_, err := NewConn(t.Context(), PostgresSettings{URL: uri})
		require.Error(t, err, uri)
		assert.NotContains(t, err.Error(), "PW")
	}
}
