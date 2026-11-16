package postgres

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	markerPassword = "s3cr3t-marker"
	closedPortURL  = "postgres://u:" + markerPassword + "@127.0.0.1:1/d?sslmode=disable"
	malformedURL   = "postgres://u:" + markerPassword + "@127.0.0.1:badport/d"
	queryPassword  = "postgres://u@127.0.0.1:1/d?sslmode=disable&password=" + markerPassword
	keywordValue   = "host=127.0.0.1 port=1 user=u password=" + markerPassword + " dbname=d sslmode=disable"
	queryMalformed = "postgres://u@127.0.0.1:1/d?sslmode=bogus&password=" + markerPassword
)

func captureLogs(t *testing.T) *bytes.Buffer {
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &buf
}

func assertNoSecrets(t *testing.T, url string, texts ...string) {
	for _, text := range texts {
		assert.NotContains(t, text, markerPassword)
		assert.NotContains(t, text, url)
	}
}

func TestNewConnErrorsHideConnectionString(t *testing.T) {
	for name, url := range map[string]string{"malformed": malformedURL, "closed port": closedPortURL, "query password": queryPassword, "query password malformed": queryMalformed} {
		t.Run(name, func(t *testing.T) {
			logs := captureLogs(t)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()

			_, err := NewConn(ctx, PostgresSettings{URL: url, SlotName: "dsync_slot"})

			require.Error(t, err)
			assertNoSecrets(t, url, err.Error(), logs.String())
		})
	}
}

func TestCreateReplicationUrlAcceptsOnlyPostgresSchemes(t *testing.T) {
	for _, url := range []string{"postgres://u@127.0.0.1:1/d", "postgresql://u@127.0.0.1:1/d"} {
		got, err := createReplicationUrl(url)
		require.NoError(t, err)
		assert.Contains(t, got, "replication=database")
	}
	for _, url := range []string{keywordValue, "mysql://u@127.0.0.1:1/d", "127.0.0.1:1/d", ""} {
		_, err := createReplicationUrl(url)
		assert.EqualError(t, err, "invalid postgres connection string", url)
	}
}

func TestNewConnRejectsKeywordValueString(t *testing.T) {
	logs := captureLogs(t)

	_, err := NewConn(t.Context(), PostgresSettings{URL: keywordValue, SlotName: "dsync_slot"})

	assert.EqualError(t, err, "invalid postgres connection string")
	assertNoSecrets(t, keywordValue, logs.String())
}

func TestNewConnRejectsInvalidSlotName(t *testing.T) {
	for name, slot := range map[string]string{
		"keyword value": "host=h password=" + markerPassword,
		"uri":           closedPortURL,
		"uppercase":     "Slot",
		"empty":         "",
		"too long":      strings.Repeat("a", 64),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := NewConn(t.Context(), PostgresSettings{URL: closedPortURL, SlotName: slot})

			var connectErr *connect.Error
			require.ErrorAs(t, err, &connectErr)
			assert.Equal(t, connect.CodeInvalidArgument, connectErr.Code())
			assert.EqualError(t, err, "invalid_argument: invalid replication slot name")
			assert.NotContains(t, err.Error(), markerPassword)
			if slot != "" {
				assert.NotContains(t, err.Error(), slot)
			}
		})
	}
}

func TestValidSlotNames(t *testing.T) {
	assert.True(t, validSlotName("dsync_slot"))
	assert.True(t, validSlotName(strings.Repeat("a", 63)))
	assert.True(t, validSlotName("s_1"))
}
