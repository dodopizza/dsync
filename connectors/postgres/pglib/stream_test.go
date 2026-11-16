package pglib

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
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
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()

			err := (&ChangeStream{ReplicationURL: url, ReplicationSlotName: "s", PublicationName: "p"}).Run(ctx, make(chan UpdateBatch, 1))

			require.Error(t, err)
			assert.NotContains(t, err.Error(), password)
			assert.NotContains(t, err.Error(), url)
			_, isParseError := errors.AsType[*pgconn.ParseConfigError](err)
			assert.False(t, isParseError)
		})
	}
}

func TestPublicationNamesArgQuotesIdentifierInLiteral(t *testing.T) {
	assert.Equal(t, `publication_names '"dsync_pub"'`, publicationNamesArg("dsync_pub"))
	assert.Equal(t, `publication_names '"My''Pub"",x"'`, publicationNamesArg(`My'Pub",x`))
}

func TestFillUnchangedValues(t *testing.T) {
	newValues := map[string]interface{}{"id": int32(1), "name": "new", "toasted": unchangedToastValue{}}
	oldValues := map[string]interface{}{"id": int32(1), "name": "old", "toasted": "large value"}

	fillUnchangedValues(newValues, oldValues)

	want := map[string]interface{}{"id": int32(1), "name": "new", "toasted": "large value"}
	if !reflect.DeepEqual(newValues, want) {
		t.Fatalf("values = %#v, want %#v", newValues, want)
	}
}

func TestUnchangedToastColumns(t *testing.T) {
	values := map[string]interface{}{"id": int32(1), "toasted": unchangedToastValue{}}
	got := unchangedToastColumns(values)
	want := []string{"toasted"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("columns = %#v, want %#v", got, want)
	}
}

func TestHandleLogicalTruncateFailsBeforeBatch(t *testing.T) {
	state := &replState{
		currentLSN: 42,
		relations: map[uint32]*pglogrepl.RelationMessageV2{
			7: {RelationMessage: pglogrepl.RelationMessage{RelationID: 7, Namespace: "public", RelationName: "orders"}},
		},
		updates: []Update{{Namespace: "public", TableName: "orders", Type: UpdateTypeInsert}},
	}
	fh := &flushHelper{}
	ch := make(chan UpdateBatch, 1)

	err := handleLogical(t.Context(), state, fh, ch, &pglogrepl.TruncateMessageV2{
		TruncateMessage: pglogrepl.TruncateMessage{RelationNum: 2, RelationIDs: []uint32{7, 9}},
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "public.orders")
	assert.Contains(t, err.Error(), "9")
	assert.Contains(t, err.Error(), "TRUNCATE")
	assert.Contains(t, err.Error(), "not supported")
	assert.Empty(t, ch)
	assert.Empty(t, fh.items)
}
