//go:build external

package postgres

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/adiom-data/dsync/connectors/postgres/pglib"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (f *writeFixture) listData(ctx context.Context, namespace string) error {
	_, err := f.conn.ListData(ctx, connect.NewRequest(&adiomv1.ListDataRequest{Partition: &adiomv1.Partition{Namespace: namespace}}))
	return err
}

func TestListDataTerminatedBackendIsUnavailable(t *testing.T) {
	ctx := testContext(t)
	f := newWriteFixture(ctx, t)
	f.conn.settings.Limit = 100
	require.NoError(t, f.writeData(ctx, item(t, 1, "a")))
	h := f.holdStatement(ctx, t, fmt.Sprintf("LOCK TABLE %s IN ACCESS EXCLUSIVE MODE", f.table))
	b := h.startWriter(ctx, ctx, t, func(wctx context.Context) error { return f.listData(wctx, f.namespace) })

	_, err := h.monitor.Exec(ctx, "SELECT pg_terminate_backend($1)", b.writerPID)
	must(t, err, "terminate")
	readErr := b.await(ctx, t)
	b.release(ctx, t)

	require.Error(t, readErr)
	assert.Equal(t, connect.CodeUnavailable, connect.CodeOf(readErr))
	require.NoError(t, f.listData(ctx, f.namespace))
}

func TestReadMissingRelationIsInternal(t *testing.T) {
	ctx := testContext(t)
	f := newWriteFixture(ctx, t)
	f.conn.settings.Limit = 100
	missing := f.namespace + "_missing"
	f.conn.pkeys[missing] = []string{"id"}

	listErr := f.listData(ctx, missing)
	_, metaErr := f.conn.GetNamespaceMetadata(ctx, connect.NewRequest(&adiomv1.GetNamespaceMetadataRequest{Namespace: missing}))

	for _, err := range []error{listErr, metaErr} {
		require.Error(t, err)
		assert.Equal(t, connect.CodeInternal, connect.CodeOf(err))
		assert.Equal(t, "42P01", sqlState(t, err))
	}
}

type replicationFixture struct {
	*writeFixture
	slot        string
	publication string
	admin       *pgx.Conn
}

func newReplicationFixture(ctx context.Context, t *testing.T) *replicationFixture {
	f := newWriteFixture(ctx, t)
	admin := adminConn(ctx, t)
	var capable bool
	must(t, admin.QueryRow(ctx, "SELECT current_setting('wal_level') = 'logical' AND (rolreplication OR rolsuper) FROM pg_roles WHERE rolname = current_user").Scan(&capable), "probe replication")
	if !capable {
		t.Skip("logical replication needs wal_level=logical and a REPLICATION role")
	}
	name := fmt.Sprintf("it_%d", time.Now().UnixNano())
	_, err := admin.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", name, f.table))
	must(t, err, "create publication")
	t.Cleanup(func() {
		cctx, cancel := cleanupContext()
		defer cancel()
		if _, err := admin.Exec(cctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", name)); err != nil {
			t.Errorf("publication drop failed (%T)", err)
		}
	})
	_, err = admin.Exec(ctx, "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", name)
	must(t, err, "create slot")
	t.Cleanup(func() {
		cctx, cancel := cleanupContext()
		defer cancel()
		for {
			_, err := admin.Exec(cctx, "SELECT pg_drop_replication_slot($1)", name)
			if err == nil {
				return
			}
			if cctx.Err() != nil {
				t.Errorf("slot drop failed (%T)", err)
				return
			}
			time.Sleep(pollInterval)
		}
	})
	replicationURL, err := createReplicationUrl(testDSN(t))
	must(t, err, "replication url")
	f.conn.replicationUrl = replicationURL
	f.conn.settings.StreamMaxBatchSize = 10
	f.conn.settings.StreamMaxBatchWait = time.Second
	f.conn.settings.StreamFlushDelay = time.Minute
	return &replicationFixture{writeFixture: f, slot: name, publication: name, admin: admin}
}

func (r *replicationFixture) cursor(t *testing.T) []byte {
	b, err := (&streamCursor{SlotName: r.slot, PublicationName: r.publication}).Encode()
	require.NoError(t, err)
	return b
}

func (r *replicationFixture) waitWalSender(ctx context.Context, t *testing.T) int32 {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		var pid int32
		err := r.admin.QueryRow(ctx, "SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1 AND active_pid IS NOT NULL", r.slot).Scan(&pid)
		if err == nil {
			return pid
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			t.Fatalf("poll walsender failed (%T)", err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("walsender never became active")
		case <-ticker.C:
		}
	}
}

func (r *replicationFixture) streamThroughServer(ctx context.Context, t *testing.T) <-chan error {
	mux := http.NewServeMux()
	mux.Handle(adiomv1connect.NewConnectorServiceHandler(r.conn))
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	client := adiomv1connect.NewConnectorServiceClient(server.Client(), server.URL)
	request := connect.NewRequest(&adiomv1.StreamUpdatesRequest{Cursor: r.cursor(t)})

	sctx, cancel := context.WithCancel(ctx)
	result := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		stream, err := client.StreamUpdates(sctx, request)
		if err != nil {
			result <- err
			return
		}
		defer stream.Close()
		for stream.Receive() {
		}
		result <- stream.Err()
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("stream goroutine did not stop")
		}
	})
	return result
}

func awaitStreamError(ctx context.Context, t *testing.T, result <-chan error) error {
	select {
	case err := <-result:
		require.Error(t, err)
		return err
	case <-ctx.Done():
		t.Fatal("stream did not return")
		return nil
	}
}

func TestStreamUpdatesTerminatedWalSenderIsUnavailable(t *testing.T) {
	ctx := testContext(t)
	r := newReplicationFixture(ctx, t)
	result := r.streamThroughServer(ctx, t)

	pid := r.waitWalSender(ctx, t)
	_, err := r.admin.Exec(ctx, "SELECT pg_terminate_backend($1)", pid)
	must(t, err, "terminate")

	assert.Equal(t, connect.CodeUnavailable, connect.CodeOf(awaitStreamError(ctx, t, result)))
}

func TestStartReplicationOnActiveSlotIsUnavailable(t *testing.T) {
	ctx := testContext(t)
	r := newReplicationFixture(ctx, t)
	run := func(ctx context.Context) error {
		cs := &pglib.ChangeStream{ReplicationURL: r.conn.replicationUrl, ReplicationSlotName: r.slot, PublicationName: r.publication, MinFlushDelay: time.Minute}
		return cs.Run(ctx, make(chan []pglib.Update, 1))
	}

	first, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- run(first) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("first stream did not stop")
		}
	})
	r.waitWalSender(ctx, t)

	err := run(ctx)

	require.Error(t, err)
	assert.Equal(t, "55006", sqlState(t, err))
	assert.Equal(t, connect.CodeUnavailable, connect.CodeOf(classifyStreamError(err)))
}
