//go:build external

package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/stretchr/testify/require"
)

func (r *replicationFixture) planWithUpdates(ctx context.Context, t *testing.T) {
	_, err := r.conn.GeneratePlan(ctx, connect.NewRequest(&adiomv1.GeneratePlanRequest{
		Namespaces: []string{r.namespace},
		Updates:    true,
	}))
	must(t, err, "generate plan")
}

func (r *replicationFixture) exists(ctx context.Context, t *testing.T, slot, publication string) (bool, bool) {
	var hasSlot, hasPublication bool
	must(t, r.admin.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", slot).Scan(&hasSlot), "probe slot")
	must(t, r.admin.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)", publication).Scan(&hasPublication), "probe publication")
	return hasSlot, hasPublication
}

func (r *replicationFixture) useOwnNames(ctx context.Context, t *testing.T) (string, string) {
	name := fmt.Sprintf("it_drop_%d", time.Now().UnixNano())
	r.conn.settings.Force = true
	r.conn.settings.SlotName = name
	r.conn.settings.PublicationName = name
	t.Cleanup(func() {
		cctx, cancel := cleanupContext()
		defer cancel()
		_, _ = r.admin.Exec(cctx, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1", name)
		_, _ = r.admin.Exec(cctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", name))
	})
	return name, name
}

func TestDropReplicationDropsWhatGeneratePlanCreated(t *testing.T) {
	ctx := testContext(t)
	f := newReplicationFixture(ctx, t)
	slot, publication := f.useOwnNames(ctx, t)

	f.planWithUpdates(ctx, t)
	hasSlot, hasPublication := f.exists(ctx, t, slot, publication)
	require.True(t, hasSlot)
	require.True(t, hasPublication)

	require.NoError(t, f.conn.DropReplication(ctx))

	hasSlot, hasPublication = f.exists(ctx, t, slot, publication)
	require.False(t, hasSlot)
	require.False(t, hasPublication)
}

func TestDropReplicationLeavesWhatManualModeDidNotCreate(t *testing.T) {
	ctx := testContext(t)
	f := newReplicationFixture(ctx, t)
	f.conn.settings.Force = false
	f.conn.settings.SlotName = f.slot
	f.conn.settings.PublicationName = f.publication

	f.planWithUpdates(ctx, t)
	require.NoError(t, f.conn.DropReplication(ctx))

	hasSlot, hasPublication := f.exists(ctx, t, f.slot, f.publication)
	require.True(t, hasSlot)
	require.True(t, hasPublication)
}

func TestDropReplicationTwiceIsANoOp(t *testing.T) {
	ctx := testContext(t)
	f := newReplicationFixture(ctx, t)
	f.useOwnNames(ctx, t)
	f.planWithUpdates(ctx, t)

	require.NoError(t, f.conn.DropReplication(ctx))
	require.NoError(t, f.conn.DropReplication(ctx))
}
