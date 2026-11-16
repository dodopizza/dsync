//go:build external

package postgres

import (
	"testing"

	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestKeyListerScansACompositeKeyDocumentTableInIDOrderAndPages(t *testing.T) {
	ctx := testContext(t)
	f := newDocFixture(ctx, t)
	f.exec(ctx, t, `CREATE TABLE %s.items (id text NOT NULL, tenant text NOT NULL, doc jsonb NOT NULL, PRIMARY KEY (id, tenant))`, f.schema)
	f.exec(ctx, t, `INSERT INTO %s.items VALUES ('s:b', 'x', '{"_id": "b", "tenant": "x"}'), ('s:a', 'y', '{"_id": "a", "tenant": "y"}'), ('s:a', 'x', '{"_id": "a", "tenant": "x"}')`, f.schema)
	lister := newKeyLister(f.pool, map[string]string{"db.col": f.schema + ".items"})
	var tuples [][]bson.RawValue
	pages := 0

	err := lister.ScanKeys(ctx, iface.Namespace{Db: "db", Col: "col"}, []string{"tenant"}, 2, func(page [][]bson.RawValue) error {
		pages++
		tuples = append(tuples, page...)
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, 2, pages)
	require.Len(t, tuples, 3)
	require.Equal(t, bson.TypeString, tuples[0][0].Type)
	require.Equal(t, "a", tuples[0][0].StringValue())
	require.Equal(t, "b", tuples[2][0].StringValue())
	require.Equal(t, "x", tuples[2][1].StringValue())
}

func TestKeyListerRejectsATableWhoseKeyDiffersFromTheShardKey(t *testing.T) {
	ctx := testContext(t)
	f := newDocFixture(ctx, t)
	f.exec(ctx, t, `CREATE TABLE %s.items (id text PRIMARY KEY, doc jsonb NOT NULL)`, f.schema)
	lister := newKeyLister(f.pool, map[string]string{"db.col": f.schema + ".items"})

	err := lister.ScanKeys(ctx, iface.Namespace{Db: "db", Col: "col"}, []string{"tenant"}, 2, func([][]bson.RawValue) error { return nil })

	require.ErrorContains(t, err, "tenant")
}
