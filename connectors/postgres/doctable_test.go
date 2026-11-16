package postgres

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const shapeNamespace = "app.items"

func baseColumns() []catalogColumn {
	return []catalogColumn{
		{Name: "id", Type: "text"},
		{Name: "doc", Type: "jsonb"},
		{Name: "Region", Type: "text"},
		{Name: "qty", Type: "integer"},
	}
}

func TestShapeDocTableKeepsSelectedColumnsInOrderWithTypes(t *testing.T) {
	got, err := shapeDocTable(shapeNamespace, baseColumns(), []string{"id", "Region"})

	require.NoError(t, err)
	assert.Equal(t, []docColumn{{"Region", "text"}, {"qty", "integer"}}, got.Columns)
	assert.Equal(t, []string{"id", "Region"}, got.PrimaryKey)
}

func TestShapeDocTableSkipsGeneratedAndAlwaysIdentityButKeepsDefaultIdentity(t *testing.T) {
	columns := append(baseColumns(),
		catalogColumn{Name: "stored", Type: "integer", Generated: true},
		catalogColumn{Name: "always", Type: "bigint", Identity: "a"},
		catalogColumn{Name: "byDefault", Type: "bigint", Identity: "d"},
	)

	got, err := shapeDocTable(shapeNamespace, columns, []string{"id"})

	require.NoError(t, err)
	assert.Equal(t, []docColumn{{"Region", "text"}, {"qty", "integer"}, {"byDefault", "bigint"}}, got.Columns)
}

func TestShapeDocTableRejectsBadShapesNamingTableAndColumn(t *testing.T) {
	without := func(name string) []catalogColumn {
		var out []catalogColumn
		for _, c := range baseColumns() {
			if c.Name != name {
				out = append(out, c)
			}
		}
		return out
	}
	retyped := func(name, typ string) []catalogColumn {
		out := baseColumns()
		for i := range out {
			if out[i].Name == name {
				out[i].Type = typ
			}
		}
		return out
	}
	generated := func(name string) []catalogColumn {
		out := baseColumns()
		for i := range out {
			if out[i].Name == name {
				out[i].Generated = true
			}
		}
		return out
	}
	for name, tc := range map[string]struct {
		columns []catalogColumn
		pk      []string
		want    string
	}{
		"no id":              {without("id"), []string{"id"}, "id"},
		"id not text":        {retyped("id", "integer"), []string{"id"}, "id"},
		"no doc":             {without("doc"), []string{"id"}, "doc"},
		"doc not jsonb":      {retyped("doc", "json"), []string{"id"}, "doc"},
		"no primary key":     {baseColumns(), nil, "primary key"},
		"key without id":     {baseColumns(), []string{"Region"}, "primary key"},
		"id generated":       {generated("id"), []string{"id"}, "generated"},
		"doc generated":      {generated("doc"), []string{"id"}, "generated"},
		"id always identity": {append(without("id"), catalogColumn{Name: "id", Type: "text", Identity: "a"}), []string{"id"}, "generated"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := shapeDocTable(shapeNamespace, tc.columns, tc.pk)

			require.Error(t, err)
			assert.Contains(t, err.Error(), shapeNamespace)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestLoadDocTableRejectsNonTwoPartNamespacesBeforeQuerying(t *testing.T) {
	for _, namespace := range []string{"items", "x.schema.items", "", ".items", "schema.", "a.b.c.d"} {
		t.Run(namespace, func(t *testing.T) {
			_, err := loadDocTable(context.Background(), nil, namespace)

			require.Error(t, err)
			assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
		})
	}
}
