package postgres

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	docIDColumn    = "id"
	docDocColumn   = "doc"
	identityAlways = "a"
	docColumnsSQL  = `SELECT a.attname, pg_catalog.format_type(a.atttypid, NULL), a.attgenerated <> '', a.attidentity::text,
	CASE WHEN array_position(i.indkey::int2[], a.attnum) < i.indnkeyatts THEN array_position(i.indkey::int2[], a.attnum) + 1 ELSE 0 END::int,
	COALESCE(NOT i.indimmediate, false), COALESCE(con.conname, ''),
	COALESCE((SELECT pg_catalog.col_description(r.attrelid, r.attnum) FROM pg_catalog.pg_attribute r
		WHERE r.attrelid = COALESCE(pg_catalog.pg_partition_root(a.attrelid), a.attrelid) AND r.attname = a.attname AND NOT r.attisdropped), ''),
	a.atttypmod <> -1
FROM pg_catalog.pg_attribute a
LEFT JOIN pg_catalog.pg_index i ON i.indrelid = a.attrelid AND i.indisprimary
LEFT JOIN pg_catalog.pg_constraint con ON con.conindid = i.indexrelid AND con.contype = 'p'
WHERE a.attrelid = to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum`
	docUniqueIndexSQL = `SELECT COALESCE(string_agg(c.relname, ', ' ORDER BY c.relname), '')
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
WHERE (i.indrelid = to_regclass($1) OR i.indrelid IN (SELECT relid FROM pg_catalog.pg_partition_tree(to_regclass($1))))
	AND NOT i.indisprimary AND (i.indisunique OR i.indisexclusion)`
)

type docColumn struct {
	Name string
	Type string
	Move bool
}

type docTable struct {
	Columns    []docColumn
	PrimaryKey []string
	Constraint string
}

type catalogColumn struct {
	Name       string
	Type       string
	Generated  bool
	Identity   string
	KeyOrder   int
	Deferrable bool
	Constraint string
	Comment    string
	Modifier   bool
}

func (c catalogColumn) unwritable() bool {
	return c.Generated || c.Identity == identityAlways
}

func shapeError(namespace, format string, args ...any) error {
	return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("table %s: "+format, append([]any{namespace}, args...)...))
}

func shapeDocTable(namespace string, columns []catalogColumn, primaryKey []string) (docTable, error) {
	byName := make(map[string]catalogColumn, len(columns))
	for _, c := range columns {
		byName[c.Name] = c
	}
	for _, required := range [][2]string{{docIDColumn, "text"}, {docDocColumn, "jsonb"}} {
		column, typ := required[0], required[1]
		found, ok := byName[column]
		if !ok || found.Type != typ {
			return docTable{}, shapeError(namespace, "needs a column %s of type %s", column, typ)
		}
		if found.unwritable() {
			return docTable{}, shapeError(namespace, "column %s must not be generated", column)
		}
	}
	if !slices.Contains(primaryKey, docIDColumn) {
		return docTable{}, shapeError(namespace, "primary key must include column %s", docIDColumn)
	}
	if slices.ContainsFunc(columns, func(c catalogColumn) bool { return c.KeyOrder > 0 && c.Deferrable }) {
		return docTable{}, shapeError(namespace, "primary key must not be deferred")
	}
	var selected []docColumn
	for _, c := range columns {
		if c.Name == docIDColumn || c.Name == docDocColumn || c.unwritable() {
			continue
		}
		move, err := moveMode(c, slices.Contains(primaryKey, c.Name))
		if err != nil {
			return docTable{}, shapeError(namespace, "%v", err)
		}
		selected = append(selected, docColumn{Name: c.Name, Type: c.Type, Move: move})
	}
	return docTable{Columns: selected, PrimaryKey: primaryKey, Constraint: constraintOf(columns)}, nil
}

func loadDocTable(ctx context.Context, c *pgxpool.Pool, namespace string) (docTable, error) {
	if parts := strings.Split(namespace, "."); len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return docTable{}, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("document mode needs a schema.table namespace"))
	}
	rows, err := c.Query(ctx, docColumnsSQL, SanitizeNamespace(namespace))
	if err != nil {
		return docTable{}, toConnectError(err)
	}
	columns, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (catalogColumn, error) {
		var col catalogColumn
		err := row.Scan(&col.Name, &col.Type, &col.Generated, &col.Identity, &col.KeyOrder, &col.Deferrable, &col.Constraint, &col.Comment, &col.Modifier)
		return col, err
	})
	if err != nil {
		return docTable{}, toConnectError(err)
	}
	if len(columns) == 0 {
		return docTable{}, shapeError(namespace, "does not exist")
	}
	return shapeDocTable(namespace, columns, primaryKeyOf(columns))
}

func constraintOf(columns []catalogColumn) string {
	for _, c := range columns {
		if c.KeyOrder > 0 {
			return c.Constraint
		}
	}
	return ""
}

func primaryKeyOf(columns []catalogColumn) []string {
	var keyed []catalogColumn
	for _, c := range columns {
		if c.KeyOrder > 0 {
			keyed = append(keyed, c)
		}
	}
	slices.SortFunc(keyed, func(a, b catalogColumn) int { return a.KeyOrder - b.KeyOrder })
	var key []string
	for _, c := range keyed {
		key = append(key, c.Name)
	}
	return key
}

func refuseUniqueIndexes(ctx context.Context, c *pgxpool.Pool, namespace string) error {
	var indexes string
	if err := c.QueryRow(ctx, docUniqueIndexSQL, SanitizeNamespace(namespace)).Scan(&indexes); err != nil {
		return toConnectError(err)
	}
	if indexes == "" {
		return nil
	}
	return shapeError(namespace, "unique or exclusion indexes %s: until the transfer ends a document-mode destination table and its partitions may have none besides the primary key", indexes)
}
