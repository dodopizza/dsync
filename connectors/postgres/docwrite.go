package postgres

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"github.com/adiom-data/dsync/connectors/postgres/pglib"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	idField     = "_id"
	typeTimeTZ  = "timestamp with time zone"
	typeTimeNTZ = "timestamp without time zone"
)

var (
	integerTypes    = map[string]bool{"smallint": true, "integer": true, "bigint": true}
	comparableTypes = map[string]bool{
		"text": true, "character varying": true, "character": true, "smallint": true, "integer": true, "bigint": true,
		"real": true, "double precision": true, "numeric": true, "boolean": true, typeTimeTZ: true, typeTimeNTZ: true,
		"date": true, "bytea": true, "uuid": true,
	}
)

type docPlan struct {
	table     docTable
	columns   []docColumn
	updatedAt string
	field     string
	query     string
	delete    string
}

type docCache[T any] struct {
	mu    sync.Mutex
	items map[string]T
}

func (s *docCache[T]) get(namespace string, load func() (T, error)) (T, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if item, ok := s.items[namespace]; ok {
		return item, nil
	}
	item, err := load()
	if err != nil {
		return item, err
	}
	if s.items == nil {
		s.items = map[string]T{}
	}
	s.items[namespace] = item
	return item, nil
}

func buildDocPlan(namespace string, table docTable, settings PostgresSettings) (*docPlan, error) {
	updatedAt, field := settings.UpdatedAtColumn, settings.UpdatedAtField
	if updatedAt == "" && field != "" {
		return nil, shapeError(namespace, "updated-at field %s needs an updated-at column", field)
	}
	var columns []docColumn
	found := updatedAt == ""
	for _, col := range table.Columns {
		if col.Name != updatedAt {
			if !slices.Contains(table.PrimaryKey, col.Name) && !comparableTypes[col.Type] && col.Type != typeJSONB {
				return nil, shapeError(namespace, "column %s has type %s, which document mode cannot write", col.Name, col.Type)
			}
			columns = append(columns, col)
			continue
		}
		if col.Type != typeTimeTZ && col.Type != typeTimeNTZ {
			return nil, shapeError(namespace, "updated-at column %s must be %s or %s", updatedAt, typeTimeTZ, typeTimeNTZ)
		}
		found = true
	}
	if !found {
		return nil, shapeError(namespace, "updated-at column %s does not exist or is not writable", updatedAt)
	}
	for _, key := range table.PrimaryKey {
		if key != docIDColumn && !slices.ContainsFunc(columns, func(c docColumn) bool { return c.Name == key }) {
			return nil, shapeError(namespace, "primary key column %s is not writable or is the updated-at column", key)
		}
	}
	plan := &docPlan{table: table, columns: columns, updatedAt: updatedAt, field: field}
	plan.query = docUpsertSQL(namespace, plan)
	plan.delete = docDeleteSQL(namespace, plan.table.PrimaryKey)
	return plan, nil
}

func quoteIdents(names []string) []string {
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = pgx.Identifier{name}.Sanitize()
	}
	return quoted
}

func docUpsertSQL(namespace string, plan *docPlan) string {
	names := []string{docIDColumn, docDocColumn}
	compare := []string{docDocColumn}
	var set []string
	set = append(set, docDocColumn)
	for _, col := range plan.columns {
		names = append(names, col.Name)
		compare = append(compare, col.Name)
		if !slices.Contains(plan.table.PrimaryKey, col.Name) {
			set = append(set, col.Name)
		}
	}
	if plan.updatedAt != "" {
		names = append(names, plan.updatedAt)
		set = append(set, plan.updatedAt)
	}
	placeholders := make([]string, len(names))
	for i := range names {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	assignments := make([]string, len(set))
	for i, name := range quoteIdents(set) {
		assignments[i] = name + " = EXCLUDED." + name
	}
	current, incoming := prefixed("t.", compare), prefixed("EXCLUDED.", compare)
	return fmt.Sprintf(
		"INSERT INTO %s AS t (%s) VALUES (%s) ON CONFLICT ON CONSTRAINT %s DO UPDATE SET %s WHERE (%s) IS DISTINCT FROM (%s)",
		SanitizeNamespace(namespace), strings.Join(quoteIdents(names), ", "), strings.Join(placeholders, ", "),
		pgx.Identifier{plan.table.Constraint}.Sanitize(), strings.Join(assignments, ", "),
		strings.Join(current, ", "), strings.Join(incoming, ", "),
	)
}

func prefixed(prefix string, names []string) []string {
	out := quoteIdents(names)
	for i := range out {
		out[i] = prefix + out[i]
	}
	return out
}

func scalarValue(rv bson.RawValue) (any, error) {
	var value any
	var ok bool
	switch rv.Type {
	case bson.TypeString:
		value, ok = rv.StringValueOK()
	case bson.TypeInt32:
		value, ok = rv.Int32OK()
	case bson.TypeInt64:
		value, ok = rv.Int64OK()
	case bson.TypeDouble:
		value, ok = rv.DoubleOK()
	case bson.TypeBoolean:
		value, ok = rv.BooleanOK()
	case bson.TypeDateTime:
		var millis int64
		millis, ok = rv.DateTimeOK()
		value = bson.DateTime(millis).Time().UTC()
	case bson.TypeObjectID:
		var oid bson.ObjectID
		oid, ok = rv.ObjectIDOK()
		value = oid.Hex()
	case bson.TypeDecimal128:
		var dec bson.Decimal128
		dec, ok = rv.Decimal128OK()
		value = dec.String()
	case bson.TypeBinary:
		_, value, ok = rv.BinaryOK()
	default:
		return nil, fmt.Errorf("unsupported BSON type %s", rv.Type)
	}
	if !ok {
		return nil, fmt.Errorf("malformed BSON %s", rv.Type)
	}
	return value, nil
}

func keyValues(primaryKey []string, id string, values map[string]any) ([]any, error) {
	key := make([]any, len(primaryKey))
	for i, name := range primaryKey {
		if name == docIDColumn {
			key[i] = id
			continue
		}
		if values[name] == nil {
			return nil, fmt.Errorf("primary key column %s needs a non-null value", name)
		}
		key[i] = values[name]
	}
	return key, nil
}

func (p *docPlan) args(raw []byte, now time.Time) ([]any, error) {
	doc := bson.Raw(raw)
	if err := validateDoc(doc); err != nil {
		return nil, err
	}
	idValue, err := doc.LookupErr(idField)
	if err != nil {
		return nil, fmt.Errorf("missing %s", idField)
	}
	id, err := encodeID(idValue.Type, idValue.Value)
	if err != nil {
		return nil, err
	}
	values := make(map[string]any, len(p.columns))
	moved := map[string]bool{}
	args := []any{id, nil}
	for _, col := range p.columns {
		value, err := columnValue(doc, col)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", col.Name, err)
		}
		values[col.Name] = value
		args = append(args, value)
		if col.Move && value != nil {
			moved[col.Name] = true
		}
	}
	if _, err := keyValues(p.table.PrimaryKey, id, values); err != nil {
		return nil, err
	}
	rest, err := withoutFields(doc, moved)
	if err != nil {
		return nil, err
	}
	if args[1], err = bson.MarshalExtJSON(rest, true, false); err != nil {
		return nil, err
	}
	if p.updatedAt == "" {
		return args, nil
	}
	stamp, err := p.updatedAtValue(doc, now)
	if err != nil {
		return nil, err
	}
	return append(args, stamp), nil
}

func columnValue(doc bson.Raw, col docColumn) (any, error) {
	rv, err := doc.LookupErr(col.Name)
	if err != nil || rv.Type == bson.TypeNull {
		return nil, nil
	}
	if col.Move && !movePairs[col.Type].accepts(rv) {
		return nil, fmt.Errorf("BSON type %s cannot move into a column of type %s", rv.Type, col.Type)
	}
	if col.Type == typeJSONB {
		return jsonFragment(rv)
	}
	return typedValue(rv, col)
}

func typedValue(rv bson.RawValue, col docColumn) (any, error) {
	value, err := scalarValue(rv)
	if f, isDouble := value.(float64); isDouble && integerTypes[col.Type] {
		if f != math.Trunc(f) || f < -(1<<63) || f >= 1<<63 {
			return nil, fmt.Errorf("double is not a whole number in int64 range for %s column", col.Type)
		}
		return int64(f), err
	}
	return value, err
}

func (p *docPlan) updatedAtValue(doc bson.Raw, now time.Time) (time.Time, error) {
	if p.field == "" {
		return now, nil
	}
	rv, err := doc.LookupErr(p.field)
	if err != nil || rv.Type == bson.TypeNull {
		return now, nil
	}
	if rv.Type != bson.TypeDateTime {
		return time.Time{}, fmt.Errorf("updated-at field %s: unsupported BSON type %s", p.field, rv.Type)
	}
	value, err := scalarValue(rv)
	if err != nil {
		return time.Time{}, fmt.Errorf("updated-at field %s: %w", p.field, err)
	}
	return value.(time.Time), nil
}

func docBatchArgs(plan *docPlan, data [][]byte, now time.Time) ([]docStatement, error) {
	batch := make([]docStatement, len(data))
	for i, raw := range data {
		args, err := plan.args(raw, now)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("document %d: %w", i, err))
		}
		batch[i] = docStatement{query: plan.query, args: args, position: i}
	}
	return batch, nil
}

func positionedWriteError(label string, index int, err error) error {
	if converted, ok := errors.AsType[*connect.Error](err); ok && converted.Unwrap() != nil {
		err = converted.Unwrap()
	}
	pgErr, answered := errors.AsType[*pgconn.PgError](err)
	_, wholeBatch := errors.AsType[pgx.ErrPreprocessingBatch](err)
	position := "write failed"
	if answered && !wholeBatch && index >= 0 {
		position = fmt.Sprintf("%s %d", label, index)
	}
	if errors.Is(err, errSetReplicaRole) {
		position += ": setting session_replication_role"
	}
	if answered {
		return connect.NewError(sqlStateCode(pgErr.Code), fmt.Errorf("%s%s: SQLSTATE %s", position, pglib.ServerErrorNames(pgErr), pgErr.Code))
	}
	if code := connectCode(err); code != connect.CodeInternal {
		return connect.NewError(code, pglib.HideConnDetails(err))
	}
	return connect.NewError(connect.CodeInternal, fmt.Errorf("%s: client error", position))
}

func (c *conn) docPlanFor(ctx context.Context, namespace string) (*docPlan, error) {
	return c.docs.get(namespace, func() (*docPlan, error) {
		table, err := loadDocTable(ctx, c.c, namespace)
		if err != nil {
			return nil, err
		}
		if err := refuseUniqueIndexes(ctx, c.c, namespace); err != nil {
			return nil, err
		}
		return buildDocPlan(namespace, table, c.settings)
	})
}

func (c *conn) writeDocuments(ctx context.Context, namespace string, data [][]byte) error {
	plan, err := c.docPlanFor(ctx, namespace)
	if err != nil {
		return err
	}
	statements, err := docBatchArgs(plan, data, c.clock())
	if err != nil {
		return err
	}
	return c.execDocStatements(ctx, "document", statements)
}

func (c *conn) clock() time.Time {
	if c.now != nil {
		return c.now().UTC()
	}
	return time.Now().UTC()
}

func (c *conn) execDocStatements(ctx context.Context, label string, statements []docStatement) error {
	batch := &pgx.Batch{}
	for _, st := range statements {
		batch.Queue(st.query, st.args...)
	}
	failedAt, failure := -1, error(nil)
	err := c.sendSinkBatch(ctx, batch, len(statements), func(i int, err error) { failedAt, failure = i, err })
	if err == nil {
		return nil
	}
	if failure == nil {
		failure = err
	}
	if failedAt >= 0 {
		failedAt = statements[failedAt].position
	}
	return positionedWriteError(label, failedAt, failure)
}
