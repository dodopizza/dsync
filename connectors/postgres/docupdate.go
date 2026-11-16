package postgres

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"

	"connectrpc.com/connect"
	"github.com/adiom-data/dsync/connectors/common"
	"github.com/jackc/pgx/v5"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type docStatement struct {
	query    string
	args     []any
	position int
}

func docDeleteSQL(namespace string, primaryKey []string) string {
	conditions := make([]string, len(primaryKey))
	for i, name := range quoteIdents(primaryKey) {
		conditions[i] = fmt.Sprintf("%s = $%d", name, i+1)
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s", SanitizeNamespace(namespace), strings.Join(conditions, " AND "))
}

func partID(part *adiomv1.BsonValue) (string, error) {
	return encodeID(bson.Type(part.GetType()), part.GetData())
}

func splitID(id []*adiomv1.BsonValue) (*adiomv1.BsonValue, []*adiomv1.BsonValue, error) {
	at := namedParts(id, idField)
	if len(at) == 0 {
		at = namedParts(id, "")
	}
	switch len(at) {
	case 0:
		return nil, nil, fmt.Errorf("no %s part", idField)
	case 1:
		return id[at[0]], slices.Delete(slices.Clone(id), at[0], at[0]+1), nil
	}
	return nil, nil, fmt.Errorf("ambiguous %s part", idField)
}

func namedParts(id []*adiomv1.BsonValue, name string) []int {
	var at []int
	for i, part := range id {
		if part.GetName() == name {
			at = append(at, i)
		}
	}
	return at
}

func (p *docPlan) columnIndex(name string) int {
	return slices.IndexFunc(p.columns, func(c docColumn) bool { return c.Name == name })
}

func (p *docPlan) keyParts(parts []*adiomv1.BsonValue) (map[string]any, error) {
	values := map[string]any{}
	for _, name := range p.table.PrimaryKey {
		at := namedParts(parts, name)
		if name == docIDColumn || len(at) == 0 {
			continue
		}
		if len(at) > 1 {
			return nil, fmt.Errorf("key column %s repeated", name)
		}
		part := parts[at[0]]
		if _, err := partID(part); err != nil {
			return nil, fmt.Errorf("key column %s: %w", name, err)
		}
		value, err := typedValue(bson.RawValue{Type: bson.Type(part.GetType()), Value: part.GetData()}, p.columns[p.columnIndex(name)])
		if err != nil {
			return nil, fmt.Errorf("key column %s: %w", name, err)
		}
		values[name] = value
	}
	return values, nil
}

func (p *docPlan) deleteArgs(id []*adiomv1.BsonValue) ([]any, error) {
	idPart, parts, err := splitID(id)
	if err != nil {
		return nil, err
	}
	text, err := partID(idPart)
	if err != nil {
		return nil, err
	}
	values, err := p.keyParts(parts)
	if err != nil {
		return nil, err
	}
	if i := p.columnIndex(idField); i >= 0 && slices.Contains(p.table.PrimaryKey, idField) {
		if values[idField], err = typedValue(bson.RawValue{Type: bson.Type(idPart.GetType()), Value: idPart.GetData()}, p.columns[i]); err != nil {
			return nil, fmt.Errorf("key column %s: %w", idField, err)
		}
	}
	return keyValues(p.table.PrimaryKey, text, values)
}

func (p *docPlan) upsertArgs(u *adiomv1.Update, now time.Time) ([]any, error) {
	args, err := p.args(u.GetData(), now)
	if err != nil || len(u.GetId()) == 0 {
		return args, err
	}
	idPart, parts, err := splitID(u.GetId())
	if err != nil {
		return nil, err
	}
	text, err := partID(idPart)
	if err != nil {
		return nil, err
	}
	if text != args[0] {
		return nil, fmt.Errorf("id differs from the document %s", idField)
	}
	values, err := p.keyParts(parts)
	if err != nil {
		return nil, err
	}
	doc := bson.Raw(u.GetData())
	for _, name := range p.table.PrimaryKey {
		if _, ok := values[name]; !ok {
			continue
		}
		want, _ := partID(parts[namedParts(parts, name)[0]])
		field, _ := doc.LookupErr(name)
		if got, _ := encodeID(field.Type, field.Value); got != want {
			return nil, fmt.Errorf("key column %s differs from the document", name)
		}
	}
	return args, nil
}

func (p *docPlan) statement(u *adiomv1.Update, now time.Time) (docStatement, error) {
	switch u.GetType() {
	case adiomv1.UpdateType_UPDATE_TYPE_INSERT, adiomv1.UpdateType_UPDATE_TYPE_UPDATE:
		args, err := p.upsertArgs(u, now)
		return docStatement{query: p.query, args: args}, err
	case adiomv1.UpdateType_UPDATE_TYPE_DELETE:
		args, err := p.deleteArgs(u.GetId())
		return docStatement{query: p.delete, args: args}, err
	default:
		return docStatement{}, fmt.Errorf("unsupported update type %s", u.GetType())
	}
}

func docUpdateStatements(plan *docPlan, updates []*adiomv1.Update, now time.Time) ([]docStatement, error) {
	statements := make([]docStatement, 0, len(updates))
	for i, u := range updates {
		if common.IsEmptyPartialUpdate(u) {
			continue
		}
		st, err := plan.statement(u, now)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("update %d: %w", i, err))
		}
		st.position = i
		statements = append(statements, st)
	}
	return statements, nil
}

func (c *conn) writeDocumentUpdates(ctx context.Context, namespace string, updates []*adiomv1.Update) error {
	plan, err := c.docPlanFor(ctx, namespace)
	if err != nil {
		return err
	}
	now := time.Now()
	if c.now != nil {
		now = c.now()
	}
	statements, err := docUpdateStatements(plan, updates, now.UTC())
	if err != nil || len(statements) == 0 {
		return err
	}
	batch := &pgx.Batch{}
	for _, st := range statements {
		batch.Queue(st.query, st.args...)
	}
	failedAt, failure := -1, error(nil)
	err = c.execBatchWith(ctx, batch, func(i int, err error) { failedAt, failure = i, err }, func(error) {})
	if err == nil {
		return nil
	}
	if failure == nil {
		failure = err
	}
	if failedAt >= 0 {
		failedAt = statements[failedAt].position
	}
	return positionedWriteError("update", failedAt, failure)
}
