package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/adiom-data/dsync/internal/util"
	"github.com/adiom-data/dsync/protocol/iface"
	pgxdecimal "github.com/jackc/pgx-shopspring-decimal"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type KeyLister struct {
	pool     *pgxpool.Pool
	mappings map[string]string
	mu       sync.Mutex
	owners   map[string]iface.Namespace
}

type keyRows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}

func NewKeyLister(ctx context.Context, dsn string, mappings map[string]string) (*KeyLister, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("invalid postgres connection string")
	}
	config.AfterConnect = func(_ context.Context, conn *pgx.Conn) error {
		pgxdecimal.Register(conn.TypeMap())
		return nil
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, errors.New("cannot create the postgres connection pool")
	}
	return newKeyLister(pool, mappings), nil
}

func newKeyLister(pool *pgxpool.Pool, mappings map[string]string) *KeyLister {
	return &KeyLister{pool: pool, mappings: mappings}
}

func (l *KeyLister) Close() error {
	l.pool.Close()
	return nil
}

func (l *KeyLister) ScanKeys(ctx context.Context, ns iface.Namespace, keyFields []string, pageSize int, emit func([][]bson.RawValue) error) error {
	table := l.Table(ns)
	if err := l.claimTable(ns); err != nil {
		return err
	}
	shape, err := loadDocTable(ctx, l.pool, table)
	if err != nil {
		return err
	}
	if err := checkKeyShape(table, shape.PrimaryKey, keyFields); err != nil {
		return err
	}
	query, args := keyQuery(table, keyFields)
	rows, err := l.pool.Query(ctx, query, args...)
	if err != nil {
		return toConnectError(err)
	}
	defer rows.Close()
	return emitKeyPages(rows, len(keyFields), pageSize, emit)
}

func (l *KeyLister) Table(ns iface.Namespace) string {
	return keyTableName(l.mappings, ns)
}

func (l *KeyLister) claimTable(ns iface.Namespace) error {
	table := l.Table(ns)
	l.mu.Lock()
	defer l.mu.Unlock()
	if owner, ok := l.owners[table]; ok && owner != ns {
		return fmt.Errorf("namespaces %s.%s and %s.%s both resolve to table %s", owner.Db, owner.Col, ns.Db, ns.Col, table)
	}
	if l.owners == nil {
		l.owners = map[string]iface.Namespace{}
	}
	l.owners[table] = ns
	return nil
}

func keyTableName(mappings map[string]string, ns iface.Namespace) string {
	return util.MapNamespace(mappings, ns.Db+"."+ns.Col, ".", ".")
}

func checkKeyShape(table string, primaryKey, keyFields []string) error {
	want := append([]string{docIDColumn}, keyFields...)
	if len(primaryKey) == len(want) && !slices.ContainsFunc(want, func(c string) bool { return !slices.Contains(primaryKey, c) }) {
		return nil
	}
	return shapeError(table, "primary key columns (%s) must be exactly (%s)", strings.Join(primaryKey, ", "), strings.Join(want, ", "))
}

func keyQuery(table string, keyFields []string) (string, []any) {
	doc := pgx.Identifier{docDocColumn}.Sanitize()
	parts := []string{fmt.Sprintf("'%s', %s->'%s'", idField, doc, idField)}
	args := make([]any, len(keyFields))
	for i, field := range keyFields {
		parts = append(parts, fmt.Sprintf("'k%d', %s #> $%d::text[]", i, doc, i+1))
		args[i] = strings.Split(field, ".")
	}
	id := pgx.Identifier{docIDColumn}.Sanitize()
	return fmt.Sprintf("SELECT %s, jsonb_build_object(%s)::text FROM %s ORDER BY %s", id, strings.Join(parts, ", "), SanitizeNamespace(table), id), args
}

func keyTuple(idText, keyText string, fieldCount int) ([]bson.RawValue, error) {
	raw, err := docFromText(keyText)
	if err != nil {
		return nil, err
	}
	_, id, err := emitDoc(idText, raw)
	if err != nil {
		return nil, err
	}
	tuple := []bson.RawValue{{Type: id.Type, Value: slices.Clone(id.Value)}}
	for i := range fieldCount {
		rv, err := raw.LookupErr(fmt.Sprintf("k%d", i))
		if err != nil || rv.Type == bson.TypeNull {
			tuple = append(tuple, bson.RawValue{Type: bson.TypeNull})
			continue
		}
		tuple = append(tuple, bson.RawValue{Type: rv.Type, Value: slices.Clone(rv.Value)})
	}
	return tuple, nil
}

func emitKeyPages(rows keyRows, fieldCount, pageSize int, emit func([][]bson.RawValue) error) error {
	page := make([][]bson.RawValue, 0, pageSize)
	for rows.Next() {
		var idText, keyText string
		if err := rows.Scan(&idText, &keyText); err != nil {
			return toConnectError(err)
		}
		tuple, err := keyTuple(idText, keyText, fieldCount)
		if err != nil {
			return err
		}
		page = append(page, tuple)
		if len(page) == pageSize {
			if err := emit(page); err != nil {
				return err
			}
			page = make([][]bson.RawValue, 0, pageSize)
		}
	}
	if err := rows.Err(); err != nil {
		return toConnectError(err)
	}
	if len(page) == 0 {
		return nil
	}
	return emit(page)
}
