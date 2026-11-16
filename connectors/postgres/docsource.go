package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"connectrpc.com/connect"
	"github.com/adiom-data/dsync/connectors/common"
	"github.com/adiom-data/dsync/connectors/postgres/pglib"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/cespare/xxhash"
	"github.com/jackc/pgx/v5"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

type sourceRow struct {
	doc  []byte
	key  []any
	text string
}

func docFromText(text string) (bson.Raw, error) {
	var raw bson.Raw
	if err := bson.UnmarshalExtJSON([]byte(text), true, &raw); err != nil {
		return nil, fmt.Errorf("doc is not a valid Extended JSON document")
	}
	return raw, nil
}

func docFromValue(values map[string]any) (bson.Raw, error) {
	text, err := json.Marshal(values[docDocColumn])
	if err != nil {
		return nil, fmt.Errorf("doc is not a valid Extended JSON document")
	}
	return docFromText(string(text))
}

func rawIDPart(rv bson.RawValue) []*adiomv1.BsonValue {
	return []*adiomv1.BsonValue{{Name: idField, Type: uint32(rv.Type), Data: rv.Value}}
}

func idHash(rv bson.RawValue) (uint64, error) {
	h := xxhash.New()
	wrapped := bson.Raw(bsoncore.BuildDocument(nil, bsoncore.AppendValueElement(nil, idField, bsoncore.Value{Type: bsoncore.Type(rv.Type), Data: rv.Value})))
	if err := common.HashBson(h, wrapped, false, nil); err != nil {
		return 0, err
	}
	return h.Sum64(), nil
}

func isStructuredID(t bson.Type) bool {
	return t == bson.TypeEmbeddedDocument || t == bson.TypeArray
}

func emitDoc(idText string, raw bson.Raw) (bson.Raw, bson.RawValue, error) {
	docID, err := raw.LookupErr(idField)
	if err != nil {
		return nil, bson.RawValue{}, fmt.Errorf("doc has no %s", idField)
	}
	if !isStructuredID(docID.Type) {
		text, err := encodeID(docID.Type, docID.Value)
		if err != nil || text != idText {
			return nil, bson.RawValue{}, fmt.Errorf("doc %s does not match the %s column (type %s)", idField, docIDColumn, docID.Type)
		}
		return raw, docID, nil
	}
	colID, err := decodeID(idText)
	if err != nil {
		return nil, bson.RawValue{}, fmt.Errorf("%s column: %w", docIDColumn, err)
	}
	want, err := idHash(colID)
	if err != nil {
		return nil, bson.RawValue{}, err
	}
	got, err := idHash(docID)
	if err != nil {
		return nil, bson.RawValue{}, err
	}
	if colID.Type != docID.Type || want != got {
		return nil, bson.RawValue{}, fmt.Errorf("doc %s does not match the %s column (type %s)", idField, docIDColumn, docID.Type)
	}
	elements, err := raw.Elements()
	if err != nil {
		return nil, bson.RawValue{}, err
	}
	out := bsoncore.AppendValueElement(nil, idField, bsoncore.Value{Type: bsoncore.Type(colID.Type), Data: colID.Value})
	for _, el := range elements {
		if el.Key() == idField {
			continue
		}
		out = append(out, el...)
	}
	return bson.Raw(bsoncore.BuildDocument(nil, out)), colID, nil
}

func docSourceUpdate(update pglib.Update) (*adiomv1.Update, error) {
	ns := update.Namespace + "." + update.TableName
	switch update.Type {
	case pglib.UpdateTypeInsert, pglib.UpdateTypeUpdate:
		idText, _ := update.New[docIDColumn].(string)
		raw, err := docFromValue(update.New)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", ns, err)
		}
		doc, id, err := emitDoc(idText, raw)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", ns, err)
		}
		kind := adiomv1.UpdateType_UPDATE_TYPE_INSERT
		if update.Type == pglib.UpdateTypeUpdate {
			kind = adiomv1.UpdateType_UPDATE_TYPE_UPDATE
		}
		return &adiomv1.Update{Id: rawIDPart(id), Type: kind, Data: doc}, nil
	case pglib.UpdateTypeDelete:
		text, ok := update.Old[docIDColumn].(string)
		if !ok {
			return nil, fmt.Errorf("%s: deleted row has no %s value", ns, docIDColumn)
		}
		var id bson.RawValue
		var err error
		if update.Old[docDocColumn] != nil {
			var raw bson.Raw
			if raw, err = docFromValue(update.Old); err == nil {
				_, id, err = emitDoc(text, raw)
			}
		} else {
			id, err = decodeID(text)
		}
		if err != nil {
			return nil, fmt.Errorf("%s: deleted row: %w", ns, err)
		}
		return &adiomv1.Update{Id: rawIDPart(id), Type: adiomv1.UpdateType_UPDATE_TYPE_DELETE}, nil
	}
	return nil, nil
}

func (c *conn) listRows(ctx context.Context, namespace string, keys []string, where string, args []any, limit int) ([]sourceRow, error) {
	primaryKeysPartial := strings.Join(quoteIdents(keys), ", ")
	if c.settings.DocumentMode {
		return c.listDocRows(ctx, namespace, keys, primaryKeysPartial, where, args, limit)
	}
	q := fmt.Sprintf("SELECT * from %v%v ORDER BY %v LIMIT %v", SanitizeNamespace(namespace), where, primaryKeysPartial, limit+1)
	rows, err := c.c.Query(ctx, q, args...)
	if err != nil {
		return nil, toConnectError(err)
	}
	ms, err := pgx.CollectRows(rows, pgx.RowToMap)
	if err != nil {
		return nil, toConnectError(err)
	}
	result := make([]sourceRow, len(ms))
	for i, m := range ms {
		for _, k := range keys {
			result[i].key = append(result[i].key, m[k])
		}
		if i >= limit {
			continue
		}
		if err := c.prepareSourceDocument(namespace, m); err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		result[i].doc, err = marshalWithRegistry(c.bsonRegistry, m)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	}
	return result, nil
}

func (c *conn) listDocRows(ctx context.Context, namespace string, keys []string, primaryKeysPartial, where string, args []any, limit int) ([]sourceRow, error) {
	q := fmt.Sprintf("SELECT %v::text, %v from %v%v ORDER BY %v LIMIT %v", pgx.Identifier{docDocColumn}.Sanitize(), primaryKeysPartial, SanitizeNamespace(namespace), where, primaryKeysPartial, limit+1)
	rows, err := c.c.Query(ctx, q, args...)
	if err != nil {
		return nil, toConnectError(err)
	}
	result, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (sourceRow, error) {
		values, err := row.Values()
		if err != nil {
			return sourceRow{}, err
		}
		text, _ := values[0].(string)
		return sourceRow{key: values[1:], text: text}, nil
	})
	if err != nil {
		return nil, toConnectError(err)
	}
	if err := fillDocs(result, keys, limit); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("%s: %w", namespace, err))
	}
	return result, nil
}

func fillDocs(rows []sourceRow, keys []string, limit int) error {
	idIndex := slices.Index(keys, docIDColumn)
	if idIndex < 0 {
		return fmt.Errorf("primary key has no %s column", docIDColumn)
	}
	for i := range rows {
		if i >= limit {
			break
		}
		idText, _ := rows[i].key[idIndex].(string)
		raw, err := docFromText(rows[i].text)
		if err != nil {
			return err
		}
		if rows[i].doc, _, err = emitDoc(idText, raw); err != nil {
			return err
		}
	}
	return nil
}
