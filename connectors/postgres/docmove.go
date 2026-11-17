package postgres

import (
	"bytes"
	"encoding/json/v2"
	"fmt"
	"slices"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

const (
	typeJSONB     = "jsonb"
	commentPrefix = "dsync:"
	modeCopy      = "copy"
	modeMove      = "move"
	fragmentOpen  = `{"v":`
)

var (
	postgresDateMin = time.Date(-4712, 1, 1, 0, 0, 0, 0, time.UTC)
	postgresDateMax = time.Date(294277, 1, 1, 0, 0, 0, 0, time.UTC)
)

type jsonText string

type movePair struct {
	accepts func(bson.RawValue) bool
	restore func(any) (bson.RawValue, bool)
}

var movePairs = map[string]movePair{
	"integer":          pairOf(ofType(bson.TypeInt32), func(v int32) any { return v }),
	"bigint":           pairOf(ofType(bson.TypeInt64), func(v int64) any { return v }),
	"double precision": pairOf(ofType(bson.TypeDouble), func(v float64) any { return v }),
	"text":             pairOf(ofType(bson.TypeString), func(v string) any { return v }),
	"boolean":          pairOf(ofType(bson.TypeBoolean), func(v bool) any { return v }),
	typeTimeTZ:         pairOf(dateInRange, func(v time.Time) any { return bson.NewDateTimeFromTime(v) }),
	"uuid":             pairOf(binaryOf(bson.TypeBinaryUUID, 16), func(v [16]byte) any { return bson.Binary{Subtype: bson.TypeBinaryUUID, Data: v[:]} }),
	"bytea":            pairOf(binaryOf(bson.TypeBinaryGeneric, -1), func(v []byte) any { return bson.Binary{Data: v} }),
	typeJSONB:          {accepts: ofType(bson.TypeEmbeddedDocument, bson.TypeArray), restore: restoreJSON},
}

func dateInRange(rv bson.RawValue) bool {
	millis, ok := rv.DateTimeOK()
	return ok && millis >= postgresDateMin.UnixMilli() && millis < postgresDateMax.UnixMilli()
}

func ofType(types ...bson.Type) func(bson.RawValue) bool {
	return func(rv bson.RawValue) bool { return slices.Contains(types, rv.Type) }
}

func binaryOf(subtype byte, size int) func(bson.RawValue) bool {
	return func(rv bson.RawValue) bool {
		got, data, ok := rv.BinaryOK()
		return ok && got == subtype && (size < 0 || len(data) == size)
	}
}

func pairOf[T any](accepts func(bson.RawValue) bool, convert func(T) any) movePair {
	return movePair{accepts: accepts, restore: func(v any) (bson.RawValue, bool) {
		typed, ok := v.(T)
		if !ok {
			return bson.RawValue{}, false
		}
		t, data, err := bson.MarshalValue(convert(typed))
		return bson.RawValue{Type: t, Value: data}, err == nil
	}}
}

func restoreJSON(v any) (bson.RawValue, bool) {
	text, isText := v.(jsonText)
	if !isText {
		encoded, err := json.Marshal(v, json.Deterministic(true))
		if err != nil {
			return bson.RawValue{}, false
		}
		text = jsonText(encoded)
	}
	var raw bson.Raw
	if err := bson.UnmarshalExtJSON([]byte(fragmentOpen+string(text)+"}"), true, &raw); err != nil {
		return bson.RawValue{}, false
	}
	rv := raw.Lookup("v")
	return rv, rv.Type == bson.TypeEmbeddedDocument || rv.Type == bson.TypeArray
}

func moveMode(c catalogColumn, key bool) (bool, error) {
	_, paired := movePairs[c.Type]
	movable := paired && !key && !c.Modifier && c.Name != idField
	spec, ok := strings.CutPrefix(c.Comment, commentPrefix)
	if !ok {
		return movable, nil
	}
	var mapping struct {
		Mode string `json:"mode"`
	}
	if err := json.Unmarshal([]byte(spec), &mapping, json.RejectUnknownMembers(true)); err != nil {
		return false, fmt.Errorf("column %s: unreadable %s comment", c.Name, commentPrefix)
	}
	switch {
	case mapping.Mode == modeCopy:
		return false, nil
	case mapping.Mode != modeMove:
		return false, fmt.Errorf("column %s: %s comment needs mode %s or %s", c.Name, commentPrefix, modeCopy, modeMove)
	case !movable:
		return false, fmt.Errorf("column %s: mode %s does not apply to a primary key column, the %s column or type %s", c.Name, modeMove, idField, c.Type)
	}
	return true, nil
}

func jsonFragment(rv bson.RawValue) ([]byte, error) {
	if rv.Type != bson.TypeEmbeddedDocument && rv.Type != bson.TypeArray {
		return nil, fmt.Errorf("BSON type %s cannot be stored in a column of type %s", rv.Type, typeJSONB)
	}
	wrapped := bsoncore.BuildDocument(nil, bsoncore.AppendValueElement(nil, "v", bsoncore.Value{Type: bsoncore.Type(rv.Type), Data: rv.Value}))
	text, err := bson.MarshalExtJSON(bson.Raw(wrapped), true, false)
	if err != nil {
		return nil, err
	}
	inner, opened := bytes.CutPrefix(text, []byte(fragmentOpen))
	inner, closed := bytes.CutSuffix(inner, []byte("}"))
	if !opened || !closed {
		return nil, fmt.Errorf("unexpected Extended JSON layout")
	}
	return inner, nil
}

func withoutFields(doc bson.Raw, drop map[string]bool) (bson.Raw, error) {
	if len(drop) == 0 {
		return doc, nil
	}
	elements, err := doc.Elements()
	if err != nil {
		return nil, fmt.Errorf("malformed BSON document")
	}
	var kept []byte
	for _, el := range elements {
		if !drop[el.Key()] {
			kept = append(kept, el...)
		}
	}
	return bsoncore.BuildDocument(nil, kept), nil
}

func restoreMoved(raw bson.Raw, moved []docColumn, values []any) (bson.Raw, error) {
	var restored []byte
	names := map[string]bool{}
	for i, col := range moved {
		if values[i] == nil {
			continue
		}
		rv, ok := movePairs[col.Type].restore(values[i])
		if !ok {
			return nil, fmt.Errorf("column %s: unexpected value for type %s", col.Name, col.Type)
		}
		names[col.Name] = true
		restored = bsoncore.AppendValueElement(restored, col.Name, bsoncore.Value{Type: bsoncore.Type(rv.Type), Data: rv.Value})
	}
	if len(names) == 0 {
		return raw, nil
	}
	elements, err := raw.Elements()
	if err != nil {
		return nil, fmt.Errorf("malformed BSON document")
	}
	var kept []byte
	for _, el := range elements {
		if !names[el.Key()] {
			kept = append(kept, el...)
			continue
		}
		if el.Value().Type != bson.TypeNull {
			return nil, fmt.Errorf("column %s is also a field of %s", el.Key(), docDocColumn)
		}
	}
	return bsoncore.BuildDocument(nil, append(kept, restored...)), nil
}
