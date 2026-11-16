package postgres

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"
	"uuid"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

const (
	tagString      = "s:"
	tagObjectID    = "o:"
	tagNumber      = "n:"
	tagFraction    = "f:"
	tagBool        = "b:"
	tagDate        = "d:"
	tagNull        = "z:"
	tagUUID        = "u:"
	tagLegacyUUID  = "l:"
	tagBinary      = "y:"
	tagDocument    = "j:"
	tagArray       = "a:"
	uuidByteLength = 16
)

func malformedID(t bson.Type) error {
	return fmt.Errorf("malformed %s id", t)
}

func encodeID(t bson.Type, data []byte) (string, error) {
	text, err := encodeIDText(t, data)
	if err != nil {
		return "", err
	}
	if _, rest, ok := bsoncore.ReadValue(data, bsoncore.Type(t)); !ok || len(rest) != 0 {
		return "", malformedID(t)
	}
	return text, nil
}

func encodeIDText(t bson.Type, data []byte) (string, error) {
	rv := bson.RawValue{Type: t, Value: data}
	switch t {
	case bson.TypeString:
		s, ok := rv.StringValueOK()
		if !ok || data[len(data)-1] != 0 {
			return "", malformedID(t)
		}
		if !utf8.ValidString(s) || strings.IndexByte(s, 0) >= 0 {
			return "", fmt.Errorf("string id contains invalid UTF-8 or U+0000")
		}
		return tagString + s, nil
	case bson.TypeObjectID:
		oid, ok := rv.ObjectIDOK()
		if !ok {
			return "", malformedID(t)
		}
		return tagObjectID + oid.Hex(), nil
	case bson.TypeInt32, bson.TypeInt64, bson.TypeDouble, bson.TypeDecimal128:
		return encodeNumberID(rv)
	case bson.TypeBoolean:
		b, ok := rv.BooleanOK()
		if !ok || data[0] > 1 {
			return "", malformedID(t)
		}
		return tagBool + strconv.FormatBool(b), nil
	case bson.TypeDateTime:
		ms, ok := rv.DateTimeOK()
		if !ok {
			return "", malformedID(t)
		}
		return tagDate + strconv.FormatInt(ms, 10), nil
	case bson.TypeNull:
		return tagNull, nil
	case bson.TypeBinary:
		return encodeBinaryID(rv)
	case bson.TypeEmbeddedDocument, bson.TypeArray:
		return encodeStructuredID(rv)
	}
	return "", fmt.Errorf("unsupported id type %s", t)
}

func encodeBinaryID(rv bson.RawValue) (string, error) {
	subtype, data, ok := rv.BinaryOK()
	if !ok {
		return "", malformedID(rv.Type)
	}
	switch {
	case subtype == bson.TypeBinaryUUID && len(data) == uuidByteLength:
		return tagUUID + uuidText(data), nil
	case subtype == bson.TypeBinaryUUIDOld && len(data) == uuidByteLength:
		return tagLegacyUUID + uuidText(swapLegacyUUID(data)), nil
	}
	return fmt.Sprintf("%s%02x:%s", tagBinary, subtype, hex.EncodeToString(data)), nil
}

func rejectDollarKey(path, key string) error {
	if strings.HasPrefix(key, "$") {
		return fmt.Errorf("id contains a $-prefixed key at %q", path)
	}
	return nil
}

func encodeStructuredID(rv bson.RawValue) (string, error) {
	if rv.Type == bson.TypeEmbeddedDocument {
		doc, ok := rv.DocumentOK()
		if !ok {
			return "", malformedID(rv.Type)
		}
		if err := checkStructuredID(rv, doc); err != nil {
			return "", err
		}
		text, err := marshalCanonical(doc)
		if err != nil {
			return "", malformedID(rv.Type)
		}
		return tagDocument + text, nil
	}
	arr, ok := rv.ArrayOK()
	if !ok {
		return "", malformedID(rv.Type)
	}
	wrapped := bson.Raw(bsoncore.BuildDocument(nil, bsoncore.AppendArrayElement(nil, "a", arr)))
	if err := checkStructuredID(rv, wrapped); err != nil {
		return "", err
	}
	text, err := marshalCanonical(wrapped)
	if err != nil {
		return "", malformedID(rv.Type)
	}
	return tagArray + strings.TrimSuffix(strings.TrimPrefix(text, `{"a":`), "}"), nil
}

func checkStructuredID(rv bson.RawValue, validated bson.Raw) error {
	if err := validated.Validate(); err != nil {
		return malformedID(rv.Type)
	}
	return checkValue(rv, "", rejectDollarKey)
}

func marshalCanonical(doc bson.Raw) (string, error) {
	if err := doc.Validate(); err != nil {
		return "", err
	}
	out, err := bson.MarshalExtJSON(doc, true, false)
	return string(out), err
}

func decodeID(text string) (bson.RawValue, error) {
	rv, err := decodeIDText(text)
	if err != nil {
		return bson.RawValue{}, err
	}
	if again, err := encodeID(rv.Type, rv.Value); err != nil || again != text {
		return bson.RawValue{}, errNotCanonical
	}
	return rv, nil
}

var errNotCanonical = fmt.Errorf("id text is not canonical")

func decodeIDText(text string) (bson.RawValue, error) {
	if len(text) < 2 {
		return bson.RawValue{}, errMalformedIDText
	}
	tag, rest := text[:2], text[2:]
	switch tag {
	case tagString:
		return marshalIDValue(rest)
	case tagObjectID:
		oid, err := bson.ObjectIDFromHex(rest)
		if err != nil {
			return bson.RawValue{}, errMalformedIDText
		}
		return marshalIDValue(oid)
	case tagNumber:
		return decodeNumber(rest)
	case tagFraction:
		return decodeFraction(rest)
	case tagBool:
		return marshalIDValue(rest == "true")
	case tagDate:
		ms, err := strconv.ParseInt(rest, 10, 64)
		if err != nil {
			return bson.RawValue{}, errMalformedIDText
		}
		return marshalIDValue(bson.DateTime(ms))
	case tagNull:
		return bson.RawValue{Type: bson.TypeNull}, nil
	case tagUUID, tagLegacyUUID, tagBinary:
		return decodeBinary(tag, rest)
	case tagDocument, tagArray:
		return decodeStructured(tag, rest)
	}
	return bson.RawValue{}, errMalformedIDText
}

func marshalIDValue(v any) (bson.RawValue, error) {
	t, data, err := bson.MarshalValue(v)
	if err != nil {
		return bson.RawValue{}, errMalformedIDText
	}
	return bson.RawValue{Type: t, Value: data}, nil
}

func decodeBinary(tag, rest string) (bson.RawValue, error) {
	if tag == tagBinary {
		subtypeText, dataText, found := strings.Cut(rest, ":")
		subtype, err := hex.DecodeString(subtypeText)
		data, dataErr := hex.DecodeString(dataText)
		if !found || err != nil || dataErr != nil || len(subtype) != 1 {
			return bson.RawValue{}, errMalformedIDText
		}
		return marshalIDValue(bson.Binary{Subtype: subtype[0], Data: data})
	}
	data, ok := parseUUIDText(rest)
	if !ok {
		return bson.RawValue{}, errMalformedIDText
	}
	if tag == tagUUID {
		return marshalIDValue(bson.Binary{Subtype: bson.TypeBinaryUUID, Data: data})
	}
	return marshalIDValue(bson.Binary{Subtype: bson.TypeBinaryUUIDOld, Data: swapLegacyUUID(data)})
}

func decodeStructured(tag, rest string) (bson.RawValue, error) {
	source := rest
	if tag == tagArray {
		source = `{"a":` + rest + `}`
	}
	var doc bson.Raw
	if err := bson.UnmarshalExtJSON([]byte(source), true, &doc); err != nil {
		return bson.RawValue{}, errMalformedIDText
	}
	if tag == tagDocument {
		return bson.RawValue{Type: bson.TypeEmbeddedDocument, Value: doc}, nil
	}
	element, err := doc.LookupErr("a")
	if err != nil || element.Type != bson.TypeArray {
		return bson.RawValue{}, errMalformedIDText
	}
	return element, nil
}

func uuidText(b []byte) string {
	return uuid.UUID(b).String()
}

func parseUUIDText(s string) ([]byte, bool) {
	u, err := uuid.Parse(s)
	if len(s) != 36 || err != nil {
		return nil, false
	}
	return u[:], true
}

func swapLegacyUUID(b []byte) []byte {
	out := bytes.Clone(b)
	slices.Reverse(out[0:4])
	slices.Reverse(out[4:6])
	slices.Reverse(out[6:8])
	return out
}
