package postgres

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func encodeDoc(raw []byte) ([]byte, error) {
	doc := bson.Raw(raw)
	if err := doc.Validate(); err != nil {
		return nil, fmt.Errorf("malformed BSON document")
	}
	if err := checkDocument(doc, "", rejectWrapperKey); err != nil {
		return nil, err
	}
	return bson.MarshalExtJSON(doc, true, false)
}

func checkDocument(doc bson.Raw, path string, keyCheck func(path, key string) error) error {
	elements, err := doc.Elements()
	if err != nil {
		return fmt.Errorf("malformed BSON document")
	}
	seen := make(map[string]struct{}, len(elements))
	for _, el := range elements {
		key := el.Key()
		child := joinPath(path, key)
		if !utf8.ValidString(key) {
			return fmt.Errorf("invalid UTF-8 in field name under %q", path)
		}
		if _, dup := seen[key]; dup {
			return fmt.Errorf("duplicate field %q", child)
		}
		seen[key] = struct{}{}
		if err := keyCheck(child, key); err != nil {
			return err
		}
		if err := checkValue(el.Value(), child, keyCheck); err != nil {
			return err
		}
	}
	return nil
}

func checkValue(v bson.RawValue, path string, keyCheck func(path, key string) error) error {
	switch v.Type {
	case bson.TypeString:
		s, _ := v.StringValueOK()
		return checkText(s, path)
	case bson.TypeJavaScript:
		s, _ := v.JavaScriptOK()
		return checkText(s, path)
	case bson.TypeSymbol:
		s, _ := v.SymbolOK()
		return checkText(s, path)
	case bson.TypeRegex:
		pattern, options, _ := v.RegexOK()
		if err := checkText(pattern, path); err != nil {
			return err
		}
		return checkText(options, path)
	case bson.TypeDBPointer:
		ns, _, _ := v.DBPointerOK()
		return checkText(ns, path)
	case bson.TypeCodeWithScope:
		code, scope, _ := v.CodeWithScopeOK()
		if err := checkText(code, path); err != nil {
			return err
		}
		return checkDocument(scope, joinPath(path, "$scope"), keyCheck)
	case bson.TypeEmbeddedDocument:
		doc, _ := v.DocumentOK()
		return checkDocument(doc, path, keyCheck)
	case bson.TypeArray:
		return checkArray(v, path, keyCheck)
	}
	return nil
}

func checkArray(v bson.RawValue, path string, keyCheck func(path, key string) error) error {
	arr, _ := v.ArrayOK()
	values, err := arr.Values()
	if err != nil {
		return fmt.Errorf("malformed BSON document")
	}
	for i, item := range values {
		if err := checkValue(item, path+"["+strconv.Itoa(i)+"]", keyCheck); err != nil {
			return err
		}
	}
	return nil
}

func checkText(s, path string) error {
	if !utf8.ValidString(s) {
		return fmt.Errorf("invalid UTF-8 in value at %q", path)
	}
	if strings.IndexByte(s, 0) >= 0 {
		return fmt.Errorf("U+0000 in value at %q", path)
	}
	return nil
}

func joinPath(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}

var wrapperKeys = map[string]struct{}{
	"$numberInt": {}, "$numberLong": {}, "$numberDouble": {}, "$numberDecimal": {}, "$oid": {}, "$date": {},
	"$binary": {}, "$uuid": {}, "$regularExpression": {}, "$timestamp": {}, "$symbol": {}, "$code": {},
	"$scope": {}, "$minKey": {}, "$maxKey": {}, "$undefined": {}, "$dbPointer": {},
}

func rejectWrapperKey(path, key string) error {
	if _, ok := wrapperKeys[key]; ok {
		return fmt.Errorf("extended JSON type wrapper key at %q", path)
	}
	return nil
}
