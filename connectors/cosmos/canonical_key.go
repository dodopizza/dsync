/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package cosmos

import (
	"math"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func canonicalKey(v bson.RawValue) string {
	switch v.Type {
	case bson.TypeInt32:
		if n, ok := v.Int32OK(); ok {
			return "n:" + strconv.FormatInt(int64(n), 10)
		}
	case bson.TypeInt64:
		if n, ok := v.Int64OK(); ok {
			return "n:" + strconv.FormatInt(n, 10)
		}
	case bson.TypeDouble:
		if f, ok := v.DoubleOK(); ok {
			if f == math.Trunc(f) && f >= -(1<<63) && f < 1<<63 {
				return "n:" + strconv.FormatInt(int64(f), 10)
			}
			return "n:" + strconv.FormatFloat(f, 'g', -1, 64)
		}
	}
	return "t" + string(rune(v.Type)) + string(v.Value)
}

func canonicalTuple(tuple []bson.RawValue) string {
	var b strings.Builder
	for _, v := range tuple {
		b.WriteString(canonicalKey(v))
	}
	return b.String()
}
