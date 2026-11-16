package postgres

import (
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	textNaN       = "NaN"
	textPosInf    = "Infinity"
	textNegInf    = "-Infinity"
	plainDigitCap = 40
	exponentCap   = 7000
)

var numberSyntax = regexp.MustCompile(`^(-?)([0-9]+)(?:\.([0-9]+))?(?:e(-?[0-9]+))?$`)

func encodeNumberID(rv bson.RawValue) (string, error) {
	switch rv.Type {
	case bson.TypeInt32:
		if v, ok := rv.Int32OK(); ok {
			return tagNumber + bigNumberText(big.NewInt(int64(v))), nil
		}
	case bson.TypeInt64:
		if v, ok := rv.Int64OK(); ok {
			return tagNumber + bigNumberText(big.NewInt(v)), nil
		}
	case bson.TypeDouble:
		if v, ok := rv.DoubleOK(); ok {
			return doubleText(v), nil
		}
	case bson.TypeDecimal128:
		if v, ok := rv.Decimal128OK(); ok {
			text, err := decimalText(v)
			return tagNumber + text, err
		}
	}
	return "", malformedID(rv.Type)
}

func bigNumberText(i *big.Int) string {
	return canonicalNumber(i.Sign() < 0, new(big.Int).Abs(i).String(), 0)
}

func doubleText(f float64) string {
	switch {
	case math.IsNaN(f):
		return tagNumber + textNaN
	case math.IsInf(f, 1):
		return tagNumber + textPosInf
	case math.IsInf(f, -1):
		return tagNumber + textNegInf
	case f == math.Trunc(f):
		integer, _ := new(big.Float).SetFloat64(f).Int(nil)
		return tagNumber + bigNumberText(integer)
	}
	mantissa, exponent, _ := strings.Cut(strconv.FormatFloat(math.Abs(f), 'e', -1, 64), "e")
	whole, fraction, _ := strings.Cut(mantissa, ".")
	exp, _ := strconv.Atoi(exponent)
	return tagFraction + canonicalNumber(f < 0, whole+fraction, exp-len(fraction))
}

func decimalText(d bson.Decimal128) (string, error) {
	if d.IsNaN() {
		return textNaN, nil
	}
	if sign := d.IsInf(); sign != 0 {
		if sign > 0 {
			return textPosInf, nil
		}
		return textNegInf, nil
	}
	significand, exponent, err := d.BigInt()
	if err != nil {
		return "", malformedID(bson.TypeDecimal128)
	}
	return canonicalNumber(significand.Sign() < 0, new(big.Int).Abs(significand).String(), exponent), nil
}

func canonicalNumber(negative bool, digits string, exponent int) string {
	digits = strings.TrimLeft(digits, "0")
	if digits == "" {
		return "0"
	}
	trimmed := strings.TrimRight(digits, "0")
	exponent += len(digits) - len(trimmed)
	digits = trimmed
	sign := ""
	if negative {
		sign = "-"
	}
	before, after := max(len(digits)+exponent, 0), max(-exponent, 0)
	if before > plainDigitCap || after > plainDigitCap {
		return sign + digits + "e" + strconv.Itoa(exponent)
	}
	return sign + placeDecimalPoint(digits, exponent)
}

func placeDecimalPoint(digits string, exponent int) string {
	if exponent >= 0 {
		return digits + strings.Repeat("0", exponent)
	}
	point := len(digits) + exponent
	if point > 0 {
		return digits[:point] + "." + digits[point:]
	}
	return "0." + strings.Repeat("0", -point) + digits
}

func decodeNumber(rest string) (bson.RawValue, error) {
	switch rest {
	case textNaN, textPosInf, textNegInf:
		f, _ := strconv.ParseFloat(rest, 64)
		return marshalIDValue(f)
	}
	negative, digits, exponent, ok := parseNumber(rest)
	if !ok {
		return bson.RawValue{}, errMalformedIDText
	}
	if exponent >= 0 {
		integer, _ := new(big.Int).SetString(digits+strings.Repeat("0", exponent), 10)
		if negative {
			integer.Neg(integer)
		}
		if integer.IsInt64() {
			return marshalIDValue(integer.Int64())
		}
		if f, accuracy := new(big.Float).SetInt(integer).Float64(); accuracy == big.Exact && !math.IsInf(f, 0) {
			return marshalIDValue(f)
		}
	}
	d, err := bson.ParseDecimal128(rest)
	if err != nil {
		return bson.RawValue{}, errMalformedIDText
	}
	return marshalIDValue(d)
}

func decodeFraction(rest string) (bson.RawValue, error) {
	if _, _, _, ok := parseNumber(rest); !ok {
		return bson.RawValue{}, errMalformedIDText
	}
	f, err := strconv.ParseFloat(rest, 64)
	if err != nil {
		return bson.RawValue{}, errMalformedIDText
	}
	return marshalIDValue(f)
}

func parseNumber(text string) (negative bool, digits string, exponent int, ok bool) {
	m := numberSyntax.FindStringSubmatch(text)
	if m == nil {
		return false, "", 0, false
	}
	if m[4] != "" {
		var err error
		if exponent, err = strconv.Atoi(m[4]); err != nil || exponent > exponentCap || exponent < -exponentCap {
			return false, "", 0, false
		}
	}
	return m[1] == "-", m[2] + m[3], exponent - len(m[3]), true
}

var errMalformedIDText = fmt.Errorf("malformed id text")
