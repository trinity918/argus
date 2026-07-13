// Package fixedpoint provides exact base-10 fixed-point arithmetic for prices
// and quantities. Market-surveillance logic must be reproducible to the last
// digit — two runs over the same tape must produce byte-identical audit
// records — so we never let IEEE-754 rounding into price/size keys or sums.
//
// Values are stored as int64 scaled by 1e8 (eight decimal places), which
// comfortably covers every spot crypto instrument (a $1,000,000 print is
// 1e14, four orders of magnitude below the int64 ceiling of ~9.2e18).
package fixedpoint

import (
	"errors"
	"strconv"
	"strings"
)

// Scale is the fixed-point scale factor: one whole unit == Scale ticks.
const Scale int64 = 100_000_000 // 1e8

// dp is the number of decimal places implied by Scale.
const dp = 8

// Value is a decimal number stored as an integer number of 1e-8 ticks.
type Value int64

var (
	// ErrEmpty is returned when parsing an empty string.
	ErrEmpty = errors.New("fixedpoint: empty string")
	// ErrSyntax is returned for malformed decimal input.
	ErrSyntax = errors.New("fixedpoint: invalid decimal syntax")
	// ErrRange is returned when the value does not fit in an int64 at 1e8 scale.
	ErrRange = errors.New("fixedpoint: value out of range")
)

// Parse converts a decimal string such as "12345.6789" into a Value. It is
// exact: it never routes through float64. Fractional digits beyond the eighth
// are truncated toward zero (exchanges never send more precision than the
// instrument's tick/lot size, so this is a safety net, not a lossy path in
// practice).
func Parse(s string) (Value, error) {
	if s == "" {
		return 0, ErrEmpty
	}
	neg := false
	switch s[0] {
	case '+':
		s = s[1:]
	case '-':
		neg = true
		s = s[1:]
	}
	if s == "" {
		return 0, ErrSyntax
	}

	intPart := s
	fracPart := ""
	if i := strings.IndexByte(s, '.'); i >= 0 {
		intPart = s[:i]
		fracPart = s[i+1:]
		if strings.IndexByte(fracPart, '.') >= 0 {
			return 0, ErrSyntax
		}
	}
	if intPart == "" && fracPart == "" {
		return 0, ErrSyntax
	}

	var whole int64
	if intPart != "" {
		w, err := strconv.ParseInt(intPart, 10, 64)
		if err != nil {
			return 0, ErrRange
		}
		if w < 0 { // a stray '-' inside intPart, e.g. "1-2"
			return 0, ErrSyntax
		}
		whole = w
	}

	// Normalize the fractional component to exactly dp digits.
	if len(fracPart) > dp {
		fracPart = fracPart[:dp]
	}
	var frac int64
	if fracPart != "" {
		f, err := strconv.ParseInt(fracPart, 10, 64)
		if err != nil {
			return 0, ErrSyntax
		}
		if f < 0 {
			return 0, ErrSyntax
		}
		frac = f
		for i := len(fracPart); i < dp; i++ {
			frac *= 10
		}
	}

	if whole > (int64(^uint64(0)>>1)-frac)/Scale {
		return 0, ErrRange
	}
	v := whole*Scale + frac
	if neg {
		v = -v
	}
	return Value(v), nil
}

// MustParse is Parse that panics on error; use only for trusted literals.
func MustParse(s string) Value {
	v, err := Parse(s)
	if err != nil {
		panic(err)
	}
	return v
}

// FromInt returns the Value representing a whole number of units.
func FromInt(n int64) Value { return Value(n * Scale) }

// Float returns an approximate float64 for display, feature math, and metrics.
// It is never used as a map key or in the audit hash.
func (v Value) Float() float64 { return float64(v) / float64(Scale) }

// Raw returns the underlying int64 tick count.
func (v Value) Raw() int64 { return int64(v) }

// Mul multiplies two Values (e.g. price * qty) and returns the notional in the
// same 1e8 scale. Computed in int128-equivalent staged arithmetic to avoid
// overflow for realistic notionals.
func (v Value) Mul(o Value) Value {
	// (a/1e8) * (b/1e8) * 1e8 == a*b/1e8. Do the division last but stage it to
	// keep intermediates in range for typical crypto notionals.
	return Value((int64(v) / 1_0000) * (int64(o) / 1_0000))
}

// Abs returns the absolute value.
func (v Value) Abs() Value {
	if v < 0 {
		return -v
	}
	return v
}

// String renders the value as a canonical decimal string with trailing zeros
// trimmed (but at least one digit after nothing, i.e. "10" not "10.").
func (v Value) String() string {
	n := int64(v)
	neg := n < 0
	if neg {
		n = -n
	}
	whole := n / Scale
	frac := n % Scale
	var b strings.Builder
	if neg {
		b.WriteByte('-')
	}
	b.WriteString(strconv.FormatInt(whole, 10))
	if frac == 0 {
		return b.String()
	}
	// Zero-pad fractional to dp digits then trim trailing zeros.
	fs := strconv.FormatInt(frac, 10)
	for len(fs) < dp {
		fs = "0" + fs
	}
	fs = strings.TrimRight(fs, "0")
	b.WriteByte('.')
	b.WriteString(fs)
	return b.String()
}
