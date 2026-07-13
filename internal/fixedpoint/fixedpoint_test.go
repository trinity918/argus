package fixedpoint

import "testing"

func TestParseRoundTrip(t *testing.T) {
	cases := []struct {
		in  string
		raw int64
		str string
	}{
		{"0", 0, "0"},
		{"1", 100_000_000, "1"},
		{"123.456", 12_345_600_000, "123.456"},
		{"0.00000001", 1, "0.00000001"},
		{"-42.5", -4_250_000_000, "-42.5"},
		{"100000.12345678", 10_000_012_345_678, "100000.12345678"},
		{".5", 50_000_000, "0.5"},
		{"7.", 700_000_000, "7"},
		// More than 8 dp: truncated toward zero, not rounded.
		{"1.123456789", 112_345_678, "1.12345678"},
	}
	for _, c := range cases {
		v, err := Parse(c.in)
		if err != nil {
			t.Fatalf("Parse(%q) error: %v", c.in, err)
		}
		if v.Raw() != c.raw {
			t.Errorf("Parse(%q).Raw() = %d, want %d", c.in, v.Raw(), c.raw)
		}
		if got := v.String(); got != c.str {
			t.Errorf("Parse(%q).String() = %q, want %q", c.in, got, c.str)
		}
	}
}

func TestParseErrors(t *testing.T) {
	for _, in := range []string{"", "abc", "1.2.3", "1-2", "--3", "."} {
		if _, err := Parse(in); err == nil {
			t.Errorf("Parse(%q) expected error, got nil", in)
		}
	}
}

func TestMul(t *testing.T) {
	price := MustParse("30000.5")
	qty := MustParse("2")
	got := price.Mul(qty).Float()
	want := 60001.0
	if diff := got - want; diff < -1 || diff > 1 {
		t.Errorf("Mul notional = %f, want ~%f", got, want)
	}
}

func TestExactnessNoFloatDrift(t *testing.T) {
	// 0.1 + 0.2 must equal 0.3 exactly in fixed point (it does not in float64).
	a := MustParse("0.1")
	b := MustParse("0.2")
	sum := Value(a.Raw() + b.Raw())
	if sum != MustParse("0.3") {
		t.Fatalf("0.1+0.2 = %s, want 0.3", sum)
	}
}
